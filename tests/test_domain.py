"""领域核心测试：幂等计数、停推、推荐、复盘与隐私边界。"""

import unittest
from datetime import datetime

from domain import (
    CoordinationService,
    FORBIDDEN_FIELDS,
    ROLE_ADMIN,
    ROLE_MERCHANT,
    ROLE_OPERATOR,
    ROLE_SUPERVISOR,
    AuthError,
    ValidationError,
    is_within_windows,
)

T0 = 1_000_000.0
ALWAYS_OPEN = [["00:00", "23:59"]]


def make_service():
    clock = {"now": T0}
    return CoordinationService(clock=lambda: clock["now"]), clock


def register(svc, venue_id, capacity, zone="Z", **kw):
    payload = {"venue_id": venue_id, "name": venue_id, "zone": zone,
               "capacity": capacity, "open_windows": ALWAYS_OPEN,
               "accessibility": {}, "transport": [], "lat": 30.0, "lon": 120.0}
    payload.update(kw)
    return svc.register_venue(payload)


def send(svc, eid, venue_id, etype, ts, role=None, actor=None, **kw):
    event = {"event_id": eid, "venue_id": venue_id, "type": etype, "client_ts": ts}
    event.update(kw)
    return svc.ingest_event(event, role=role, actor_venue=actor)


class EventIdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = make_service()
        register(self.svc, "mall", 10)

    def test_duplicate_resend_counts_once(self):
        first = send(self.svc, "e1", "mall", "entry", T0 - 60, count=3)
        again = send(self.svc, "e1", "mall", "entry", T0 - 60, count=3)
        third = send(self.svc, "e1", "mall", "entry", T0 - 60, count=3)
        self.assertFalse(first["duplicate"])
        self.assertTrue(again["duplicate"])
        self.assertTrue(third["duplicate"])
        status = self.svc.venue_status("mall")
        self.assertEqual(status["current"], 3)

    def test_headcount_overrides_running_total(self):
        send(self.svc, "e1", "mall", "entry", T0 - 100, count=8)
        send(self.svc, "e2", "mall", "exit", T0 - 50, count=2)
        self.assertEqual(self.svc.venue_status("mall")["current"], 6)
        send(self.svc, "e3", "mall", "headcount", T0 - 10, value=4)
        status = self.svc.venue_status("mall")
        self.assertEqual(status["current"], 4)
        self.assertEqual(status["remaining"], 6)

    def test_identity_fields_rejected(self):
        for field in FORBIDDEN_FIELDS:
            with self.assertRaises(ValidationError):
                send(self.svc, "x", "mall", "entry", T0, **{field: "abc"})

    def test_stale_data_flag(self):
        send(self.svc, "e1", "mall", "entry", T0 - 400, count=1)
        self.assertFalse(self.svc.venue_status("mall")["fresh"])
        self.assertEqual(self.svc.venue_status("mall")["data_age_seconds"], 400.0)

    def test_pause_resume(self):
        send(self.svc, "p", "mall", "pause", T0 - 5)
        self.assertTrue(self.svc.venue_status("mall")["paused"])
        send(self.svc, "r", "mall", "resume", T0 - 1)
        self.assertFalse(self.svc.venue_status("mall")["paused"])


class ScopeEnforcementTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = make_service()
        register(self.svc, "mall", 10, zone="A")
        register(self.svc, "plaza", 20, zone="B")

    def test_operator_confined_to_own_venue(self):
        send(self.svc, "e1", "mall", "entry", T0, role=ROLE_OPERATOR, actor="mall", count=2)
        with self.assertRaises(AuthError):
            send(self.svc, "e2", "plaza", "entry", T0, role=ROLE_MERCHANT, actor="mall")
        with self.assertRaises(AuthError):
            self.svc.venue_status("plaza", role=ROLE_OPERATOR, actor_venue="mall")
        # admin 不受点位限制
        send(self.svc, "e3", "plaza", "entry", T0, role=ROLE_ADMIN, count=5)
        self.assertEqual(self.svc.venue_status("plaza")["current"], 5)

    def test_halt_requires_known_reason(self):
        with self.assertRaises(ValidationError):
            self.svc.issue_halt({"scope": "event", "reason": "noise"}, issued_by=ROLE_SUPERVISOR)


class RecommendationTest(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()
        register(self.svc, "mall", 10, zone="A", lat=30.0, lon=120.0,
                 accessibility={"step_free": False})
        register(self.svc, "plaza", 100, zone="B", lat=30.01, lon=120.0,
                 accessibility={"step_free": True})

    def test_ranks_by_remaining_space_and_eta(self):
        send(self.svc, "m", "mall", "entry", T0 - 30, count=10)
        send(self.svc, "p", "plaza", "headcount", T0 - 20, value=5)
        result = self.svc.recommend({"from": {"lat": 30.0, "lon": 120.0}, "party_size": 4})
        self.assertEqual([o["venue_id"] for o in result["options"]], ["plaza"])
        option = result["options"][0]
        self.assertGreater(option["eta_seconds"], 0)
        self.assertGreaterEqual(option["remaining_at_arrival"], 90)

    def test_excludes_stale_and_paused_with_rationale(self):
        send(self.svc, "p", "plaza", "headcount", T0 - 400, value=1)
        result = self.svc.recommend({"from": {"lat": 30.0, "lon": 120.0}})
        self.assertEqual(result["options"], [])
        record = self.svc.get_recommendation(result["recommendation_id"])
        reasons = {c["venue_id"]: c["excluded_reasons"] for c in record["candidate_rationale"]}
        self.assertIn("stale_data", reasons["plaza"])
        self.assertIn("no_occupancy_data", reasons["mall"])

        send(self.svc, "p2", "plaza", "headcount", T0 - 10, value=1)
        send(self.svc, "pp", "plaza", "pause", T0 - 5)
        result = self.svc.recommend({"from": {"lat": 30.0, "lon": 120.0}})
        record = self.svc.get_recommendation(result["recommendation_id"])
        plaza = next(c for c in record["candidate_rationale"] if c["venue_id"] == "plaza")
        self.assertIn("paused", plaza["excluded_reasons"])

    def test_accessibility_filter(self):
        send(self.svc, "m", "mall", "headcount", T0 - 10, value=0)
        send(self.svc, "p", "plaza", "headcount", T0 - 10, value=0)
        result = self.svc.recommend({"filters": {"accessible_only": True}})
        self.assertEqual([o["venue_id"] for o in result["options"]], ["plaza"])

    def test_closed_at_arrival(self):
        self.svc.register_venue({"venue_id": "late", "name": "late", "zone": "C",
                                 "capacity": 50, "open_windows": [["12:00", "13:00"]],
                                 "accessibility": {}, "lat": 30.0, "lon": 120.0})
        send(self.svc, "l", "late", "headcount", T0, value=0)
        at = datetime(2026, 9, 27, 12, 30).timestamp()
        result = self.svc.recommend({"at": at, "travel_seconds": {"late": 7200}})
        record = self.svc.get_recommendation(result["recommendation_id"])
        late = next(c for c in record["candidate_rationale"] if c["venue_id"] == "late")
        self.assertIn("closed_at_arrival", late["excluded_reasons"])


class HaltTest(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()
        register(self.svc, "mall", 10, zone="A")
        register(self.svc, "plaza", 100, zone="B")
        send(self.svc, "p", "plaza", "headcount", T0 - 10, value=0)

    def test_venue_halt_removes_one_option(self):
        self.svc.issue_halt({"scope": "venue", "venue_id": "plaza",
                             "reason": "power_outage"}, issued_by=ROLE_SUPERVISOR)
        result = self.svc.recommend({})
        self.assertFalse(result["halted"])  # 非全局停推
        self.assertNotIn("plaza", [o["venue_id"] for o in result["options"]])

    def test_global_halt_stops_recommendations_but_keeps_rationale(self):
        result_before = self.svc.recommend({})
        self.assertEqual([o["venue_id"] for o in result_before["options"]], ["plaza"])
        halt = self.svc.issue_halt({"scope": "event", "reason": "broadcast_rights",
                                    "detail": "转播授权中断"}, issued_by=ROLE_SUPERVISOR)
        result = self.svc.recommend({})
        self.assertTrue(result["halted"])
        self.assertEqual(result["options"], [])
        self.assertEqual(result["halt_reasons"], ["broadcast_rights"])
        # 此前的建议与本次为何排除都保留可查
        history = self.svc.get_recommendation(result["recommendation_id"])
        self.assertTrue(history["halted"])
        self.assertEqual(len(history["candidate_rationale"]), 2)
        # 解除后恢复推荐
        self.clock["now"] += 60
        self.svc.resolve_halt(halt.halt_id, resolved_by=ROLE_SUPERVISOR)
        self.assertFalse(self.svc.recommend({})["halted"])


class ZonePressureTest(unittest.TestCase):
    def test_zone_view_is_aggregate_only(self):
        svc, _ = make_service()
        register(svc, "m1", 10, zone="A")
        register(svc, "m2", 30, zone="A")
        send(svc, "a", "m1", "entry", T0 - 5, count=10)
        send(svc, "b", "m2", "entry", T0 - 5, count=6)
        view = svc.zone_pressure()
        zone_a = next(z for z in view["zones"] if z["zone"] == "A")
        self.assertEqual(zone_a["occupancy"], 16)
        self.assertEqual(zone_a["capacity"], 40)
        self.assertEqual(zone_a["pressure_ratio"], 0.4)
        self.assertNotIn("venue_id", zone_a)
        self.assertNotIn("current", zone_a)


class ReplayTest(unittest.TestCase):
    def test_overflow_flows_k_anonymity_and_handling_speed(self):
        svc, clock = make_service()
        register(svc, "mall", 5, zone="A")
        register(svc, "plaza", 100, zone="B")
        register(svc, "alley", 100, zone="C")
        # mall 在窗口起点即满员并暂停；plaza 承接 6 人，alley 只来 2 人
        send(svc, "m0", "mall", "entry", T0 - 200, count=5)
        send(svc, "mp", "mall", "pause", T0 - 180)
        send(svc, "mr", "mall", "resume", T0 - 60)
        send(svc, "p1", "plaza", "entry", T0 - 120, count=6)
        send(svc, "a1", "alley", "entry", T0 - 120, count=2)

        halt = svc.issue_halt({"scope": "venue", "venue_id": "mall",
                               "reason": "severe_weather"}, issued_by=ROLE_SUPERVISOR)
        clock["now"] = T0 + 300
        svc.resolve_halt(halt.halt_id, resolved_by=ROLE_SUPERVISOR)
        # 解除后再次进入满员：区间合并后不影响 plaza 的溢出归因
        clock["now"] = T0 + 400
        send(svc, "m2", "mall", "entry", T0 + 400, count=3)

        report = svc.replay(frm=T0 - 300, to=T0 + 600, k=5)
        flows = {(f["source_zone"], f["target_zone"]): f["count"]
                 for f in report["overflow_flows"]}
        self.assertGreaterEqual(flows.get(("A", "B"), 0), 6)
        self.assertNotIn(("A", "C"), flows)  # 小样本整条抑制
        self.assertGreaterEqual(report["suppressed_flow_count"], 1)

        pause_stats = report["handling_speed"]["pause"]
        self.assertEqual(pause_stats["count"], 1)
        self.assertEqual(pause_stats["min_seconds"], 120.0)
        halt_stats = report["handling_speed"]["halt"]
        self.assertEqual(halt_stats["count"], 1)
        self.assertEqual(halt_stats["max_seconds"], 300.0)
        # occupancy 序列只含聚合数值，没有任何身份字段
        series = report["occupancy_series"]
        self.assertIn("mall", series)
        self.assertTrue(all(set(point) == {"ts", "occupancy", "ratio"} for point in series["mall"]))


class WindowTest(unittest.TestCase):
    def test_cross_midnight(self):
        ts = datetime(2026, 9, 27, 1, 30).timestamp()
        self.assertTrue(is_within_windows(ts, [["20:00", "02:00"]]))
        self.assertFalse(is_within_windows(ts, [["09:00", "18:00"]]))
        evening = datetime(2026, 9, 27, 21, 0).timestamp()
        self.assertTrue(is_within_windows(evening, [["20:00", "02:00"]]))


if __name__ == "__main__":
    unittest.main()
