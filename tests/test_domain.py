"""领域内核测试：幂等补传、熔断、导流、匿名复盘。"""

import unittest

from domain import (
    ALERT_BROADCAST,
    ALERT_POWER,
    ALERT_WEATHER,
    ENTER,
    EXIT,
    K_ANON,
    RESUME,
    STALE_LIMIT_SECONDS,
    SUSPEND,
    DomainError,
    Store,
)

T0 = 1_800_000_000  # 固定基准时刻，避免随墙钟漂移
WINDOW = [{"open_at": T0, "close_at": T0 + 4 * 3600}]


class Clock:
    def __init__(self, t=T0 + 10_000):
        self.t = t

    def now(self):
        return self.t


def make_store(clock=None):
    clock = clock or Clock()
    return clock, Store(now_fn=clock.now)


def venue_payload(vid="v1", district="静安", cap=100, **extra):
    payload = {
        "id": vid,
        "name": f"点位{vid}",
        "district": district,
        "fire_capacity": cap,
        "opening_hours": WINDOW,
        "lat": 31.23,
        "lng": 121.47,
    }
    payload.update(extra)
    return payload


class EventIdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.clock, self.store = make_store()
        self.store.create_venue(venue_payload())

    def _enter(self, event_id, count=10, at=T0 + 60):
        return self.store.record_event(
            "v1",
            {"event_id": event_id, "op": ENTER, "count": count, "occurred_at": at},
        )

    def test_duplicate_reupload_counts_once(self):
        first = self._enter("e-1")
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(first["occupancy_after"], 10)

        # 断网恢复后同 event_id 重复补传：不二次计数
        duplicate = self._enter("e-1")
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["status"], "accepted")
        state = self.store.venue_state(self.store.venues["v1"])
        self.assertEqual(state["occupancy"], 10)

    def test_late_arriving_events_are_ordered_by_occurred_at(self):
        self._enter("e-late", count=10, at=T0 + 600)
        self._enter("e-early", count=5, at=T0 + 60)
        # 后到的早刻事件不能把占用叠加到 15 之上的错误时点
        self.assertEqual(self.store.occupancy_at(self.store.venues["v1"], T0 + 300), 5)
        self.assertEqual(self.store.occupancy_at(self.store.venues["v1"], T0 + 900), 15)

    def test_batch_replay_is_partial_success(self):
        results = self.store.record_events(
            "v1",
            {
                "events": [
                    {"event_id": "b1", "op": ENTER, "count": 3, "occurred_at": T0 + 10},
                    {"event_id": "b1", "op": ENTER, "count": 3, "occurred_at": T0 + 10},
                    {"event_id": "b2", "op": EXIT, "count": 9, "occurred_at": T0 + 20},
                ]
            },
        )
        self.assertEqual([r["status"] for r in results], ["accepted", "accepted", "rejected"])
        self.assertTrue(results[1]["duplicate"])
        self.assertEqual(self.store.occupancy_at(self.store.venues["v1"], T0 + 30), 3)

    def test_exit_cannot_go_negative(self):
        result = self.store.record_event(
            "v1", {"event_id": "x1", "op": EXIT, "count": 1, "occurred_at": T0 + 5}
        )
        self.assertEqual(result["status"], "rejected")
        self.assertIn("超过在场", result["reason"])


class CapacityAndGateTest(unittest.TestCase):
    def setUp(self):
        self.clock, self.store = make_store()
        self.store.create_venue(venue_payload(cap=10))

    def test_fire_capacity_blocks_overflow(self):
        ok = self.store.record_event(
            "v1", {"event_id": "c1", "op": ENTER, "count": 10, "occurred_at": T0 + 5}
        )
        self.assertEqual(ok["status"], "accepted")
        blocked = self.store.record_event(
            "v1", {"event_id": "c2", "op": ENTER, "count": 1, "occurred_at": T0 + 6}
        )
        self.assertEqual(blocked["status"], "rejected")
        self.assertIn("消防上限", blocked["reason"])
        # 被拒需求不进占用账
        self.assertEqual(self.store.occupancy_at(self.store.venues["v1"], T0 + 10), 10)

    def test_suspend_blocks_entry_and_resume_restores(self):
        self.store.record_event(
            "v1", {"event_id": "g1", "op": SUSPEND, "occurred_at": T0 + 100}
        )
        blocked = self.store.record_event(
            "v1", {"event_id": "g2", "op": ENTER, "count": 1, "occurred_at": T0 + 120}
        )
        self.assertEqual(blocked["status"], "rejected")
        self.assertIn("暂停", blocked["reason"])

        self.store.record_event(
            "v1", {"event_id": "g3", "op": RESUME, "occurred_at": T0 + 200}
        )
        allowed = self.store.record_event(
            "v1", {"event_id": "g4", "op": ENTER, "count": 1, "occurred_at": T0 + 210}
        )
        self.assertEqual(allowed["status"], "accepted")

    def test_double_suspend_is_rejected(self):
        self.store.record_event(
            "v1", {"event_id": "g1", "op": SUSPEND, "occurred_at": T0 + 100}
        )
        again = self.store.record_event(
            "v1", {"event_id": "g2", "op": SUSPEND, "occurred_at": T0 + 110}
        )
        self.assertEqual(again["status"], "rejected")

    def test_clock_skew_rejected(self):
        with self.assertRaises(DomainError):
            self.store.record_event(
                "v1",
                {"event_id": "future", "op": ENTER, "count": 1, "occurred_at": self.clock.t + 9999},
            )


class RecommendationTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock(T0 + 1800)
        self.store = Store(now_fn=self.clock.now)
        # 近满点（A 区）与有余量点（B 区）
        self.store.create_venue(venue_payload("va", district="静安", cap=100))
        self.store.create_venue(
            venue_payload("vb", district="普陀", cap=100, accessible=True)
        )
        self.store.create_venue(
            venue_payload("vc", district="虹口", cap=100, accessible=False)
        )

    def _feed(self):
        self.store.record_event(
            "va", {"event_id": "a1", "op": ENTER, "count": 95, "occurred_at": self.clock.t - 30}
        )
        self.store.record_event(
            "vb", {"event_id": "b1", "op": ENTER, "count": 20, "occurred_at": self.clock.t - 30}
        )
        self.store.record_event(
            "vc", {"event_id": "c1", "op": ENTER, "count": 30, "occurred_at": self.clock.t - 30}
        )

    def test_ranking_uses_remaining_eta_freshness(self):
        self._feed()
        result = self.store.recommend(
            {"origin": {"district": "静安"}, "travel": {"va": 120, "vb": 300, "vc": 600}}
        )
        ranked = result["recommendations"]
        self.assertEqual([r["venue_id"] for r in ranked], ["vb", "vc", "va"])
        top = ranked[0]
        self.assertTrue(any("剩余空间" in r for r in top["reasons"]))
        self.assertTrue(any("开放时段" in r for r in top["reasons"]))

    def test_arrival_after_close_is_excluded(self):
        self._feed()
        # vb 路程 4 小时，到达时已闭场
        result = self.store.recommend(
            {"origin": {"district": "未知"}, "travel": {"va": 60, "vb": 4 * 3600, "vc": 60}}
        )
        excluded = {e["venue_id"]: e["reason"] for e in result["excluded"]}
        self.assertIn("开放时段", excluded["vb"])

    def test_stale_counts_excluded(self):
        self._feed()
        # va 的计数推到很久以前：过期不导流
        self.store.venues["va"].events[0].occurred_at = self.clock.t - STALE_LIMIT_SECONDS - 1
        result = self.store.recommend(
            {"origin": {"district": "静安"}, "travel": {"va": 60, "vb": 60, "vc": 60}}
        )
        excluded = {e["venue_id"] for e in result["excluded"]}
        self.assertIn("va", excluded)
        self.assertTrue(self.store.venue_state(self.store.venues["va"])["stale"])

    def test_no_counts_means_unknown_capacity(self):
        result = self.store.recommend(
            {"origin": {"district": "静安"}, "travel": {"va": 60, "vb": 60, "vc": 60}}
        )
        self.assertEqual(result["recommendations"], [])
        self.assertTrue(any("不可知" in e["reason"] for e in result["excluded"]))

    def test_accessible_requirement_filters(self):
        self._feed()
        result = self.store.recommend(
            {
                "origin": {"district": "静安"},
                "travel": {"va": 60, "vb": 60, "vc": 60},
                "requires_accessible": True,
            }
        )
        ids = {r["venue_id"] for r in result["recommendations"]}
        self.assertEqual(ids, {"vb"})

    def test_global_alert_halts_but_advice_history_keeps_reasons(self):
        self._feed()
        before = self.store.recommend(
            {"origin": {"district": "静安"}, "travel": {"va": 60, "vb": 60, "vc": 60}}
        )
        self.assertFalse(before["halted"])
        advice_id = before["advice_id"]

        self.store.raise_alert(
            {"type": ALERT_WEATHER, "message": "强对流橙色预警", "scope": "global"}
        )
        after = self.store.recommend(
            {"origin": {"district": "静安"}, "travel": {"va": 60, "vb": 60, "vc": 60}}
        )
        self.assertTrue(after["halted"])
        self.assertEqual(after["recommendations"], [])
        self.assertIn("强对流", after["halt_reasons"][0])

        # 熔断后仍能回看此前为什么推荐
        kept = self.store.get_advice(advice_id)
        self.assertFalse(kept["halted"])
        self.assertEqual(len(kept["recommendations"]), 3)
        self.assertTrue(any("剩余空间" in r for r in kept["recommendations"][0]["reasons"]))

    def test_scoped_power_alert_only_excludes_target(self):
        self._feed()
        self.store.raise_alert(
            {"type": ALERT_POWER, "scope": "venue", "target": "vb", "message": "商圈停电"}
        )
        result = self.store.recommend(
            {"origin": {"district": "静安"}, "travel": {"va": 60, "vb": 60, "vc": 60}}
        )
        excluded = {e["venue_id"]: e["reason"] for e in result["excluded"]}
        self.assertIn("预警", excluded["vb"])
        ids = {r["venue_id"] for r in result["recommendations"]}
        self.assertNotIn("vb", ids)
        self.assertIn("vc", ids)

    def test_broadcast_halt_state_visible(self):
        self._feed()
        self.store.raise_alert({"type": ALERT_BROADCAST, "scope": "district", "target": "普陀"})
        state = self.store.venue_state(self.store.venues["vb"])
        self.assertEqual(state["status"], "broadcast_halted")

    def test_advice_does_not_store_precise_origin(self):
        self._feed()
        result = self.store.recommend(
            {
                "origin": {"district": "静安", "lat": 31.2304, "lng": 121.4737, "phone": "13x"},
                "travel": {"va": 60, "vb": 60, "vc": 60},
            }
        )
        kept = self.store.get_advice(result["advice_id"])
        self.assertEqual(kept["origin_district"], "静安")
        flat = repr(kept)
        self.assertNotIn("31.2304", flat)
        self.assertNotIn("13x", flat)


class PressureTest(unittest.TestCase):
    def test_district_aggregation_is_anonymous(self):
        clock, store = make_store()
        store.create_venue(venue_payload("va", district="静安", cap=100))
        store.create_venue(venue_payload("vb", district="静安", cap=100))
        store.create_venue(venue_payload("vc", district="普陀", cap=50))
        store.record_event("va", {"event_id": "1", "op": ENTER, "count": 80, "occurred_at": T0 + 1})
        store.record_event("vb", {"event_id": "2", "op": ENTER, "count": 40, "occurred_at": T0 + 1})
        store.record_event("vc", {"event_id": "3", "op": ENTER, "count": 50, "occurred_at": T0 + 1})

        pressure = store.pressure()
        by_district = {d["district"]: d for d in pressure["districts"]}
        self.assertEqual(by_district["静安"]["occupancy"], 120)
        self.assertEqual(by_district["静安"]["ratio"], 0.6)
        self.assertEqual(by_district["普陀"]["ratio"], 1.0)
        # 排序：压力最高在前
        self.assertEqual(pressure["districts"][0]["district"], "普陀")
        flat = repr(pressure)
        self.assertNotIn("event_id", flat)


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock(T0 + 7200)
        self.store = Store(now_fn=self.clock.now)
        self.store.create_venue(venue_payload("va", district="静安", cap=100))
        self.store.create_venue(venue_payload("vb", district="普陀", cap=100))

    def test_overflow_flow_reconstruction_and_k_anonymity(self):
        # 静安 10 人进场被满员拒（先放 95 人再压入 10 人需求，cap=100）
        for i, c in enumerate([40, 30, 25]):
            self.store.record_event(
                "va", {"event_id": f"a{i}", "op": ENTER, "count": c, "occurred_at": T0 + 100 + i}
            )
        rejected = self.store.record_event(
            "va", {"event_id": "spill", "op": ENTER, "count": 10, "occurred_at": T0 + 600}
        )
        self.assertEqual(rejected["status"], "rejected")
        # 随后普陀有进场
        self.store.record_event(
            "vb", {"event_id": "b1", "op": ENTER, "count": 20, "occurred_at": T0 + 900}
        )
        self.store.record_event(
            "vb", {"event_id": "b2", "op": ENTER, "count": 5, "occurred_at": T0 + 1200}
        )

        replay = self.store.replay()
        self.assertEqual(replay["unmet_demand_total"], 10)
        self.assertEqual(len(replay["flows"]), 1)
        flow = replay["flows"][0]
        self.assertEqual((flow["from_district"], flow["to_district"]), ("静安", "普陀"))
        # 10 * 25/25 = 10，达到 k 匿名门槛
        self.assertEqual(flow["estimated_count"], 10)
        self.assertFalse(flow["suppressed"])
        self.assertEqual(replay["k_anonymity"], K_ANON)

    def test_small_flow_is_suppressed(self):
        self.store.record_event(
            "va", {"event_id": "fill", "op": ENTER, "count": 100, "occurred_at": T0 + 1}
        )
        self.store.record_event(
            "va", {"event_id": "one", "op": ENTER, "count": 1, "occurred_at": T0 + 600}
        )
        self.store.record_event(
            "vb", {"event_id": "b1", "op": ENTER, "count": 5, "occurred_at": T0 + 900}
        )
        replay = self.store.replay()
        self.assertEqual(replay["flows"], [])
        self.assertGreaterEqual(replay["suppressed_flow_count"], 1)

    def test_incident_response_and_clear_speed(self):
        # 暂停时在场 20 人，随后分批离场，降到消防上限 10% 以下视为清空
        self.store.record_event(
            "va", {"event_id": "occ", "op": ENTER, "count": 20, "occurred_at": T0 + 50}
        )
        self.store.record_event(
            "va", {"event_id": "s", "op": SUSPEND, "occurred_at": T0 + 100}
        )
        self.store.record_event(
            "va", {"event_id": "e1", "op": EXIT, "count": 3, "occurred_at": T0 + 200}
        )
        self.store.record_event(
            "va", {"event_id": "e2", "op": EXIT, "count": 4, "occurred_at": T0 + 300}
        )
        self.store.record_event(
            "va", {"event_id": "e3", "op": EXIT, "count": 3, "occurred_at": T0 + 400}
        )
        self.store.record_event(
            "va", {"event_id": "r", "op": RESUME, "occurred_at": T0 + 500}
        )
        replay = self.store.replay()
        suspensions = [i for i in replay["incidents"] if i["kind"] == "suspension"]
        self.assertEqual(len(suspensions), 1)
        incident = suspensions[0]
        self.assertEqual(incident["response_seconds"], 100)
        self.assertEqual(incident["clear_seconds"], 300)
        self.assertFalse(incident["low_sample"])
        self.assertAlmostEqual(incident["exit_throughput_per_minute"], 1.5, places=1)

    def test_low_sample_throughput_suppressed(self):
        self.store.record_event(
            "va", {"event_id": "occ", "op": ENTER, "count": 2, "occurred_at": T0 + 50}
        )
        self.store.record_event(
            "va", {"event_id": "s", "op": SUSPEND, "occurred_at": T0 + 100}
        )
        self.store.record_event(
            "va", {"event_id": "e1", "op": EXIT, "count": 2, "occurred_at": T0 + 200}
        )
        self.store.record_event(
            "va", {"event_id": "r", "op": RESUME, "occurred_at": T0 + 300}
        )
        incident = [i for i in self.store.replay()["incidents"] if i["kind"] == "suspension"][0]
        self.assertTrue(incident["low_sample"])
        self.assertIsNone(incident["exit_throughput_per_minute"])


if __name__ == "__main__":
    unittest.main()
