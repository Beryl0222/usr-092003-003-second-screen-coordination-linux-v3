"""验证服务身份、HTTP 路由契约与角色边界。"""

import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from service import SERVICE_ID, SERVICE_NAME, build_server, health_payload
from domain import Store

T0 = 1_800_000_000
NOW = T0 + 200
WINDOW = [{"open_at": T0, "close_at": T0 + 4 * 3600}]


class HttpCase(unittest.TestCase):
    def setUp(self):
        # 固定时钟，模拟"开赛前"现场：断网补传的事件时间早于当前时刻
        self.store = Store(now_fn=lambda: NOW)
        self.server = build_server("127.0.0.1", 0, store=self.store)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def call(self, method, path, payload=None, headers=None):
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}", data=data, method=method, headers=headers or {}
        )
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            body = json.load(error)
            error.close()
            return error.code, body

    def seed_venue(self, vid="v1", district="静安", cap=100, headers=None):
        return self.call(
            "POST",
            "/admin/venues",
            {
                "id": vid,
                "name": f"点位{vid}",
                "district": district,
                "fire_capacity": cap,
                "opening_hours": WINDOW,
                "accessible": True,
                "transit": ["地铁2号线"],
            },
            headers=headers,
        )


class ServiceContractTest(HttpCase):
    def test_health_payload(self):
        self.assertEqual(health_payload(), {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME})

    def test_health_route(self):
        status, body = self.call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, health_payload())

    def test_unknown_route(self):
        status, body = self.call("GET", "/unknown")
        self.assertEqual(status, 404)
        self.assertIn("error", body)


class VenueAndRoleTest(HttpCase):
    def test_venue_creation_requires_supervisor(self):
        status, _ = self.seed_venue()
        self.assertEqual(status, 403)
        status, body = self.seed_venue(headers={"X-Role": "supervisor"})
        self.assertEqual(status, 201)
        self.assertEqual(body["fire_capacity"], 100)
        self.assertEqual(body["opening_hours"][0]["close_at"] is not None, True)

    def test_public_venue_list_hides_raw_counts(self):
        self.seed_venue(headers={"X-Role": "supervisor"})
        status, body = self.call("GET", "/venues")
        self.assertEqual(status, 200)
        self.assertEqual(body["venues"][0]["name"], "点位v1")

        status, detail = self.call("GET", "/venues/v1")
        self.assertEqual(status, 200)
        # 公网只见状态档位，不见精确人数
        self.assertEqual(set(detail["state"].keys()), {"status", "remaining_band", "stale"})

    def test_merchant_confined_to_own_venue(self):
        self.seed_venue("v1", headers={"X-Role": "supervisor"})
        self.seed_venue("v2", headers={"X-Role": "supervisor"})
        merchant = {"X-Role": "merchant", "X-Venue-Id": "v1"}

        status, body = self.call("GET", "/venues", headers=merchant)
        self.assertEqual([v["venue_id"] for v in body["venues"]], ["v1"])

        status, _ = self.call("GET", "/venues/v2", headers=merchant)
        self.assertEqual(status, 403)

        status, _ = self.call(
            "POST",
            "/venues/v2/events",
            {"event_id": "x", "op": "enter", "count": 1, "occurred_at": T0 + 10},
            headers=merchant,
        )
        self.assertEqual(status, 403)

    def test_supervisor_only_views(self):
        self.assertEqual(self.call("GET", "/pressure")[0], 403)
        self.assertEqual(self.call("GET", "/replay")[0], 403)
        self.assertEqual(
            self.call("POST", "/alerts", {"type": "weather", "scope": "global"})[0],
            403,
        )


class EventFlowTest(HttpCase):
    def test_idempotent_reupload_over_http(self):
        self.seed_venue(headers={"X-Role": "supervisor"})
        merchant = {"X-Role": "merchant", "X-Venue-Id": "v1"}
        event = {"event_id": "e1", "op": "enter", "count": 12, "occurred_at": T0 + 100}
        status, first = self.call("POST", "/venues/v1/events", event, headers=merchant)
        self.assertEqual(status, 200)
        self.assertEqual(first["occupancy_after"], 12)

        # 断网恢复后的同号补传
        status, duplicate = self.call("POST", "/venues/v1/events", event, headers=merchant)
        self.assertEqual(status, 200)
        self.assertTrue(duplicate["duplicate"])

        status, detail = self.call("GET", "/venues/v1", headers={"X-Role": "supervisor"})
        self.assertEqual(detail["state"]["occupancy"], 12)

    def test_batch_replay_and_supervisor_state(self):
        self.seed_venue(cap=5, headers={"X-Role": "supervisor"})
        merchant = {"X-Role": "merchant", "X-Venue-Id": "v1"}
        status, body = self.call(
            "POST",
            "/venues/v1/events",
            {
                "events": [
                    {"event_id": "b1", "op": "enter", "count": 5, "occurred_at": T0 + 10},
                    {"event_id": "b1", "op": "enter", "count": 5, "occurred_at": T0 + 10},
                    {"event_id": "b2", "op": "enter", "count": 1, "occurred_at": T0 + 11},
                ]
            },
            headers=merchant,
        )
        self.assertEqual(status, 200)
        self.assertEqual([r["status"] for r in body["results"]], ["accepted", "accepted", "rejected"])

        status, detail = self.call("GET", "/venues/v1", headers={"X-Role": "supervisor"})
        self.assertEqual(detail["state"]["occupancy"], 5)
        self.assertEqual(detail["state"]["status"], "full")

    def test_alert_halts_recommendation_but_history_remains(self):
        self.seed_venue("v1", headers={"X-Role": "supervisor"})
        merchant = {"X-Role": "merchant", "X-Venue-Id": "v1"}
        self.call(
            "POST",
            "/venues/v1/events",
            {"event_id": "e1", "op": "enter", "count": 10, "occurred_at": T0 + 100},
            headers=merchant,
        )
        rec_payload = {
            "origin": {"district": "静安"},
            "travel": {"v1": 120},
            "depart_at": T0 + 100,
        }
        status, before = self.call("POST", "/recommendations", rec_payload)
        self.assertEqual(status, 200)
        self.assertEqual(len(before["recommendations"]), 1)
        advice_id = before["advice_id"]

        status, alert = self.call(
            "POST",
            "/alerts",
            {"type": "weather", "scope": "global", "message": "强对流预警"},
            headers={"X-Role": "supervisor"},
        )
        self.assertEqual(status, 201)

        status, after = self.call("POST", "/recommendations", rec_payload)
        self.assertTrue(after["halted"])
        self.assertEqual(after["recommendations"], [])

        # 建议留痕仍可回看
        status, kept = self.call("GET", f"/advice/{advice_id}")
        self.assertEqual(status, 200)
        self.assertEqual(kept["recommendations"][0]["venue_id"], "v1")
        self.assertTrue(any("剩余空间" in r for r in kept["recommendations"][0]["reasons"]))

    def test_pressure_and_replay_for_supervisor(self):
        self.seed_venue("v1", district="静安", headers={"X-Role": "supervisor"})
        status, pressure = self.call("GET", "/pressure", headers={"X-Role": "supervisor"})
        self.assertEqual(status, 200)
        self.assertEqual(pressure["districts"][0]["district"], "静安")

        status, replay = self.call("GET", "/replay", headers={"X-Role": "supervisor"})
        self.assertEqual(status, 200)
        self.assertIn("flows", replay)
        self.assertGreaterEqual(replay["k_anonymity"], 3)


if __name__ == "__main__":
    unittest.main()
