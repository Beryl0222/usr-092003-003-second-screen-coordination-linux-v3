"""HTTP 端到端契约：路由、角色隔离、批量补传幂等、停推与复盘。"""

import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from domain import CoordinationService
from service import Handler, SERVICE_ID, SERVICE_NAME, build_handler, health_payload

T0 = 1_000_000.0
ALWAYS_OPEN = [["00:00", "23:59"]]


class HttpContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clock = {"now": T0}
        cls.service = CoordinationService(clock=lambda: cls.clock["now"])
        handler = build_handler(cls.service)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"
        # 统一建档：跨测试共享同一服务实例，避免字母序依赖
        for venue_id, capacity, zone in (("mall", 10, "M"), ("plaza", 100, "P")):
            cls.service.register_venue({
                "venue_id": venue_id, "name": venue_id, "zone": zone,
                "capacity": capacity, "open_windows": ALWAYS_OPEN,
                "accessibility": {"step_free": True}, "transport": ["地铁"],
                "lat": 30.0, "lon": 120.0})

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def request(self, method, path, payload=None, headers=None, expect_error=False):
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(self.base_url + path, data=data, method=method,
                          headers={"Content-Type": "application/json; charset=utf-8", **(headers or {})})
        try:
            with urlopen(request, timeout=3) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            if not expect_error:
                raise
            return error.code, json.load(error)

    def test_health_contract_unchanged(self):
        status, body = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME})
        self.assertEqual(health_payload()["service"], SERVICE_ID)

    def test_unknown_route(self):
        status, _ = self.request("GET", "/unknown", expect_error=True)
        self.assertEqual(status, 404)

    def test_only_admin_registers_venue(self):
        status, _ = self.request("POST", "/admin/venues",
                                 {"venue_id": "x", "name": "x", "zone": "X",
                                  "capacity": 1, "open_windows": ALWAYS_OPEN},
                                 {"X-Role": "merchant"}, expect_error=True)
        self.assertEqual(status, 403)
        status, catalog = self.request("GET", "/venues")
        self.assertEqual(status, 200)
        self.assertTrue(any(v["venue_id"] == "mall" for v in catalog["venues"]))
        # 公开目录不含实时人数
        self.assertNotIn("current", catalog["venues"][0])

    def test_sync_deduplicates_across_repeated_uploads(self):
        events = [
            {"event_id": "p1", "venue_id": "plaza", "type": "entry", "client_ts": T0 - 30, "count": 4},
            {"event_id": "p2", "venue_id": "plaza", "type": "entry", "client_ts": T0 - 10, "count": 3},
        ]
        headers = {"X-Role": "operator", "X-Venue-Id": "plaza"}
        status, first = self.request("POST", "/sync", {"events": events}, headers)
        self.assertEqual(first["ingested"], 2)
        self.assertEqual(first["duplicates"], 0)
        # 同一批再次补传（重连重发）
        status, second = self.request("POST", "/sync", {"events": events}, headers)
        self.assertEqual(second["ingested"], 0)
        self.assertEqual(second["duplicates"], 2)
        status, state = self.request("GET", "/venues/plaza/status", headers=headers)
        self.assertEqual(state["current"], 7)

    def test_operator_cannot_cross_venue_in_sync(self):
        events = [{"event_id": "x1", "venue_id": "mall", "type": "entry",
                   "client_ts": T0, "count": 1}]
        status, body = self.request("POST", "/sync", {"events": events},
                                    {"X-Role": "operator", "X-Venue-Id": "plaza"},
                                    expect_error=True)
        self.assertEqual(status, 403)

    def test_identity_field_rejected_over_http(self):
        status, body = self.request("POST", "/events",
                                    {"event_id": "bad", "venue_id": "plaza", "type": "entry",
                                     "client_ts": T0, "phone": "13800000000"},
                                    {"X-Role": "operator", "X-Venue-Id": "plaza"},
                                    expect_error=True)
        self.assertEqual(status, 400)

    def test_halt_stops_recommendation_and_replay_requires_staff(self):
        # 商户看不了主管视图
        status, _ = self.request("GET", "/supervisor/zones",
                                 headers={"X-Role": "merchant"}, expect_error=True)
        self.assertEqual(status, 403)

        status, zones = self.request("GET", "/supervisor/zones",
                                     headers={"X-Role": "supervisor"})
        self.assertEqual(status, 200)
        self.assertTrue(any(z["zone"] == "P" for z in zones["zones"]))

        status, halt = self.request("POST", "/halts",
                                    {"scope": "event", "reason": "severe_weather",
                                     "detail": "强对流预警"},
                                    {"X-Role": "supervisor"})
        self.assertEqual(status, 201)
        status, rec = self.request("POST", "/recommend", {})
        self.assertTrue(rec["halted"])
        self.assertEqual(rec["options"], [])
        # 留痕可回看
        status, history = self.request("GET", f"/recommendations/{rec['recommendation_id']}")
        self.assertTrue(history["halted"])
        self.assertEqual(history["global_halt_reasons"], ["severe_weather"])
        # 解除
        self.request("POST", f"/halts/{halt['halt']['halt_id']}/resolve", None,
                     {"X-Role": "supervisor"})
        status, rec2 = self.request("POST", "/recommend", {})
        self.assertFalse(rec2["halted"])

    def test_replay_endpoint_returns_aggregates(self):
        status, report = self.request("GET", f"/replay?from={T0-300}&to={T0+300}&k=5",
                                      headers={"X-Role": "supervisor"})
        self.assertEqual(status, 200)
        self.assertEqual(report["privacy"]["k"], 5)
        self.assertIn("overflow_flows", report)
        self.assertIn("handling_speed", report)

    def test_bad_json_returns_400(self):
        request = Request(self.base_url + "/events", data=b"{not json", method="POST",
                          headers={"Content-Type": "application/json",
                                   "X-Role": "operator", "X-Venue-Id": "plaza"})
        with self.assertRaises(HTTPError) as error:
            urlopen(request, timeout=3)
        self.assertEqual(error.exception.code, 400)
        error.exception.close()


if __name__ == "__main__":
    unittest.main()
