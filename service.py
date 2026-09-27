"""第二现场承载协同服务入口。

路由：
- GET  /health                 健康检查
- POST /admin/venues           主管建档（开放时段 / 消防上限 / 无障碍 / 交通）
- GET  /venues                 点位档案（公网，不含精确压力）
- GET  /venues/{id}            本点位实时状态（商户限本点位）
- POST /venues/{id}/events     现场进出 / 暂停上报，event_id 幂等，支持断网批量补传
- POST /alerts                 主管发布停电 / 强对流 / 转播中断预警
- POST /alerts/{id}/resolve    解除预警
- POST /recommendations        公众导流：按到达时刻 / 新鲜度 / 剩余空间推荐
- GET  /advice/{advice_id}     回看当时为何给出那条建议（熔断后仍可查）
- GET /pressure                主管席匿名跨区压力
- GET /replay                  主管席赛后复盘：溢出流向与处置速度（k 匿名抑制）

角色通过请求头声明（演示用）：X-Role: merchant|supervisor，
商户再带 X-Venue-Id 标明所属点位。
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import urlparse

from domain import DomainError, Store

SERVICE_ID = "second-screen-coordination"
SERVICE_NAME = "第二现场承载协同"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def make_handler(store: Store):
    """按给定存储构造 Handler，便于测试与多实例部署。"""

    class Handler(BaseHTTPRequestHandler):
        server_version = "SecondScreen/1.0"

        # ------------------------------------------------------------ 工具

        def _send_json(self, payload, status: int = 200):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise DomainError("请求体不是合法 JSON") from exc
            if not isinstance(payload, (dict, list)):
                raise DomainError("请求体需为 JSON 对象或数组")
            return payload

        def _role(self) -> str:
            return (self.headers.get("X-Role") or "public").lower()

        def _venue_scope(self) -> str:
            return self.headers.get("X-Venue-Id") or ""

        def _require_supervisor(self):
            if self._role() != "supervisor":
                raise DomainError("仅主管席可操作", status=403)

        def _require_venue_access(self, venue_id: str):
            role = self._role()
            if role == "supervisor":
                return
            if role != "merchant":
                raise DomainError("现场上报需商户身份", status=403)
            if self._venue_scope() != venue_id:
                raise DomainError("商户只能操作本点位", status=403)

        # ------------------------------------------------------------- 路由

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _dispatch(self, method: str):
            path = urlparse(self.path).path.rstrip("/") or "/"
            try:
                if method == "GET" and path == "/health":
                    self._send_json(health_payload())
                elif method == "POST" and path == "/admin/venues":
                    self._require_supervisor()
                    venue = store.create_venue(self._read_json())
                    self._send_json(venue.public_profile(), status=201)
                elif method == "GET" and path == "/venues":
                    self._list_venues()
                elif method == "GET" and path.startswith("/venues/"):
                    self._get_venue(path.split("/")[2])
                elif method == "POST" and path.startswith("/venues/") and path.endswith("/events"):
                    parts = path.split("/")
                    # /venues/{id}/events
                    self._require_venue_access(parts[2])
                    body = self._read_json()
                    if isinstance(body, list) or "events" in body:
                        results = store.record_events(parts[2], body)
                        self._send_json({"results": results})
                    else:
                        self._send_json(store.record_event(parts[2], body))
                elif method == "POST" and path == "/alerts":
                    self._require_supervisor()
                    alert = store.raise_alert(self._read_json())
                    self._send_json(_alert_payload(alert), status=201)
                elif method == "POST" and path.startswith("/alerts/") and path.endswith("/resolve"):
                    self._require_supervisor()
                    alert = store.resolve_alert(path.split("/")[2])
                    self._send_json(_alert_payload(alert))
                elif method == "POST" and path == "/recommendations":
                    self._send_json(store.recommend(self._read_json()))
                elif method == "GET" and path.startswith("/advice/"):
                    self._send_json(store.get_advice(path.split("/")[2]))
                elif method == "GET" and path == "/pressure":
                    self._require_supervisor()
                    self._send_json(store.pressure())
                elif method == "GET" and path == "/replay":
                    self._require_supervisor()
                    self._send_json(store.replay())
                else:
                    self._send_json({"error": "not found"}, status=404)
            except DomainError as exc:
                self._send_json({"error": exc.reason}, status=exc.status)

        # ----------------------------------------------------------- 处理函数

        def _list_venues(self):
            role, scope = self._role(), self._venue_scope()
            venues = list(store.venues.values())
            if role == "merchant":
                venues = [v for v in venues if v.id == scope]
            self._send_json({"venues": [v.public_profile() for v in venues]})

        def _get_venue(self, venue_id: str):
            if venue_id not in store.venues:
                raise DomainError("点位不存在", status=404)
            role = self._role()
            if role == "merchant" and self._venue_scope() != venue_id:
                raise DomainError("商户只能查看本点位", status=403)
            venue = store.venues[venue_id]
            payload = venue.public_profile()
            payload["state"] = store.venue_state(venue)
            if role == "public":
                # 公网只暴露导流所需粗粒度信息，不暴露精确在途/占用原始计数
                state = payload["state"]
                payload["state"] = {
                    "status": state["status"],
                    "remaining_band": _remaining_band(state),
                    "stale": state["stale"],
                }
            self._send_json(payload)

        def log_message(self, *_args):
            return

    return Handler


def _remaining_band(state: dict) -> str:
    remaining = state["remaining"]
    if remaining <= 0:
        return "none"
    ratio = remaining / state["fire_capacity"]
    if ratio < 0.1:
        return "low"
    if ratio < 0.3:
        return "medium"
    return "high"


def _alert_payload(alert) -> dict:
    from domain import iso

    return {
        "alert_id": alert.id,
        "type": alert.type,
        "scope": alert.scope,
        "target": alert.target,
        "message": alert.message,
        "started_at": iso(alert.started_at),
        "ended_at": iso(alert.ended_at),
        "active": alert.ended_at is None,
    }


def build_server(host: str, port: int, store: Optional[Store] = None) -> ThreadingHTTPServer:
    store = store or Store()
    return ThreadingHTTPServer((host, port), make_handler(store))


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        print("基础检查通过")
        return
    build_server(args.host, args.port).serve_forever()


if __name__ == "__main__":
    main()
