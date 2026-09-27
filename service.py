"""第二现场承载协同的 HTTP 适配层。

路由一览：
  GET  /health                         巡检
  GET  /venues                         公开点位目录（时段/无障碍/交通，不含实时人数）
  POST /admin/venues                   维护点位台账（admin）
  POST /events                         记录单条进出/盘点/暂停（现场账号限本点位）
  POST /sync                           联网后批量补传，按 event_id 幂等
  GET  /venues/{id}/status             点位实时状态
  POST /recommend                      导流推荐（ETA/新鲜度/余量，停推即收口）
  GET  /recommendations/{id}           回看当时为何给出那条建议
  POST /halts                          停电/强对流/转播中断时停推（supervisor/admin）
  POST /halts/{id}/resolve             解除停推
  GET  /halts                          停推指令列表
  GET  /supervisor/zones               主管席：匿名跨区压力（supervisor/admin）
  GET  /replay                         赛后复原溢出流向与处置速度（supervisor/admin）

角色经 X-Role 头传入；operator/merchant 以 X-Venue-Id 限定本点位。
"""

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from domain import (
    AuthError,
    CoordinationService,
    DomainError,
    NotFoundError,
    ROLE_ADMIN,
    ROLE_MERCHANT,
    ROLE_OPERATOR,
    ROLE_SUPERVISOR,
    ValidationError,
)
from offline_queue import OfflineQueue

SERVICE_ID = "second-screen-coordination"
SERVICE_NAME = "第二现场承载协同"

_ROLES = {ROLE_ADMIN, ROLE_SUPERVISOR, ROLE_OPERATOR, ROLE_MERCHANT}
_STAFF = {ROLE_ADMIN, ROLE_SUPERVISOR}


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def build_handler(service: CoordinationService, queue: OfflineQueue = None):
    """构造绑定指定领域服务实例的 Handler（测试注入用）。"""

    class BoundHandler(Handler):
        pass

    BoundHandler.service = service
    BoundHandler.queue = queue
    return BoundHandler


class Handler(BaseHTTPRequestHandler):
    """承载协同接口。默认持有进程级领域服务，可用 build_handler 注入。"""

    service = CoordinationService()
    queue = None  # 配置 --queue 后，POST /events 失败的事件会落入本地队列

    # ---------- 基础收发 ----------

    def _send_json(self, payload, status: int = 200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValidationError(f"请求体不是合法 JSON：{exc}") from exc
        if not isinstance(payload, dict):
            raise ValidationError("请求体必须是 JSON 对象")
        return payload

    def _identity(self):
        role = self.headers.get("X-Role", ROLE_OPERATOR)
        if role not in _ROLES:
            raise AuthError(f"未知角色：{role}")
        return role, self.headers.get("X-Venue-Id") or None

    def _require_staff(self):
        role, _ = self._identity()
        if role not in _STAFF:
            raise AuthError("该操作需要主管权限")
        return role

    def _handle_domain_error(self, exc: DomainError):
        status = {
            ValidationError: 400,
            AuthError: 403,
            NotFoundError: 404,
        }.get(type(exc), 500)
        self._send_json({"error": type(exc).__name__, "message": str(exc)}, status)

    # ---------- 路由 ----------

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == "/health":
                self._send_json(health_payload())
            elif path == "/venues":
                self._send_json({"venues": self.service.list_catalog()})
            elif path == "/halts":
                self._require_staff()
                self._send_json({"halts": [h.to_dict() for h in self.service.list_halts()]})
            elif path == "/supervisor/zones":
                self._require_staff()
                self._send_json(self.service.zone_pressure())
            elif path == "/replay":
                self._handle_replay()
            else:
                match = re.fullmatch(r"/venues/([^/]+)/status", path)
                if match:
                    role, actor_venue = self._identity()
                    self._send_json(self.service.venue_status(
                        match.group(1), role=role, actor_venue=actor_venue))
                    return
                match = re.fullmatch(r"/recommendations/([^/]+)", path)
                if match:
                    self._send_json(self.service.get_recommendation(match.group(1)))
                    return
                self._send_json({"error": "NotFound", "message": f"无此路由：{path}"}, 404)
        except DomainError as exc:
            self._handle_domain_error(exc)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/admin/venues":
                role, _ = self._identity()
                if role != ROLE_ADMIN:
                    raise AuthError("台账维护仅 admin 可操作")
                venue = self.service.register_venue(self._read_json())
                self._send_json({"ok": True, "venue": venue.public_catalog()}, 201)
            elif path == "/events":
                self._handle_single_event()
            elif path == "/sync":
                self._handle_sync()
            elif path == "/recommend":
                self._send_json(self.service.recommend(self._read_json()))
            elif path == "/halts":
                role = self._require_staff()
                halt = self.service.issue_halt(self._read_json(), issued_by=role)
                self._send_json({"ok": True, "halt": halt.to_dict()}, 201)
            else:
                match = re.fullmatch(r"/halts/([^/]+)/resolve", path)
                if match:
                    role = self._require_staff()
                    halt = self.service.resolve_halt(match.group(1), resolved_by=role)
                    self._send_json({"ok": True, "halt": halt.to_dict()})
                    return
                self._send_json({"error": "NotFound", "message": f"无此路由：{path}"}, 404)
        except DomainError as exc:
            self._handle_domain_error(exc)

    # ---------- 业务处理 ----------

    def _handle_single_event(self):
        role, actor_venue = self._identity()
        payload = self._read_json()
        try:
            result = self.service.ingest_event(
                payload, role=role, actor_venue=actor_venue)
        except NotFoundError:
            # 点位台账尚未同步到位等瞬态情形：落本地队列，联网/台账就绪后补传。
            # 校验与鉴权错误不排队，避免无效事件被无限重试。
            if self.queue is not None and role in (ROLE_OPERATOR, ROLE_MERCHANT):
                self.queue.append(payload)
                self._send_json({"queued": True, "event_id": payload.get("event_id")}, 202)
                return
            raise
        self._send_json({"ok": True, **result}, 200 if result["duplicate"] else 201)

    def _handle_sync(self):
        role, actor_venue = self._identity()
        payload = self._read_json()
        events = payload.get("events")
        if not isinstance(events, list) or not events:
            raise ValidationError("sync 需要非空 events 数组")
        # 越权整批拒绝，避免部分写入造成现场对不上账。
        if role in (ROLE_OPERATOR, ROLE_MERCHANT):
            for event in events:
                if not actor_venue or event.get("venue_id") != actor_venue:
                    raise AuthError("批量补传混入了非本点位事件")
        results = []
        for event in events:
            try:
                outcome = self.service.ingest_event(
                    event, role=role, actor_venue=actor_venue)
                results.append({"ok": True, **outcome})
            except DomainError as exc:
                results.append({"ok": False, "event_id": event.get("event_id"),
                                "error": type(exc).__name__, "message": str(exc)})
        ingested = sum(1 for r in results if r["ok"] and not r["duplicate"])
        duplicates = sum(1 for r in results if r["ok"] and r["duplicate"])
        self._send_json({"ok": True, "ingested": ingested, "duplicates": duplicates,
                         "rejected": sum(1 for r in results if not r["ok"]),
                         "results": results})

    def _handle_replay(self):
        self._require_staff()
        query = urlparse(self.path).query
        params = dict(pair.split("=", 1) for pair in query.split("&") if "=" in pair)
        kwargs = {}
        for key in ("from", "to"):
            if key in params:
                try:
                    kwargs["frm" if key == "from" else key] = float(params[key])
                except ValueError as exc:
                    raise ValidationError(f"{key} 必须是 epoch 秒") from exc
        if "k" in params:
            try:
                kwargs["k"] = int(params["k"])
            except ValueError as exc:
                raise ValidationError("k 必须是整数") from exc
            if kwargs["k"] < 1:
                raise ValidationError("k 必须 >= 1")
        self._send_json(self.service.replay(**kwargs))

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--queue", help="现场模式：无法入账的事件落此 JSONL 队列，供联网补传")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        assert CoordinationService().list_catalog() == []
        print("基础检查通过")
        return
    handler = build_handler(CoordinationService(),
                            OfflineQueue(args.queue) if args.queue else None)
    ThreadingHTTPServer(("127.0.0.1", args.port), handler).serve_forever()


if __name__ == "__main__":
    main()
