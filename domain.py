"""第二现场承载协同的领域内核。

职责边界：
- 点位档案：开放时段、消防上限、无障碍条件、周边交通；
- 现场计数：进出 / 暂停事件，客户端生成事件号，断网补传按事件号幂等；
- 导流：结合预计到达时刻、数据新鲜度、剩余空间给出建议，
  停电 / 强对流 / 转播授权中断等预警按范围熔断，但已给出的建议留痕可查；
- 主管席：匿名跨区压力；
- 赛后复盘：用同一批事件复原溢出流向与处置速度，并做 k 匿名抑制。

本模块只做纯领域逻辑，时间由 ``now_fn`` 注入，便于离线补传与测试。
"""

from __future__ import annotations

import math
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

UTC = timezone.utc

# 计数超过该秒数视为不新鲜，不参与导流
STALE_LIMIT_SECONDS = 900
# 允许的终端时钟偏移（秒），未来事件超过偏移直接拒绝
CLOCK_SKEW_SECONDS = 30
# 复盘统计的时间桶
BUCKET_SECONDS = 300
# 溢出流向关联窗口：被拒后 15 分钟内他区进场计入候选
FLOW_WINDOW_BUCKETS = 3
# k 匿名门槛：聚合人数不足 k 的边与处置指标做抑制
K_ANON = 3
# 暂停 / 突发事件中，占用降到消防上限的该比例视为清空
CLEAR_RATIO = 0.1
# 建议留痕上限
ADVICE_HISTORY = 1000

ENTER = "enter"
EXIT = "exit"
SUSPEND = "suspend"
RESUME = "resume"
COUNT_OPS = (ENTER, EXIT)
GATE_OPS = (SUSPEND, RESUME)

ALERT_POWER = "power_outage"
ALERT_WEATHER = "weather"
ALERT_BROADCAST = "broadcast"


class DomainError(Exception):
    """可映射为 HTTP 状态码的领域错误。"""

    def __init__(self, reason: str, status: int = 400):
        super().__init__(reason)
        self.reason = reason
        self.status = status


def parse_ts(value) -> int:
    """接受 epoch 秒或 ISO8601 字符串，统一为整数秒。"""
    if value is None:
        raise DomainError("缺少时间字段")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise DomainError(f"无法解析时间：{value}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp())
    raise DomainError("时间字段格式不支持")


def iso(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), UTC).isoformat().replace("+00:00", "Z")


def _haversine_meters(lat1, lng1, lat2, lng2) -> int:
    radius = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return int(2 * radius * math.asin(math.sqrt(a)))


@dataclass
class Venue:
    id: str
    name: str
    district: str
    fire_capacity: int
    windows: list[tuple[int, int]]  # 开放时段 [(开, 闭)] epoch 秒
    lat: Optional[float] = None
    lng: Optional[float] = None
    accessible: bool = False
    transit: list[str] = field(default_factory=list)
    events: list["Event"] = field(default_factory=list)  # 按 (occurred_at, id) 有序

    def public_profile(self) -> dict:
        return {
            "venue_id": self.id,
            "name": self.name,
            "district": self.district,
            "fire_capacity": self.fire_capacity,
            "accessible": self.accessible,
            "transit": list(self.transit),
            "location": (
                {"lat": self.lat, "lng": self.lng}
                if self.lat is not None and self.lng is not None
                else None
            ),
            "opening_hours": [
                {"open_at": iso(s), "close_at": iso(e)} for s, e in self.windows
            ],
        }


@dataclass
class Event:
    event_id: str
    venue_id: str
    op: str
    count: int
    occurred_at: int
    received_at: int


@dataclass
class Alert:
    id: str
    type: str
    scope: str  # global / district / venue
    target: Optional[str]
    message: str
    started_at: int
    ended_at: Optional[int] = None

    def is_active(self, now: int) -> bool:
        return self.started_at <= now and (self.ended_at is None or self.ended_at > now)

    def affects(self, venue: Venue) -> bool:
        if self.scope == "global":
            return True
        if self.scope == "district":
            return venue.district == self.target
        return self.scope == "venue" and venue.id == self.target


class Store:
    """内存态协同存储，方法均线程安全。"""

    def __init__(self, now_fn: Callable[[], int] = lambda: int(datetime.now(UTC).timestamp())):
        self._now = now_fn
        self._lock = threading.RLock()
        self.venues: dict[str, Venue] = {}
        self._events: dict[str, Event] = {}
        self._results: dict[str, dict] = {}
        self.alerts: dict[str, Alert] = {}
        self.advice: dict[str, dict] = {}
        self._advice_order: list[str] = []
        self.rejections: list[dict] = []  # 未满足的进场需求（超容 / 暂停）

    now = property(lambda self: self._now())

    # ------------------------------------------------------------------ 点位

    def create_venue(self, data: dict) -> Venue:
        required = ("id", "name", "district", "fire_capacity", "opening_hours")
        missing = [k for k in required if k not in data]
        if missing:
            raise DomainError(f"缺少字段：{','.join(missing)}")
        vid = str(data["id"])
        cap = int(data["fire_capacity"])
        if cap <= 0:
            raise DomainError("消防上限必须为正整数")
        windows = []
        for window in data["opening_hours"]:
            s, e = parse_ts(window["open_at"]), parse_ts(window["close_at"])
            if e <= s:
                raise DomainError("开放时段闭点必须晚于开点")
            windows.append((s, e))
        with self._lock:
            if vid in self.venues:
                raise DomainError("点位已存在", status=409)
            venue = Venue(
                id=vid,
                name=str(data["name"]),
                district=str(data["district"]),
                fire_capacity=cap,
                windows=windows,
                lat=data.get("lat"),
                lng=data.get("lng"),
                accessible=bool(data.get("accessible", False)),
                transit=list(data.get("transit", [])),
            )
            self.venues[vid] = venue
            return venue

    def is_open(self, venue: Venue, at: int) -> bool:
        return any(s <= at < e for s, e in venue.windows)

    # ------------------------------------------------------------------ 事件

    def _sorted_count_events(self, venue: Venue) -> list[Event]:
        return [e for e in venue.events if e.op in COUNT_OPS]

    def occupancy_at(self, venue: Venue, at: int) -> int:
        total = 0
        for event in self._sorted_count_events(venue):
            if event.occurred_at > at:
                break
            total += event.count if event.op == ENTER else -event.count
        return total

    def _occupancy_before(self, venue: Venue, at: int, event_id: str) -> int:
        """模拟候选事件插入点之前的占用，同刻按事件号排序。"""
        total = 0
        for event in self._sorted_count_events(venue):
            if (event.occurred_at, event.event_id) >= (at, event_id):
                break
            total += event.count if event.op == ENTER else -event.count
        return total

    def _suspension_active(self, venue: Venue, at: int, exclude_id: Optional[str]) -> bool:
        active = False
        for event in sorted(venue.events, key=lambda e: (e.occurred_at, e.event_id)):
            if event.op not in GATE_OPS:
                continue
            if event.event_id == exclude_id:
                continue
            if event.occurred_at > at:
                break
            active = event.op == SUSPEND
        return active

    def _last_count_at(self, venue: Venue) -> Optional[int]:
        events = self._sorted_count_events(venue)
        return events[-1].occurred_at if events else None

    def record_event(self, venue_id: str, payload: dict) -> dict:
        """记录一条现场事件；同 event_id 重放只返回原结果，绝不重复计数。"""
        event_id = payload.get("event_id")
        if not event_id:
            raise DomainError("缺少 event_id（现场端需生成稳定幂等号）")
        if venue_id not in self.venues:
            raise DomainError("点位不存在", status=404)
        venue = self.venues[venue_id]
        op = payload.get("op")
        if op not in COUNT_OPS + GATE_OPS:
            raise DomainError(f"不支持的事件类型：{op}")
        occurred_at = parse_ts(payload.get("occurred_at"))
        count = int(payload.get("count", 1))
        if op in COUNT_OPS and count <= 0:
            raise DomainError("进出人数必须为正整数")
        if occurred_at > self._now() + CLOCK_SKEW_SECONDS:
            raise DomainError("事件时间超出允许的时钟偏移")

        with self._lock:
            if event_id in self._events:
                stored = self._results[event_id]
                return {**stored, "duplicate": True}

            before = self._occupancy_before(venue, occurred_at, event_id)
            reason = None
            if op == ENTER and self._suspension_active(venue, occurred_at, event_id):
                reason = "点位处于暂停接待状态，进场被拒"
            elif op == ENTER and before + count > venue.fire_capacity:
                reason = (
                    f"超过消防上限：当前在场 {before}，上限 {venue.fire_capacity}"
                )
            elif op == EXIT and before - count < 0:
                reason = f"出场人数超过在场人数：当前在场 {before}"
            elif op == SUSPEND and self._suspension_active(
                venue, occurred_at, event_id
            ):
                reason = "点位已经处于暂停状态"
            elif op == RESUME and not self._suspension_active(
                venue, occurred_at, event_id
            ):
                reason = "点位未暂停，无可恢复的接待"

            event = Event(
                event_id=event_id,
                venue_id=venue_id,
                op=op,
                count=count if op in COUNT_OPS else 0,
                occurred_at=occurred_at,
                received_at=self._now(),
            )
            if reason:
                # 拒绝的进场仍记入需求账，供赛后复原溢出
                if op == ENTER:
                    self.rejections.append(
                        {
                            "venue_id": venue_id,
                            "district": venue.district,
                            "occurred_at": occurred_at,
                            "count": count,
                            "reason": reason,
                        }
                    )
                result = {
                    "event_id": event_id,
                    "status": "rejected",
                    "reason": reason,
                    "occupancy_after": before,
                }
                # 拒绝事件不留事件号占位之外的事实；占位防止修正后重放歧义
                self._events[event_id] = event
                self._results[event_id] = result
                return result

            venue.events.append(event)
            venue.events.sort(key=lambda e: (e.occurred_at, e.event_id))
            self._events[event_id] = event
            after = self.occupancy_at(venue, occurred_at)
            result = {
                "event_id": event_id,
                "status": "accepted",
                "reason": None,
                "occupancy_after": after,
            }
            self._results[event_id] = result
            return result

    def record_events(self, venue_id: str, payload) -> list[dict]:
        """批量补传（断网恢复后）。逐条幂等，部分成功不影响其他记录。"""
        if isinstance(payload, dict) and "events" in payload:
            items = payload["events"]
        elif isinstance(payload, list):
            items = payload
        else:
            raise DomainError("请求体需为事件数组或 {'events': [...]}")
        return [self.record_event(venue_id, item) for item in items]

    # ------------------------------------------------------------------ 预警

    def raise_alert(self, data: dict) -> Alert:
        alert_type = data.get("type")
        if alert_type not in (ALERT_POWER, ALERT_WEATHER, ALERT_BROADCAST):
            raise DomainError("不支持的预警类型")
        scope = data.get("scope", "global")
        if scope not in ("global", "district", "venue"):
            raise DomainError("预警范围必须是 global / district / venue")
        if scope != "global" and not data.get("target"):
            raise DomainError("district / venue 预警必须提供 target")
        with self._lock:
            alert = Alert(
                id=str(uuid.uuid4()),
                type=alert_type,
                scope=scope,
                target=data.get("target"),
                message=str(data.get("message", "")),
                started_at=parse_ts(data["started_at"]) if data.get("started_at") else self._now(),
            )
            self.alerts[alert.id] = alert
            return alert

    def resolve_alert(self, alert_id: str) -> Alert:
        with self._lock:
            if alert_id not in self.alerts:
                raise DomainError("预警不存在", status=404)
            alert = self.alerts[alert_id]
            if alert.ended_at is not None:
                return alert
            alert.ended_at = self._now()
            return alert

    def active_alerts(self, now: Optional[int] = None) -> list[Alert]:
        now = self._now() if now is None else now
        with self._lock:
            return [a for a in self.alerts.values() if a.is_active(now)]

    # -------------------------------------------------------------- 点位状态

    def venue_state(self, venue: Venue, now: Optional[int] = None) -> dict:
        now = self._now() if now is None else now
        with self._lock:
            occupancy = self.occupancy_at(venue, now)
            last_count_at = self._last_count_at(venue)
            age = None if last_count_at is None else max(0, now - last_count_at)
            suspended = self._suspension_active(venue, now, None)
            alerts = [a for a in self.active_alerts(now) if a.affects(venue)]
            open_now = self.is_open(venue, now)
            if any(a.type == ALERT_POWER for a in alerts):
                status = "power_outage"
            elif any(a.type == ALERT_WEATHER for a in alerts):
                status = "weather_alert"
            elif any(a.type == ALERT_BROADCAST for a in alerts):
                status = "broadcast_halted"
            elif suspended:
                status = "suspended"
            elif not open_now:
                status = "closed"
            elif occupancy >= venue.fire_capacity:
                status = "full"
            else:
                status = "open"
            return {
                "venue_id": venue.id,
                "name": venue.name,
                "district": venue.district,
                "status": status,
                "open": open_now,
                "suspended": suspended,
                "occupancy": occupancy,
                "fire_capacity": venue.fire_capacity,
                "remaining": venue.fire_capacity - occupancy,
                "ratio": round(occupancy / venue.fire_capacity, 3),
                "last_count_at": iso(last_count_at),
                "age_seconds": age,
                "stale": age is None or age > STALE_LIMIT_SECONDS,
                "active_alert_ids": [a.id for a in alerts],
            }

    # ------------------------------------------------------------------ 导流

    def _eta_seconds(self, venue: Venue, origin: dict, travel: dict) -> Optional[int]:
        if venue.id in travel:
            return max(0, int(travel[venue.id]))
        lat, lng = origin.get("lat"), origin.get("lng")
        if (
            lat is not None
            and lng is not None
            and venue.lat is not None
            and venue.lng is not None
        ):
            meters = _haversine_meters(float(lat), float(lng), venue.lat, venue.lng)
            # 无导航上游时按步行 1.4 m/s 保守估算
            return int(meters / 1.4)
        return None

    def recommend(self, payload: dict) -> dict:
        origin = payload.get("origin") or {}
        travel = payload.get("travel") or {}
        requires_accessible = bool(payload.get("requires_accessible"))
        depart_at = (
            parse_ts(payload["depart_at"]) if payload.get("depart_at") else self._now()
        )
        now = self._now()

        with self._lock:
            active = self.active_alerts(now)
            global_alerts = [a for a in active if a.scope == "global"]
            halt_reasons = [
                f"{a.type}：{a.message or '全局熔断，停止导流'}" for a in global_alerts
            ]

            candidates = []
            for venue in self.venues.values():
                state = self.venue_state(venue, now)
                venue_alerts = [a for a in active if a.affects(venue)]
                eta = self._eta_seconds(venue, origin, travel)
                arrival = depart_at + eta if eta is not None else None

                excluded = None
                if venue_alerts:
                    excluded = "命中生效预警：" + "、".join(
                        a.type for a in venue_alerts
                    )
                elif requires_accessible and not venue.accessible:
                    excluded = "不满足无障碍要求"
                elif eta is None:
                    excluded = "缺少到达时间预估"
                elif not self.is_open(venue, arrival):
                    excluded = "预计到达时已不在开放时段"
                elif state["suspended"]:
                    excluded = "点位暂停接待"
                elif state["age_seconds"] is None:
                    excluded = "尚无计数上报，剩余空间不可知"
                elif state["age_seconds"] > STALE_LIMIT_SECONDS:
                    excluded = f"计数数据已过期 {state['age_seconds']} 秒"
                elif state["remaining"] <= 0:
                    excluded = "已满员"

                if excluded:
                    candidates.append(
                        {"venue_id": venue.id, "eligible": False, "reason": excluded}
                    )
                    continue

                age = state["age_seconds"]
                score = (
                    100 * state["remaining"] / venue.fire_capacity
                    - (eta / 60) * 2
                    - (age / 60) * 1.0
                )
                reasons = [
                    f"剩余空间约 {state['remaining']} / 消防上限 {venue.fire_capacity}",
                    f"步行约 {round(eta / 60)} 分钟，到达时仍在开放时段",
                    f"计数 {age} 秒前更新",
                ]
                if venue.accessible:
                    reasons.append("具备无障碍条件")
                candidates.append(
                    {
                        "venue_id": venue.id,
                        "name": venue.name,
                        "district": venue.district,
                        "eligible": True,
                        "eta_seconds": eta,
                        "arrival_at": iso(arrival),
                        "remaining": state["remaining"],
                        "fire_capacity": venue.fire_capacity,
                        "score": round(score, 2),
                        "reasons": reasons,
                    }
                )

            ranked = sorted(
                (c for c in candidates if c["eligible"]),
                key=lambda c: (-c["score"], c["venue_id"]),
            )[:5]
            excluded = [
                {"venue_id": c["venue_id"], "reason": c["reason"]}
                for c in candidates
                if not c["eligible"]
            ]

            # 只留存区级粗粒度来源，不落精确坐标，避免赛后拼出个人行程
            record = {
                "advice_id": str(uuid.uuid4()),
                "generated_at": iso(now),
                "origin_district": origin.get("district"),
                "depart_at": iso(depart_at),
                "halted": bool(global_alerts),
                "halt_reasons": halt_reasons,
                "active_alert_ids": [a.id for a in active],
                "recommendations": ranked if not global_alerts else [],
                "excluded": excluded,
            }
            self.advice[record["advice_id"]] = record
            self._advice_order.append(record["advice_id"])
            if len(self._advice_order) > ADVICE_HISTORY:
                stale_id = self._advice_order.pop(0)
                self.advice.pop(stale_id, None)

            return {
                "advice_id": record["advice_id"],
                "generated_at": record["generated_at"],
                "halted": record["halted"],
                "halt_reasons": halt_reasons,
                "recommendations": record["recommendations"],
                "excluded": excluded,
            }

    def get_advice(self, advice_id: str) -> dict:
        with self._lock:
            if advice_id not in self.advice:
                raise DomainError("建议记录不存在", status=404)
            return self.advice[advice_id]

    # ------------------------------------------------------------ 匿名压力图

    def pressure(self) -> dict:
        now = self._now()
        with self._lock:
            venues = [self.venue_state(v, now) for v in self.venues.values()]
        districts: dict[str, dict] = {}
        for state in venues:
            bucket = districts.setdefault(
                state["district"],
                {"district": state["district"], "occupancy": 0, "capacity": 0, "venues": 0},
            )
            bucket["occupancy"] += state["occupancy"]
            bucket["capacity"] += state["fire_capacity"]
            bucket["venues"] += 1
        for bucket in districts.values():
            bucket["ratio"] = (
                round(bucket["occupancy"] / bucket["capacity"], 3)
                if bucket["capacity"]
                else 0
            )
        return {
            "generated_at": iso(now),
            "districts": sorted(districts.values(), key=lambda d: -d["ratio"]),
            "venues": sorted(venues, key=lambda v: (-v["ratio"], v["venue_id"])),
        }

    # ---------------------------------------------------------------- 复盘

    def _suspension_intervals(self, venue: Venue) -> list[tuple[int, Optional[int]]]:
        intervals: list[tuple[int, Optional[int]]] = []
        start: Optional[int] = None
        for event in sorted(venue.events, key=lambda e: (e.occurred_at, e.event_id)):
            if event.op == SUSPEND:
                start = event.occurred_at
            elif event.op == RESUME and start is not None:
                intervals.append((start, event.occurred_at))
                start = None
        if start is not None:
            intervals.append((start, None))
        return intervals

    def _incidents(self, now: int) -> list[dict]:
        incidents = []
        with self._lock:
            for venue in self.venues.values():
                for start, end in self._suspension_intervals(venue):
                    incidents.append(
                        self._incident_metrics(
                            "suspension", venue, start, end or now, now, end is None
                        )
                    )
            for alert in self.alerts.values():
                if alert.scope != "venue" or alert.target not in self.venues:
                    continue
                venue = self.venues[alert.target]
                end = alert.ended_at or now
                incidents.append(
                    self._incident_metrics(
                        alert.type, venue, alert.started_at, end, now,
                        alert.ended_at is None,
                    )
                )
        return sorted(incidents, key=lambda x: x["started_at"])

    def _incident_metrics(
        self, kind: str, venue: Venue, start: int, end: int, now: int, ongoing: bool
    ) -> dict:
        count_events = [
            e
            for e in self._sorted_count_events(venue)
            if start <= e.occurred_at <= end
        ]
        exits = [e for e in count_events if e.op == EXIT]
        exit_people = sum(e.count for e in exits)
        first_exit = exits[0].occurred_at if exits else None
        peak = self.occupancy_at(venue, start)
        cleared_at = start if peak <= venue.fire_capacity * CLEAR_RATIO else None
        for event in count_events:
            occ = self.occupancy_at(venue, event.occurred_at)
            peak = max(peak, occ)
            if cleared_at is None and occ <= venue.fire_capacity * CLEAR_RATIO:
                cleared_at = event.occurred_at
        # 离场人数不足 k 时，处置速度可能对应到具体个人，指标抑制
        low_sample = exit_people < K_ANON
        duration = max(0, end - start)
        return {
            "kind": kind,
            "venue_id": venue.id,
            "district": venue.district,
            "started_at": iso(start),
            "ended_at": None if ongoing else iso(end),
            "peak_occupancy": peak,
            "response_seconds": None if first_exit is None else first_exit - start,
            "clear_seconds": None if cleared_at is None else cleared_at - start,
            "exit_throughput_per_minute": (
                None
                if low_sample or duration == 0
                else round(exit_people / (duration / 60), 2)
            ),
            "low_sample": low_sample,
        }

    def _overflow_flows(self) -> list[dict]:
        """按时间桶把被拒需求与他区后续进场做比例关联。

        只输出聚合后的区级边，人数不足 k 匿名门槛的边抑制为未归因需求，
        任何一条边都无法回溯到具体观众。
        """
        now = self._now()
        # 目的桶 -> 区 -> 进场量
        enter_buckets: dict[int, dict[str, int]] = {}
        with self._lock:
            for venue in self.venues.values():
                for event in self._sorted_count_events(venue):
                    if event.op != ENTER:
                        continue
                    b = (event.occurred_at // BUCKET_SECONDS) * BUCKET_SECONDS
                    enter_buckets.setdefault(b, {})
                    enter_buckets[b][venue.district] = (
                        enter_buckets[b].get(venue.district, 0) + event.count
                    )

        # 源桶 -> 源区 -> 被拒量
        demand: dict[int, dict[str, int]] = {}
        for item in self.rejections:
            b = (item["occurred_at"] // BUCKET_SECONDS) * BUCKET_SECONDS
            demand.setdefault(b, {})
            demand[b][item["district"]] = (
                demand[b].get(item["district"], 0) + item["count"]
            )

        flows: list[dict] = []
        suppressed = 0
        for b in sorted(demand):
            for from_district, rejected in demand[b].items():
                candidates: dict[str, int] = {}
                for offset in range(FLOW_WINDOW_BUCKETS):
                    for district, count in enter_buckets.get(b + offset * BUCKET_SECONDS, {}).items():
                        if district != from_district:
                            candidates[district] = candidates.get(district, 0) + count
                total = sum(candidates.values())
                if total == 0:
                    suppressed += rejected
                    continue
                for to_district, entered in sorted(candidates.items()):
                    volume = round(rejected * entered / total)
                    if volume == 0:
                        continue
                    edge = {
                        "bucket_at": iso(b),
                        "from_district": from_district,
                        "to_district": to_district,
                        "estimated_count": volume,
                        "suppressed": volume < K_ANON,
                    }
                    if volume < K_ANON:
                        suppressed += volume
                    else:
                        flows.append(edge)
        return flows, suppressed

    def replay(self) -> dict:
        now = self._now()
        flows, suppressed = self._overflow_flows()
        with self._lock:
            rejected_total = sum(item["count"] for item in self.rejections)
        return {
            "generated_at": iso(now),
            "k_anonymity": K_ANON,
            "bucket_seconds": BUCKET_SECONDS,
            "unmet_demand_total": rejected_total,
            "suppressed_flow_count": suppressed,
            "flows": flows,
            "incidents": self._incidents(now),
        }
