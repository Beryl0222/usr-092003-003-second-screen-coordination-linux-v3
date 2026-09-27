"""第二现场承载协同的领域核心。

不依赖 HTTP 与存储介质：点位台账、进出事件、停推指令、导流推荐、
主管压力视图与赛后复原都在这里完成，便于离线推演与单元测试。

设计约定：
- 事件由现场端生成 event_id，服务端按 id 幂等去重，断网补传不会重复计数。
- 服务不接收任何观众身份字段（白名单校验），人数只以聚合形式存在。
- 时间戳一律为 epoch 秒；时钟通过构造函数注入，测试可确定化。
"""

from __future__ import annotations

import math
import statistics
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

# 允许的事件类型：进出与设备计数维护在场人数，暂停/恢复维护推荐资格。
EVENT_TYPES = {"entry", "exit", "headcount", "pause", "resume"}
COUNT_EVENTS = {"entry", "exit", "headcount"}
# 停电、强对流预警、转播授权中断等停推原因（other 供运营补充）。
HALT_REASONS = {"power_outage", "severe_weather", "broadcast_rights", "other"}
# 任何入口都不得携带可识别观众的字段，防的就是事后用统计拼出行程。
FORBIDDEN_FIELDS = {"person_id", "user_id", "device_id", "token", "id_card", "phone", "openid"}

STALE_AFTER_SECONDS = 300          # 人数数据超过 5 分钟视为不新鲜，不再推荐
RATE_WINDOW_SECONDS = 600          # 用最近 10 分钟进出估计净流速
RATE_MIN_SPAN_SECONDS = 120        # 样本跨度不足 2 分钟不外推
OVERFLOW_WINDOW_SECONDS = 1800     # 受压后 30 分钟内的他点增量视为溢出承接
DEFAULT_K_ANON = 5                 # 流向聚合的最小匿名阈值
WALK_SPEED_MPS = 1.4               # 步行均速，用于按直线距离估 ETA
GRID_DEGREES = 0.01                # 建议留痕时把出发点约到 1km 网格，避免轨迹化

ROLE_ADMIN = "admin"
ROLE_SUPERVISOR = "supervisor"
ROLE_OPERATOR = "operator"
ROLE_MERCHANT = "merchant"


class DomainError(Exception):
    """领域错误基类，HTTP 层据此映射状态码。"""


class ValidationError(DomainError):
    """请求数据不合法，映射 400。"""


class AuthError(DomainError):
    """角色或点位权限不足，映射 403。"""


class NotFoundError(DomainError):
    """对象不存在，映射 404。"""


@dataclass
class Venue:
    venue_id: str
    name: str
    zone: str
    capacity: int
    open_windows: list[list[str]]            # [["18:00", "23:00"]]，支持跨午夜
    accessibility: dict                     # step_free / wheelchair_toilet / quiet_room 等
    transport: list[str]                    # 周边交通描述，如 ["地铁2号线","公交专线"]
    lat: Optional[float] = None
    lon: Optional[float] = None

    def public_catalog(self) -> dict:
        """城市活动页面可见的静态目录，不含实时人数。"""
        return {
            "venue_id": self.venue_id,
            "name": self.name,
            "zone": self.zone,
            "open_windows": self.open_windows,
            "accessibility": self.accessibility,
            "transport": self.transport,
        }


@dataclass
class Halt:
    halt_id: str
    reason: str
    issued_by: str
    issued_at: float
    scope: str = "venue"                    # venue 或 event（event 为全局停推）
    venue_id: Optional[str] = None
    detail: str = ""
    resolved_at: Optional[float] = None
    resolved_by: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "halt_id": self.halt_id,
            "scope": self.scope,
            "venue_id": self.venue_id,
            "reason": self.reason,
            "detail": self.detail,
            "issued_by": self.issued_by,
            "issued_at": self.issued_at,
            "resolved_at": self.resolved_at,
            "resolved_by": self.resolved_by,
        }


@dataclass
class _VenueState:
    current: int = 0
    paused: bool = False
    last_count_ts: Optional[float] = None
    total_entries: int = 0
    total_exits: int = 0
    count_events: list[tuple[float, int]] = field(default_factory=list)  # (ts, 净增量)


def _parse_hhmm(value: str) -> tuple[int, int]:
    try:
        hour_str, minute_str = value.split(":", 1)
        hour, minute = int(hour_str), int(minute_str)
    except (ValueError, AttributeError) as exc:
        raise ValidationError(f"开放时段格式应为 HH:MM：{value!r}") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValidationError(f"开放时刻越界：{value!r}")
    return hour, minute


def is_within_windows(at: float, windows: list[list[str]]) -> bool:
    """判定时刻是否落在开放时段内；空表视为未维护，按未知处理（不开放推荐）。"""
    if not windows:
        return False
    local = datetime.fromtimestamp(at)
    now_minutes = local.hour * 60 + local.minute
    for window in windows:
        start_h, start_m = _parse_hhmm(window[0])
        end_h, end_m = _parse_hhmm(window[1])
        start = start_h * 60 + start_m
        end = end_h * 60 + end_m
        if start == end:
            return True  # 00:00-00:00 约定为全天开放
        if start < end:
            if start <= now_minutes < end:
                return True
        else:  # 跨午夜，如 20:00-02:00
            if now_minutes >= start or now_minutes < end:
                return True
    return False


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


class CoordinationService:
    """线程安全的内存领域服务。"""

    def __init__(self, clock: Callable[[], float] = None):
        self._lock = threading.RLock()
        self._clock = clock or __import__("time").time
        self._venues: dict[str, Venue] = {}
        self._events: list[dict] = []
        self._seq = 0
        self._seen_event_ids: dict[str, dict] = {}
        self._halts: list[Halt] = []
        self._recommendations: dict[str, dict] = {}

    # ---------- 点位台账 ----------

    def register_venue(self, data: dict) -> Venue:
        with self._lock:
            required = ("venue_id", "name", "zone", "capacity", "open_windows")
            missing = [key for key in required if key not in data]
            if missing:
                raise ValidationError(f"点位缺少必填字段：{','.join(missing)}")
            venue_id = str(data["venue_id"])
            if venue_id in self._venues:
                raise ValidationError(f"点位已存在：{venue_id}")
            capacity = int(data["capacity"])
            if capacity <= 0:
                raise ValidationError("消防上限必须为正数")
            windows = data["open_windows"]
            if not isinstance(windows, list) or not all(isinstance(w, list) and len(w) == 2 for w in windows):
                raise ValidationError("open_windows 应为 [起, 止] 列表")
            for start, end in windows:
                _parse_hhmm(start)
                _parse_hhmm(end)
            venue = Venue(
                venue_id=venue_id,
                name=str(data["name"]),
                zone=str(data["zone"]),
                capacity=capacity,
                open_windows=windows,
                accessibility=dict(data.get("accessibility", {})),
                transport=list(data.get("transport", [])),
                lat=data.get("lat"),
                lon=data.get("lon"),
            )
            self._venues[venue_id] = venue
            return venue

    def list_catalog(self) -> list[dict]:
        with self._lock:
            return [v.public_catalog() for v in self._venues.values()]

    def list_halts(self, include_resolved: bool = True) -> list[Halt]:
        with self._lock:
            return [h for h in self._halts if include_resolved or h.resolved_at is None]

    def _venue(self, venue_id: str) -> Venue:
        try:
            return self._venues[venue_id]
        except KeyError:
            raise NotFoundError(f"点位不存在：{venue_id}") from None

    # ---------- 事件与幂等 ----------

    def ingest_event(self, event: dict, *, role: str = None, actor_venue: str = None) -> dict:
        """收录一条现场事件。

        operator/merchant 只能写本点位；重复 event_id 直接返回首次结果，
        无论重传多少次，人数只算一次。
        """
        cleaned = self._validate_event(event)
        if role in (ROLE_OPERATOR, ROLE_MERCHANT):
            if not actor_venue or actor_venue != cleaned["venue_id"]:
                raise AuthError("现场账号只能记录本点位事件")
        with self._lock:
            existing = self._seen_event_ids.get(cleaned["event_id"])
            if existing is not None:
                return {"duplicate": True, "event_id": existing["event_id"], "type": existing["type"]}
            self._venue(cleaned["venue_id"])  # 点位必须已登记
            self._seq += 1
            cleaned["seq"] = self._seq
            cleaned["received_ts"] = self._clock()
            self._events.append(cleaned)
            self._seen_event_ids[cleaned["event_id"]] = cleaned
            return {"duplicate": False, "event_id": cleaned["event_id"], "type": cleaned["type"]}

    @staticmethod
    def _validate_event(event: dict) -> dict:
        if not isinstance(event, dict):
            raise ValidationError("事件必须是对象")
        illegal = sorted(FORBIDDEN_FIELDS.intersection(event.keys()))
        if illegal:
            raise ValidationError(f"事件不得携带观众身份字段：{','.join(illegal)}")
        missing = [key for key in ("event_id", "venue_id", "type", "client_ts") if not event.get(key)]
        if missing:
            raise ValidationError(f"事件缺少必填字段：{','.join(missing)}")
        etype = event["type"]
        if etype not in EVENT_TYPES:
            raise ValidationError(f"未知事件类型：{etype}")
        client_ts = float(event["client_ts"])
        cleaned = {
            "event_id": str(event["event_id"]),
            "venue_id": str(event["venue_id"]),
            "type": etype,
            "client_ts": client_ts,
        }
        if etype in ("entry", "exit"):
            count = int(event.get("count", 1))
            if count <= 0:
                raise ValidationError("count 必须为正整数")
            cleaned["count"] = count
        elif etype == "headcount":
            if "value" not in event:
                raise ValidationError("headcount 事件需要 value")
            value = int(event["value"])
            if value < 0:
                raise ValidationError("headcount 值不能为负")
            cleaned["value"] = value
        return cleaned

    # ---------- 派生状态 ----------

    def _venue_events_locked(self, venue_id: str) -> list[dict]:
        return sorted(
            (e for e in self._events if e["venue_id"] == venue_id),
            key=lambda e: (e["client_ts"], e["seq"]),
        )

    def _replay_locked(self, venue_id: str, until: float = None) -> _VenueState:
        """按事件发生时刻回放，得到点位在场人数等状态。"""
        state = _VenueState()
        for event in self._venue_events_locked(venue_id):
            if until is not None and event["client_ts"] > until:
                break
            etype = event["type"]
            if etype == "entry":
                state.current += event["count"]
                state.total_entries += event["count"]
                state.last_count_ts = event["client_ts"]
                state.count_events.append((event["client_ts"], event["count"]))
            elif etype == "exit":
                state.current = max(0, state.current - event["count"])
                state.total_exits += event["count"]
                state.last_count_ts = event["client_ts"]
                state.count_events.append((event["client_ts"], -event["count"]))
            elif etype == "headcount":
                state.current = event["value"]
                state.last_count_ts = event["client_ts"]
            elif etype == "pause":
                state.paused = True
            elif etype == "resume":
                state.paused = False
        return state

    def _active_halts_locked(self, at: float, venue_id: str = None) -> list[Halt]:
        active = [h for h in self._halts if h.resolved_at is None and h.issued_at <= at]
        if venue_id is None:
            return [h for h in active if h.scope == "event"]
        return [h for h in active if h.scope == "event" or h.venue_id == venue_id]

    def venue_status(self, venue_id: str, *, role: str = None, actor_venue: str = None) -> dict:
        with self._lock:
            if role in (ROLE_OPERATOR, ROLE_MERCHANT) and actor_venue != venue_id:
                raise AuthError("现场账号只能查看本点位状态")
            venue = self._venue(venue_id)
            at = self._clock()
            state = self._replay_locked(venue_id)
            halts = self._active_halts_locked(at, venue_id)
            freshness = None if state.last_count_ts is None else at - state.last_count_ts
            return {
                "venue_id": venue_id,
                "zone": venue.zone,
                "capacity": venue.capacity,
                "current": state.current,
                "remaining": max(0, venue.capacity - state.current),
                "paused": state.paused,
                "open": is_within_windows(at, venue.open_windows),
                "halted": bool(halts),
                "halt_reasons": sorted({h.reason for h in halts}),
                "last_count_ts": state.last_count_ts,
                "data_age_seconds": freshness,
                "fresh": freshness is not None and freshness <= STALE_AFTER_SECONDS,
            }

    # ---------- 停推 / 恢复 ----------

    def issue_halt(self, data: dict, *, issued_by: str) -> Halt:
        with self._lock:
            reason = data.get("reason")
            if reason not in HALT_REASONS:
                raise ValidationError(f"停推原因须为：{','.join(sorted(HALT_REASONS))}")
            scope = data.get("scope", "venue")
            venue_id = data.get("venue_id")
            if scope == "venue":
                if not venue_id:
                    raise ValidationError("点位级停推需要 venue_id")
                self._venue(venue_id)
            elif scope != "event":
                raise ValidationError("scope 须为 venue 或 event")
            halt = Halt(
                halt_id=uuid.uuid4().hex[:12],
                scope=scope,
                venue_id=venue_id if scope == "venue" else None,
                reason=reason,
                detail=str(data.get("detail", ""))[:200],
                issued_by=issued_by,
                issued_at=self._clock(),
            )
            self._halts.append(halt)
            return halt

    def resolve_halt(self, halt_id: str, *, resolved_by: str) -> Halt:
        with self._lock:
            halt = next((h for h in self._halts if h.halt_id == halt_id), None)
            if halt is None:
                raise NotFoundError(f"停推指令不存在：{halt_id}")
            if halt.resolved_at is not None:
                return halt
            halt.resolved_at = self._clock()
            halt.resolved_by = resolved_by
            return halt

    # ---------- 导流推荐 ----------

    def _rate_locked(self, state: _VenueState, at: float) -> float:
        recent = [(ts, d) for ts, d in state.count_events if ts >= at - RATE_WINDOW_SECONDS]
        if len(recent) < 2:
            return 0.0
        span = recent[-1][0] - recent[0][0]
        if span < RATE_MIN_SPAN_SECONDS:
            return 0.0
        return sum(d for _, d in recent) / span

    def recommend(self, query: dict) -> dict:
        """结合预计到达时刻、数据新鲜度与剩余空间给出导流建议。

        任一全局/点位停推生效时不再给候选，但本次判定与历史建议一并留痕。
        """
        with self._lock:
            at = float(query.get("at") or self._clock())
            party_size = max(1, int(query.get("party_size", 1)))
            accessible_only = bool(query.get("filters", {}).get("accessible_only"))
            origin = query.get("from") or {}
            has_origin = origin.get("lat") is not None and origin.get("lon") is not None
            travel_override = query.get("travel_seconds") or {}

            global_halts = self._active_halts_locked(at)
            options: list[dict] = []
            candidates: list[dict] = []
            for venue in self._venues.values():
                if accessible_only and not venue.accessibility.get("step_free"):
                    continue
                state = self._replay_locked(venue_id=venue.venue_id)
                halts = self._active_halts_locked(at, venue.venue_id)

                travel_seconds = travel_override.get(venue.venue_id)
                if travel_seconds is None and has_origin and venue.lat is not None:
                    travel_seconds = int(
                        _haversine_m(origin["lat"], origin["lon"], venue.lat, venue.lon) / WALK_SPEED_MPS
                    )
                travel_seconds = None if travel_seconds is None else max(0, int(travel_seconds))
                arrival = at + (travel_seconds or 0)

                reasons = []
                if halts:
                    reasons.append("halted:" + ",".join(sorted({h.reason for h in halts})))
                if state.paused:
                    reasons.append("paused")
                if not is_within_windows(arrival, venue.open_windows):
                    reasons.append("closed_at_arrival")
                freshness = None if state.last_count_ts is None else at - state.last_count_ts
                if freshness is None:
                    reasons.append("no_occupancy_data")
                elif freshness > STALE_AFTER_SECONDS:
                    reasons.append("stale_data")

                rate = self._rate_locked(state, at)
                projected = state.current
                if freshness is not None and freshness <= STALE_AFTER_SECONDS and rate:
                    projected += rate * (arrival - (state.last_count_ts or at))
                remaining_arrival = max(0.0, venue.capacity - projected)
                if reasons == [] and remaining_arrival + 1e-9 < party_size:
                    reasons.append("insufficient_capacity")

                candidate = {
                    "venue_id": venue.venue_id,
                    "zone": venue.zone,
                    "eta_seconds": travel_seconds,
                    "arrival_ts": arrival,
                    "remaining_now": max(0, venue.capacity - state.current),
                    "remaining_at_arrival": round(remaining_arrival, 1),
                    "data_age_seconds": None if freshness is None else round(freshness, 1),
                    "open_at_arrival": is_within_windows(arrival, venue.open_windows),
                    "paused": state.paused,
                    "halt_reasons": sorted({h.reason for h in halts}),
                    "excluded_reasons": reasons,
                }
                candidates.append(candidate)
                if not reasons:
                    score = round(remaining_arrival - 0.05 * (travel_seconds or 0), 2)
                    options.append({**candidate, "score": score})

            options.sort(key=lambda c: (-c["score"], c["eta_seconds"] or 10**9))
            limit = int(query.get("limit", 3))
            options = options[:limit]

            origin_grid = None
            if has_origin:
                origin_grid = {
                    "lat": round(float(origin["lat"]) / GRID_DEGREES) * GRID_DEGREES,
                    "lon": round(float(origin["lon"]) / GRID_DEGREES) * GRID_DEGREES,
                }
            record = {
                "recommendation_id": uuid.uuid4().hex[:12],
                "at": at,
                "party_size": party_size,
                "accessible_only": accessible_only,
                "origin_grid_1km": origin_grid,  # 只留约 1km 网格，不留精确出发点
                "halted": bool(global_halts),
                "global_halt_reasons": sorted({h.reason for h in global_halts}),
                "options": options,
                "candidate_rationale": candidates,
            }
            self._recommendations[record["recommendation_id"]] = record

            response = {
                "recommendation_id": record["recommendation_id"],
                "at": at,
                "halted": record["halted"],
                "halt_reasons": record["global_halt_reasons"],
                # 全局停推时整体停止推荐；仅点位级停推时其余点仍可推荐。
                "options": [] if global_halts else options,
            }
            return response

    def get_recommendation(self, recommendation_id: str) -> dict:
        with self._lock:
            record = self._recommendations.get(recommendation_id)
            if record is None:
                raise NotFoundError(f"建议不存在：{recommendation_id}")
            return record

    # ---------- 主管席：匿名跨区压力 ----------

    def zone_pressure(self) -> dict:
        """只给区级聚合，不给单点位明细，商户之间无法互探底数。"""
        with self._lock:
            at = self._clock()
            zones: dict[str, dict] = {}
            for venue in self._venues.values():
                bucket = zones.setdefault(
                    venue.zone,
                    {"zone": venue.zone, "venues": 0, "open": 0, "paused": 0, "halted": 0,
                     "occupancy": 0, "capacity": 0, "fresh_venues": 0, "reporting_venues": 0},
                )
                state = self._replay_locked(venue.venue_id)
                halts = self._active_halts_locked(at, venue.venue_id)
                bucket["venues"] += 1
                bucket["open"] += int(is_within_windows(at, venue.open_windows))
                bucket["paused"] += int(state.paused)
                bucket["halted"] += int(bool(halts))
                bucket["occupancy"] += state.current
                bucket["capacity"] += venue.capacity
                age = None if state.last_count_ts is None else at - state.last_count_ts
                if age is not None:
                    bucket["reporting_venues"] += 1
                    bucket["fresh_venues"] += int(age <= STALE_AFTER_SECONDS)
            for bucket in zones.values():
                bucket["pressure_ratio"] = round(
                    bucket["occupancy"] / bucket["capacity"], 3) if bucket["capacity"] else None
            return {"at": at, "zones": sorted(zones.values(), key=lambda z: -(z["pressure_ratio"] or 0))}

    # ---------- 赛后复原 ----------

    def replay(self, frm: float = None, to: float = None, k: int = DEFAULT_K_ANON) -> dict:
        """用同一批事件复原溢出流向与处置速度。

        输出只有聚合：处置时长、点位 occupancy 序列、k 匿名后的跨区流向，
        无法从中还原任何观众的个人行程。
        """
        with self._lock:
            to = to if to is not None else self._clock()
            frm = frm if frm is not None else to - 3600

            # 处置速度：暂停时长与停推解除时长。
            pause_durations = self._pause_durations_locked(frm, to)
            halt_rows = []
            halt_durations = []
            for halt in self._halts:
                if halt.issued_at > to:
                    continue
                end = halt.resolved_at if halt.resolved_at is not None else None
                duration = None if end is None else max(0.0, end - halt.issued_at)
                if duration is not None and halt.issued_at >= frm:
                    halt_durations.append(duration)
                halt_rows.append({
                    "scope": halt.scope,
                    "venue_id": halt.venue_id,
                    "reason": halt.reason,
                    "issued_at": halt.issued_at,
                    "resolved_at": end,
                    "duration_seconds": None if duration is None else round(duration, 1),
                    "open_ended": end is None,
                })

            occupancy_series = self._occupancy_series_locked(frm, to)
            flows, suppressed = self._overflow_flows_locked(frm, to, k)

            return {
                "window": {"from": frm, "to": to},
                "privacy": {"k": k, "model": "aggregates-only; events carry no viewer identity"},
                "handling_speed": {
                    "pause": _summarize_durations(pause_durations),
                    "halt": _summarize_durations(halt_durations),
                    "segments": halt_rows,
                },
                "occupancy_series": occupancy_series,
                "overflow_flows": flows,
                "suppressed_flow_count": suppressed,
            }

    def _pause_durations_locked(self, frm: float, to: float) -> list[float]:
        durations = []
        for venue_id in self._venues:
            pause_start = None
            for event in self._venue_events_locked(venue_id):
                if event["type"] == "pause":
                    pause_start = event["client_ts"]
                elif event["type"] == "resume" and pause_start is not None:
                    if pause_start <= to:
                        durations.append(max(0.0, min(event["client_ts"], to) - max(pause_start, frm)))
                    pause_start = None
            if pause_start is not None and pause_start <= to:
                durations.append(max(0.0, to - max(pause_start, frm)))
        return [d for d in durations if d > 0]

    def _occupancy_series_locked(self, frm: float, to: float, bucket: int = 300) -> dict:
        series = {}
        for venue_id, venue in self._venues.items():
            events = self._venue_events_locked(venue_id)
            points = []
            cursor = frm
            idx = 0
            current = 0
            timeline = [e for e in events if e["client_ts"] <= to]
            while cursor <= to:
                while idx < len(timeline) and timeline[idx]["client_ts"] <= cursor:
                    e = timeline[idx]
                    if e["type"] == "entry":
                        current += e["count"]
                    elif e["type"] == "exit":
                        current = max(0, current - e["count"])
                    elif e["type"] == "headcount":
                        current = e["value"]
                    idx += 1
                points.append({"ts": cursor, "occupancy": current,
                               "ratio": round(current / venue.capacity, 3)})
                cursor += bucket
            series[venue_id] = points
        return series

    def _blocked_intervals_locked(self, venue_id: str, frm: float, to: float) -> list[tuple[float, float]]:
        """合并满员、暂停、停推三种受压来源，输出连续受压区间。"""
        venue = self._venues[venue_id]
        cuts = {frm, to}
        for event in self._venue_events_locked(venue_id):
            # 进出与盘点决定满员临界，暂停/恢复决定资格，都要作为切分点。
            if frm <= event["client_ts"] <= to:
                cuts.add(event["client_ts"])
        for halt in self._halts:
            if halt.scope == "event" or halt.venue_id == venue_id:
                if halt.issued_at <= to:
                    cuts.add(max(frm, halt.issued_at))
                if halt.resolved_at is not None and frm <= halt.resolved_at <= to:
                    cuts.add(halt.resolved_at)
        cuts = sorted(cuts)
        intervals = []
        current = 0
        for i, start in enumerate(cuts[:-1]):
            end = cuts[i + 1]
            mid = (start + end) / 2
            state = self._replay_locked(venue_id, mid)
            halts = self._active_halts_locked(mid, venue_id)
            blocked = state.current >= venue.capacity or state.paused or bool(halts)
            if blocked:
                if intervals and intervals[-1][1] == start:
                    intervals[-1] = (intervals[-1][0], end)
                else:
                    intervals.append((start, end))
        return intervals

    def _net_change_locked(self, venue_id: str, frm: float, to: float) -> int:
        """窗口内净增人数：优先用 headcount 差值，否则用进出场累计。"""
        events = self._venue_events_locked(venue_id)
        headcounts = [e for e in events if e["type"] == "headcount" and frm - 1 <= e["client_ts"] <= to + 1]
        if len(headcounts) >= 2:
            return max(0, headcounts[-1]["value"] - headcounts[0]["value"])
        net = 0
        for e in events:
            if frm <= e["client_ts"] <= to:
                if e["type"] == "entry":
                    net += e["count"]
                elif e["type"] == "exit":
                    net -= e["count"]
        return max(0, net)

    def _overflow_flows_locked(self, frm: float, to: float, k: int) -> tuple[list[dict], int]:
        raw: dict[tuple[str, str], int] = {}
        for source_id, source in self._venues.items():
            # 多次受压的承接窗口可能重叠，先合并，避免同一批增量被算两次。
            windows = []
            for start, _end in self._blocked_intervals_locked(source_id, frm, to):
                win = (start, min(to, start + OVERFLOW_WINDOW_SECONDS))
                if windows and start <= windows[-1][1]:
                    windows[-1] = (windows[-1][0], max(windows[-1][1], win[1]))
                else:
                    windows.append(win)
            for win_start, win_end in windows:
                for target_id, target in self._venues.items():
                    if target_id == source_id:
                        continue
                    gained = self._net_change_locked(target_id, win_start, win_end)
                    if gained > 0:
                        key = (source.zone, target.zone, target_id)
                        raw[key] = raw.get(key, 0) + gained
        flows = []
        suppressed = 0
        for (source_zone, target_zone, target_id), count in sorted(raw.items()):
            if count >= k:
                flows.append({"source_zone": source_zone, "target_zone": target_zone,
                              "target_venue": target_id, "count": count})
            else:
                suppressed += 1  # 小样本整条抑制，不暴露端点也不报零头总量
        return flows, suppressed


def _summarize_durations(values: list[float]) -> dict:
    if not values:
        return {"count": 0}
    values = sorted(values)
    summary = {
        "count": len(values),
        "min_seconds": round(values[0], 1),
        "median_seconds": round(statistics.median(values), 1),
        "max_seconds": round(values[-1], 1),
        "mean_seconds": round(statistics.fmean(values), 1),
    }
    if len(values) >= 2:
        summary["p90_seconds"] = round(statistics.quantiles(values, n=10)[8], 1)
    return summary
