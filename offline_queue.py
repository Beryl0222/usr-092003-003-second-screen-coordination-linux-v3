"""现场端离线事件队列。

短时断网时现场人员照常记录：事件先追加到本地 JSONL，联网后整批补传。
是否重复由服务端按 event_id 幂等判定，因此本队列在不确定是否送达时
宁可重发、绝不丢条，重发不会把人数算两次。
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from typing import Callable, Optional


class OfflineQueue:
    """append-only 的本地队列；flush 成功后压缩清理已送达条目。"""

    def __init__(self, path: str):
        self.path = path

    def append(self, event: dict) -> None:
        record = {"queued_ts": time.time(), "status": "pending", "event": event}
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def pending(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        events = []
        with open(self.path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record.get("status") == "pending":
                    events.append(record["event"])
        return events

    def flush(self, sender: Callable[[dict], dict]) -> dict:
        """逐条调用 sender 补传；任一失败则保留剩余条目待下次重试。

        返回 {sent, duplicate, failed, results}，服务端的 duplicate 标记
        说明这条此前已入账，同样视为补传成功并从队列清除。
        """
        pending = self.pending()
        results = []
        done_ids = []
        sent = duplicate = failed = 0
        for event in pending:
            try:
                result = sender(event)
            except Exception as exc:  # 网络仍不通：保留本条及后续，稍后整批重试
                failed += 1
                results.append({"event_id": event.get("event_id"), "ok": False, "error": str(exc)})
                break
            results.append({"event_id": result.get("event_id"), "ok": True, "duplicate": result.get("duplicate", False)})
            done_ids.append(event["event_id"])
            if result.get("duplicate"):
                duplicate += 1
            else:
                sent += 1
        if done_ids:
            self._remove(done_ids)
        return {"sent": sent, "duplicate": duplicate, "failed": failed,
                "remaining": len(pending) - len(done_ids), "results": results}

    def _remove(self, sent_ids: list[str]) -> None:
        done = set(sent_ids)
        kept = []
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as handle:
                for line in handle:
                    record = json.loads(line)
                    event = record.get("event") or {}
                    if event.get("event_id") in done and record.get("status") == "pending":
                        continue
                    kept.append(line if line.endswith("\n") else line + "\n")
        directory = os.path.dirname(os.path.abspath(self.path))
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".queue-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.writelines(kept)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            os.unlink(tmp)
            raise


def flush_queue(path: str, server_url: str, role: str, venue_id: Optional[str] = None) -> dict:
    """命令行补传入口：把队列里的事件经 /sync 批量送回服务。"""
    import urllib.request

    queue = OfflineQueue(path)
    events = queue.pending()
    if not events:
        return {"sent": 0, "duplicate": 0, "failed": 0, "remaining": 0, "results": []}
    body = json.dumps({"events": events}).encode("utf-8")
    request = urllib.request.Request(
        server_url.rstrip("/") + "/sync", data=body, method="POST",
        headers={"Content-Type": "application/json; charset=utf-8",
                 "X-Role": role, "X-Venue-Id": venue_id or ""},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.load(response)
    accepted = {row["event_id"] for row in payload["results"] if row.get("ok")}
    if accepted:
        queue._remove(list(accepted))
    return payload
