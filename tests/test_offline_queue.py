"""离线队列测试：断网照记、联网补传、重复不双计、失败保留。"""

import os
import tempfile
import unittest

from offline_queue import OfflineQueue


class OfflineQueueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "queue.jsonl")
        self.queue = OfflineQueue(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def _event(self, eid):
        return {"event_id": eid, "venue_id": "mall", "type": "entry",
                "client_ts": 1000.0, "count": 1}

    def test_append_and_flush_dedupes_via_server(self):
        self.queue.append(self._event("e1"))
        self.queue.append(self._event("e2"))
        seen = []

        def sender(event):
            seen.append(event["event_id"])
            # 服务端判定 e1 此前已入账（例如重连后的重复补传）
            return {"event_id": event["event_id"], "duplicate": event["event_id"] == "e1"}

        result = self.queue.flush(sender)
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["duplicate"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(self.queue.pending(), [])
        self.assertEqual(seen, ["e1", "e2"])

    def test_failure_keeps_remaining_for_retry(self):
        self.queue.append(self._event("e1"))
        self.queue.append(self._event("e2"))
        calls = {"n": 0}

        def flaky_sender(event):
            calls["n"] += 1
            raise ConnectionError("仍在断网")

        result = self.queue.flush(flaky_sender)
        self.assertEqual(result["failed"], 1)
        pending = self.queue.pending()
        self.assertEqual([e["event_id"] for e in pending], ["e1", "e2"])

        # 网络恢复后整批补传成功
        result = self.queue.flush(lambda e: {"event_id": e["event_id"], "duplicate": False})
        self.assertEqual(result["sent"], 2)
        self.assertEqual(self.queue.pending(), [])

    def test_partial_failure_preserves_suffix(self):
        self.queue.append(self._event("e1"))
        self.queue.append(self._event("e2"))
        self.queue.append(self._event("e3"))

        def sender(event):
            if event["event_id"] == "e2":
                raise ConnectionError("断了")
            return {"event_id": event["event_id"], "duplicate": False}

        result = self.queue.flush(sender)
        self.assertEqual(result["sent"], 1)
        self.assertEqual([e["event_id"] for e in self.queue.pending()], ["e2", "e3"])

    def test_repeated_restart_flush_is_idempotent(self):
        self.queue.append(self._event("e1"))
        # 模拟“已送达但应答丢失”：队列以为没发，服务端却已入账
        first = self.queue.flush(lambda e: {"event_id": "e1", "duplicate": False})
        self.queue.append(self._event("e1"))  # 现场端重连后再次写入同一条
        second = self.queue.flush(lambda e: {"event_id": "e1", "duplicate": True})
        self.assertEqual(second["duplicate"], 1)
        self.assertEqual(second["sent"], 0)
        self.assertEqual(self.queue.pending(), [])


if __name__ == "__main__":
    unittest.main()
