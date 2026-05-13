import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional


class JsonlLogStorage:
    """Append-only JSONL log storage with fsync durability.

    Directory layout:
        data/
          meta.json                              - topics/subscriptions registry
          topics/<name>/messages.jsonl           - append-only message log
          offsets/<subscriber_id>/<type>__<name>.json  - per-subscriber read offset
          dedup/producer.json                    - producer dedup cache {client_msg_id: server_msg_id}
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self._lock = threading.RLock()
        self._ensure_dirs()

    def _ensure_dirs(self):
        for d in ("topics", "offsets", "dedup"):
            (self.data_dir / d).mkdir(parents=True, exist_ok=True)

    def _channel_dir(self, channel_type: str, channel_name: str) -> Path:
        return self.data_dir / f"{channel_type}s" / channel_name

    # ── Message log ───────────────────────────────────────────────────────────

    def append(self, channel_type: str, channel_name: str, msg_dict: Dict) -> int:
        """Append message dict to JSONL log. Returns the offset (0-based line number)."""
        with self._lock:
            ch_dir = self._channel_dir(channel_type, channel_name)
            ch_dir.mkdir(parents=True, exist_ok=True)
            log_path = ch_dir / "messages.jsonl"

            offset = 0
            if log_path.exists():
                with open(log_path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            offset += 1

            msg_dict = dict(msg_dict)
            msg_dict["offset"] = offset

            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(msg_dict, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())

            return offset

    def read_from(
        self,
        channel_type: str,
        channel_name: str,
        from_offset: int,
        limit: int = 1000,
    ) -> List[Dict]:
        """Read messages starting from `from_offset` (inclusive)."""
        with self._lock:
            log_path = self._channel_dir(channel_type, channel_name) / "messages.jsonl"
            if not log_path.exists():
                return []
            results: List[Dict] = []
            with open(log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    msg = json.loads(line)
                    if msg.get("offset", 0) >= from_offset:
                        results.append(msg)
                    if len(results) >= limit:
                        break
            return results

    def load_all_messages(self, channel_type: str, channel_name: str) -> List[Dict]:
        return self.read_from(channel_type, channel_name, 0)

    def channel_exists(self, channel_type: str, channel_name: str) -> bool:
        log_path = self._channel_dir(channel_type, channel_name) / "messages.jsonl"
        return log_path.exists()

    # ── Offsets ───────────────────────────────────────────────────────────────

    def save_offset(
        self, subscriber_id: str, channel_type: str, channel_name: str, offset: int
    ):
        with self._lock:
            off_dir = self.data_dir / "offsets" / subscriber_id
            off_dir.mkdir(parents=True, exist_ok=True)
            off_path = off_dir / f"{channel_type}__{channel_name}.json"
            with open(off_path, "w", encoding="utf-8") as f:
                json.dump({"offset": offset}, f)

    def load_offset(
        self, subscriber_id: str, channel_type: str, channel_name: str
    ) -> int:
        """Returns saved offset, or -1 if not found (means "start from beginning")."""
        off_path = (
            self.data_dir
            / "offsets"
            / subscriber_id
            / f"{channel_type}__{channel_name}.json"
        )
        if off_path.exists():
            with open(off_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("offset", -1)
        return -1

    def load_all_offsets(self) -> Dict[str, Dict[str, int]]:
        """Returns {subscriber_id: {"topic__name": offset, ...}}"""
        result: Dict[str, Dict[str, int]] = {}
        off_base = self.data_dir / "offsets"
        if not off_base.exists():
            return result
        for sub_dir in off_base.iterdir():
            if sub_dir.is_dir():
                sub_id = sub_dir.name
                result[sub_id] = {}
                for off_file in sub_dir.iterdir():
                    if off_file.suffix == ".json":
                        with open(off_file, encoding="utf-8") as f:
                            data = json.load(f)
                            result[sub_id][off_file.stem] = data.get("offset", -1)
        return result

    # ── Metadata ──────────────────────────────────────────────────────────────

    def save_meta(self, meta: Dict):
        with self._lock:
            meta_path = self.data_dir / "meta.json"
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)

    def load_meta(self) -> Dict:
        meta_path = self.data_dir / "meta.json"
        if meta_path.exists():
            with open(meta_path, encoding="utf-8") as f:
                return json.load(f)
        return {"topics": [], "subscriptions": []}

    # ── Producer dedup cache ──────────────────────────────────────────────────

    def save_dedup_cache(self, cache: Dict[str, str], max_size: int = 10000):
        with self._lock:
            dedup_path = self.data_dir / "dedup" / "producer.json"
            if len(cache) > max_size:
                keys = list(cache.keys())[-max_size:]
                cache = {k: cache[k] for k in keys}
            with open(dedup_path, "w", encoding="utf-8") as f:
                json.dump(cache, f)

    def load_dedup_cache(self) -> Dict[str, str]:
        dedup_path = self.data_dir / "dedup" / "producer.json"
        if dedup_path.exists():
            with open(dedup_path, encoding="utf-8") as f:
                return json.load(f)
        return {}
