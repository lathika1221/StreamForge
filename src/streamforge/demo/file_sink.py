"""A file-backed stand-in for the Snowflake RAW sink, for the local demo only.

Same contract as ``snowflake_sink.RawEventSink.write_batch``: one append-only JSON Lines
file per event type (``raw_<event_type>.jsonl``), idempotent on the Kafka coordinates
(topic, partition, offset) like the RAW ``MERGE``, and one audit line per batch in
``ingestion_batches.jsonl`` (like ``OPS.INGESTION_BATCHES``). Message bytes are stored
unchanged (``raw_value``). Existing files are read at start-up, so re-running into the
same directory skips rows already written.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from streamforge.consumer.records import ValidRecord
from streamforge.consumer.snowflake_sink import BatchInfo, SinkResult

AUDIT_FILE = "ingestion_batches.jsonl"

Coordinates = tuple[str, int, int]


class FileRawSink:
    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._seen: set[Coordinates] = set()
        for path in self.directory.glob("raw_*.jsonl"):
            with path.open(encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        row = json.loads(line)
                        self._seen.add((row["topic"], row["partition"], row["offset"]))

    def raw_file(self, event_type: str) -> Path:
        return self.directory / f"raw_{event_type}.jsonl"

    def write_batch(self, records: Sequence[ValidRecord], batch: BatchInfo) -> SinkResult:
        new: dict[str, list[str]] = {}
        coords_added: list[Coordinates] = []
        duplicates = 0
        for v in records:
            r = v.record
            coords = (r.topic, r.partition, r.offset)
            if coords in self._seen or coords in coords_added:
                duplicates += 1
                continue
            coords_added.append(coords)
            row = {
                "topic": r.topic,
                "partition": r.partition,
                "offset": r.offset,
                "event_id": str(v.event.event_id),
                "event_type": v.event.event_type,
                "order_id": v.event.order_id,
                "sequence_number": v.event.sequence_number,
                "raw_value": (r.value or b"").decode("utf-8"),
                "batch_id": batch.batch_id,
            }
            new.setdefault(v.event.event_type, []).append(json.dumps(row, sort_keys=True))
        for event_type, lines in new.items():
            with self.raw_file(event_type).open("a", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(lines) + "\n")
        audit = {
            **dataclasses.asdict(batch),
            "valid_records": len(records),
            "rows_inserted": len(coords_added),
            "duplicates_skipped": duplicates,
            "loaded_at": datetime.now(UTC).isoformat(),
        }
        with (self.directory / AUDIT_FILE).open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(audit, sort_keys=True) + "\n")
        self._seen.update(coords_added)
        return SinkResult(len(coords_added), duplicates)

    def rows(self) -> dict[str, int]:
        """Rows stored per event type."""
        out = {}
        for path in sorted(self.directory.glob("raw_*.jsonl")):
            with path.open(encoding="utf-8") as f:
                out[path.stem.removeprefix("raw_")] = sum(1 for line in f if line.strip())
        return out
