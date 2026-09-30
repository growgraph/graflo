"""Migration history persistence."""

from __future__ import annotations

import json
from pathlib import Path

from graflo.migrate.models import MigrationRecord


class FileMigrationStore:
    """File-backed migration history store."""

    def __init__(self, path: str | Path = ".graflo/migrations.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({"records": []})

    def history(self) -> list[MigrationRecord]:
        payload = self._read()
        records = payload.get("records", [])
        return [MigrationRecord.model_validate(item) for item in records]

    def has_revision(self, revision: str, backend: str) -> bool:
        return any(
            record.revision == revision and record.backend == backend
            for record in self.history()
        )

    def get_revision(self, revision: str, backend: str) -> MigrationRecord | None:
        for record in self.history():
            if record.revision == revision and record.backend == backend:
                return record
        return None

    def has_schema_hash(self, schema_hash: str, backend: str) -> bool:
        return any(
            record.schema_hash == schema_hash and record.backend == backend
            for record in self.history()
        )

    def add_record(self, record: MigrationRecord) -> None:
        payload = self._read()
        records = payload.get("records", [])
        records.append(record.model_dump(mode="json"))
        payload["records"] = records
        self._write(payload)

    def latest(self, backend: str | None = None) -> MigrationRecord | None:
        records = self.history()
        if backend is not None:
            records = [record for record in records if record.backend == backend]
        if not records:
            return None
        return records[-1]

    def _read(self) -> dict:
        if not self.path.exists():
            return {"records": []}
        raw = self.path.read_text(encoding="utf-8")
        if not raw.strip():
            return {"records": []}
        return json.loads(raw)

    def _write(self, payload: dict) -> None:
        self.path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
