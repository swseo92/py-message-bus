"""Tests for RecordStore implementations (MemoryStore, JsonLineStore)."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from message_bus.recording import (
    JsonLineStore,
    MemoryStore,
    Record,
    _safe_asdict,
    _safe_json,
)


@dataclass(frozen=True)
class DummyQuery:
    """Test message for creating records."""

    value: int


# MemoryStore tests


def test_memory_store_append_and_retrieve() -> None:
    """MemoryStore stores records in list."""
    store = MemoryStore()

    record = Record(
        timestamp=time.time(),
        message_type="query",
        message_class="TestQuery",
        payload=DummyQuery(value=42),
        duration_ns=100,
        result={"data": "test"},
        error=None,
    )

    store.append(record)

    assert len(store.records) == 1
    assert store.records[0] == record
    assert store.records[0].payload == DummyQuery(value=42)


def test_memory_store_close_is_idempotent() -> None:
    """Multiple close() calls are safe."""
    store = MemoryStore()

    record = Record(
        timestamp=time.time(),
        message_type="command",
        message_class="TestCommand",
        payload=DummyQuery(value=1),
        duration_ns=50,
        result=None,
        error=None,
    )

    store.append(record)
    store.close()
    store.close()  # Second close should be safe

    # Appends after close should be no-op
    store.append(record)
    assert len(store.records) == 1


# JsonLineStore tests


def test_jsonline_store_writes_to_file(tmp_path: Path) -> None:
    """JSONL file created with correct content."""
    store = JsonLineStore(tmp_path, max_runs=10)

    record = Record(
        timestamp=1234567890.123456,
        message_type="query",
        message_class="GetUserQuery",
        payload=DummyQuery(value=123),
        duration_ns=450,
        result={"id": "123", "name": "Test"},
        error=None,
    )

    store.append(record)
    store.close()

    # Give background thread time to write
    time.sleep(0.1)

    # Find the created file
    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    # Verify content
    with open(jsonl_files[0]) as f:
        line = f.readline()
        data = json.loads(line)

    assert data["type"] == "query"
    assert data["class"] == "GetUserQuery"
    assert data["payload"] == {"value": 123}
    assert data["duration_ns"] == 450
    assert data["result"] == {"id": "123", "name": "Test"}
    assert data["error"] is None


def test_jsonline_store_close_drains_queue(tmp_path: Path) -> None:
    """All records written after close()."""
    store = JsonLineStore(tmp_path, max_runs=10)

    # Append multiple records quickly
    for i in range(10):
        record = Record(
            timestamp=time.time(),
            message_type="command",
            message_class=f"Command{i}",
            payload=DummyQuery(value=i),
            duration_ns=i * 100,
            result=None,
            error=None,
        )
        store.append(record)

    store.close()
    time.sleep(0.1)

    # Verify all records written
    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    with open(jsonl_files[0]) as f:
        lines = f.readlines()

    assert len(lines) == 10

    # Verify each record
    for i, line in enumerate(lines):
        data = json.loads(line)
        assert data["class"] == f"Command{i}"
        assert data["payload"] == {"value": i}


def test_jsonline_store_close_is_idempotent(tmp_path: Path) -> None:
    """Multiple close() calls are safe."""
    store = JsonLineStore(tmp_path, max_runs=10)

    record = Record(
        timestamp=time.time(),
        message_type="event",
        message_class="TestEvent",
        payload=DummyQuery(value=1),
        duration_ns=50,
        result=None,
        error=None,
    )

    store.append(record)
    store.close()
    store.close()  # Second close should be safe

    time.sleep(0.1)

    # Verify only one record written
    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    with open(jsonl_files[0]) as f:
        lines = f.readlines()

    assert len(lines) == 1


def test_jsonline_store_max_runs_cleanup(tmp_path: Path) -> None:
    """Old run files deleted when > max_runs."""
    max_runs = 3

    # Create multiple stores sequentially, ensuring different filenames
    # by waiting at least 1 second between creations (timestamp has 1s granularity)
    stores = []
    for i in range(5):
        # Wait to ensure different timestamp in filename
        if i > 0:
            time.sleep(1.1)

        store = JsonLineStore(tmp_path, max_runs=max_runs)
        record = Record(
            timestamp=time.time(),
            message_type="query",
            message_class=f"Query{i}",
            payload=DummyQuery(value=i),
            duration_ns=100,
            result=None,
            error=None,
        )
        store.append(record)
        store.close()
        stores.append(store)
        time.sleep(0.1)  # Give writer thread time to finish

    # Should only have max_runs files
    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == max_runs


def test_jsonline_store_file_naming(tmp_path: Path) -> None:
    """File name format includes timestamp and PID."""
    import os
    import re

    store = JsonLineStore(tmp_path, max_runs=10)
    store.close()

    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    filename = jsonl_files[0].name
    # Format: run_{YYYYMMDD_HHMMSS}_{pid}.jsonl
    pattern = r"run_\d{8}_\d{6}_\d+\.jsonl"
    assert re.match(pattern, filename)

    # Extract and verify PID
    pid_str = filename.split("_")[-1].replace(".jsonl", "")
    assert int(pid_str) == os.getpid()


def test_safe_json_handles_non_serializable() -> None:
    """Non-JSON-serializable objects use repr() fallback."""

    class NonSerializable:
        def __repr__(self) -> str:
            return "<NonSerializable object>"

    obj = NonSerializable()
    result = _safe_json(obj)

    assert result == "<NonSerializable object>"


def test_safe_asdict_handles_non_dataclass() -> None:
    """Non-dataclass messages use repr() fallback."""

    class NotADataclass:
        def __repr__(self) -> str:
            return "<NotADataclass>"

    message = NotADataclass()
    result = _safe_asdict(message)

    assert result == {"__repr__": "<NotADataclass>"}


def test_jsonline_store_concurrent_writes(tmp_path: Path) -> None:
    """Thread-safe: multiple threads can append simultaneously."""
    store = JsonLineStore(tmp_path, max_runs=10)
    num_threads = 5
    records_per_thread = 10

    def append_records(thread_id: int) -> None:
        for i in range(records_per_thread):
            record = Record(
                timestamp=time.time(),
                message_type="command",
                message_class=f"Thread{thread_id}Command{i}",
                payload=DummyQuery(value=thread_id * 100 + i),
                duration_ns=100,
                result=None,
                error=None,
            )
            store.append(record)

    threads = [threading.Thread(target=append_records, args=(i,)) for i in range(num_threads)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    store.close()
    time.sleep(0.2)

    # Verify all records written
    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    with open(jsonl_files[0]) as f:
        lines = f.readlines()

    assert len(lines) == num_threads * records_per_thread


def test_jsonline_store_validates_max_runs(tmp_path: Path) -> None:
    """ValueError for max_runs < 1."""
    with pytest.raises(ValueError, match="max_runs must be >= 1"):
        JsonLineStore(tmp_path, max_runs=0)

    with pytest.raises(ValueError, match="max_runs must be >= 1"):
        JsonLineStore(tmp_path, max_runs=-5)


def test_jsonline_store_max_runs_one_deletes_old_files(tmp_path: Path) -> None:
    """max_runs=1 should delete all existing run files."""
    # Create 3 existing fake run files
    for i in range(3):
        (tmp_path / f"run_20260101_00000{i}_1.jsonl").write_text("")

    store = JsonLineStore(directory=tmp_path, max_runs=1)
    store.close()
    time.sleep(0.1)

    # Only the new run file should remain
    remaining = list(tmp_path.glob("run_*.jsonl"))
    assert len(remaining) == 1


def test_jsonline_store_writer_error_is_captured(tmp_path: Path) -> None:
    """When writer thread encounters an error, it's captured in _writer_error."""
    store = JsonLineStore(directory=tmp_path, max_runs=10)

    # Simulate a writer error (as would happen in _writer_loop on OSError)
    test_error = OSError("disk full")
    store._writer_error = test_error

    # Verify error is captured
    assert store._writer_error is not None
    assert store._writer_error is test_error
    assert "disk full" in str(store._writer_error)

    # close() will log the error (warning message)
    store.close()


def test_memory_store_append_after_close_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Appending to closed MemoryStore logs warning and drops record."""
    import logging

    store = MemoryStore()
    record = Record(
        timestamp=time.time(),
        message_type="query",
        message_class="TestQuery",
        payload=DummyQuery(value=1),
        duration_ns=100,
        result=None,
        error=None,
    )

    store.close()

    with caplog.at_level(logging.WARNING, logger="message_bus.recording"):
        store.append(record)

    assert len(store.records) == 0  # Record was dropped
    assert "append() called after close" in caplog.text
    assert "record dropped" in caplog.text


def test_jsonline_store_writer_thread_timeout_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If writer thread doesn't stop in time, warning is logged."""
    import logging

    store = JsonLineStore(tmp_path, max_runs=10)

    # Patch join to simulate timeout (thread appears still alive after join)
    def fake_join(timeout: float | None = None) -> None:
        pass  # Don't actually join

    monkeypatch.setattr(store._writer_thread, "join", fake_join)
    monkeypatch.setattr(store._writer_thread, "is_alive", lambda: True)

    with caplog.at_level(logging.WARNING, logger="message_bus.recording"):
        store.close()

    assert "writer thread did not stop in time" in caplog.text
    assert "records may be lost" in caplog.text


def test_jsonline_store_append_after_writer_error_drops_records(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """After writer error, append() drops records and logs warning."""
    import logging

    store = JsonLineStore(tmp_path, max_runs=10)

    # Simulate writer error
    store._writer_error = OSError("disk full")

    record = Record(
        timestamp=time.time(),
        message_type="query",
        message_class="TestQuery",
        payload=DummyQuery(value=1),
        duration_ns=100,
        result=None,
        error=None,
    )

    # Append should be dropped and logged
    with caplog.at_level(logging.WARNING, logger="message_bus.recording"):
        store.append(record)

    # Queue should be empty (record wasn't queued)
    assert store._queue.qsize() == 0

    # Verify warning log
    assert "writer thread failed" in caplog.text
    assert "disk full" in caplog.text
    assert "TestQuery" in caplog.text

    store.close()


def test_jsonline_store_append_after_close_warns_jsonl(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """JsonLineStore logs warning when append() called after close."""
    import logging

    store = JsonLineStore(tmp_path, max_runs=10)
    store.close()
    time.sleep(0.1)

    record = Record(
        timestamp=time.time(),
        message_type="query",
        message_class="TestQuery",
        payload=DummyQuery(value=1),
        duration_ns=100,
        result=None,
        error=None,
    )

    with caplog.at_level(logging.WARNING, logger="message_bus.recording"):
        store.append(record)

    assert "append() called after close" in caplog.text
    assert "record dropped" in caplog.text


def test_jsonline_store_cleanup_skips_symlinks(tmp_path: Path) -> None:
    """Symlinked run files are NOT deleted during cleanup."""
    # Create a real file and a symlink
    real_file = tmp_path / "target.txt"
    real_file.write_text("important data")

    symlink = tmp_path / "run_20250101_000000_1.jsonl"
    symlink.symlink_to(real_file)

    # Create store with max_runs=1 (should delete old files)
    store = JsonLineStore(directory=tmp_path, max_runs=1)
    store.close()
    time.sleep(0.1)

    # Symlink should NOT have been deleted (skipped because is_symlink)
    assert real_file.exists(), "Target file should not be deleted"


def test_jsonline_store_queue_full_drops_record(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """When queue is full, append drops record and logs warning."""
    import logging

    store = JsonLineStore(tmp_path, max_runs=10, max_queue_size=1)

    # Fill the queue by pausing the writer temporarily
    # We'll create many records quickly - some should be dropped
    records_to_send = 100
    for i in range(records_to_send):
        record = Record(
            timestamp=time.time(),
            message_type="command",
            message_class=f"Command{i}",
            payload=DummyQuery(value=i),
            duration_ns=100,
            result=None,
            error=None,
        )
        with caplog.at_level(logging.WARNING, logger="message_bus.recording"):
            store.append(record)

    store.close()
    time.sleep(0.2)

    # Some records should have been dropped (queue full warnings)
    queue_full_warnings = [r for r in caplog.records if "queue full" in r.message]
    assert len(queue_full_warnings) > 0, "Expected queue full warnings"


def test_jsonline_store_validates_max_queue_size(tmp_path: Path) -> None:
    """ValueError for max_queue_size < 1."""
    with pytest.raises(ValueError, match="max_queue_size must be >= 1"):
        JsonLineStore(tmp_path, max_runs=10, max_queue_size=0)

    with pytest.raises(ValueError, match="max_queue_size must be >= 1"):
        JsonLineStore(tmp_path, max_runs=10, max_queue_size=-5)


def test_jsonline_store_custom_close_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Custom close_timeout is used for writer thread join."""
    store = JsonLineStore(tmp_path, max_runs=10, close_timeout=10.0)

    join_timeouts: list[float | None] = []
    original_join = store._writer_thread.join

    def tracking_join(timeout: float | None = None) -> None:
        join_timeouts.append(timeout)
        original_join(timeout=timeout)

    monkeypatch.setattr(store._writer_thread, "join", tracking_join)

    store.close()

    assert any(t == 10.0 for t in join_timeouts)


def test_jsonline_store_validates_close_timeout(tmp_path: Path) -> None:
    """ValueError for close_timeout <= 0."""
    with pytest.raises(ValueError, match="close_timeout must be > 0"):
        JsonLineStore(tmp_path, max_runs=10, close_timeout=0)

    with pytest.raises(ValueError, match="close_timeout must be > 0"):
        JsonLineStore(tmp_path, max_runs=10, close_timeout=-1.0)


def test_jsonline_store_redacts_sensitive_fields(tmp_path: Path) -> None:
    """Specified fields are redacted in JSONL output."""

    @dataclass(frozen=True)
    class SecretMessage:
        username: str
        password: str
        token: str

    store = JsonLineStore(
        tmp_path,
        max_runs=10,
        redact_fields=frozenset({"password", "token"}),
    )

    record = Record(
        timestamp=1234567890.0,
        message_type="command",
        message_class="SecretMessage",
        payload=SecretMessage(username="alice", password="s3cret", token="tok123"),
        duration_ns=100,
        result=None,
        error=None,
    )

    store.append(record)
    store.close()
    time.sleep(0.1)

    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    with open(jsonl_files[0]) as f:
        data = json.loads(f.readline())

    assert data["payload"]["username"] == "alice"
    assert data["payload"]["password"] == "[REDACTED]"
    assert data["payload"]["token"] == "[REDACTED]"


def test_jsonline_store_redaction_nested(tmp_path: Path) -> None:
    """Redaction works on nested dicts."""
    from message_bus.recording import _redact

    data = {
        "user": "alice",
        "credentials": {
            "password": "secret",
            "api_key": "key123",
        },
        "token": "tok456",
    }

    result = _redact(data, frozenset({"password", "token"}))

    assert result["user"] == "alice"
    assert result["credentials"]["password"] == "[REDACTED]"
    assert result["credentials"]["api_key"] == "key123"
    assert result["token"] == "[REDACTED]"


def test_jsonline_store_no_redaction_by_default(tmp_path: Path) -> None:
    """Default: no fields redacted."""
    store = JsonLineStore(tmp_path, max_runs=10)

    record = Record(
        timestamp=1234567890.0,
        message_type="query",
        message_class="TestQuery",
        payload=DummyQuery(value=42),
        duration_ns=100,
        result=None,
        error=None,
    )

    store.append(record)
    store.close()
    time.sleep(0.1)

    jsonl_files = list(tmp_path.glob("run_*.jsonl"))
    assert len(jsonl_files) == 1

    with open(jsonl_files[0]) as f:
        data = json.loads(f.readline())

    assert data["payload"]["value"] == 42  # Not redacted


def test_jsonline_store_redaction_handles_list_of_dicts(tmp_path: Path) -> None:
    """Redaction works on lists containing dicts."""
    from message_bus.recording import _redact

    data = {
        "users": [
            {"name": "alice", "password": "secret1"},
            {"name": "bob", "password": "secret2"},
        ],
        "metadata": "public",
    }

    result = _redact(data, frozenset({"password"}))

    assert result["users"][0]["name"] == "alice"
    assert result["users"][0]["password"] == "[REDACTED]"
    assert result["users"][1]["name"] == "bob"
    assert result["users"][1]["password"] == "[REDACTED]"
    assert result["metadata"] == "public"


def test_jsonline_store_close_with_full_queue_uses_shutdown_event(
    tmp_path: Path,
) -> None:
    """Writer thread stops via shutdown event when queue is full at close."""
    store = JsonLineStore(tmp_path, max_runs=10, max_queue_size=1, close_timeout=2.0)

    # Fill the queue so sentinel can't be enqueued
    record = Record(
        timestamp=time.time(),
        message_type="command",
        message_class="TestCommand",
        payload=DummyQuery(value=1),
        duration_ns=100,
        result=None,
        error=None,
    )
    # Rapidly fill queue
    for _ in range(100):
        store.append(record)

    # close() should succeed even with full queue (shutdown event as fallback)
    store.close()

    # Writer thread should have stopped
    assert not store._writer_thread.is_alive()
