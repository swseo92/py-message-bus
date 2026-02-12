"""Integration tests for RecordingBus + JsonLineStore.

End-to-end tests combining RecordingBus middleware with JsonLineStore backend.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from message_bus import AsyncLocalMessageBus, LocalMessageBus
from message_bus.ports import Command, Event, Query
from message_bus.recording import AsyncRecordingBus, JsonLineStore, RecordingBus


# Test message types
@dataclass(frozen=True)
class SampleQuery(Query[str]):
    """Sample query message for testing."""

    value: int


@dataclass(frozen=True)
class SampleCommand(Command):
    """Sample command message for testing."""

    action: str


@dataclass(frozen=True)
class SampleEvent(Event):
    """Sample event message for testing."""

    data: str


# Helper function
def _read_jsonl(directory: Path) -> list[dict]:
    """Read all records from the JSONL run file in directory."""
    files = list(directory.glob("run_*.jsonl"))
    assert len(files) == 1, f"Expected 1 run file, got {len(files)}"
    lines = files[0].read_text().strip().splitlines()
    return [json.loads(line) for line in lines]


# Sync RecordingBus + JsonLineStore tests


def test_recording_bus_writes_query_to_jsonl(tmp_path: Path) -> None:
    """Send a query through RecordingBus with JsonLineStore, verify JSONL output."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")

    result = bus.send(SampleQuery(value=42))

    assert result == "result-42"

    bus.close()
    time.sleep(0.1)  # Give writer thread time to finish

    # Read JSONL file
    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "query"
    assert record["class"] == "SampleQuery"
    assert record["payload"] == {"value": 42}
    assert record["result"] == "result-42"
    assert record["error"] is None
    assert record["duration_ns"] > 0


def test_recording_bus_writes_command_to_jsonl(tmp_path: Path) -> None:
    """Execute a command, verify JSONL output."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = RecordingBus(inner, store)

    executed = []
    bus.register_command(SampleCommand, lambda c: executed.append(c.action))

    bus.execute(SampleCommand(action="test-action"))

    assert executed == ["test-action"]

    bus.close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "command"
    assert record["class"] == "SampleCommand"
    assert record["payload"] == {"action": "test-action"}
    assert record["result"] is None
    assert record["error"] is None
    assert record["duration_ns"] > 0


def test_recording_bus_writes_event_to_jsonl(tmp_path: Path) -> None:
    """Publish an event, verify JSONL output."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = RecordingBus(inner, store)

    received = []
    bus.subscribe(SampleEvent, lambda e: received.append(e.data))

    bus.publish(SampleEvent(data="event-data"))

    assert received == ["event-data"]

    bus.close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "event"
    assert record["class"] == "SampleEvent"
    assert record["payload"] == {"data": "event-data"}
    assert record["result"] is None
    assert record["error"] is None
    assert record["duration_ns"] > 0


def test_recording_bus_writes_error_to_jsonl(tmp_path: Path) -> None:
    """Handler raises exception, verify error field in JSONL."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = RecordingBus(inner, store)

    def failing_handler(q: Query[str]) -> str:
        raise ValueError("Test error message")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(ValueError, match="Test error message"):
        bus.send(SampleQuery(value=1))

    bus.close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "query"
    assert record["class"] == "SampleQuery"
    assert record["error"] is not None
    assert "ValueError" in record["error"]
    assert "Test error message" in record["error"]
    assert record["result"] is None


def test_recording_bus_errors_mode_with_jsonline_store(tmp_path: Path) -> None:
    """record_mode='errors': successful query not recorded, failing command recorded."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = RecordingBus(inner, store, record_mode="errors")

    # Successful query - should NOT be recorded
    bus.register_query(SampleQuery, lambda q: "success")
    bus.send(SampleQuery(value=1))

    # Failing command - SHOULD be recorded
    def failing_handler(c: Command) -> None:
        raise RuntimeError("Command failure")

    bus.register_command(SampleCommand, failing_handler)

    with pytest.raises(RuntimeError, match="Command failure"):
        bus.execute(SampleCommand(action="fail"))

    bus.close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    # Only the failed command should be recorded
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "command"
    assert record["class"] == "SampleCommand"
    assert record["error"] is not None
    assert "RuntimeError" in record["error"]
    assert "Command failure" in record["error"]


def test_recording_bus_multiple_dispatches_to_jsonl(tmp_path: Path) -> None:
    """Multiple dispatches, verify all records written in order."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")
    bus.register_command(SampleCommand, lambda c: None)
    received = []
    bus.subscribe(SampleEvent, lambda e: received.append(e.data))

    # Dispatch multiple messages
    bus.send(SampleQuery(value=1))
    bus.execute(SampleCommand(action="action-1"))
    bus.send(SampleQuery(value=2))
    bus.publish(SampleEvent(data="event-1"))
    bus.execute(SampleCommand(action="action-2"))

    bus.close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 5

    # Verify order and content
    assert records[0]["type"] == "query"
    assert records[0]["class"] == "SampleQuery"
    assert records[0]["payload"] == {"value": 1}

    assert records[1]["type"] == "command"
    assert records[1]["class"] == "SampleCommand"
    assert records[1]["payload"] == {"action": "action-1"}

    assert records[2]["type"] == "query"
    assert records[2]["class"] == "SampleQuery"
    assert records[2]["payload"] == {"value": 2}

    assert records[3]["type"] == "event"
    assert records[3]["class"] == "SampleEvent"
    assert records[3]["payload"] == {"data": "event-1"}

    assert records[4]["type"] == "command"
    assert records[4]["class"] == "SampleCommand"
    assert records[4]["payload"] == {"action": "action-2"}


def test_recording_bus_context_manager_flushes_jsonl(tmp_path: Path) -> None:
    """Use `with RecordingBus(...)` context manager, verify file is flushed after exit."""
    inner = LocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)

    with RecordingBus(inner, store) as bus:
        bus.register_query(SampleQuery, lambda q: f"result-{q.value}")
        bus.send(SampleQuery(value=999))

    # Context manager should have called close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "query"
    assert record["class"] == "SampleQuery"
    assert record["payload"] == {"value": 999}
    assert record["result"] == "result-999"


# Async RecordingBus + JsonLineStore tests


async def test_async_recording_bus_writes_query_to_jsonl(tmp_path: Path) -> None:
    """Async: Send a query through AsyncRecordingBus with JsonLineStore, verify JSONL."""
    inner = AsyncLocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)
    bus = AsyncRecordingBus(inner, store)

    async def handler(q: Query[str]) -> str:
        return f"async-result-{q.value}"

    bus.register_query(SampleQuery, handler)

    result = await bus.send(SampleQuery(value=123))

    assert result == "async-result-123"

    bus.close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "query"
    assert record["class"] == "SampleQuery"
    assert record["payload"] == {"value": 123}
    assert record["result"] == "async-result-123"
    assert record["error"] is None
    assert record["duration_ns"] > 0


async def test_async_recording_bus_context_manager_flushes_jsonl(tmp_path: Path) -> None:
    """Async: `async with` context manager, verify file is flushed after exit."""
    inner = AsyncLocalMessageBus()
    store = JsonLineStore(tmp_path, max_runs=10)

    async with AsyncRecordingBus(inner, store) as bus:

        async def handler(q: Query[str]) -> str:
            return f"async-result-{q.value}"

        bus.register_query(SampleQuery, handler)
        await bus.send(SampleQuery(value=456))

    # Context manager should have called close()
    time.sleep(0.1)

    records = _read_jsonl(tmp_path)
    assert len(records) == 1

    record = records[0]
    assert record["type"] == "query"
    assert record["class"] == "SampleQuery"
    assert record["payload"] == {"value": 456}
    assert record["result"] == "async-result-456"
