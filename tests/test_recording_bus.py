"""Tests for sync RecordingBus."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from message_bus import LocalMessageBus
from message_bus.ports import Command, Event, Query, Task
from message_bus.recording import MemoryStore, RecordingBus


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


@dataclass(frozen=True)
class SampleTask(Task):
    """Sample task message for testing."""

    task_id: int


# RecordingBus tests


def test_recording_bus_records_query() -> None:
    """Query dispatch creates Record with result."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")

    result = bus.send(SampleQuery(value=42))

    assert result == "result-42"
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "query"
    assert record.message_class == "SampleQuery"
    assert record.payload == SampleQuery(value=42)
    assert record.result == "result-42"
    assert record.error is None
    assert record.duration_ns > 0


def test_recording_bus_records_command() -> None:
    """Command dispatch creates Record."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    executed = []
    bus.register_command(SampleCommand, lambda c: executed.append(c.action))

    bus.execute(SampleCommand(action="test"))

    assert executed == ["test"]
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "command"
    assert record.message_class == "SampleCommand"
    assert record.payload == SampleCommand(action="test")
    assert record.result is None
    assert record.error is None
    assert record.duration_ns > 0


def test_recording_bus_records_event() -> None:
    """Event publish creates Record."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    received = []
    bus.subscribe(SampleEvent, lambda e: received.append(e.data))

    bus.publish(SampleEvent(data="event-data"))

    assert received == ["event-data"]
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "event"
    assert record.message_class == "SampleEvent"
    assert record.payload == SampleEvent(data="event-data")
    assert record.result is None
    assert record.error is None
    assert record.duration_ns > 0


def test_recording_bus_records_task() -> None:
    """Task dispatch creates Record."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    processed = []
    bus.register_task(SampleTask, lambda t: processed.append(t.task_id))

    bus.dispatch(SampleTask(task_id=123))

    assert processed == [123]
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "task"
    assert record.message_class == "SampleTask"
    assert record.payload == SampleTask(task_id=123)
    assert record.result is None
    assert record.error is None
    assert record.duration_ns > 0


def test_recording_bus_records_error() -> None:
    """Failed handler creates Record with error field."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    def failing_handler(q: Query[str]) -> str:
        raise ValueError("Test error")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(ValueError, match="Test error"):
        bus.send(SampleQuery(value=1))

    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "query"
    assert record.error is not None
    assert "ValueError" in record.error
    assert "Test error" in record.error
    assert record.result is None


def test_recording_bus_errors_mode_skips_success() -> None:
    """record_mode="errors" does not record successful dispatches."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store, record_mode="errors")

    bus.register_query(SampleQuery, lambda q: "success")
    bus.register_command(SampleCommand, lambda c: None)

    bus.send(SampleQuery(value=1))
    bus.execute(SampleCommand(action="test"))

    # No records for successful dispatches
    assert len(store.records) == 0


def test_recording_bus_errors_mode_records_failure() -> None:
    """record_mode="errors" records when handler fails."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store, record_mode="errors")

    def failing_handler(q: Query[str]) -> str:
        raise RuntimeError("Failure")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(RuntimeError, match="Failure"):
        bus.send(SampleQuery(value=1))

    # Error should be recorded
    assert len(store.records) == 1
    assert store.records[0].error is not None


def test_recording_bus_propagates_exceptions() -> None:
    """Inner bus exceptions pass through unchanged."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    class CustomError(Exception):
        pass

    def handler(q: Query[str]) -> str:
        raise CustomError("Original error")

    bus.register_query(SampleQuery, handler)

    with pytest.raises(CustomError, match="Original error"):
        bus.send(SampleQuery(value=1))


def test_recording_bus_query_result_captured() -> None:
    """Query result stored in Record.result."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: f"value={q.value}")

    bus.send(SampleQuery(value=999))

    assert store.records[0].result == "value=999"


def test_recording_bus_duration_is_positive() -> None:
    """duration_ns > 0."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: "result")

    bus.send(SampleQuery(value=1))

    assert store.records[0].duration_ns > 0


def test_recording_bus_close_delegates() -> None:
    """close() calls store.close()."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    assert not store._closed

    bus.close()

    assert store._closed


def test_recording_bus_close_is_idempotent() -> None:
    """Multiple close() calls are safe."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    bus.close()
    bus.close()  # Should not raise

    assert store._closed


def test_recording_bus_context_manager() -> None:
    """Context manager calls close on exit."""
    inner = LocalMessageBus()
    store = MemoryStore()

    with RecordingBus(inner, store) as bus:
        bus.register_query(SampleQuery, lambda q: "result")
        bus.send(SampleQuery(value=1))

    # Store should be closed
    assert store._closed
    assert len(store.records) == 1


def test_recording_bus_invalid_record_mode() -> None:
    """ValueError raised for invalid record_mode."""
    inner = LocalMessageBus()
    store = MemoryStore()

    with pytest.raises(ValueError, match="Invalid record_mode"):
        RecordingBus(inner, store, record_mode="invalid")


def test_recording_bus_payload_is_raw_object() -> None:
    """Record.payload stores message object (not dict)."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: "result")

    query = SampleQuery(value=42)
    bus.send(query)

    # Payload should be the same object instance
    assert store.records[0].payload is query
    assert isinstance(store.records[0].payload, SampleQuery)


def test_record_mode_errors_skips_successful_command() -> None:
    """record_mode='errors' should NOT record successful commands."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store, record_mode="errors")

    executed = []
    bus.register_command(SampleCommand, lambda c: executed.append(c.action))

    bus.execute(SampleCommand(action="test"))

    assert executed == ["test"]
    assert len(store.records) == 0  # No record for successful command


def test_record_mode_errors_records_failed_command() -> None:
    """record_mode='errors' should record failed commands."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store, record_mode="errors")

    def failing_handler(c: Command) -> None:
        raise ValueError("Command failed")

    bus.register_command(SampleCommand, failing_handler)

    with pytest.raises(ValueError, match="Command failed"):
        bus.execute(SampleCommand(action="test"))

    assert len(store.records) == 1
    assert store.records[0].message_type == "command"
    assert store.records[0].error is not None
    assert "ValueError" in store.records[0].error
    assert "Command failed" in store.records[0].error


def test_record_mode_errors_skips_successful_event() -> None:
    """record_mode='errors' should NOT record successful events."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store, record_mode="errors")

    received = []
    bus.subscribe(SampleEvent, lambda e: received.append(e.data))

    bus.publish(SampleEvent(data="test-data"))

    assert received == ["test-data"]
    assert len(store.records) == 0  # No record for successful event


def test_record_mode_errors_records_failed_event() -> None:
    """record_mode='errors' should record failed events."""
    inner = LocalMessageBus()
    store = MemoryStore()
    bus = RecordingBus(inner, store, record_mode="errors")

    def failing_handler(e: Event) -> None:
        raise RuntimeError("Event handler failed")

    bus.subscribe(SampleEvent, failing_handler)

    with pytest.raises(RuntimeError, match="Event handler failed"):
        bus.publish(SampleEvent(data="test-data"))

    assert len(store.records) == 1
    assert store.records[0].message_type == "event"
    assert store.records[0].error is not None
    assert "RuntimeError" in store.records[0].error
    assert "Event handler failed" in store.records[0].error
