"""Tests for async RecordingBus."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from message_bus import AsyncLocalMessageBus
from message_bus.ports import Command, Event, Query, Task
from message_bus.recording import AsyncRecordingBus, MemoryStore


# Test message types
@dataclass(frozen=True)
class SampleQuery(Query[str]):
    """Test query message."""

    value: int


@dataclass(frozen=True)
class SampleCommand(Command):
    """Test command message."""

    action: str


@dataclass(frozen=True)
class SampleEvent(Event):
    """Test event message."""

    data: str


@dataclass(frozen=True)
class SampleTask(Task):
    """Test task message."""

    task_id: int


# AsyncRecordingBus tests


async def test_async_recording_bus_records_query() -> None:
    """Query dispatch creates Record with result."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    async def handler(q: Query[str]) -> str:
        return f"result-{q.value}"

    bus.register_query(SampleQuery, handler)

    result = await bus.send(SampleQuery(value=42))

    assert result == "result-42"
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "query"
    assert record.message_class == "SampleQuery"
    assert record.payload == SampleQuery(value=42)
    assert record.result == "result-42"
    assert record.error is None
    assert record.duration_ns > 0


async def test_async_recording_bus_records_command() -> None:
    """Command dispatch creates Record."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    executed = []

    async def handler(c: Command) -> None:
        executed.append(c.action)

    bus.register_command(SampleCommand, handler)

    await bus.execute(SampleCommand(action="test"))

    assert executed == ["test"]
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "command"
    assert record.message_class == "SampleCommand"
    assert record.payload == SampleCommand(action="test")
    assert record.result is None
    assert record.error is None
    assert record.duration_ns > 0


async def test_async_recording_bus_records_event() -> None:
    """Event publish creates Record."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    received = []

    async def handler(e: Event) -> None:
        received.append(e.data)

    bus.subscribe(SampleEvent, handler)

    await bus.publish(SampleEvent(data="event-data"))

    assert received == ["event-data"]
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "event"
    assert record.message_class == "SampleEvent"
    assert record.payload == SampleEvent(data="event-data")
    assert record.result is None
    assert record.error is None
    assert record.duration_ns > 0


async def test_async_recording_bus_records_task() -> None:
    """Task dispatch creates Record."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    processed = []

    async def handler(t: Task) -> None:
        processed.append(t.task_id)

    bus.register_task(SampleTask, handler)

    await bus.dispatch(SampleTask(task_id=123))

    assert processed == [123]
    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "task"
    assert record.message_class == "SampleTask"
    assert record.payload == SampleTask(task_id=123)
    assert record.result is None
    assert record.error is None
    assert record.duration_ns > 0


async def test_async_recording_bus_records_error() -> None:
    """Failed handler creates Record with error field."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    async def failing_handler(q: Query[str]) -> str:
        raise ValueError("Test error")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(ValueError, match="Test error"):
        await bus.send(SampleQuery(value=1))

    assert len(store.records) == 1

    record = store.records[0]
    assert record.message_type == "query"
    assert record.error is not None
    assert "ValueError" in record.error
    assert "Test error" in record.error
    assert record.result is None


async def test_async_recording_bus_errors_mode_skips_success() -> None:
    """record_mode="errors" does not record successful dispatches."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store, record_mode="errors")

    async def query_handler(q: Query[str]) -> str:
        return "success"

    async def command_handler(c: Command) -> None:
        pass

    bus.register_query(SampleQuery, query_handler)
    bus.register_command(SampleCommand, command_handler)

    await bus.send(SampleQuery(value=1))
    await bus.execute(SampleCommand(action="test"))

    # No records for successful dispatches
    assert len(store.records) == 0


async def test_async_recording_bus_errors_mode_records_failure() -> None:
    """record_mode="errors" records when handler fails."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store, record_mode="errors")

    async def failing_handler(q: Query[str]) -> str:
        raise RuntimeError("Failure")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(RuntimeError, match="Failure"):
        await bus.send(SampleQuery(value=1))

    # Error should be recorded
    assert len(store.records) == 1
    assert store.records[0].error is not None


async def test_async_recording_bus_propagates_exceptions() -> None:
    """Inner bus exceptions pass through unchanged."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    class CustomError(Exception):
        pass

    async def handler(q: Query[str]) -> str:
        raise CustomError("Original error")

    bus.register_query(SampleQuery, handler)

    with pytest.raises(CustomError, match="Original error"):
        await bus.send(SampleQuery(value=1))


async def test_async_recording_bus_query_result_captured() -> None:
    """Query result stored in Record.result."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    async def handler(q: Query[str]) -> str:
        return f"value={q.value}"

    bus.register_query(SampleQuery, handler)

    await bus.send(SampleQuery(value=999))

    assert store.records[0].result == "value=999"


async def test_async_recording_bus_duration_is_positive() -> None:
    """duration_ns > 0."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    async def handler(q: Query[str]) -> str:
        return "result"

    bus.register_query(SampleQuery, handler)

    await bus.send(SampleQuery(value=1))

    assert store.records[0].duration_ns > 0


async def test_async_recording_bus_close_delegates() -> None:
    """close() calls store.close()."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    assert not store._closed

    bus.close()

    assert store._closed


async def test_async_recording_bus_close_is_idempotent() -> None:
    """Multiple close() calls are safe."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    bus.close()
    bus.close()  # Should not raise

    assert store._closed


async def test_async_recording_bus_context_manager() -> None:
    """Context manager calls close on exit."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()

    async def handler(q: Query[str]) -> str:
        return "result"

    async with AsyncRecordingBus(inner, store) as bus:
        bus.register_query(SampleQuery, handler)
        await bus.send(SampleQuery(value=1))

    # Store should be closed
    assert store._closed
    assert len(store.records) == 1


async def test_async_recording_bus_invalid_record_mode() -> None:
    """ValueError raised for invalid record_mode."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()

    with pytest.raises(ValueError, match="Invalid record_mode"):
        AsyncRecordingBus(inner, store, record_mode="invalid")


async def test_async_recording_bus_payload_is_raw_object() -> None:
    """Record.payload stores message object (not dict)."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    async def handler(q: Query[str]) -> str:
        return "result"

    bus.register_query(SampleQuery, handler)

    query = SampleQuery(value=42)
    await bus.send(query)

    # Payload should be the same object instance
    assert store.records[0].payload is query
    assert isinstance(store.records[0].payload, SampleQuery)


async def test_async_recording_bus_async_context_manager() -> None:
    """async with context manager works."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()

    async def handler(q: Query[str]) -> str:
        return "test"

    async with AsyncRecordingBus(inner, store) as bus:
        bus.register_query(SampleQuery, handler)
        result = await bus.send(SampleQuery(value=1))
        assert result == "test"

    # Should be closed after exit
    assert store._closed


def test_async_recording_bus_close_is_sync() -> None:
    """close() is not a coroutine."""
    inner = AsyncLocalMessageBus()
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    # close() should be synchronous, not async
    import inspect

    assert not inspect.iscoroutinefunction(bus.close)

    # Should be callable without await
    bus.close()
    assert store._closed
