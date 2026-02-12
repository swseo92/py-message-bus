"""Integration tests for dead letter queue with other middleware."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from message_bus import (
    AsyncDeadLetterMiddleware,
    AsyncLocalMessageBus,
    AsyncMiddlewareBus,
    AsyncRecordingBus,
    Command,
    DeadLetterMiddleware,
    Event,
    LocalMessageBus,
    MemoryDeadLetterStore,
    MemoryStore,
    MiddlewareBus,
    RecordingBus,
)


# Test messages
@dataclass(frozen=True)
class TestEvent(Event):
    value: int


@dataclass(frozen=True)
class TestCommand(Command):
    value: int


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestDeadLetterWithRecording:
    """Test DeadLetterMiddleware composition with RecordingBus."""

    def test_both_record_failures(self) -> None:
        """Failed dispatches should be recorded in both DLQ and RecordingBus."""
        dlq_store = MemoryDeadLetterStore()
        record_store = MemoryStore()

        inner = LocalMessageBus()
        dlq_middleware = DeadLetterMiddleware(dlq_store)
        middleware_bus = MiddlewareBus(inner, [dlq_middleware])
        recording_bus = RecordingBus(middleware_bus, record_store)

        def failing_handler(command: TestCommand) -> None:
            raise ValueError("Handler failed")

        recording_bus.register_command(TestCommand, failing_handler)

        command = TestCommand(value=42)
        with pytest.raises(ValueError, match="Handler failed"):
            recording_bus.execute(command)

        # DLQ should have the failure
        assert len(dlq_store.records) == 1
        assert dlq_store.records[0].message == command

        # RecordingBus should also have the failure
        assert len(record_store.records) == 1
        assert record_store.records[0].message_class == "TestCommand"
        assert record_store.records[0].error is not None

    def test_success_only_in_recording(self) -> None:
        """Successful dispatches should only be in RecordingBus, not DLQ."""
        dlq_store = MemoryDeadLetterStore()
        record_store = MemoryStore()

        inner = LocalMessageBus()
        dlq_middleware = DeadLetterMiddleware(dlq_store)
        middleware_bus = MiddlewareBus(inner, [dlq_middleware])
        recording_bus = RecordingBus(middleware_bus, record_store)

        def success_handler(command: TestCommand) -> None:
            pass

        recording_bus.register_command(TestCommand, success_handler)

        command = TestCommand(value=99)
        recording_bus.execute(command)

        # DLQ should be empty
        assert len(dlq_store.records) == 0

        # RecordingBus should have the success
        assert len(record_store.records) == 1
        assert record_store.records[0].message_class == "TestCommand"
        assert record_store.records[0].error is None


class TestDeadLetterInMiddlewareBus:
    """Test DeadLetterMiddleware within MiddlewareBus chain."""

    def test_middleware_chain_with_dlq(self) -> None:
        """DeadLetterMiddleware should work in middleware chain."""
        dlq_store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        dlq_middleware = DeadLetterMiddleware(dlq_store)
        bus = MiddlewareBus(inner, [dlq_middleware])

        def failing_handler(event: TestEvent) -> None:
            raise RuntimeError("Chain failure")

        bus.subscribe(TestEvent, failing_handler)

        event = TestEvent(value=123)
        with pytest.raises(RuntimeError, match="Chain failure"):
            bus.publish(event)

        assert len(dlq_store.records) == 1
        assert dlq_store.records[0].message == event
        assert isinstance(dlq_store.records[0].error, RuntimeError)


class TestAsyncDeadLetterWithRecording:
    """Test AsyncDeadLetterMiddleware composition with AsyncRecordingBus."""

    @pytest.mark.asyncio
    async def test_both_record_failures(self) -> None:
        """Async failed dispatches should be recorded in both DLQ and RecordingBus."""
        dlq_store = MemoryDeadLetterStore()
        record_store = MemoryStore()

        inner = AsyncLocalMessageBus()
        dlq_middleware = AsyncDeadLetterMiddleware(dlq_store)
        middleware_bus = AsyncMiddlewareBus(inner, [dlq_middleware])
        recording_bus = AsyncRecordingBus(middleware_bus, record_store)

        async def failing_handler(command: TestCommand) -> None:
            raise ValueError("Async handler failed")

        recording_bus.register_command(TestCommand, failing_handler)

        command = TestCommand(value=42)
        with pytest.raises(ValueError, match="Async handler failed"):
            await recording_bus.execute(command)

        # DLQ should have the failure
        assert len(dlq_store.records) == 1
        assert dlq_store.records[0].message == command

        # RecordingBus should also have the failure
        assert len(record_store.records) == 1
        assert record_store.records[0].message_class == "TestCommand"
        assert record_store.records[0].error is not None

    @pytest.mark.asyncio
    async def test_success_only_in_recording(self) -> None:
        """Async successful dispatches should only be in RecordingBus, not DLQ."""
        dlq_store = MemoryDeadLetterStore()
        record_store = MemoryStore()

        inner = AsyncLocalMessageBus()
        dlq_middleware = AsyncDeadLetterMiddleware(dlq_store)
        middleware_bus = AsyncMiddlewareBus(inner, [dlq_middleware])
        recording_bus = AsyncRecordingBus(middleware_bus, record_store)

        async def success_handler(command: TestCommand) -> None:
            pass

        recording_bus.register_command(TestCommand, success_handler)

        command = TestCommand(value=99)
        await recording_bus.execute(command)

        # DLQ should be empty
        assert len(dlq_store.records) == 0

        # RecordingBus should have the success
        assert len(record_store.records) == 1
        assert record_store.records[0].message_class == "TestCommand"
        assert record_store.records[0].error is None
