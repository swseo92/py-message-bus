"""Integration tests for dead letter queue with other middleware."""

from __future__ import annotations

import importlib.util
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


# ---------------------------------------------------------------------------
# RedisDeadLetterStore integration tests
# ---------------------------------------------------------------------------

_fakeredis_available = importlib.util.find_spec("fakeredis") is not None

_redis_integration = pytest.mark.skipif(not _fakeredis_available, reason="fakeredis not installed")


@_redis_integration
class TestRedisDeadLetterStoreWithMiddleware:
    """Integration tests: RedisDeadLetterStore + DeadLetterMiddleware."""

    def _make_store(self) -> RedisDeadLetterStore:  # noqa: F821
        import fakeredis

        from message_bus.redis_dead_letter import RedisDeadLetterStore

        return RedisDeadLetterStore(
            redis_url="redis://localhost:6379/0",
            _redis_client=fakeredis.FakeRedis(decode_responses=True),
        )

    def test_sync_command_failure_persists_to_redis(self) -> None:
        """Failed command is stored in RedisDeadLetterStore via DeadLetterMiddleware."""
        from message_bus import DeadLetterMiddleware, LocalMessageBus, MiddlewareBus

        store = self._make_store()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        def failing_handler(command: TestCommand) -> None:
            raise ValueError("Persistent failure")

        bus.register_command(TestCommand, failing_handler)

        command = TestCommand(value=42)
        with pytest.raises(ValueError, match="Persistent failure"):
            bus.execute(command)

        # Failure is persisted in Redis
        assert store.count() == 1
        entries = store.list()
        assert len(entries) == 1
        assert entries[0].message_type == "TestCommand"
        assert entries[0].error_type == "ValueError"
        assert entries[0].error_message == "Persistent failure"
        assert entries[0].handler_name == "TestCommand"

    def test_sync_command_failure_survives_store_recreation(self) -> None:
        """Entries written via middleware remain queryable after store recreation."""
        import fakeredis

        from message_bus import DeadLetterMiddleware, LocalMessageBus, MiddlewareBus
        from message_bus.redis_dead_letter import RedisDeadLetterStore

        fake_redis_client = fakeredis.FakeRedis(decode_responses=True)

        # Write via middleware
        store1 = RedisDeadLetterStore(
            redis_url="redis://localhost:6379/0",
            _redis_client=fake_redis_client,
        )
        inner = LocalMessageBus()
        bus = MiddlewareBus(inner, [DeadLetterMiddleware(store1)])

        def failing_handler(command: TestCommand) -> None:
            raise RuntimeError("Simulated failure")

        bus.register_command(TestCommand, failing_handler)
        with pytest.raises(RuntimeError):
            bus.execute(TestCommand(value=99))

        assert store1.count() == 1

        # Re-open on same fake-redis instance (simulates process restart)
        store2 = RedisDeadLetterStore(
            redis_url="redis://localhost:6379/0",
            _redis_client=fake_redis_client,
        )
        assert store2.count() == 1
        entry = store2.list()[0]
        assert entry.error_type == "RuntimeError"

    @pytest.mark.asyncio
    async def test_async_command_failure_persists_to_redis(self) -> None:
        """Failed async command is stored in RedisDeadLetterStore via AsyncDeadLetterMiddleware."""
        from message_bus import (
            AsyncDeadLetterMiddleware,
            AsyncLocalMessageBus,
            AsyncMiddlewareBus,
        )

        store = self._make_store()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_handler(command: TestCommand) -> None:
            raise ValueError("Async persistent failure")

        bus.register_command(TestCommand, failing_handler)

        command = TestCommand(value=7)
        with pytest.raises(ValueError, match="Async persistent failure"):
            await bus.execute(command)

        assert store.count() == 1
        entries = store.list()
        assert entries[0].message_type == "TestCommand"
        assert entries[0].error_type == "ValueError"

    def test_entry_can_be_deleted_after_investigation(self) -> None:
        """Entries can be removed from the store after manual investigation."""
        from message_bus import DeadLetterMiddleware, LocalMessageBus, MiddlewareBus

        store = self._make_store()
        bus = MiddlewareBus(LocalMessageBus(), [DeadLetterMiddleware(store)])

        def failing_handler(command: TestCommand) -> None:
            raise ValueError("transient error")

        bus.register_command(TestCommand, failing_handler)
        with pytest.raises(ValueError):
            bus.execute(TestCommand(value=1))

        entries = store.list()
        assert len(entries) == 1
        store.delete(entries[0].id)
        assert store.count() == 0
