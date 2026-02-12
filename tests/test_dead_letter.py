"""Unit tests for dead letter queue functionality."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from message_bus import (
    AsyncDeadLetterMiddleware,
    AsyncLocalMessageBus,
    AsyncMiddlewareBus,
    Command,
    DeadLetterMiddleware,
    DeadLetterStore,
    Event,
    LocalMessageBus,
    MemoryDeadLetterStore,
    MiddlewareBus,
    Query,
    Task,
)


# Test messages
@dataclass(frozen=True)
class TestEvent(Event):
    value: int


@dataclass(frozen=True)
class TestCommand(Command):
    value: int


@dataclass(frozen=True)
class TestTask(Task):
    value: int


@dataclass(frozen=True)
class TestQuery(Query[int]):
    value: int


# Test handlers that fail
def failing_event_handler(event: TestEvent) -> None:
    raise ValueError(f"Event handler failed: {event.value}")


def failing_command_handler(command: TestCommand) -> None:
    raise ValueError(f"Command handler failed: {command.value}")


def failing_task_handler(task: TestTask) -> None:
    raise ValueError(f"Task handler failed: {task.value}")


# Test handlers that succeed
def success_event_handler(event: TestEvent) -> None:
    pass


def success_command_handler(command: TestCommand) -> None:
    pass


def success_task_handler(task: TestTask) -> None:
    pass


# FailingDeadLetterStore for testing store.append() failures
class FailingDeadLetterStore(DeadLetterStore):
    """Store that always fails on append (for testing error propagation)."""

    def append(self, message: Event | Command | Task, error: Exception, handler_name: str) -> None:
        raise OSError("DLQ storage failure")

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Sync tests
# ---------------------------------------------------------------------------


class TestDeadLetterMiddleware:
    """Test DeadLetterMiddleware with sync bus."""

    def test_event_handler_failure_recorded(self) -> None:
        """Event handler failure should be recorded in DLQ."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.subscribe(TestEvent, failing_event_handler)

        event = TestEvent(value=42)
        with pytest.raises(ValueError, match="Event handler failed: 42"):
            bus.publish(event)

        assert len(store.records) == 1
        record = store.records[0]
        assert record.message == event
        assert isinstance(record.error, ValueError)
        assert "Event handler failed: 42" in str(record.error)
        assert record.handler_name == "TestEvent"

    def test_command_failure_recorded(self) -> None:
        """Command failure should be recorded in DLQ and exception propagated."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.register_command(TestCommand, failing_command_handler)

        command = TestCommand(value=99)
        with pytest.raises(ValueError, match="Command handler failed: 99"):
            bus.execute(command)

        assert len(store.records) == 1
        record = store.records[0]
        assert record.message == command
        assert isinstance(record.error, ValueError)
        assert "Command handler failed: 99" in str(record.error)
        assert record.handler_name == "TestCommand"

    def test_task_failure_recorded(self) -> None:
        """Task failure should be recorded in DLQ and exception propagated."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.register_task(TestTask, failing_task_handler)

        task = TestTask(value=7)
        with pytest.raises(ValueError, match="Task handler failed: 7"):
            bus.dispatch(task)

        assert len(store.records) == 1
        record = store.records[0]
        assert record.message == task
        assert isinstance(record.error, ValueError)
        assert "Task handler failed: 7" in str(record.error)
        assert record.handler_name == "TestTask"

    def test_success_not_recorded(self) -> None:
        """Successful dispatches should not be recorded in DLQ."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.subscribe(TestEvent, success_event_handler)
        bus.register_command(TestCommand, success_command_handler)
        bus.register_task(TestTask, success_task_handler)

        bus.publish(TestEvent(value=1))
        bus.execute(TestCommand(value=2))
        bus.dispatch(TestTask(value=3))

        assert len(store.records) == 0

    def test_exception_group_multiple_handlers(self) -> None:
        """ExceptionGroup from multiple failed handlers should record all failures."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        def fail1(event: TestEvent) -> None:
            raise ValueError("Handler 1 failed")

        def fail2(event: TestEvent) -> None:
            raise RuntimeError("Handler 2 failed")

        bus.subscribe(TestEvent, fail1)
        bus.subscribe(TestEvent, fail2)

        event = TestEvent(value=5)
        with pytest.raises(ExceptionGroup) as exc_info:
            bus.publish(event)

        # Should have 2 handlers failed
        assert "2 handler(s) failed" in str(exc_info.value)

        # DLQ should have 2 records
        assert len(store.records) == 2
        assert all(r.message == event for r in store.records)
        assert isinstance(store.records[0].error, ValueError)
        assert isinstance(store.records[1].error, RuntimeError)
        assert store.records[0].handler_name == "TestEvent_handler_0"
        assert store.records[1].handler_name == "TestEvent_handler_1"

    def test_mixed_success_and_failure(self) -> None:
        """Mixed success/failure handlers should only record failures."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        def success(event: TestEvent) -> None:
            pass

        def fail(event: TestEvent) -> None:
            raise ValueError("Failed")

        bus.subscribe(TestEvent, success)
        bus.subscribe(TestEvent, fail)

        event = TestEvent(value=10)
        with pytest.raises(ValueError):
            bus.publish(event)

        # Only one failure should be recorded
        assert len(store.records) == 1
        assert store.records[0].message == event

    def test_store_closed_drops_records(self) -> None:
        """Records appended after close should be dropped."""
        store = MemoryDeadLetterStore()
        store.close()

        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.register_command(TestCommand, failing_command_handler)

        command = TestCommand(value=1)
        with pytest.raises(ValueError):
            bus.execute(command)

        # Record should be dropped (store is closed)
        assert len(store.records) == 0

    def test_command_store_append_failure_propagates_original_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When store.append() fails, original handler error still propagates."""
        store = FailingDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.register_command(TestCommand, failing_command_handler)

        command = TestCommand(value=123)
        with (
            caplog.at_level(logging.ERROR),
            pytest.raises(ValueError, match="Command handler failed: 123"),
        ):
            bus.execute(command)

        # Verify DLQ failure was logged
        assert any(
            "Failed to record command failure to DLQ" in record.message for record in caplog.records
        )

    def test_event_store_append_failure_propagates_original_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When store.append() fails for event, original handler error still propagates."""
        store = FailingDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        bus.subscribe(TestEvent, failing_event_handler)

        event = TestEvent(value=456)
        with (
            caplog.at_level(logging.ERROR),
            pytest.raises(ValueError, match="Event handler failed: 456"),
        ):
            bus.publish(event)

        # Verify DLQ failure was logged
        assert any(
            "Failed to record event failure to DLQ" in record.message for record in caplog.records
        )

    def test_query_failure_not_recorded_in_dlq(self) -> None:
        """Query failures should not be recorded in DLQ (pass-through behavior)."""
        store = MemoryDeadLetterStore()
        inner = LocalMessageBus()
        middleware = DeadLetterMiddleware(store)
        bus = MiddlewareBus(inner, [middleware])

        def failing_query_handler(query: TestQuery) -> int:
            raise ValueError(f"Query failed: {query.value}")

        bus.register_query(TestQuery, failing_query_handler)

        query = TestQuery(value=789)
        with pytest.raises(ValueError, match="Query failed: 789"):
            bus.send(query)

        # Query failures should NOT be in DLQ
        assert len(store.records) == 0


# ---------------------------------------------------------------------------
# Async tests
# ---------------------------------------------------------------------------


class TestAsyncDeadLetterMiddleware:
    """Test AsyncDeadLetterMiddleware with async bus."""

    @pytest.mark.asyncio
    async def test_event_handler_failure_recorded(self) -> None:
        """Async event handler failure should be recorded in DLQ."""
        store = MemoryDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_handler(event: TestEvent) -> None:
            raise ValueError(f"Async handler failed: {event.value}")

        bus.subscribe(TestEvent, failing_handler)

        event = TestEvent(value=42)
        with pytest.raises(ValueError, match="Async handler failed: 42"):
            await bus.publish(event)

        assert len(store.records) == 1
        record = store.records[0]
        assert record.message == event
        assert isinstance(record.error, ValueError)
        assert "Async handler failed: 42" in str(record.error)

    @pytest.mark.asyncio
    async def test_command_failure_recorded(self) -> None:
        """Async command failure should be recorded in DLQ."""
        store = MemoryDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_handler(command: TestCommand) -> None:
            raise ValueError(f"Async command failed: {command.value}")

        bus.register_command(TestCommand, failing_handler)

        command = TestCommand(value=99)
        with pytest.raises(ValueError, match="Async command failed: 99"):
            await bus.execute(command)

        assert len(store.records) == 1
        record = store.records[0]
        assert record.message == command
        assert isinstance(record.error, ValueError)

    @pytest.mark.asyncio
    async def test_task_failure_recorded(self) -> None:
        """Async task failure should be recorded in DLQ."""
        store = MemoryDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_handler(task: TestTask) -> None:
            raise ValueError(f"Async task failed: {task.value}")

        bus.register_task(TestTask, failing_handler)

        task = TestTask(value=7)
        with pytest.raises(ValueError, match="Async task failed: 7"):
            await bus.dispatch(task)

        assert len(store.records) == 1
        record = store.records[0]
        assert record.message == task
        assert isinstance(record.error, ValueError)

    @pytest.mark.asyncio
    async def test_success_not_recorded(self) -> None:
        """Successful async dispatches should not be recorded in DLQ."""
        store = MemoryDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def success_event(event: TestEvent) -> None:
            pass

        async def success_command(command: TestCommand) -> None:
            pass

        async def success_task(task: TestTask) -> None:
            pass

        bus.subscribe(TestEvent, success_event)
        bus.register_command(TestCommand, success_command)
        bus.register_task(TestTask, success_task)

        await bus.publish(TestEvent(value=1))
        await bus.execute(TestCommand(value=2))
        await bus.dispatch(TestTask(value=3))

        assert len(store.records) == 0

    @pytest.mark.asyncio
    async def test_exception_group_multiple_handlers(self) -> None:
        """Async ExceptionGroup should record all failures."""
        store = MemoryDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def fail1(event: TestEvent) -> None:
            raise ValueError("Async handler 1 failed")

        async def fail2(event: TestEvent) -> None:
            raise RuntimeError("Async handler 2 failed")

        bus.subscribe(TestEvent, fail1)
        bus.subscribe(TestEvent, fail2)

        event = TestEvent(value=5)
        with pytest.raises(ExceptionGroup) as exc_info:
            await bus.publish(event)

        assert "2 handler(s) failed" in str(exc_info.value)

        # DLQ should have 2 records
        assert len(store.records) == 2
        assert all(r.message == event for r in store.records)
        assert isinstance(store.records[0].error, ValueError)
        assert isinstance(store.records[1].error, RuntimeError)

    @pytest.mark.asyncio
    async def test_async_command_store_append_failure_propagates_original_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Async: When store.append() fails, original handler error still propagates."""
        store = FailingDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_handler(command: TestCommand) -> None:
            raise ValueError(f"Async command failed: {command.value}")

        bus.register_command(TestCommand, failing_handler)

        command = TestCommand(value=321)
        with (
            caplog.at_level(logging.ERROR),
            pytest.raises(ValueError, match="Async command failed: 321"),
        ):
            await bus.execute(command)

        # Verify DLQ failure was logged
        assert any(
            "Failed to record command failure to DLQ" in record.message for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_async_event_store_append_failure_propagates_original_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Async: When store.append() fails for event, original handler error still propagates."""
        store = FailingDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_handler(event: TestEvent) -> None:
            raise ValueError(f"Async event failed: {event.value}")

        bus.subscribe(TestEvent, failing_handler)

        event = TestEvent(value=654)
        with (
            caplog.at_level(logging.ERROR),
            pytest.raises(ValueError, match="Async event failed: 654"),
        ):
            await bus.publish(event)

        # Verify DLQ failure was logged
        assert any(
            "Failed to record event failure to DLQ" in record.message for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_async_query_failure_not_recorded_in_dlq(self) -> None:
        """Async: Query failures should not be recorded in DLQ (pass-through behavior)."""
        store = MemoryDeadLetterStore()
        inner = AsyncLocalMessageBus()
        middleware = AsyncDeadLetterMiddleware(store)
        bus = AsyncMiddlewareBus(inner, [middleware])

        async def failing_query_handler(query: TestQuery) -> int:
            raise ValueError(f"Async query failed: {query.value}")

        bus.register_query(TestQuery, failing_query_handler)

        query = TestQuery(value=987)
        with pytest.raises(ValueError, match="Async query failed: 987"):
            await bus.send(query)

        # Query failures should NOT be in DLQ
        assert len(store.records) == 0
