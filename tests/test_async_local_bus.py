"""Tests for AsyncLocalMessageBus - Implementation-specific tests only."""

from dataclasses import dataclass

import pytest

from message_bus import AsyncLocalMessageBus, Command, Event, Query, Task


# Test messages for introspection tests
@dataclass(frozen=True)
class GetUserQuery(Query[dict]):
    user_id: str


@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    order_id: str
    user_id: str


@dataclass(frozen=True)
class SendEmailTask(Task):
    email: str
    subject: str


class TestAsyncLocalMessageBusIntrospection:
    """Introspection tests - specific to AsyncLocalMessageBus implementation."""

    @pytest.mark.asyncio
    async def test_registered_queries(self):
        bus = AsyncLocalMessageBus()

        async def handler(q: GetUserQuery) -> dict[str, str]:
            return {}

        bus.register_query(GetUserQuery, handler)

        assert "GetUserQuery" in bus.registered_queries()

    @pytest.mark.asyncio
    async def test_registered_commands(self):
        bus = AsyncLocalMessageBus()

        @dataclass(frozen=True)
        class TestCommand(Command):
            pass

        async def handler(c: TestCommand) -> None:
            pass

        bus.register_command(TestCommand, handler)

        assert "TestCommand" in bus.registered_commands()

    @pytest.mark.asyncio
    async def test_registered_events_with_counts(self):
        bus = AsyncLocalMessageBus()

        async def handler(e: OrderCreatedEvent) -> None:
            pass

        bus.subscribe(OrderCreatedEvent, handler)
        bus.subscribe(OrderCreatedEvent, handler)

        events = bus.registered_events()
        assert events["OrderCreatedEvent"] == 2

    @pytest.mark.asyncio
    async def test_registered_tasks(self):
        bus = AsyncLocalMessageBus()

        async def handler(t: SendEmailTask) -> None:
            pass

        bus.register_task(SendEmailTask, handler)

        assert "SendEmailTask" in bus.registered_tasks()


class TestAsyncEventFaultIsolation:
    """Tests for async event handler fault isolation."""

    @pytest.mark.asyncio
    async def test_all_handlers_execute_when_one_fails(self):
        """All handlers execute even if one fails."""
        bus = AsyncLocalMessageBus()
        results = []

        async def handler1(e):
            results.append("handler1")

        async def handler2(e):
            raise ValueError("Handler2 failed")

        async def handler3(e):
            results.append("handler3")

        bus.subscribe(OrderCreatedEvent, handler1)
        bus.subscribe(OrderCreatedEvent, handler2)
        bus.subscribe(OrderCreatedEvent, handler3)

        with pytest.raises(ValueError, match="Handler2 failed"):
            await bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        # All handlers should have been called
        assert results == ["handler1", "handler3"]

    @pytest.mark.asyncio
    async def test_multiple_handler_failures_raises_exception_group(self):
        """Multiple handler failures are aggregated into ExceptionGroup."""
        bus = AsyncLocalMessageBus()

        async def handler1(e):
            raise ValueError("Handler1 failed")

        async def handler2(e):
            raise RuntimeError("Handler2 failed")

        bus.subscribe(OrderCreatedEvent, handler1)
        bus.subscribe(OrderCreatedEvent, handler2)

        with pytest.raises(ExceptionGroup) as exc_info:
            await bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        assert len(exc_info.value.exceptions) == 2
