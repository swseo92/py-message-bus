"""Tests for LocalMessageBus - Implementation-specific tests only."""

from dataclasses import dataclass

import pytest

from message_bus import Command, Event, LocalMessageBus, Query, Task


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


class TestLocalMessageBusIntrospection:
    """Introspection tests - specific to LocalMessageBus implementation."""

    def test_registered_queries(self):
        bus = LocalMessageBus()
        bus.register_query(GetUserQuery, lambda q: {})

        assert "GetUserQuery" in bus.registered_queries()

    def test_registered_commands(self):
        bus = LocalMessageBus()

        @dataclass(frozen=True)
        class TestCommand(Command):
            pass

        bus.register_command(TestCommand, lambda c: None)

        assert "TestCommand" in bus.registered_commands()

    def test_registered_events_with_counts(self):
        bus = LocalMessageBus()
        bus.subscribe(OrderCreatedEvent, lambda e: None)
        bus.subscribe(OrderCreatedEvent, lambda e: None)

        events = bus.registered_events()
        assert events["OrderCreatedEvent"] == 2

    def test_registered_tasks(self):
        bus = LocalMessageBus()
        bus.register_task(SendEmailTask, lambda t: None)

        assert "SendEmailTask" in bus.registered_tasks()


class TestEventFaultIsolation:
    """Tests for event handler fault isolation."""

    def test_all_handlers_execute_when_one_fails(self):
        """All handlers execute even if one fails."""
        bus = LocalMessageBus()
        results = []

        def handler1(e):
            results.append("handler1")

        def handler2(e):
            raise ValueError("Handler2 failed")

        def handler3(e):
            results.append("handler3")

        bus.subscribe(OrderCreatedEvent, handler1)
        bus.subscribe(OrderCreatedEvent, handler2)
        bus.subscribe(OrderCreatedEvent, handler3)

        with pytest.raises(ValueError, match="Handler2 failed"):
            bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        # All handlers should have been called
        assert results == ["handler1", "handler3"]

    def test_multiple_handler_failures_raises_exception_group(self):
        """Multiple handler failures are aggregated into ExceptionGroup."""
        bus = LocalMessageBus()

        def handler1(e):
            raise ValueError("Handler1 failed")

        def handler2(e):
            raise RuntimeError("Handler2 failed")

        bus.subscribe(OrderCreatedEvent, handler1)
        bus.subscribe(OrderCreatedEvent, handler2)

        with pytest.raises(ExceptionGroup) as exc_info:
            bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        assert len(exc_info.value.exceptions) == 2

    def test_no_error_when_all_succeed(self):
        """No exception when all handlers succeed."""
        bus = LocalMessageBus()
        results = []

        bus.subscribe(OrderCreatedEvent, lambda e: results.append("h1"))
        bus.subscribe(OrderCreatedEvent, lambda e: results.append("h2"))

        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        assert results == ["h1", "h2"]
