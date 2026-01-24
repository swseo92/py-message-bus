"""Tests for LocalMessageBus."""

from dataclasses import dataclass

import pytest

from message_bus import Command, Event, LocalMessageBus, Query, Task


# Test messages
@dataclass(frozen=True)
class GetUserQuery(Query[dict]):
    user_id: str


@dataclass(frozen=True)
class CreateOrderCommand(Command):
    user_id: str
    item: str


@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    order_id: str
    user_id: str


@dataclass(frozen=True)
class SendEmailTask(Task):
    email: str
    subject: str


class TestQueryHandling:
    def test_send_query_returns_handler_result(self):
        bus = LocalMessageBus()
        bus.register_query(GetUserQuery, lambda q: {"id": q.user_id, "name": "Alice"})

        result = bus.send(GetUserQuery(user_id="123"))

        assert result == {"id": "123", "name": "Alice"}

    def test_send_unregistered_query_raises(self):
        bus = LocalMessageBus()

        with pytest.raises(LookupError, match="No handler registered"):
            bus.send(GetUserQuery(user_id="123"))

    def test_register_duplicate_query_raises(self):
        bus = LocalMessageBus()
        bus.register_query(GetUserQuery, lambda q: {})

        with pytest.raises(ValueError, match="already registered"):
            bus.register_query(GetUserQuery, lambda q: {})


class TestCommandHandling:
    def test_execute_command_calls_handler(self):
        bus = LocalMessageBus()
        executed = []
        bus.register_command(CreateOrderCommand, lambda c: executed.append(c))

        bus.execute(CreateOrderCommand(user_id="123", item="book"))

        assert len(executed) == 1
        assert executed[0].user_id == "123"

    def test_execute_unregistered_command_raises(self):
        bus = LocalMessageBus()

        with pytest.raises(LookupError, match="No handler registered"):
            bus.execute(CreateOrderCommand(user_id="123", item="book"))


class TestEventHandling:
    def test_publish_event_calls_all_subscribers(self):
        bus = LocalMessageBus()
        results = []

        bus.subscribe(OrderCreatedEvent, lambda e: results.append(f"handler1:{e.order_id}"))
        bus.subscribe(OrderCreatedEvent, lambda e: results.append(f"handler2:{e.order_id}"))

        bus.publish(OrderCreatedEvent(order_id="order-1", user_id="123"))

        assert results == ["handler1:order-1", "handler2:order-1"]

    def test_publish_event_with_no_subscribers_is_silent(self):
        bus = LocalMessageBus()
        # Should not raise
        bus.publish(OrderCreatedEvent(order_id="order-1", user_id="123"))


class TestTaskHandling:
    def test_dispatch_task_calls_handler(self):
        bus = LocalMessageBus()
        dispatched = []
        bus.register_task(SendEmailTask, lambda t: dispatched.append(t))

        bus.dispatch(SendEmailTask(email="test@example.com", subject="Hello"))

        assert len(dispatched) == 1
        assert dispatched[0].email == "test@example.com"

    def test_dispatch_unregistered_task_raises(self):
        bus = LocalMessageBus()

        with pytest.raises(LookupError, match="No handler registered"):
            bus.dispatch(SendEmailTask(email="test@example.com", subject="Hello"))

    def test_register_duplicate_task_raises(self):
        bus = LocalMessageBus()
        bus.register_task(SendEmailTask, lambda t: None)

        with pytest.raises(ValueError, match="already registered"):
            bus.register_task(SendEmailTask, lambda t: None)


class TestIntrospection:
    def test_registered_queries(self):
        bus = LocalMessageBus()
        bus.register_query(GetUserQuery, lambda q: {})

        assert "GetUserQuery" in bus.registered_queries()

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
