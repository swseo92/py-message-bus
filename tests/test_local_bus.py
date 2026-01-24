"""Tests for LocalMessageBus - Implementation-specific tests only."""

from dataclasses import dataclass

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
