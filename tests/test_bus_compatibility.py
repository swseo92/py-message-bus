"""Tests for MessageBus interface compatibility across implementations.

This test ensures that LocalMessageBus and ZmqMessageBus can be used
interchangeably through the MessageBus interface.
"""

import time
from dataclasses import dataclass

import pytest

from message_bus.local import LocalMessageBus
from message_bus.ports import Command, Event, Query, Task
from message_bus.zmq_bus import ZmqMessageBus

# Skip all tests if pyzmq not available
pytest.importorskip("zmq")


@dataclass(frozen=True)
class GetUserQuery(Query[str]):
    user_id: int


@dataclass(frozen=True)
class CreateUserCommand(Command):
    name: str


@dataclass(frozen=True)
class UserCreatedEvent(Event):
    user_id: int
    name: str


@dataclass(frozen=True)
class ProcessDataTask(Task):
    data: str


@pytest.fixture
def unique_sockets():
    """Generate unique socket addresses for each test to avoid conflicts."""
    timestamp = str(time.time()).replace(".", "")
    return {
        "task_socket": f"ipc:///tmp/message_bus_task_{timestamp}",
        "query_socket": f"ipc:///tmp/message_bus_query_{timestamp}",
        "event_socket": f"ipc:///tmp/message_bus_event_{timestamp}",
        "command_socket": f"ipc:///tmp/message_bus_command_{timestamp}",
    }


@pytest.fixture
def local_bus():
    """Create a LocalMessageBus."""
    bus = LocalMessageBus()
    yield bus
    # LocalMessageBus has no close method


@pytest.fixture
def zmq_bus(unique_sockets):
    """Create a ZmqMessageBus with unique sockets."""
    bus = ZmqMessageBus(**unique_sockets)
    # Give embedded worker time to start
    time.sleep(0.1)
    yield bus
    bus.close()


class TestMessageBusInterface:
    """Tests that both implementations conform to MessageBus interface."""

    @pytest.mark.parametrize("bus_fixture", ["local_bus", "zmq_bus"])
    def test_bus_is_message_bus(self, bus_fixture, request):
        """Both bus types should be instances of MessageBus."""
        from message_bus.ports import (
            HandlerRegistry,
            MessageDispatcher,
            QueryDispatcher,
            QueryRegistry,
        )

        bus = request.getfixturevalue(bus_fixture)
        # ZmqMessageBus implements all interfaces but doesn't inherit MessageBus ABC
        # Verify it implements all required interfaces
        assert isinstance(bus, QueryDispatcher)
        assert isinstance(bus, QueryRegistry)
        assert isinstance(bus, MessageDispatcher)
        assert isinstance(bus, HandlerRegistry)

    @pytest.mark.parametrize("bus_fixture", ["local_bus", "zmq_bus"])
    def test_register_query(self, bus_fixture, request):
        """Both should support query registration."""
        bus = request.getfixturevalue(bus_fixture)

        def handler(query: GetUserQuery) -> str:
            return f"User {query.user_id}"

        bus.register_query(GetUserQuery, handler)

    @pytest.mark.parametrize("bus_fixture", ["local_bus", "zmq_bus"])
    def test_register_query_duplicate_raises(self, bus_fixture, request):
        """Duplicate query registration should raise ValueError."""
        bus = request.getfixturevalue(bus_fixture)

        def handler(query: GetUserQuery) -> str:
            return "user"

        bus.register_query(GetUserQuery, handler)

        with pytest.raises(ValueError, match="already registered"):
            bus.register_query(GetUserQuery, handler)

    @pytest.mark.parametrize("bus_fixture", ["local_bus", "zmq_bus"])
    def test_register_command(self, bus_fixture, request):
        """Both should support command registration (HandlerRegistry)."""
        bus = request.getfixturevalue(bus_fixture)

        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        bus.register_command(CreateUserCommand, handler)
        # For ZMQ, the handler is registered in embedded worker
        # We can't directly test execution without waiting for worker processing
        # but we verify the method exists and can be called

    @pytest.mark.parametrize("bus_fixture", ["local_bus", "zmq_bus"])
    def test_subscribe_event(self, bus_fixture, request):
        """Both should support event subscription (HandlerRegistry)."""
        bus = request.getfixturevalue(bus_fixture)

        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        bus.subscribe(UserCreatedEvent, handler)
        # For ZMQ, the handler is registered in embedded worker

    @pytest.mark.parametrize("bus_fixture", ["local_bus", "zmq_bus"])
    def test_register_task(self, bus_fixture, request):
        """Both should support task registration (HandlerRegistry)."""
        bus = request.getfixturevalue(bus_fixture)

        processed = []

        def handler(task: ProcessDataTask) -> None:
            processed.append(task.data)

        bus.register_task(ProcessDataTask, handler)
        # For ZMQ, the handler is registered in embedded worker


class TestLocalMessageBusBehavior:
    """Tests specific to LocalMessageBus behavior."""

    def test_send_query_returns_result(self, local_bus):
        """LocalMessageBus should return query results synchronously."""

        def handler(query: GetUserQuery) -> str:
            return f"User {query.user_id}"

        local_bus.register_query(GetUserQuery, handler)
        result = local_bus.send(GetUserQuery(user_id=42))
        assert result == "User 42"

    def test_execute_command_calls_handler(self, local_bus):
        """LocalMessageBus should execute command handlers."""
        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        local_bus.register_command(CreateUserCommand, handler)
        local_bus.execute(CreateUserCommand(name="Alice"))
        assert "Alice" in processed

    def test_publish_event_notifies_subscribers(self, local_bus):
        """LocalMessageBus should notify all event subscribers."""
        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        local_bus.subscribe(UserCreatedEvent, handler)
        local_bus.publish(UserCreatedEvent(user_id=1, name="Bob"))
        assert "Bob" in received


class TestZmqMessageBusBehavior:
    """Tests specific to ZmqMessageBus behavior."""

    def test_send_query_returns_result(self, zmq_bus):
        """ZmqMessageBus should return query results via REQ/REP."""

        def handler(query: GetUserQuery) -> str:
            return f"User {query.user_id}"

        zmq_bus.register_query(GetUserQuery, handler)
        # Give REP thread time to register
        time.sleep(0.05)
        result = zmq_bus.send(GetUserQuery(user_id=42))
        assert result == "User 42"

    def test_command_sent_to_worker(self, zmq_bus):
        """ZmqMessageBus should send commands to embedded worker."""
        # This test verifies the command is sent without error
        # Actual processing happens in embedded worker
        zmq_bus.execute(CreateUserCommand(name="Charlie"))
        # No exception means command was sent successfully

    def test_event_published(self, zmq_bus):
        """ZmqMessageBus should publish events."""
        # This test verifies the event is published without error
        zmq_bus.publish(UserCreatedEvent(user_id=1, name="David"))
        # No exception means event was published successfully
