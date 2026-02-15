"""Tests for ZeroMQ message bus."""

import time
from dataclasses import dataclass
from threading import Thread

import pytest

# Skip all tests if pyzmq not available
pytest.importorskip("zmq")

from message_bus.ports import Command, Event, Query, Task
from message_bus.zmq_bus import ZmqMessageBus, ZmqWorker


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
def bus(unique_sockets):
    """Create a ZmqMessageBus with unique sockets."""
    bus = ZmqMessageBus(**unique_sockets)
    yield bus
    bus.close()


@pytest.fixture
def worker(unique_sockets):
    """Create a ZmqWorker with unique sockets."""
    # ZmqWorker doesn't use query_socket
    worker_sockets = {k: v for k, v in unique_sockets.items() if k != "query_socket"}
    worker = ZmqWorker(**worker_sockets)
    yield worker
    worker.close()


class TestZmqMessageBusBasic:
    """Basic tests for ZmqMessageBus without worker processes."""

    def test_register_query_duplicate_raises(self, bus):
        def handler(query: GetUserQuery) -> str:
            return "user"

        bus.register_query(GetUserQuery, handler)

        with pytest.raises(ValueError, match="already registered"):
            bus.register_query(GetUserQuery, handler)

    def test_send_query_returns_response(self, bus, worker):
        # Register handler on bus (for REP socket)
        def handler(query: GetUserQuery) -> str:
            return f"User {query.user_id}"

        bus.register_query(GetUserQuery, handler)

        # Start worker
        worker_thread = Thread(target=worker.run, daemon=True)
        worker_thread.start()

        time.sleep(0.05)

        # Send query
        result = bus.send(GetUserQuery(user_id=42))

        worker.stop()
        worker_thread.join(timeout=1.0)

        assert result == "User 42"

    def test_send_unregistered_query_raises(self, bus):
        with pytest.raises(LookupError, match="No handler registered"):
            bus.send(GetUserQuery(user_id=42))


class TestZmqWorkerIntegration:
    """Tests for drop-in replacement functionality."""

    def test_drop_in_replacement_task(self, bus):
        """ZmqMessageBus works as drop-in replacement for LocalMessageBus."""
        processed = []

        def handler(task: ProcessDataTask) -> None:
            processed.append(task.data)

        # Same API as LocalMessageBus
        bus.register_task(ProcessDataTask, handler)

        # Dispatch multiple tasks - internal worker handles them
        for i in range(5):
            bus.dispatch(ProcessDataTask(data=f"task_{i}"))

        time.sleep(0.2)

        # All tasks should be processed by internal worker
        assert len(processed) == 5

    def test_drop_in_replacement_command(self, bus):
        """Command handlers work with drop-in API."""
        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        # Same API as LocalMessageBus
        bus.register_command(CreateUserCommand, handler)

        bus.execute(CreateUserCommand(name="TestUser"))
        time.sleep(0.1)

        assert "TestUser" in processed

    def test_drop_in_replacement_event(self, bus):
        """Event handlers work with drop-in API."""
        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        # Same API as LocalMessageBus
        bus.subscribe(UserCreatedEvent, handler)

        time.sleep(0.05)
        bus.publish(UserCreatedEvent(user_id=1, name="EventUser"))
        time.sleep(0.1)

        assert "EventUser" in received


class TestZmqResourceCleanup:
    """Test resource cleanup and socket management."""

    def test_bus_close_cleans_resources(self, unique_sockets):
        bus = ZmqMessageBus(**unique_sockets)
        bus.close()
        # Should not raise
        assert not bus._running

    def test_worker_close_cleans_resources(self, unique_sockets):
        # ZmqWorker only uses task_socket, event_socket, command_socket
        worker_sockets = {
            k: v
            for k, v in unique_sockets.items()
            if k in ("task_socket", "event_socket", "command_socket")
        }
        worker = ZmqWorker(**worker_sockets)
        worker.close()
        # Should not raise
        assert not worker._running


class TestSerializerProtocol:
    """Tests for Serializer protocol and PickleSerializer."""

    def test_pickle_serializer_roundtrip(self):
        from message_bus.zmq_bus import PickleSerializer

        serializer = PickleSerializer()

        obj = GetUserQuery(user_id=42)
        data = serializer.dumps(obj)
        result = serializer.loads(data)

        assert result == obj

    def test_custom_serializer_is_used(self):
        """Custom serializer replaces pickle."""
        from typing import Any

        from message_bus.zmq_bus import PickleSerializer

        class TrackingSerializer:
            def __init__(self) -> None:
                self.dumps_count: int = 0
                self.loads_count: int = 0
                self._inner: PickleSerializer = PickleSerializer()

            def dumps(self, obj: Any) -> bytes:
                self.dumps_count += 1
                result: bytes = self._inner.dumps(obj)
                return result

            def loads(self, data: bytes) -> Any:
                self.loads_count += 1
                return self._inner.loads(data)

        serializer = TrackingSerializer()
        bus = ZmqMessageBus(serializer=serializer)

        bus.register_query(GetUserQuery, lambda q: f"User {q.user_id}")
        result = bus.send(GetUserQuery(user_id=42))

        assert result == "User 42"
        assert serializer.dumps_count >= 2  # At least query and response
        assert serializer.loads_count >= 2

        bus.close()
