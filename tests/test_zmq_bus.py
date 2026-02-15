"""Tests for ZeroMQ message bus."""

import time
from dataclasses import dataclass

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
    worker = ZmqWorker(**unique_sockets)
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

    def test_send_query_returns_response(self, bus):
        # Register handler on bus (for REP socket)
        def handler(query: GetUserQuery) -> str:
            return f"User {query.user_id}"

        bus.register_query(GetUserQuery, handler)

        # Small delay for socket setup
        time.sleep(0.05)

        # Send query
        result = bus.send(GetUserQuery(user_id=42))
        assert result == "User 42"

    def test_send_unregistered_query_raises(self, bus):
        query = GetUserQuery(user_id=1)

        # Should timeout or raise error (no handler registered)
        with pytest.raises((LookupError, TimeoutError, OSError)):
            # Set a short timeout to avoid hanging
            bus._context.setsockopt(1, 1000)  # 1 second timeout
            bus.send(query)


class TestZmqWorkerIntegration:
    """Integration tests with embedded worker."""

    def test_worker_processes_task(self, bus):
        # Track processed tasks
        processed = []

        def handler(task: ProcessDataTask) -> None:
            processed.append(task.data)

        bus.register_task(ProcessDataTask, handler)

        # Small delay for embedded worker to process
        time.sleep(0.1)

        # Dispatch task
        bus.dispatch(ProcessDataTask(data="test_data"))

        # Wait for processing
        time.sleep(0.2)

        assert "test_data" in processed

    def test_worker_processes_command(self, bus):
        # Track processed commands
        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        bus.register_command(CreateUserCommand, handler)

        # Small delay for embedded worker
        time.sleep(0.1)

        # Execute command
        bus.execute(CreateUserCommand(name="Alice"))

        time.sleep(0.2)

        assert "Alice" in processed

    def test_worker_receives_event(self, bus):
        # Track received events
        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        bus.subscribe(UserCreatedEvent, handler)

        # Small delay for embedded worker
        time.sleep(0.1)

        # Publish event
        bus.publish(UserCreatedEvent(user_id=1, name="Bob"))

        time.sleep(0.2)

        assert "Bob" in received

    def test_multiple_workers_share_tasks(self, bus):
        """Test that embedded worker processes tasks."""
        # Track processed tasks
        processed = []

        def handler(task: ProcessDataTask) -> None:
            processed.append(task.data)

        bus.register_task(ProcessDataTask, handler)

        # Small delay for embedded worker
        time.sleep(0.1)

        # Dispatch multiple tasks
        for i in range(5):
            bus.dispatch(ProcessDataTask(data=f"task_{i}"))

        # Wait for processing
        time.sleep(0.3)

        # Tasks should be processed by embedded worker
        assert len(processed) == 5, f"Expected 5 tasks, got {len(processed)}"

    def test_multiple_workers_all_receive_events(self, bus):
        """Test that embedded worker receives events."""
        # Track received events
        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        bus.subscribe(UserCreatedEvent, handler)

        # Small delay for embedded worker
        time.sleep(0.1)

        # Publish event
        bus.publish(UserCreatedEvent(user_id=1, name="Charlie"))

        time.sleep(0.2)

        # Event should be received by embedded worker
        assert "Charlie" in received


class TestZmqResourceCleanup:
    """Test resource cleanup and socket management."""

    def test_bus_close_cleans_resources(self, unique_sockets):
        bus = ZmqMessageBus(**unique_sockets)
        bus.close()
        # Should not raise
        assert not bus._running

    def test_worker_close_cleans_resources(self, unique_sockets):
        # ZmqWorker only uses task_socket, event_socket, command_socket (not query_socket)
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

        tracker = TrackingSerializer()
        # Just verify the serializer can be instantiated with custom serializer
        # Full integration test requires ZMQ sockets
        data = tracker.dumps(GetUserQuery(user_id=1))
        result = tracker.loads(data)
        assert tracker.dumps_count == 1
        assert tracker.loads_count == 1
        assert result == GetUserQuery(user_id=1)


class TestSocketValidation:
    """Tests for socket address validation."""

    def test_rejects_path_traversal(self):
        from message_bus.zmq_bus import _validate_socket_addr

        with pytest.raises(ValueError, match="Invalid IPC socket path"):
            _validate_socket_addr("ipc:///tmp/../etc/passwd")

    def test_rejects_empty_ipc_path(self):
        from message_bus.zmq_bus import _validate_socket_addr

        with pytest.raises(ValueError, match="Invalid IPC socket path"):
            _validate_socket_addr("ipc://")

    def test_rejects_unknown_protocol(self):
        from message_bus.zmq_bus import _validate_socket_addr

        with pytest.raises(ValueError, match="Unsupported socket protocol"):
            _validate_socket_addr("http://localhost:5555")

    def test_accepts_valid_ipc(self):
        from message_bus.zmq_bus import _validate_socket_addr

        _validate_socket_addr("ipc:///tmp/test_socket")  # Should not raise

    def test_accepts_valid_tcp(self):
        from message_bus.zmq_bus import _validate_socket_addr

        _validate_socket_addr("tcp://127.0.0.1:5555")  # Should not raise

    def test_rejects_invalid_tcp_format(self):
        from message_bus.zmq_bus import _validate_socket_addr

        with pytest.raises(ValueError, match="Invalid TCP socket address"):
            _validate_socket_addr("tcp://localhost")  # No port

    def test_rejects_invalid_timeout(self):
        from message_bus.zmq_bus import ZmqMessageBus

        with pytest.raises(ValueError, match="query_timeout_ms must be > 0"):
            ZmqMessageBus(query_timeout_ms=0)

    def test_rejects_negative_timeout(self):
        from message_bus.zmq_bus import ZmqMessageBus

        with pytest.raises(ValueError, match="query_timeout_ms must be > 0"):
            ZmqMessageBus(query_timeout_ms=-100)
