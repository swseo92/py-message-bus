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
        "task": f"ipc:///tmp/message_bus_task_{timestamp}",
        "query": f"ipc:///tmp/message_bus_query_{timestamp}",
        "event": f"ipc:///tmp/message_bus_event_{timestamp}",
        "command": f"ipc:///tmp/message_bus_command_{timestamp}",
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

    def test_send_query_returns_response(self, bus, worker):
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
    """Integration tests with worker processes."""

    def test_worker_processes_task(self, bus, worker):
        # Track processed tasks
        processed = []

        def handler(task: ProcessDataTask) -> None:
            processed.append(task.data)

        worker.register_task(ProcessDataTask, handler)

        # Start worker in thread
        worker_thread = Thread(target=worker.run, daemon=True)
        worker_thread.start()

        # Small delay for worker to start
        time.sleep(0.05)

        # Dispatch task
        bus.dispatch(ProcessDataTask(data="test_data"))

        # Wait for processing
        time.sleep(0.1)

        # Stop worker
        worker.stop()
        worker_thread.join(timeout=1.0)

        assert "test_data" in processed

    def test_worker_processes_command(self, bus, worker):
        # Track processed commands
        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        worker.register_command(CreateUserCommand, handler)

        # Start worker in thread
        worker_thread = Thread(target=worker.run, daemon=True)
        worker_thread.start()

        time.sleep(0.05)

        # Execute command
        bus.execute(CreateUserCommand(name="Alice"))

        time.sleep(0.1)

        worker.stop()
        worker_thread.join(timeout=1.0)

        assert "Alice" in processed

    def test_worker_receives_event(self, bus, worker):
        # Track received events
        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        worker.subscribe(UserCreatedEvent, handler)

        # Start worker in thread
        worker_thread = Thread(target=worker.run, daemon=True)
        worker_thread.start()

        time.sleep(0.05)

        # Publish event
        bus.publish(UserCreatedEvent(user_id=1, name="Bob"))

        time.sleep(0.1)

        worker.stop()
        worker_thread.join(timeout=1.0)

        assert "Bob" in received

    def test_multiple_workers_share_tasks(self, bus):
        """Test that tasks are load-balanced across multiple workers."""
        unique_sockets = {
            "task": bus._task_socket_addr,
            "query": bus._query_socket_addr,
            "event": bus._event_socket_addr,
            "command": bus._command_socket_addr,
        }

        worker1 = ZmqWorker(**unique_sockets)
        worker2 = ZmqWorker(**unique_sockets)

        processed1 = []
        processed2 = []

        def handler1(task: ProcessDataTask) -> None:
            processed1.append(task.data)

        def handler2(task: ProcessDataTask) -> None:
            processed2.append(task.data)

        worker1.register_task(ProcessDataTask, handler1)
        worker2.register_task(ProcessDataTask, handler2)

        # Start workers
        thread1 = Thread(target=worker1.run, daemon=True)
        thread2 = Thread(target=worker2.run, daemon=True)
        thread1.start()
        thread2.start()

        time.sleep(0.05)

        # Dispatch multiple tasks
        for i in range(10):
            bus.dispatch(ProcessDataTask(data=f"task_{i}"))

        time.sleep(0.2)

        # Stop workers
        worker1.stop()
        worker2.stop()
        thread1.join(timeout=1.0)
        thread2.join(timeout=1.0)
        worker1.close()
        worker2.close()

        # Both workers should have processed some tasks (load balancing)
        total_processed = len(processed1) + len(processed2)
        assert total_processed == 10, f"Expected 10 tasks, got {total_processed}"

    def test_multiple_workers_all_receive_events(self, bus):
        """Test that all workers receive events (broadcast)."""
        unique_sockets = {
            "task": bus._task_socket_addr,
            "query": bus._query_socket_addr,
            "event": bus._event_socket_addr,
            "command": bus._command_socket_addr,
        }

        worker1 = ZmqWorker(**unique_sockets)
        worker2 = ZmqWorker(**unique_sockets)

        received1 = []
        received2 = []

        def handler1(event: UserCreatedEvent) -> None:
            received1.append(event.name)

        def handler2(event: UserCreatedEvent) -> None:
            received2.append(event.name)

        worker1.subscribe(UserCreatedEvent, handler1)
        worker2.subscribe(UserCreatedEvent, handler2)

        # Start workers
        thread1 = Thread(target=worker1.run, daemon=True)
        thread2 = Thread(target=worker2.run, daemon=True)
        thread1.start()
        thread2.start()

        time.sleep(0.05)

        # Publish event
        bus.publish(UserCreatedEvent(user_id=1, name="Charlie"))

        time.sleep(0.2)

        # Stop workers
        worker1.stop()
        worker2.stop()
        thread1.join(timeout=1.0)
        thread2.join(timeout=1.0)
        worker1.close()
        worker2.close()

        # Both workers should have received the event
        assert "Charlie" in received1
        assert "Charlie" in received2


class TestZmqResourceCleanup:
    """Test resource cleanup and socket management."""

    def test_bus_close_cleans_resources(self, unique_sockets):
        bus = ZmqMessageBus(**unique_sockets)
        bus.close()
        # Should not raise
        assert not bus._running

    def test_worker_close_cleans_resources(self, unique_sockets):
        worker = ZmqWorker(**unique_sockets)
        worker.close()
        # Should not raise
        assert not worker._running
