"""ZeroMQ-based message bus for multi-process communication."""

# mypy: disable-error-code="import-not-found, no-untyped-call, attr-defined"

import logging
import os
import pickle
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast

from message_bus.ports import (
    Command,
    Event,
    HandlerRegistry,
    MessageDispatcher,
    Query,
    QueryDispatcher,
    QueryRegistry,
    Task,
)

logger = logging.getLogger(__name__)

try:
    import zmq
except ImportError:
    zmq = None

T = TypeVar("T")


class Serializer(Protocol):
    """Protocol for message serialization."""

    def dumps(self, obj: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...


class PickleSerializer:
    """Pickle-based serializer. WARNING: unsafe with untrusted data.

    Only use between trusted processes on the same machine.
    Pickle deserialization can execute arbitrary code.
    """

    def dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)  # noqa: S301


def _default_socket(name: str, port: int) -> str:
    """
    Generate default socket address based on platform.

    Windows: Uses TCP (IPC not supported)
    Unix: Uses IPC with PID to avoid conflicts
    """
    if sys.platform == "win32":
        return f"tcp://127.0.0.1:{port}"
    return f"ipc:///tmp/message_bus_{name}_{os.getpid()}"


def _validate_socket_addr(addr: str) -> None:
    """Validate ZMQ socket address format.

    Raises:
        ValueError: If address format is invalid or contains path traversal
    """
    if addr.startswith("ipc://"):
        path = addr[6:]
        if not path or ".." in path:
            raise ValueError(f"Invalid IPC socket path: {addr!r}")
    elif addr.startswith("tcp://"):
        parts = addr[6:].split(":")
        if len(parts) != 2 or not parts[1].isdigit():
            raise ValueError(f"Invalid TCP socket address: {addr!r}")
    else:
        raise ValueError(f"Unsupported socket protocol: {addr!r}. Use 'ipc://' or 'tcp://'")


class ZmqMessageBus(QueryDispatcher, QueryRegistry, MessageDispatcher):
    """
    ZeroMQ-based message bus for local multi-process communication.

    Implements QueryDispatcher, QueryRegistry, MessageDispatcher.
    Does NOT implement HandlerRegistry - use ZmqWorker for handler registration.

    WARNING: This implementation uses pickle for serialization.
    Only use between trusted processes. Pickle deserialization can execute arbitrary code.

    Architecture:
    - Task/Command: PUSH/PULL pattern (load-balanced across workers)
    - Query: REQ/REP pattern (synchronous request-response)
    - Event: PUB/SUB pattern (broadcast to all subscribers)

    Sockets used for local communication:
    - Windows: tcp://127.0.0.1:port (IPC not supported)
    - Unix: ipc:///tmp/message_bus_*_<pid>

    Use cases:
    - Multi-process worker pools
    - Local service mesh
    - Process-level isolation

    Limitations:
    - Local machine only
    - pickle serialization (Python-only, security risk with untrusted data)
    - No persistence or durability
    """

    __slots__ = (
        "_context",
        "_task_push_socket",
        "_query_rep_socket",
        "_event_pub_socket",
        "_command_push_socket",
        "_query_handlers",
        "_task_socket_addr",
        "_query_socket_addr",
        "_event_socket_addr",
        "_command_socket_addr",
        "_rep_thread",
        "_running",
        "_handlers_lock",
        "_serializer",
        "_query_timeout_ms",
        "_query_req_socket",
        "_query_lock",
    )

    def __init__(
        self,
        task_socket: str | None = None,
        query_socket: str | None = None,
        event_socket: str | None = None,
        command_socket: str | None = None,
        serializer: Serializer | None = None,
        query_timeout_ms: int = 5000,
    ) -> None:
        if zmq is None:
            raise ImportError(
                "pyzmq is required for ZmqMessageBus. Install with: pip install pyzmq"
            )

        # Validate timeout
        if query_timeout_ms <= 0:
            raise ValueError(f"query_timeout_ms must be > 0, got {query_timeout_ms}")

        self._context: zmq.Context[Any] = zmq.Context()
        self._task_socket_addr = task_socket or _default_socket("task", 5555)
        self._query_socket_addr = query_socket or _default_socket("query", 5556)
        self._event_socket_addr = event_socket or _default_socket("event", 5557)
        self._command_socket_addr = command_socket or _default_socket("command", 5558)

        # Validate socket addresses
        for addr in (
            self._task_socket_addr,
            self._query_socket_addr,
            self._event_socket_addr,
            self._command_socket_addr,
        ):
            _validate_socket_addr(addr)

        # Initialize serializer and timeout
        self._serializer: Serializer = serializer or PickleSerializer()
        self._query_timeout_ms = query_timeout_ms

        # PUSH sockets for distributing tasks/commands
        self._task_push_socket: zmq.Socket[Any] = self._context.socket(zmq.PUSH)
        self._task_push_socket.bind(self._task_socket_addr)

        self._command_push_socket: zmq.Socket[Any] = self._context.socket(zmq.PUSH)
        self._command_push_socket.bind(self._command_socket_addr)

        # REP socket for query responses (runs in separate thread)
        self._query_rep_socket: zmq.Socket[Any] = self._context.socket(zmq.REP)
        self._query_rep_socket.bind(self._query_socket_addr)

        # PUB socket for broadcasting events
        self._event_pub_socket: zmq.Socket[Any] = self._context.socket(zmq.PUB)
        self._event_pub_socket.bind(self._event_socket_addr)

        # REQ socket for sending queries (persistent, thread-safe)
        self._query_lock = threading.Lock()
        self._query_req_socket: zmq.Socket[Any] = self._context.socket(zmq.REQ)
        self._query_req_socket.setsockopt(zmq.RCVTIMEO, self._query_timeout_ms)
        self._query_req_socket.connect(self._query_socket_addr)

        # Query handlers (for REP socket)
        self._query_handlers: dict[type[Query[Any]], Callable[[Query[Any]], Any]] = {}
        self._handlers_lock = threading.Lock()

        # Start REP socket thread
        self._running = True
        self._rep_thread = threading.Thread(target=self._run_rep_loop, daemon=True)
        self._rep_thread.start()

    def _run_rep_loop(self) -> None:
        """Run REP socket loop in background thread."""
        while self._running:
            try:
                # Receive query
                message = self._query_rep_socket.recv(flags=zmq.NOBLOCK)
                try:
                    query = self._serializer.loads(message)
                except Exception as e:
                    logger.error("Failed to deserialize query: %s", e)
                    error_response = {"error": ValueError(f"Deserialization failed: {e}")}
                    self._query_rep_socket.send(self._serializer.dumps(error_response))
                    continue

                # Type validation: ensure deserialized object is a Query
                if not isinstance(query, Query):
                    type_error = TypeError(f"Expected Query, got {type(query).__name__}")
                    self._query_rep_socket.send(self._serializer.dumps({"error": type_error}))
                    continue

                query_type = type(query)

                # Execute handler
                with self._handlers_lock:
                    handler = self._query_handlers.get(query_type)
                if handler is None:
                    # Send error response
                    lookup_error = LookupError(
                        f"No handler registered for query {query_type.__name__}"
                    )
                    self._query_rep_socket.send(self._serializer.dumps({"error": lookup_error}))
                else:
                    try:
                        result = handler(query)
                        self._query_rep_socket.send(self._serializer.dumps({"result": result}))
                    except Exception as e:
                        logger.exception("Query handler failed: %s", e)
                        self._query_rep_socket.send(self._serializer.dumps({"error": e}))
            except zmq.Again:
                # No message available, sleep briefly
                time.sleep(0.001)
            except Exception as e:
                # Log error and continue
                logger.exception("Error in REP loop: %s", e)

    # Registration methods

    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        with self._handlers_lock:
            if query_type in self._query_handlers:
                raise ValueError(f"Query handler already registered for {query_type.__name__}")
            self._query_handlers[query_type] = handler

    # Dispatch methods

    def send(self, query: Query[T]) -> T:
        with self._query_lock:
            self._query_req_socket.send(self._serializer.dumps(query))
            try:
                response_data = self._query_req_socket.recv()
            except zmq.Again as e:
                # Timeout: REQ socket is now in bad state, reset it
                self._reset_query_socket()
                raise TimeoutError(f"Query timed out after {self._query_timeout_ms}ms") from e

            response = self._serializer.loads(response_data)

            if not isinstance(response, dict):
                raise TypeError(
                    f"Invalid response format: expected dict, got {type(response).__name__}"
                )

            if "error" in response:
                error = response["error"]
                if isinstance(error, Exception):
                    raise error
                raise RuntimeError(f"Query failed: {error!r}")
            if "result" not in response:
                raise RuntimeError(
                    f"Malformed response: missing 'result' key. Got keys: {list(response.keys())}"
                )
            return cast(T, response["result"])

    def _reset_query_socket(self) -> None:
        """Reset REQ socket after timeout (REQ/REP state machine requires this)."""
        self._query_req_socket.close()
        self._query_req_socket = self._context.socket(zmq.REQ)
        self._query_req_socket.setsockopt(zmq.RCVTIMEO, self._query_timeout_ms)
        self._query_req_socket.connect(self._query_socket_addr)

    def execute(self, command: Command) -> None:
        self._command_push_socket.send(self._serializer.dumps(("command", command)))

    def publish(self, event: Event) -> None:
        event_type = type(event).__name__
        self._event_pub_socket.send_multipart([event_type.encode(), self._serializer.dumps(event)])

    def dispatch(self, task: Task) -> None:
        self._task_push_socket.send(self._serializer.dumps(("task", task)))

    def close(self) -> None:
        """Close all sockets and terminate context."""
        self._running = False
        self._rep_thread.join(timeout=1.0)

        self._task_push_socket.close()
        self._command_push_socket.close()
        self._query_req_socket.close()
        self._query_rep_socket.close()
        self._event_pub_socket.close()
        self._context.term()

    def __enter__(self) -> "ZmqMessageBus":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


class ZmqWorker(HandlerRegistry):
    """
    Worker process for ZmqMessageBus.

    Implements HandlerRegistry for command/event/task handlers.

    WARNING: This implementation uses pickle for serialization.
    Only use between trusted processes. Pickle deserialization can execute arbitrary code.

    Responsibilities:
    - Pull tasks/commands from PUSH/PULL sockets
    - Subscribe to events from PUB/SUB socket
    - Execute registered handlers
    """

    __slots__ = (
        "_context",
        "_task_pull_socket",
        "_command_pull_socket",
        "_event_sub_socket",
        "_task_handlers",
        "_command_handlers",
        "_event_subscribers",
        "_running",
        "_serializer",
    )

    def __init__(
        self,
        task_socket: str | None = None,
        event_socket: str | None = None,
        command_socket: str | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        if zmq is None:
            raise ImportError("pyzmq is required for ZmqWorker. Install with: pip install pyzmq")

        self._context: zmq.Context[Any] = zmq.Context()

        # Use defaults or provided sockets
        task_addr = task_socket or _default_socket("task", 5555)
        event_addr = event_socket or _default_socket("event", 5557)
        command_addr = command_socket or _default_socket("command", 5558)

        # Validate socket addresses
        for addr in (task_addr, event_addr, command_addr):
            _validate_socket_addr(addr)

        # Initialize serializer
        self._serializer: Serializer = serializer or PickleSerializer()

        # PULL sockets for receiving tasks/commands
        self._task_pull_socket: zmq.Socket[Any] = self._context.socket(zmq.PULL)
        self._task_pull_socket.connect(task_addr)

        self._command_pull_socket: zmq.Socket[Any] = self._context.socket(zmq.PULL)
        self._command_pull_socket.connect(command_addr)

        # SUB socket for receiving events
        self._event_sub_socket: zmq.Socket[Any] = self._context.socket(zmq.SUB)
        self._event_sub_socket.connect(event_addr)

        # Handlers
        self._task_handlers: dict[type[Task], Callable[[Task], None]] = {}
        self._command_handlers: dict[type[Command], Callable[[Command], None]] = {}
        self._event_subscribers: dict[type[Event], list[Callable[[Event], None]]] = defaultdict(
            list
        )

        self._running = False

    # Registration methods

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        if task_type in self._task_handlers:
            raise ValueError(f"Task handler already registered for {task_type.__name__}")
        self._task_handlers[task_type] = handler

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        if command_type in self._command_handlers:
            raise ValueError(f"Command handler already registered for {command_type.__name__}")
        self._command_handlers[command_type] = handler

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        # Subscribe to event type
        event_type_name = event_type.__name__.encode()
        self._event_sub_socket.setsockopt(zmq.SUBSCRIBE, event_type_name)
        self._event_subscribers[event_type].append(handler)

    def run(self) -> None:
        """Start worker loop. Blocks until stop() is called."""
        self._running = True
        poller = zmq.Poller()
        poller.register(self._task_pull_socket, zmq.POLLIN)
        poller.register(self._command_pull_socket, zmq.POLLIN)
        poller.register(self._event_sub_socket, zmq.POLLIN)

        while self._running:
            try:
                socks = dict(poller.poll(timeout=100))

                # Handle tasks
                if self._task_pull_socket in socks:
                    message = self._task_pull_socket.recv()
                    try:
                        msg_type, task = self._serializer.loads(message)
                    except Exception as e:
                        logger.error("Failed to deserialize task message: %s", e)
                        continue
                    if msg_type == "task":
                        # Type validation: ensure task is a Task instance
                        if not isinstance(task, Task):
                            logger.warning("Invalid task type received: %s", type(task).__name__)
                            continue
                        task_type = type(task)
                        task_handler = self._task_handlers.get(task_type)
                        if task_handler:
                            task_handler(task)
                        else:
                            logger.error(
                                "No handler registered for task type: %s. "
                                "Message will be dropped. Register handler with register_task().",
                                task_type.__name__,
                            )

                # Handle commands
                if self._command_pull_socket in socks:
                    message = self._command_pull_socket.recv()
                    try:
                        msg_type, command = self._serializer.loads(message)
                    except Exception as e:
                        logger.error("Failed to deserialize command message: %s", e)
                        continue
                    if msg_type == "command":
                        # Type validation: ensure command is a Command instance
                        if not isinstance(command, Command):
                            logger.warning(
                                "Invalid command type received: %s", type(command).__name__
                            )
                            continue
                        command_type = type(command)
                        cmd_handler = self._command_handlers.get(command_type)
                        if cmd_handler:
                            cmd_handler(command)
                        else:
                            logger.error(
                                "No handler registered for command type: %s. "
                                "Message will be dropped. "
                                "Register handler with register_command().",
                                command_type.__name__,
                            )

                # Handle events
                if self._event_sub_socket in socks:
                    event_type_name, event_data = self._event_sub_socket.recv_multipart()
                    try:
                        event = self._serializer.loads(event_data)
                    except Exception as e:
                        logger.error("Failed to deserialize event: %s", e)
                        continue
                    # Type validation: ensure event is an Event instance
                    if not isinstance(event, Event):
                        logger.warning("Invalid event type received: %s", type(event).__name__)
                        continue
                    event_type = type(event)
                    event_handlers = self._event_subscribers.get(event_type)
                    if event_handlers:
                        for event_handler in event_handlers:
                            try:
                                event_handler(event)
                            except Exception as e:
                                logger.exception(
                                    "Event handler failed for %s: %s", event_type.__name__, e
                                )
                    else:
                        # Events can have no subscribers - this is normal behavior
                        logger.debug("No subscribers for event type: %s", event_type.__name__)
            except Exception as e:
                # Log error and continue
                logger.exception("Error in worker loop: %s", e)

    def stop(self) -> None:
        """Stop worker loop."""
        self._running = False

    def close(self) -> None:
        """Close all sockets and terminate context."""
        self.stop()
        self._task_pull_socket.close()
        self._command_pull_socket.close()
        self._event_sub_socket.close()
        self._context.term()

    def __enter__(self) -> "ZmqWorker":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
