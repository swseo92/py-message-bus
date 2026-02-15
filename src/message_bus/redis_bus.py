"""Redis-based message bus for distributed communication."""

# mypy: disable-error-code="import-not-found, no-untyped-call, attr-defined"

import json
import logging
import threading
import time
import uuid
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
    import redis
    from redis import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError
except ImportError:
    redis = None  # type: ignore[assignment]
    Redis = None  # type: ignore[assignment,misc]
    RedisConnectionError = None  # type: ignore[assignment,misc]
    RedisTimeoutError = None  # type: ignore[assignment,misc]

T = TypeVar("T")


class Serializer(Protocol):
    """Protocol for message serialization."""

    def dumps(self, obj: Any) -> bytes: ...

    def loads(self, data: bytes) -> Any: ...


class JsonSerializer:
    """JSON-based serializer for cross-language compatibility."""

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj, default=str).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


def _default_stream_key(prefix: str, name: str) -> str:
    """Generate default Redis stream key."""
    return f"message_bus:{prefix}:{name}"


def _serialize_message(msg: Any, serializer: Serializer) -> dict[bytes, bytes]:
    """Serialize a message to Redis stream field-value pairs."""
    data = serializer.dumps(msg)
    return {
        b"type": type(msg).__name__.encode("utf-8"),
        b"module": type(msg).__module__.encode("utf-8"),
        b"data": data,
    }


def _deserialize_message(fields: dict[bytes, bytes], serializer: Serializer) -> Any:
    """Deserialize a message from Redis stream fields."""
    try:
        data = fields[b"data"]
        return serializer.loads(data)
    except KeyError as e:
        raise ValueError(f"Missing 'data' field in message: {fields!r}") from e


class RedisMessageBus(QueryDispatcher, QueryRegistry, MessageDispatcher, HandlerRegistry):
    """
    Redis-based message bus for distributed communication.

    Implements all MessageBus interfaces for drop-in compatibility with LocalMessageBus.

    Architecture:
    - Query: XADD + XREADGROUP with response stream (synchronous request-response)
    - Command: XADD + consumer group (load-balanced)
    - Task: XADD + consumer group (load-balanced)
    - Event: XADD + PUB/SUB (fan-out to all subscribers)

    Stream key format:
    - message_bus:query:{name} - Query streams (one per query type)
    - message_bus:command:{name} - Command streams
    - message_bus:task:{name} - Task streams
    - message_bus:event:{name} - Event notification streams
    - message_bus:response:{correlation_id} - Query response streams (ephemeral)

    Use cases:
    - Drop-in replacement for LocalMessageBus with distributed capability
    - Multi-process/multi-machine communication
    - Message persistence and replay
    - Cross-language integration (via JSON serialization)

    Limitations:
    - Requires Redis server
    - Network latency vs LocalMessageBus
    - JSON serialization (limited to JSON-serializable types)
    """

    __slots__ = (
        "_redis",
        "_app_name",
        "_serializer",
        "_query_handlers",
        "_command_handlers",
        "_task_handlers",
        "_event_subscribers",
        "_handlers_lock",
        "_running",
        "_worker_thread",
        "_pubsub_thread",
        "_pubsub",
        "_query_timeout_sec",
        "_consumer_group",
    )

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        app_name: str = "default",
        serializer: Serializer | None = None,
        query_timeout_sec: float = 5.0,
        consumer_group: str = "workers",
    ) -> None:
        if redis is None:
            raise ImportError(
                "redis is required for RedisMessageBus. Install with: pip install redis"
            )

        # Validate timeout
        if query_timeout_sec <= 0:
            raise ValueError(f"query_timeout_sec must be > 0, got {query_timeout_sec}")

        self._redis: Redis = Redis.from_url(
            redis_url,
            decode_responses=False,  # We need bytes for serialization
        )

        self._app_name = app_name
        self._serializer: Serializer = serializer or JsonSerializer()
        self._query_timeout_sec = query_timeout_sec
        self._consumer_group = consumer_group

        # Handlers
        self._query_handlers: dict[type[Query[Any]], Callable[[Query[Any]], Any]] = {}
        self._command_handlers: dict[type[Command], Callable[[Command], None]] = {}
        self._task_handlers: dict[type[Task], Callable[[Task], None]] = {}
        self._event_subscribers: dict[type[Event], list[Callable[[Event], None]]] = defaultdict(
            list
        )
        self._handlers_lock = threading.Lock()

        # State
        self._running = True
        self._worker_thread: threading.Thread | None = None
        self._pubsub_thread: threading.Thread | None = None
        self._pubsub: Any | None = None

        # Start background threads
        self._start_worker()

    def _stream_key(self, prefix: str, name: str) -> str:
        """Generate stream key for this app."""
        return f"{self._app_name}:{prefix}:{name}"

    def _response_key(self, correlation_id: str) -> str:
        """Generate response stream key for a query."""
        return f"{self._app_name}:response:{correlation_id}"

    def _start_worker(self) -> None:
        """Start background worker for stream consumption."""
        # Worker thread for commands/tasks
        self._worker_thread = threading.Thread(
            target=self._run_stream_worker, daemon=True, name="RedisStreamWorker"
        )
        self._worker_thread.start()

        # Pub/Sub thread for events
        self._pubsub_thread = threading.Thread(
            target=self._run_pubsub_worker, daemon=True, name="RedisPubSubWorker"
        )
        self._pubsub_thread.start()

    def _run_stream_worker(self) -> None:
        """Consume command/task streams in background."""
        # Create consumer for command/task streams
        streams_to_watch: list[str] = []

        with self._handlers_lock:
            # Get registered command/task stream names
            for cmd_type in self._command_handlers:
                streams_to_watch.append(self._stream_key("command", cmd_type.__name__))
            for task_type in self._task_handlers:
                streams_to_watch.append(self._stream_key("task", task_type.__name__))

        if not streams_to_watch:
            # No handlers registered yet, wait and retry
            time.sleep(0.1)
            return

        # Ensure consumer group exists for each stream
        for stream in streams_to_watch:
            try:
                self._redis.xgroup_create(stream, self._consumer_group, id="0", mkstream=True)
            except Exception:
                # Group already exists
                pass

        # Main consumption loop
        while self._running:
            try:
                # Read from streams (block for 100ms)
                streams_dict: dict[str, str] = dict.fromkeys(streams_to_watch, ">")
                if not streams_dict:
                    time.sleep(0.1)
                    continue

                messages = self._redis.xreadgroup(
                    groupname=self._consumer_group,
                    consumername=f"worker-{threading.get_ident()}",
                    streams={
                        k.encode() if isinstance(k, str) else k: v for k, v in streams_dict.items()
                    },
                    count=1,
                    block=100,
                )

                if not messages:
                    continue

                # Type: ignore for redis-py sync/async union type
                for stream, stream_messages in messages:  # type: ignore[union-attr]
                    self._process_stream_message(stream, stream_messages)

            except (RedisConnectionError, RedisTimeoutError):
                logger.warning("Redis connection error in stream worker, retrying...")
                time.sleep(0.5)
            except Exception as e:
                logger.exception("Error in stream worker: %s", e)
                time.sleep(0.1)

    def _process_stream_message(self, stream: bytes, messages: list[Any]) -> None:
        """Process messages from a stream."""
        stream_name = stream.decode("utf-8") if isinstance(stream, bytes) else stream

        for message_id, fields in messages:
            try:
                # Deserialize message
                msg = _deserialize_message(fields, self._serializer)

                # Determine message type from stream name
                if ":command:" in stream_name and isinstance(msg, Command):
                    cmd_handler = self._command_handlers.get(type(msg))
                    if cmd_handler:
                        cmd_handler(msg)
                    else:
                        logger.error(
                            "No handler registered for command type: %s",
                            type(msg).__name__,
                        )
                elif ":task:" in stream_name and isinstance(msg, Task):
                    task_handler = self._task_handlers.get(type(msg))
                    if task_handler:
                        task_handler(msg)
                    else:
                        logger.error(
                            "No handler registered for task type: %s",
                            type(msg).__name__,
                        )

                # Acknowledge message
                self._redis.xack(stream_name, self._consumer_group, message_id)

            except Exception as e:
                logger.exception("Error processing stream message: %s", e)

    def _run_pubsub_worker(self) -> None:
        """Consume event pub/sub in background."""
        self._pubsub = self._redis.pubsub()

        # Subscribe to event channels
        with self._handlers_lock:
            for event_type in self._event_subscribers:
                channel = self._stream_key("event", event_type.__name__)
                self._pubsub.subscribe(channel)

        while self._running:
            try:
                message = self._pubsub.get_message(timeout=0.1)
                if message and message["type"] == "message":
                    try:
                        # Decode event data
                        data = message["data"]
                        if isinstance(data, bytes):
                            data = {"data": data}
                        elif isinstance(data, str):
                            data = {"data": data.encode("utf-8")}
                        else:
                            continue

                        event = _deserialize_message(data, self._serializer)

                        if isinstance(event, Event):
                            handlers = self._event_subscribers.get(type(event), [])
                            for handler in handlers:
                                try:
                                    handler(event)
                                except Exception as e:
                                    logger.exception(
                                        "Event handler failed for %s: %s",
                                        type(event).__name__,
                                        e,
                                    )
                    except Exception as e:
                        logger.exception("Error processing pubsub message: %s", e)

            except (RedisConnectionError, RedisTimeoutError):
                logger.warning("Redis connection error in pubsub worker, retrying...")
                time.sleep(0.5)
            except Exception as e:
                logger.exception("Error in pubsub worker: %s", e)
                time.sleep(0.1)

    # QueryRegistry methods

    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        with self._handlers_lock:
            if query_type in self._query_handlers:
                raise ValueError(f"Query handler already registered for {query_type.__name__}")
            self._query_handlers[query_type] = handler

            # Create query stream
            stream_key = self._stream_key("query", query_type.__name__)
            try:
                self._redis.xgroup_create(stream_key, self._consumer_group, id="0", mkstream=True)
            except Exception:
                # Group already exists or stream exists
                pass

    # HandlerRegistry methods

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        with self._handlers_lock:
            if command_type in self._command_handlers:
                raise ValueError(f"Command handler already registered for {command_type.__name__}")
            self._command_handlers[command_type] = handler

            # Create command stream
            stream_key = self._stream_key("command", command_type.__name__)
            try:
                self._redis.xgroup_create(stream_key, self._consumer_group, id="0", mkstream=True)
            except Exception:
                pass

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        with self._handlers_lock:
            self._event_subscribers[event_type].append(handler)

            # Subscribe to pub/sub channel
            if self._pubsub:
                channel = self._stream_key("event", event_type.__name__)
                self._pubsub.subscribe(channel)

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        with self._handlers_lock:
            if task_type in self._task_handlers:
                raise ValueError(f"Task handler already registered for {task_type.__name__}")
            self._task_handlers[task_type] = handler

            # Create task stream
            stream_key = self._stream_key("task", task_type.__name__)
            try:
                self._redis.xgroup_create(stream_key, self._consumer_group, id="0", mkstream=True)
            except Exception:
                pass

    # QueryDispatcher method

    def send(self, query: Query[T]) -> T:
        """Send query and wait for response."""
        correlation_id = str(uuid.uuid4())
        response_key = self._response_key(correlation_id)

        # Create response stream
        try:
            self._redis.xgroup_create(response_key, "response", id="0", mkstream=True)
        except Exception:
            pass

        # Send query to stream
        query_stream = self._stream_key("query", type(query).__name__)
        fields = _serialize_message(query, self._serializer)
        fields[b"correlation_id"] = correlation_id.encode("utf-8")
        fields[b"response_stream"] = response_key.encode("utf-8")

        self._redis.xadd(
            query_stream,
            {
                k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                for k, v in fields.items()
            },
        )

        # Wait for response
        start_time = time.time()
        while time.time() - start_time < self._query_timeout_sec:
            try:
                messages = self._redis.xreadgroup(
                    groupname="response",
                    consumername="query-client",
                    streams={response_key: "0"},
                    count=1,
                    block=100,
                )

                if messages:
                    for _, stream_messages in messages:  # type: ignore[union-attr]
                        for message_id, response_fields in stream_messages:
                            # Ack and delete
                            self._redis.xack(response_key, "response", message_id)
                            self._redis.xdel(response_key, message_id)

                            # Parse response
                            if b"error" in response_fields:
                                error_data = response_fields[b"error"]
                                error = self._serializer.loads(error_data)
                                if isinstance(error, Exception):
                                    raise error
                                raise RuntimeError(f"Query failed: {error!r}")

                            if b"result" in response_fields:
                                result_data = response_fields[b"result"]
                                return cast(T, self._serializer.loads(result_data))

                            raise ValueError(f"Malformed response: {response_fields!r}")

                time.sleep(0.01)
            except (RedisConnectionError, RedisTimeoutError):
                logger.warning("Redis connection error during query, retrying...")
                time.sleep(0.1)

        # Timeout
        raise TimeoutError(f"Query timed out after {self._query_timeout_sec} seconds")

    # MessageDispatcher methods

    def execute(self, command: Command) -> None:
        """Execute a command."""
        stream_key = self._stream_key("command", type(command).__name__)
        fields = _serialize_message(command, self._serializer)
        self._redis.xadd(
            stream_key,
            {
                k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                for k, v in fields.items()
            },
        )

    def publish(self, event: Event) -> None:
        """Publish an event to all subscribers."""
        # Add to event stream (for persistence/history)
        stream_key = self._stream_key("event", type(event).__name__)
        fields = _serialize_message(event, self._serializer)
        self._redis.xadd(
            stream_key,
            {
                k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                for k, v in fields.items()
            },
        )

        # Also publish to pub/sub for immediate fan-out
        channel = self._stream_key("event", type(event).__name__)
        event_data = self._serializer.dumps(event)
        self._redis.publish(channel, event_data)

    def dispatch(self, task: Task) -> None:
        """Dispatch a task to one worker."""
        stream_key = self._stream_key("task", type(task).__name__)
        fields = _serialize_message(task, self._serializer)
        self._redis.xadd(stream_key, fields)  # type: ignore[arg-type]

    def close(self) -> None:
        """Close connections and stop workers."""
        self._running = False

        # Close pubsub
        if self._pubsub:
            try:
                self._pubsub.close()
            except Exception:
                pass

        # Close Redis connection
        try:
            self._redis.close()
        except Exception:
            pass

        # Wait for threads
        if self._worker_thread:
            self._worker_thread.join(timeout=1.0)
        if self._pubsub_thread:
            self._pubsub_thread.join(timeout=1.0)

    def __enter__(self) -> "RedisMessageBus":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


class RedisWorker(HandlerRegistry):
    """
    Standalone worker for RedisMessageBus.

    Implements HandlerRegistry for command/event/task handlers.
    Can be run in separate processes for distributed processing.

    Responsibilities:
    - Consume from command/task streams (consumer group)
    - Subscribe to event pub/sub channels
    - Execute registered handlers
    """

    __slots__ = (
        "_redis",
        "_app_name",
        "_serializer",
        "_command_handlers",
        "_task_handlers",
        "_event_subscribers",
        "_handlers_lock",
        "_running",
        "_pubsub",
        "_consumer_name",
        "_consumer_group",
    )

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        app_name: str = "default",
        serializer: Serializer | None = None,
        consumer_group: str = "workers",
        consumer_name: str | None = None,
    ) -> None:
        if redis is None:
            raise ImportError("redis is required for RedisWorker. Install with: pip install redis")

        self._redis: Redis = Redis.from_url(
            redis_url,
            decode_responses=False,
        )

        self._app_name = app_name
        self._serializer: Serializer = serializer or JsonSerializer()
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name or f"worker-{uuid.uuid4()}"

        # Handlers
        self._command_handlers: dict[type[Command], Callable[[Command], None]] = {}
        self._task_handlers: dict[type[Task], Callable[[Task], None]] = {}
        self._event_subscribers: dict[type[Event], list[Callable[[Event], None]]] = defaultdict(
            list
        )
        self._handlers_lock = threading.Lock()

        # State
        self._running = False
        self._pubsub: Any | None = None

    def _stream_key(self, prefix: str, name: str) -> str:
        """Generate stream key for this app."""
        return f"{self._app_name}:{prefix}:{name}"

    # HandlerRegistry methods

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        if command_type in self._command_handlers:
            raise ValueError(f"Command handler already registered for {command_type.__name__}")
        self._command_handlers[command_type] = handler

        # Create stream if not exists
        stream_key = self._stream_key("command", command_type.__name__)
        try:
            self._redis.xgroup_create(stream_key, self._consumer_group, id="0", mkstream=True)
        except Exception:
            pass

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        if task_type in self._task_handlers:
            raise ValueError(f"Task handler already registered for {task_type.__name__}")
        self._task_handlers[task_type] = handler

        # Create stream if not exists
        stream_key = self._stream_key("task", task_type.__name__)
        try:
            self._redis.xgroup_create(stream_key, self._consumer_group, id="0", mkstream=True)
        except Exception:
            pass

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        with self._handlers_lock:
            self._event_subscribers[event_type].append(handler)

    def run(self) -> None:
        """Start worker loop. Blocks until stop() is called."""
        self._running = True

        # Setup pubsub for events
        self._pubsub = self._redis.pubsub()
        with self._handlers_lock:
            for event_type in self._event_subscribers:
                channel = self._stream_key("event", event_type.__name__)
                self._pubsub.subscribe(channel)

        # Collect streams to watch
        streams_to_watch: list[str] = []
        with self._handlers_lock:
            for cmd_type in self._command_handlers:
                streams_to_watch.append(self._stream_key("command", cmd_type.__name__))
            for task_type in self._task_handlers:
                streams_to_watch.append(self._stream_key("task", task_type.__name__))

        # Ensure consumer groups exist
        for stream in streams_to_watch:
            try:
                self._redis.xgroup_create(stream, self._consumer_group, id="0", mkstream=True)
            except Exception:
                pass

        # Main loop
        while self._running:
            try:
                # Process pub/sub messages (non-blocking)
                self._process_pubsub()

                # Process stream messages
                if streams_to_watch:
                    self._process_streams(streams_to_watch)

                time.sleep(0.01)

            except (RedisConnectionError, RedisTimeoutError):
                logger.warning("Redis connection error in worker, retrying...")
                time.sleep(0.5)
            except Exception as e:
                logger.exception("Error in worker loop: %s", e)
                time.sleep(0.1)

    def _process_streams(self, streams: list[str]) -> None:
        """Process stream messages."""
        streams_dict: dict[str, str] = dict.fromkeys(streams, ">")
        messages = self._redis.xreadgroup(
            groupname=self._consumer_group,
            consumername=self._consumer_name,
            streams=streams_dict,  # type: ignore[arg-type]
            count=10,
            block=100,
        )

        if not messages:
            return

        for stream, stream_messages in messages:  # type: ignore[union-attr]
            stream_name = stream.decode("utf-8") if isinstance(stream, bytes) else stream

            for message_id, fields in stream_messages:
                try:
                    msg = _deserialize_message(fields, self._serializer)

                    if ":command:" in stream_name and isinstance(msg, Command):
                        cmd_handler = self._command_handlers.get(type(msg))
                        if cmd_handler:
                            cmd_handler(msg)
                    elif ":task:" in stream_name and isinstance(msg, Task):
                        task_handler = self._task_handlers.get(type(msg))
                        if task_handler:
                            task_handler(msg)

                    # Acknowledge
                    self._redis.xack(stream_name, self._consumer_group, message_id)

                except Exception as e:
                    logger.exception("Error processing stream message: %s", e)

    def _process_pubsub(self) -> None:
        """Process pub/sub messages."""
        if not self._pubsub:
            return

        message = self._pubsub.get_message(timeout=0)
        if not message or message["type"] != "message":
            return

        try:
            data = message["data"]
            if not isinstance(data, bytes):
                return

            event = self._serializer.loads(data)
            if not isinstance(event, Event):
                return

            handlers = self._event_subscribers.get(type(event), [])
            for handler in handlers:
                try:
                    handler(event)
                except Exception as e:
                    logger.exception(
                        "Event handler failed for %s: %s",
                        type(event).__name__,
                        e,
                    )
        except Exception as e:
            logger.exception("Error processing pubsub message: %s", e)

    def stop(self) -> None:
        """Stop worker loop."""
        self._running = False

    def close(self) -> None:
        """Close connections."""
        self.stop()
        if self._pubsub:
            try:
                self._pubsub.close()
            except Exception:
                pass
        try:
            self._redis.close()
        except Exception:
            pass

    def __enter__(self) -> "RedisWorker":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
