"""Async Redis Streams–based message bus for distributed communication."""

# mypy: disable-error-code="import-not-found, no-untyped-call, attr-defined"

import asyncio
import dataclasses
import json
import logging
import socket
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from typing import Any, TypeVar

from message_bus.ports import (
    AsyncMessageBus,
    Command,
    Event,
    Query,
    Task,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

try:
    import redis.asyncio as aioredis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import ResponseError as RedisResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError
except ImportError:
    aioredis = None  # type: ignore[assignment]
    RedisConnectionError = OSError  # type: ignore[assignment, misc]
    RedisTimeoutError = TimeoutError  # type: ignore[assignment, misc]
    RedisResponseError = Exception  # type: ignore[assignment, misc]


# ---------------------------------------------------------------------------
# Type registry
# ---------------------------------------------------------------------------


def _type_key(cls: type) -> str:
    """Return the fully-qualified name used as the JSON ``__type__`` marker."""
    return f"{cls.__module__}.{cls.__qualname__}"


class TypeRegistry:
    """Explicit type registry mapping fully-qualified names to Python classes.

    Required for JSON deserialization. Register all message types before
    dispatching or consuming.

    Example::

        registry = TypeRegistry()
        registry.register(MyCommand, MyEvent, MyTask)
    """

    def __init__(self) -> None:
        self._registry: dict[str, type] = {}

    def register(self, *classes: type) -> "TypeRegistry":
        """Register one or more message classes. Returns *self* for chaining."""
        for cls in classes:
            self._registry[_type_key(cls)] = cls
        return self

    def lookup(self, type_key: str) -> type | None:
        """Return the class for *type_key*, or ``None`` if not registered."""
        return self._registry.get(type_key)


# ---------------------------------------------------------------------------
# JSON serializer
# ---------------------------------------------------------------------------


class AsyncJsonSerializer:
    """JSON serializer with explicit type registry and special-type support.

    Handles :class:`~decimal.Decimal`, :class:`~datetime.datetime`, and
    :class:`~uuid.UUID` roundtrips via inline JSON markers.
    Pickle is intentionally prohibited.

    The serializer auto-populates its registry when handlers are registered
    on :class:`AsyncRedisMessageBus`; manual registration is only required
    for producer-only processes.

    Usage::

        registry = TypeRegistry()
        registry.register(MyCommand)
        ser = AsyncJsonSerializer(registry)
        raw = ser.dumps(MyCommand(amount=Decimal("9.99")))
        msg = ser.loads(raw)   # → MyCommand(amount=Decimal('9.99'))
    """

    def __init__(self, registry: TypeRegistry | None = None) -> None:
        self.registry: TypeRegistry = registry or TypeRegistry()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(self._encode(obj), ensure_ascii=False).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return self._decode(json.loads(data.decode("utf-8")))

    # ------------------------------------------------------------------
    # Internal encoding / decoding
    # ------------------------------------------------------------------

    def _encode(self, obj: Any) -> Any:
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            fields = {f.name: self._encode(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
            return {"__type__": _type_key(type(obj)), **fields}
        if isinstance(obj, Decimal):
            return {"__decimal__": str(obj)}
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, uuid.UUID):
            return {"__uuid__": str(obj)}
        if isinstance(obj, (list, tuple)):
            return [self._encode(item) for item in obj]
        if isinstance(obj, dict):
            return {str(k): self._encode(v) for k, v in obj.items()}
        return obj

    def _decode(self, obj: Any) -> Any:
        if isinstance(obj, list):
            return [self._decode(item) for item in obj]
        if not isinstance(obj, dict):
            return obj
        if "__type__" in obj:
            cls = self.registry.lookup(obj["__type__"])
            if cls is None:
                raise ValueError(
                    f"Unknown type '{obj['__type__']}'. Register it with TypeRegistry before use."
                )
            return cls(**{k: self._decode(v) for k, v in obj.items() if k != "__type__"})
        if "__decimal__" in obj:
            return Decimal(obj["__decimal__"])
        if "__datetime__" in obj:
            return datetime.fromisoformat(obj["__datetime__"])
        if "__uuid__" in obj:
            return uuid.UUID(obj["__uuid__"])
        return {k: self._decode(v) for k, v in obj.items()}


# ---------------------------------------------------------------------------
# Async Redis message bus
# ---------------------------------------------------------------------------


class AsyncRedisMessageBus(AsyncMessageBus):
    """Async Redis Streams–based message bus.

    Drop-in async replacement for :class:`~message_bus.AsyncLocalMessageBus`
    for distributed / multi-process deployments.

    Architecture
    ------------
    * **Command** – ``XADD`` + ``XREADGROUP`` with a *shared* consumer group
      (competitive consumption: only one worker handles each message).
    * **Event** – ``XADD`` + a *per-subscriber* consumer group (fan-out:
      every subscriber receives every event independently).
    * **Task** – identical to Command.
    * **Query** – not implemented; raises :exc:`NotImplementedError`.
      See SWS2-42.

    Parameters
    ----------
    redis_url:
        Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
    consumer_group:
        **Required (no default).** Name of the shared consumer group used
        for Commands and Tasks. Choose a meaningful, service-specific name.
    app_name:
        Prefix for all Redis stream keys. Defaults to ``"message_bus"``.
    serializer:
        Custom :class:`AsyncJsonSerializer`. A fresh one is created if
        omitted.
    consumer_name:
        Identity of this consumer within its group.
        Auto-generated from ``hostname-<random>`` if omitted.
    _block_ms:
        Milliseconds passed to ``XREADGROUP``'s ``block`` parameter.
        **Testing only** – set to ``0`` when using fakeredis, which does
        not implement async blocking correctly. Defaults to ``100`` ms
        for production use with a real Redis server.
    _redis_client:
        **Testing only.** Inject a pre-built async Redis client
        (e.g. ``fakeredis.aioredis.FakeRedis()``). When provided, the
        *redis_url* is ignored during :meth:`start`.

    Lifecycle
    ---------
    Register all handlers **before** calling :meth:`start`.  Use as an
    async context manager or call :meth:`start` / :meth:`close` explicitly.

    Example::

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost:6379/0",
            consumer_group="order-service",
        )
        bus.register_command(CreateOrderCommand, handle_create_order)
        bus.subscribe(PaymentReceivedEvent, handle_payment)

        async with bus:
            await bus.execute(CreateOrderCommand(order_id="123"))
    """

    __slots__ = (
        "_redis_url",
        "_consumer_group",
        "_app_name",
        "_serializer",
        "_consumer_name",
        "_block_ms",
        "_redis",
        "_running",
        "_consumer_tasks",
        "_command_handlers",
        "_task_handlers",
        "_event_subscriptions",
    )

    def __init__(
        self,
        redis_url: str,
        consumer_group: str,
        app_name: str = "message_bus",
        serializer: AsyncJsonSerializer | None = None,
        consumer_name: str | None = None,
        _block_ms: int = 100,
        _redis_client: Any | None = None,
    ) -> None:
        if aioredis is None:
            raise ImportError(
                "redis package is required for AsyncRedisMessageBus. "
                "Install with: pip install 'redis>=5.0.0'"
            )
        self._redis_url = redis_url
        self._consumer_group = consumer_group
        self._app_name = app_name
        self._serializer: AsyncJsonSerializer = serializer or AsyncJsonSerializer()
        self._consumer_name: str = consumer_name or (
            f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        )
        self._block_ms = _block_ms
        # Injected client takes priority (used in tests with fakeredis)
        self._redis: Any = _redis_client

        # Handler registries
        self._command_handlers: dict[type[Command], Callable[[Command], Awaitable[None]]] = {}
        self._task_handlers: dict[type[Task], Callable[[Task], Awaitable[None]]] = {}
        # (event_type, per-subscriber consumer group, handler)
        self._event_subscriptions: list[
            tuple[type[Event], str, Callable[[Event], Awaitable[None]]]
        ] = []

        # Runtime state
        self._running = False
        self._consumer_tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------
    # Registration (sync) – AsyncHandlerRegistry + AsyncQueryRegistry
    # ------------------------------------------------------------------

    def register_query(
        self,
        query_type: type[Query[T]],
        handler: Callable[[Query[T]], Awaitable[T]],
    ) -> None:
        raise NotImplementedError("AsyncRedisMessageBus does not support Query yet. See SWS2-42.")

    def register_command(
        self,
        command_type: type[Command],
        handler: Callable[[Command], Awaitable[None]],
    ) -> None:
        if command_type in self._command_handlers:
            raise ValueError(f"Command handler already registered for {command_type.__name__}")
        self._serializer.registry.register(command_type)
        self._command_handlers[command_type] = handler

    def subscribe(
        self,
        event_type: type[Event],
        handler: Callable[[Event], Awaitable[None]],
    ) -> None:
        # Each subscription gets its own consumer group → fan-out semantics.
        # Use deterministic index so the group name is stable across restarts.
        sub_index = len(self._event_subscriptions)
        subscriber_group = f"{self._consumer_group}:evt:{event_type.__name__}:{sub_index}"
        self._serializer.registry.register(event_type)
        self._event_subscriptions.append((event_type, subscriber_group, handler))

    def register_task(
        self,
        task_type: type[Task],
        handler: Callable[[Task], Awaitable[None]],
    ) -> None:
        if task_type in self._task_handlers:
            raise ValueError(f"Task handler already registered for {task_type.__name__}")
        self._serializer.registry.register(task_type)
        self._task_handlers[task_type] = handler

    # ------------------------------------------------------------------
    # Dispatch (async) – AsyncMessageDispatcher + AsyncQueryDispatcher
    # ------------------------------------------------------------------

    async def send(self, query: Query[T]) -> T:
        raise NotImplementedError("AsyncRedisMessageBus does not support Query yet. See SWS2-42.")

    async def execute(self, command: Command) -> None:
        """Publish *command* to its Redis stream (competitive consumption)."""
        stream_key = self._stream_key("command", type(command).__name__)
        await self._xadd(stream_key, command)

    async def publish(self, event: Event) -> None:
        """Publish *event* to its Redis stream (fan-out to all subscribers)."""
        stream_key = self._stream_key("event", type(event).__name__)
        await self._xadd(stream_key, event)

    async def dispatch(self, task: Task) -> None:
        """Dispatch *task* to its Redis stream (competitive consumption)."""
        stream_key = self._stream_key("task", type(task).__name__)
        await self._xadd(stream_key, task)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect to Redis and launch background consumer coroutines.

        Must be called before dispatching or consuming messages.
        Register all handlers **before** calling this method.
        Idempotent: calling again while already running is a no-op.
        """
        if self._running:
            return  # Already started – idempotent
        if self._redis is None:
            self._redis = aioredis.from_url(self._redis_url, decode_responses=False)
        self._running = True

        # Single consumer loop for all commands and tasks
        if self._command_handlers or self._task_handlers:
            self._consumer_tasks.append(asyncio.create_task(self._run_command_task_consumer()))

        # Dedicated consumer loop per event subscriber (fan-out)
        for event_type, subscriber_group, handler in self._event_subscriptions:
            stream_key = self._stream_key("event", event_type.__name__)
            self._consumer_tasks.append(
                asyncio.create_task(self._run_event_consumer(stream_key, subscriber_group, handler))
            )

    async def close(self) -> None:
        """Cancel background consumers and close the Redis connection."""
        self._running = False
        for task in self._consumer_tasks:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._consumer_tasks.clear()
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception as e:
                logger.warning("Error closing Redis connection: %s", e)
            self._redis = None

    async def __aenter__(self) -> "AsyncRedisMessageBus":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stream_key(self, prefix: str, name: str) -> str:
        return f"{self._app_name}:{prefix}:{name}"

    async def _xadd(self, stream_key: str, message: Any) -> None:
        """Serialize *message* and append it to *stream_key*."""
        data = self._serializer.dumps(message)
        await self._redis.xadd(stream_key, {"data": data})

    async def _ensure_group(self, stream_key: str, group: str) -> None:
        """Create consumer group (and stream) if they do not yet exist."""
        try:
            await self._redis.xgroup_create(stream_key, group, id="0", mkstream=True)
        except RedisResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise  # Re-raise unexpected errors (ACL violations, stream errors, etc.)

    async def _run_command_task_consumer(self) -> None:
        """Background coroutine: competitive consumption of commands and tasks."""
        streams: list[str] = [
            self._stream_key("command", t.__name__) for t in self._command_handlers
        ] + [self._stream_key("task", t.__name__) for t in self._task_handlers]

        for key in streams:
            await self._ensure_group(key, self._consumer_group)

        retry_delay = 0.1
        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    groupname=self._consumer_group,
                    consumername=self._consumer_name,
                    streams=dict.fromkeys(streams, ">"),
                    count=10,
                    block=self._block_ms,
                )
                if messages:
                    for stream_name_raw, stream_msgs in messages:
                        stream_str = (
                            stream_name_raw.decode()
                            if isinstance(stream_name_raw, bytes)
                            else stream_name_raw
                        )
                        await self._handle_command_task_batch(stream_str, stream_msgs)
                else:
                    # Yield to event loop to avoid busy-wait (critical when block_ms=0)
                    await asyncio.sleep(0)
                retry_delay = 0.1  # Reset on success
            except asyncio.CancelledError:
                return
            except (RedisConnectionError, RedisTimeoutError):
                logger.warning(
                    "Redis connection error in command consumer, reconnecting in %.1fs",
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30.0)
                await self._reconnect()
            except Exception:
                logger.exception("Unexpected error in command consumer")
                await asyncio.sleep(0.1)

    async def _handle_command_task_batch(self, stream_str: str, batch: list[Any]) -> None:
        for message_id, fields in batch:
            try:
                raw: bytes | None = fields.get(b"data") or fields.get("data")
                if raw is None:
                    logger.warning("Message missing 'data' field; skipping id=%s", message_id)
                    continue
                msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())

                if ":command:" in stream_str and isinstance(msg, Command):
                    handler = self._command_handlers.get(type(msg))
                    if handler is not None:
                        await handler(msg)
                        await self._redis.xack(stream_str, self._consumer_group, message_id)
                    else:
                        logger.warning(
                            "No handler for command %s; leaving in PEL for retry",
                            type(msg).__name__,
                        )
                elif ":task:" in stream_str and isinstance(msg, Task):
                    task_handler = self._task_handlers.get(type(msg))
                    if task_handler is not None:
                        await task_handler(msg)
                        await self._redis.xack(stream_str, self._consumer_group, message_id)
                    else:
                        logger.warning(
                            "No handler for task %s; leaving in PEL for retry",
                            type(msg).__name__,
                        )
            except Exception:
                logger.exception("Error processing command/task message id=%s", message_id)

    async def _run_event_consumer(
        self,
        stream_key: str,
        subscriber_group: str,
        handler: Callable[[Event], Awaitable[None]],
    ) -> None:
        """Background coroutine: consume events for one subscriber."""
        await self._ensure_group(stream_key, subscriber_group)

        retry_delay = 0.1
        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    groupname=subscriber_group,
                    consumername=self._consumer_name,
                    streams={stream_key: ">"},
                    count=10,
                    block=self._block_ms,
                )
                if messages:
                    for _, stream_msgs in messages:
                        for message_id, fields in stream_msgs:
                            try:
                                raw = fields.get(b"data") or fields.get("data")
                                if raw is None:
                                    logger.warning(
                                        "Event message missing 'data' field; skipping id=%s",
                                        message_id,
                                    )
                                    continue
                                event = self._serializer.loads(
                                    raw if isinstance(raw, bytes) else raw.encode()
                                )
                                if isinstance(event, Event):
                                    await handler(event)
                                    await self._redis.xack(stream_key, subscriber_group, message_id)
                                else:
                                    logger.warning(
                                        "Received non-Event payload (type=%s); skipping id=%s",
                                        type(event).__name__,
                                        message_id,
                                    )
                            except Exception:
                                logger.exception("Error processing event message id=%s", message_id)
                else:
                    # Yield to event loop to avoid busy-wait (critical when block_ms=0)
                    await asyncio.sleep(0)
                retry_delay = 0.1
            except asyncio.CancelledError:
                return
            except (RedisConnectionError, RedisTimeoutError):
                logger.warning(
                    "Redis connection error in event consumer, reconnecting in %.1fs",
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30.0)
                await self._reconnect()
            except Exception:
                logger.exception("Unexpected error in event consumer")
                await asyncio.sleep(0.1)

    async def _reconnect(self) -> None:
        """Replace the Redis client after a connection failure."""
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception as e:
                logger.warning("Error closing Redis connection during reconnect: %s", e)
        try:
            self._redis = aioredis.from_url(self._redis_url, decode_responses=False)
        except Exception:
            logger.exception("Failed to reconnect to Redis")
