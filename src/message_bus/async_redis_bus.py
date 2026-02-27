"""Async Redis Streams–based message bus for distributed communication."""

# mypy: disable-error-code="import-not-found, no-untyped-call, attr-defined"

import asyncio
import dataclasses
import json
import logging
import socket
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypeVar

from message_bus.dead_letter import DeadLetterStore
from message_bus.ports import (
    AsyncMessageBus,
    Command,
    Event,
    Query,
    Task,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class MaxRetriesExceededError(Exception):
    """Raised when a message exceeds the maximum number of delivery attempts."""

    def __init__(self, message_id: str, delivery_count: int, max_retry: int) -> None:
        super().__init__(
            f"Message {message_id} exceeded max retries "
            f"(delivery_count={delivery_count}, max_retry={max_retry})"
        )
        self.message_id = message_id
        self.delivery_count = delivery_count
        self.max_retry = max_retry


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
    * **Query** – ``XADD`` to a query stream + ``XREADGROUP`` (competitive
      consumption).  Each call generates a unique ``correlation_id``; the
      consumer writes the reply to a per-request reply stream; the caller
      polls until the reply arrives or *query_reply_timeout* elapses.

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
    query_reply_timeout:
        Maximum seconds to wait for a query reply before raising
        :exc:`asyncio.TimeoutError`.  Defaults to ``30.0``.
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
        "_query_handlers",
        "_query_reply_timeout",
        "_max_retry",
        "_claim_idle_ms",
        "_dead_letter_store",
        "_max_stream_length",
        "_idempotency_ttl",
    )

    def __init__(
        self,
        redis_url: str,
        consumer_group: str,
        app_name: str = "message_bus",
        serializer: AsyncJsonSerializer | None = None,
        consumer_name: str | None = None,
        query_reply_timeout: float = 30.0,
        max_retry: int = 3,
        claim_idle_ms: int = 30_000,
        dead_letter_store: DeadLetterStore | None = None,
        max_stream_length: int = 10_000,
        idempotency_ttl: int = 86400,
        _block_ms: int = 100,
        _redis_client: Any | None = None,
    ) -> None:
        if aioredis is None:
            raise ImportError(
                "redis package is required for AsyncRedisMessageBus. "
                "Install with: pip install 'redis>=5.0.0'"
            )
        if max_stream_length <= 0:
            raise ValueError(
                f"max_stream_length must be a positive integer, got {max_stream_length}"
            )
        self._redis_url = redis_url
        self._consumer_group = consumer_group
        self._app_name = app_name
        self._serializer: AsyncJsonSerializer = serializer or AsyncJsonSerializer()
        self._consumer_name: str = consumer_name or (
            f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        )
        self._max_retry = max_retry
        self._claim_idle_ms = claim_idle_ms
        self._dead_letter_store: DeadLetterStore | None = dead_letter_store
        self._max_stream_length = max_stream_length
        if idempotency_ttl <= 0:
            raise ValueError(f"idempotency_ttl must be a positive integer (got {idempotency_ttl})")
        self._idempotency_ttl = idempotency_ttl
        self._block_ms = _block_ms
        # Injected client takes priority (used in tests with fakeredis)
        self._redis: Any = _redis_client

        self._query_reply_timeout = query_reply_timeout

        # Handler registries
        self._command_handlers: dict[type[Command], Callable[[Command], Awaitable[None]]] = {}
        self._task_handlers: dict[type[Task], Callable[[Task], Awaitable[None]]] = {}
        self._query_handlers: dict[type[Query[Any]], Callable[[Query[Any]], Awaitable[Any]]] = {}
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
        if query_type in self._query_handlers:
            raise ValueError(f"Query handler already registered for {query_type.__name__}")
        self._serializer.registry.register(query_type)
        self._query_handlers[query_type] = handler

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
        """Send *query* and await its reply via a correlation_id–based reply stream.

        A unique reply stream is created per call and cleaned up after the reply
        is received (or on timeout).

        Raises
        ------
        asyncio.TimeoutError
            When no reply arrives within *query_reply_timeout* seconds.
        RuntimeError
            When the handler raises an exception (type and message are preserved
            in the error message).
        """
        correlation_id = uuid.uuid4().hex
        reply_stream = self._stream_key("reply", correlation_id)
        stream_key = self._stream_key("query", type(query).__name__)

        data = self._serializer.dumps(query)
        await self._redis.xadd(
            stream_key,
            {
                "data": data,
                "correlation_id": correlation_id.encode(),
                "reply_stream": reply_stream.encode(),
            },
            maxlen=self._max_stream_length,
            approximate=True,
        )

        loop = asyncio.get_running_loop()
        last_id = "0-0"
        deadline = loop.time() + self._query_reply_timeout
        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Query {type(query).__name__!r} timed out "
                        f"after {self._query_reply_timeout}s"
                    )

                if self._block_ms > 0:
                    block_ms = max(1, int(remaining * 1000))
                    result_msgs = await self._redis.xread(
                        {reply_stream: last_id}, count=1, block=block_ms
                    )
                else:
                    result_msgs = await self._redis.xread({reply_stream: last_id}, count=1)

                if result_msgs:
                    for _, msgs in result_msgs:
                        for msg_id, fields in msgs:
                            last_id = msg_id
                            error_type_raw = fields.get(b"error_type") or fields.get("error_type")
                            if error_type_raw:
                                error_msg_raw = (
                                    fields.get(b"error_message")
                                    or fields.get("error_message")
                                    or b""
                                )
                                et = (
                                    error_type_raw.decode()
                                    if isinstance(error_type_raw, bytes)
                                    else error_type_raw
                                )
                                em = (
                                    error_msg_raw.decode()
                                    if isinstance(error_msg_raw, bytes)
                                    else error_msg_raw
                                )
                                raise RuntimeError(f"{et}: {em}")
                            raw = fields.get(b"data") or fields.get("data")
                            if raw is not None:
                                return self._serializer.loads(  # type: ignore[no-any-return]
                                    raw if isinstance(raw, bytes) else raw.encode()
                                )
                elif self._block_ms == 0:
                    await asyncio.sleep(0)  # yield to event loop in non-blocking mode
        finally:
            try:
                await self._redis.delete(reply_stream)
            except Exception as e:
                logger.warning("Failed to delete reply stream %s: %s", reply_stream, e)

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
        command_task_streams: list[str] = [
            self._stream_key("command", t.__name__) for t in self._command_handlers
        ] + [self._stream_key("task", t.__name__) for t in self._task_handlers]

        if command_task_streams:
            self._consumer_tasks.append(asyncio.create_task(self._run_command_task_consumer()))
            self._consumer_tasks.append(
                asyncio.create_task(
                    self._run_xautoclaim_loop(command_task_streams, self._consumer_group)
                )
            )

        # Single consumer loop for all queries (competitive consumption + reply)
        if self._query_handlers:
            self._consumer_tasks.append(asyncio.create_task(self._run_query_consumer()))

        # Dedicated consumer loop per event subscriber (fan-out)
        for event_type, subscriber_group, handler in self._event_subscriptions:
            stream_key = self._stream_key("event", event_type.__name__)
            self._consumer_tasks.append(
                asyncio.create_task(self._run_event_consumer(stream_key, subscriber_group, handler))
            )
            self._consumer_tasks.append(
                asyncio.create_task(
                    self._run_xautoclaim_loop([stream_key], subscriber_group, event_handler=handler)
                )
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

    async def _xadd(
        self, stream_key: str, message: Any, idempotency_key: str | None = None
    ) -> None:
        """Serialize *message* and append it to *stream_key* with audit metadata."""
        data = self._serializer.dumps(message)
        msg_id = uuid.uuid4().hex
        await self._redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": msg_id,
                "producer_id": self._consumer_name,
                "idempotency_key": idempotency_key or msg_id,
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
            maxlen=self._max_stream_length,
            approximate=True,
        )

    def _dedup_key(self, idempotency_key: str, scope: str = "") -> str:
        prefix = f"{scope}:" if scope else ""
        return f"{self._app_name}:dedup:{prefix}{idempotency_key}"

    async def _try_claim_dedup(self, idempotency_key: str, scope: str = "") -> bool:
        """Atomically claim *idempotency_key* for processing.

        Uses ``SET NX EX`` so the check-and-mark is a single Redis command,
        eliminating the race window present in a separate EXISTS + SET pair.

        Returns ``True`` when this call successfully claimed the key (i.e. the
        message has **not** been processed before and this consumer should
        handle it).  Returns ``False`` when the key already exists, meaning
        the message is a duplicate and must be skipped.

        **Important**: call :meth:`_release_dedup_key` if the handler raises
        so that the message can be retried via XAUTOCLAIM.
        """
        key = self._dedup_key(idempotency_key, scope)
        result = await self._redis.set(key, "1", nx=True, ex=self._idempotency_ttl)
        return result is not None  # not None → key was new → claimed

    async def _release_dedup_key(self, idempotency_key: str, scope: str = "") -> None:
        """Delete the dedup key so a failed message can be retried.

        Must be called when a handler raises an exception **before** returning,
        so that XAUTOCLAIM can re-deliver the message to another consumer.
        """
        try:
            await self._redis.delete(self._dedup_key(idempotency_key, scope))
        except Exception as e:
            logger.warning(
                "Failed to release dedup key for idempotency_key=%s scope=%s: %s",
                idempotency_key,
                scope,
                e,
            )

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

                ikey_raw = fields.get(b"idempotency_key") or fields.get("idempotency_key")
                ikey: str | None = None
                if ikey_raw is not None:
                    ikey = ikey_raw.decode() if isinstance(ikey_raw, bytes) else ikey_raw
                    if not await self._try_claim_dedup(ikey, scope=stream_str):
                        logger.info(
                            "Skipping duplicate message (idempotency_key=%s) id=%s",
                            ikey,
                            message_id,
                        )
                        try:
                            await self._redis.xack(stream_str, self._consumer_group, message_id)
                        except Exception as e:
                            logger.warning(
                                "XACK failed for duplicate message id=%s "
                                "(idempotency_key=%s stream=%s): %s",
                                message_id,
                                ikey,
                                stream_str,
                                e,
                            )
                        continue

                msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())

                if ":command:" in stream_str and isinstance(msg, Command):
                    handler = self._command_handlers.get(type(msg))
                    if handler is not None:
                        try:
                            await handler(msg)
                        except Exception:
                            if ikey is not None:
                                await self._release_dedup_key(ikey, scope=stream_str)
                            raise
                        await self._redis.xack(stream_str, self._consumer_group, message_id)
                    else:
                        if ikey is not None:
                            await self._release_dedup_key(ikey, scope=stream_str)
                        logger.warning(
                            "No handler for command %s; leaving in PEL for retry",
                            type(msg).__name__,
                        )
                elif ":task:" in stream_str and isinstance(msg, Task):
                    task_handler = self._task_handlers.get(type(msg))
                    if task_handler is not None:
                        try:
                            await task_handler(msg)
                        except Exception:
                            if ikey is not None:
                                await self._release_dedup_key(ikey, scope=stream_str)
                            raise
                        await self._redis.xack(stream_str, self._consumer_group, message_id)
                    else:
                        if ikey is not None:
                            await self._release_dedup_key(ikey, scope=stream_str)
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

                                ikey_raw = fields.get(b"idempotency_key") or fields.get(
                                    "idempotency_key"
                                )
                                evt_ikey: str | None = None
                                if ikey_raw is not None:
                                    evt_ikey = (
                                        ikey_raw.decode()
                                        if isinstance(ikey_raw, bytes)
                                        else ikey_raw
                                    )
                                    if not await self._try_claim_dedup(
                                        evt_ikey, scope=subscriber_group
                                    ):
                                        logger.info(
                                            "Skipping duplicate event (idempotency_key=%s) id=%s",
                                            evt_ikey,
                                            message_id,
                                        )
                                        try:
                                            await self._redis.xack(
                                                stream_key, subscriber_group, message_id
                                            )
                                        except Exception as e:
                                            logger.warning(
                                                "XACK failed for duplicate event id=%s "
                                                "(idempotency_key=%s group=%s): %s",
                                                message_id,
                                                evt_ikey,
                                                subscriber_group,
                                                e,
                                            )
                                        continue

                                event = self._serializer.loads(
                                    raw if isinstance(raw, bytes) else raw.encode()
                                )
                                if isinstance(event, Event):
                                    try:
                                        await handler(event)
                                    except Exception:
                                        if evt_ikey is not None:
                                            await self._release_dedup_key(
                                                evt_ikey, scope=subscriber_group
                                            )
                                        raise
                                    await self._redis.xack(stream_key, subscriber_group, message_id)
                                else:
                                    if evt_ikey is not None:
                                        await self._release_dedup_key(
                                            evt_ikey, scope=subscriber_group
                                        )
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

    async def _run_query_consumer(self) -> None:
        """Background coroutine: competitive consumption of queries with reply delivery."""
        streams: list[str] = [self._stream_key("query", t.__name__) for t in self._query_handlers]
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
                        await self._handle_query_batch(stream_str, stream_msgs)
                else:
                    await asyncio.sleep(0)
                retry_delay = 0.1
            except asyncio.CancelledError:
                return
            except (RedisConnectionError, RedisTimeoutError):
                logger.warning(
                    "Redis connection error in query consumer, reconnecting in %.1fs",
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30.0)
                await self._reconnect()
            except Exception:
                logger.exception("Unexpected error in query consumer")
                await asyncio.sleep(0.1)

    # TTL for orphaned reply streams (seconds); guards against late consumer writes
    # after the caller has already timed out and deleted its local copy.
    _REPLY_STREAM_TTL_SECONDS = 60

    async def _handle_query_batch(self, stream_str: str, batch: list[Any]) -> None:
        """Process a batch of query messages, writing replies to their reply streams."""
        for message_id, fields in batch:
            reply_stream: str | None = None
            try:
                raw = fields.get(b"data") or fields.get("data")
                reply_stream_raw = fields.get(b"reply_stream") or fields.get("reply_stream")

                # Decode reply_stream early so we can send error replies even on bad data
                if reply_stream_raw is not None:
                    reply_stream = (
                        reply_stream_raw.decode()
                        if isinstance(reply_stream_raw, bytes)
                        else reply_stream_raw
                    )

                if raw is None or reply_stream_raw is None:
                    logger.warning(
                        "Query message missing required fields "
                        "(data=%s, reply_stream=%s); skipping id=%s",
                        raw is not None,
                        reply_stream_raw is not None,
                        message_id,
                    )
                    if reply_stream:
                        await self._send_error_reply(
                            reply_stream, "ValueError", "Missing required query fields"
                        )
                    await self._redis.xack(stream_str, self._consumer_group, message_id)
                    continue

                msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())

                if isinstance(msg, Query):
                    handler = self._query_handlers.get(type(msg))
                    if handler is not None:
                        try:
                            result = await handler(msg)
                            result_data = self._serializer.dumps(result)
                            await self._redis.xadd(
                                reply_stream,
                                {"data": result_data},
                                maxlen=self._max_stream_length,
                                approximate=True,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Query handler %s raised %s (reply_stream=%s): %s",
                                type(msg).__name__,
                                type(exc).__name__,
                                reply_stream,
                                exc,
                            )
                            await self._send_error_reply(reply_stream, type(exc).__name__, str(exc))
                        finally:
                            await self._redis.expire(reply_stream, self._REPLY_STREAM_TTL_SECONDS)
                        await self._redis.xack(stream_str, self._consumer_group, message_id)
                    else:
                        logger.warning(
                            "No handler for query %s; leaving in PEL for retry",
                            type(msg).__name__,
                        )
                else:
                    logger.warning(
                        "Non-Query payload on query stream (type=%s); skipping id=%s",
                        type(msg).__name__,
                        message_id,
                    )
                    await self._redis.xack(stream_str, self._consumer_group, message_id)
            except Exception:
                logger.exception("Error processing query message id=%s", message_id)
                if reply_stream:
                    await self._send_error_reply(
                        reply_stream, "RuntimeError", "Internal error processing query"
                    )
                # Always ACK to prevent PEL growth on unrecoverable errors
                try:
                    await self._redis.xack(stream_str, self._consumer_group, message_id)
                except Exception as e:
                    logger.warning("Failed to XACK query message id=%s: %s", message_id, e)

    async def _send_error_reply(
        self, reply_stream: str, error_type: str, error_message: str
    ) -> None:
        """Write an error reply to *reply_stream*, logging failures instead of swallowing."""
        try:
            await self._redis.xadd(
                reply_stream,
                {"error_type": error_type, "error_message": error_message},
                maxlen=self._max_stream_length,
                approximate=True,
            )
        except Exception:
            logger.exception(
                "Failed to write error reply to %s (%s: %s); "
                "requester will receive TimeoutError instead of the original error",
                reply_stream,
                error_type,
                error_message,
            )

    async def _run_xautoclaim_loop(
        self,
        streams: list[str],
        group: str,
        event_handler: Callable[[Event], Awaitable[None]] | None = None,
    ) -> None:
        """Claim idle messages via XAUTOCLAIM; route to DLQ when max_retry exceeded."""
        next_ids: dict[str, str] = dict.fromkeys(streams, "0-0")
        sleep_secs = max(0.05, self._claim_idle_ms / 1000)
        error_delay = 0.1

        while self._running:
            await asyncio.sleep(sleep_secs)
            if not self._running:
                return

            for stream_key in streams:
                try:
                    result = await self._redis.xautoclaim(
                        name=stream_key,
                        groupname=group,
                        consumername=self._consumer_name,
                        min_idle_time=self._claim_idle_ms,
                        start_id=next_ids[stream_key],
                        count=10,
                    )
                    next_id_raw = result[0]
                    claimed = result[1]
                    next_ids[stream_key] = (
                        next_id_raw.decode() if isinstance(next_id_raw, bytes) else str(next_id_raw)
                    )

                    if not claimed:
                        next_ids[stream_key] = "0-0"
                        continue

                    # Fetch delivery counts for all claimed messages from PEL.
                    # Use a large count to avoid undercount when PEL has many entries.
                    pending = await self._redis.xpending_range(
                        name=stream_key,
                        groupname=group,
                        min="-",
                        max="+",
                        count=10_000,
                    )
                    delivery_counts: dict[bytes, int] = {}
                    for p in pending:
                        mid = p["message_id"]
                        if not isinstance(mid, bytes):
                            mid = mid.encode()
                        delivery_counts[mid] = int(p["times_delivered"])

                    for message_id, fields in claimed:
                        mid_bytes = (
                            message_id if isinstance(message_id, bytes) else message_id.encode()
                        )
                        delivery_count = delivery_counts.get(mid_bytes, 1)

                        if self._dead_letter_store is not None and delivery_count > self._max_retry:
                            await self._send_to_dead_letter(
                                stream_key, group, message_id, fields, delivery_count
                            )
                        elif event_handler is not None:
                            await self._reprocess_event_message(
                                stream_key, group, message_id, fields, event_handler
                            )
                        else:
                            await self._handle_command_task_batch(
                                stream_key, [(message_id, fields)]
                            )

                    error_delay = 0.1  # Reset on success
                except asyncio.CancelledError:
                    return
                except RedisResponseError as e:
                    if "NOGROUP" in str(e):
                        # Consumer group not yet created; wait for main consumer to initialise.
                        await asyncio.sleep(min(sleep_secs, 1.0))
                    else:
                        logger.exception(
                            "XAUTOCLAIM Redis error for stream=%s group=%s", stream_key, group
                        )
                        await asyncio.sleep(error_delay)
                        error_delay = min(error_delay * 2, 30.0)
                except (RedisConnectionError, RedisTimeoutError):
                    logger.warning(
                        "Redis connection error in XAUTOCLAIM loop "
                        "for stream=%s, retrying in %.1fs",
                        stream_key,
                        error_delay,
                    )
                    await asyncio.sleep(error_delay)
                    error_delay = min(error_delay * 2, 30.0)
                    await self._reconnect()
                except Exception:
                    logger.exception(
                        "XAUTOCLAIM unexpected error for stream=%s group=%s consumer=%s",
                        stream_key,
                        group,
                        self._consumer_name,
                    )
                    await asyncio.sleep(error_delay)
                    error_delay = min(error_delay * 2, 30.0)

    async def _send_to_dead_letter(
        self,
        stream_key: str,
        group: str,
        message_id: Any,
        fields: dict[bytes | str, Any],
        delivery_count: int,
    ) -> None:
        """Deserialize message, record in dead letter store, then ACK to remove from PEL.

        XACK is sent only after a successful DLQ append.  If the append fails the
        message intentionally stays in PEL so it can be recovered manually.
        """
        msg_id_str = message_id.decode() if isinstance(message_id, bytes) else str(message_id)

        raw = fields.get(b"data") or fields.get("data")
        if raw is None:
            # Malformed message with no payload — ACK immediately to clear from PEL.
            logger.warning(
                "Dead letter message %s has no 'data' field; ACKing to clear from PEL",
                msg_id_str,
            )
            try:
                await self._redis.xack(stream_key, group, message_id)
            except Exception:
                logger.exception("Failed to ACK malformed dead letter message %s", msg_id_str)
            return

        if self._dead_letter_store is None:
            return

        try:
            msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())
            error = MaxRetriesExceededError(msg_id_str, delivery_count, self._max_retry)
            self._dead_letter_store.append(msg, error, type(msg).__name__)
        except Exception:
            logger.exception(
                "Failed to record message %s to dead letter store; leaving in PEL for recovery",
                msg_id_str,
            )
            return  # Do NOT ACK — leave in PEL for manual recovery.

        # ACK only after the DLQ append succeeded.
        try:
            await self._redis.xack(stream_key, group, message_id)
        except Exception:
            logger.exception("Failed to ACK dead letter message %s", msg_id_str)
            return

        logger.warning(
            "Message %s exceeded max retries (%d), routed to dead letter queue",
            msg_id_str,
            self._max_retry,
        )

    async def _reprocess_event_message(
        self,
        stream_key: str,
        group: str,
        message_id: Any,
        fields: dict[bytes | str, Any],
        handler: Callable[[Event], Awaitable[None]],
    ) -> None:
        """Reprocess a single claimed event message and ACK on success.

        Malformed messages (missing data, wrong payload type) are ACKed immediately
        to prevent them from being re-claimed indefinitely.
        """
        try:
            raw = fields.get(b"data") or fields.get("data")
            if raw is None:
                logger.warning(
                    "Claimed event message missing 'data' field; ACKing to clear from PEL id=%s",
                    message_id,
                )
                await self._redis.xack(stream_key, group, message_id)
                return

            ikey_raw = fields.get(b"idempotency_key") or fields.get("idempotency_key")
            reprocess_ikey: str | None = None
            if ikey_raw is not None:
                reprocess_ikey = ikey_raw.decode() if isinstance(ikey_raw, bytes) else ikey_raw
                if not await self._try_claim_dedup(reprocess_ikey, scope=group):
                    logger.info(
                        "Skipping duplicate claimed event (idempotency_key=%s) id=%s",
                        reprocess_ikey,
                        message_id,
                    )
                    await self._redis.xack(stream_key, group, message_id)
                    return

            event = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())
            if not isinstance(event, Event):
                logger.warning(
                    "Claimed non-Event payload (type=%s); ACKing to clear from PEL id=%s",
                    type(event).__name__,
                    message_id,
                )
                if reprocess_ikey is not None:
                    await self._release_dedup_key(reprocess_ikey, scope=group)
                await self._redis.xack(stream_key, group, message_id)
                return
            try:
                await handler(event)
            except Exception:
                if reprocess_ikey is not None:
                    await self._release_dedup_key(reprocess_ikey, scope=group)
                raise
            await self._redis.xack(stream_key, group, message_id)
        except Exception:
            logger.exception("Error reprocessing claimed event message id=%s", message_id)

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
