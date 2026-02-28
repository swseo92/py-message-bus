"""Async Redis Streams–based message bus for distributed communication."""

# mypy: disable-error-code="import-not-found, no-untyped-call, attr-defined"

import asyncio
import dataclasses
import json
import logging
import math
import socket
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, TypeVar

from message_bus.dead_letter import DeadLetterStore
from message_bus.latency import LatencyStats
from message_bus.metrics import BusMetricsCollector, BusMetricsSnapshot, StreamMetrics
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


class _FallbackRedisResponseError(Exception):
    """Narrow sentinel used as RedisResponseError when redis is not installed.

    Using a specific class (instead of the broad ``Exception``) prevents
    :meth:`health_check` from accidentally swallowing unrelated runtime errors.
    """


try:
    import redis.asyncio as aioredis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import ResponseError as RedisResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError
except ImportError:
    aioredis = None  # type: ignore[assignment]
    RedisConnectionError = OSError  # type: ignore[assignment, misc]
    RedisTimeoutError = TimeoutError  # type: ignore[assignment, misc]
    RedisResponseError = _FallbackRedisResponseError  # type: ignore[assignment, misc]


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
      caller subscribes to a Pub/Sub reply channel before sending, and the
      consumer ``PUBLISH``-es the reply directly to that channel.  This
      reduces round-trips from 4 (XADD→XREADGROUP→XADD→XREAD) to 2
      (XADD→XREADGROUP→PUBLISH→push), eliminating reply-stream polling.

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
        Milliseconds passed to ``XREADGROUP``'s ``block`` parameter
        (consumer side).  **Testing only** – set to ``0`` when using
        fakeredis, which does not implement async blocking correctly.
        Defaults to ``10`` ms for production use with a real Redis server.
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
        "_concurrency_config",
        "_shutdown_timeout",
        "_metrics_collector",
        "_latency_stats",
        # Connection modes
        "_connection_pool",
        "_sentinel_urls",
        "_sentinel_service_name",
        "_sentinel_kwargs",
        "_cluster_mode",
        # Metadata mode
        "_metadata_mode",
        "_source_id",
        # Shared Pub/Sub reply listener
        "_pubsub",
        "_pending_queries",
        "_stream_key_cache",
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
        shutdown_timeout: float = 5.0,
        metrics_collector: BusMetricsCollector | None = None,
        latency_stats: LatencyStats | None = None,
        connection_pool: Any | None = None,
        sentinel_urls: list[tuple[str, int]] | None = None,
        sentinel_service_name: str | None = None,
        sentinel_kwargs: dict[str, Any] | None = None,
        cluster_mode: bool = False,
        metadata_mode: Literal["standard", "none"] = "standard",
        _block_ms: int = 10,
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
        _valid_metadata_modes = ("standard", "none")
        if metadata_mode not in _valid_metadata_modes:
            raise ValueError(
                f"metadata_mode must be one of {_valid_metadata_modes!r}, got {metadata_mode!r}"
            )
        # Validate connection mode mutual exclusivity
        _connection_modes = sum(
            [
                connection_pool is not None,
                sentinel_urls is not None,
                cluster_mode,
            ]
        )
        if _connection_modes > 1:
            raise ValueError(
                "Only one connection mode can be active at a time: "
                "connection_pool, sentinel_urls, or cluster_mode."
            )
        if sentinel_urls is not None:
            if not sentinel_urls:
                raise ValueError("sentinel_urls must contain at least one (host, port) tuple.")
            if not sentinel_service_name:
                raise ValueError(
                    "sentinel_service_name is required when sentinel_urls is provided."
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
        if not (shutdown_timeout > 0 and math.isfinite(shutdown_timeout)):
            raise ValueError(
                f"shutdown_timeout must be a finite positive number, got {shutdown_timeout!r}"
            )
        self._shutdown_timeout = shutdown_timeout
        self._block_ms = _block_ms
        # Connection mode configuration
        self._connection_pool: Any | None = connection_pool
        self._sentinel_urls: list[tuple[str, int]] | None = sentinel_urls
        self._sentinel_service_name: str | None = sentinel_service_name
        self._sentinel_kwargs: dict[str, Any] = sentinel_kwargs or {}
        self._cluster_mode: bool = cluster_mode
        # Metadata mode: "standard" (lightweight fields) or "none" (data only)
        self._metadata_mode: Literal["standard", "none"] = metadata_mode
        self._source_id: str = socket.gethostname()
        self._stream_key_cache: dict[tuple[str, str], str] = {}
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

        # Metrics
        self._metrics_collector: BusMetricsCollector | None = metrics_collector
        self._latency_stats: LatencyStats | None = latency_stats

        # Runtime state
        self._running = False
        self._consumer_tasks: list[asyncio.Task[None]] = []
        # (message_type → max_concurrent) – None means unlimited (sequential)
        self._concurrency_config: dict[type, int | None] = {}
        # Shared Pub/Sub reply listener (correlation_id → Future)
        self._pubsub: Any = None
        self._pending_queries: dict[str, asyncio.Future[bytes]] = {}

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
        concurrency: int | None = None,
    ) -> None:
        if command_type in self._command_handlers:
            raise ValueError(f"Command handler already registered for {command_type.__name__}")
        if concurrency is not None and concurrency <= 0:
            raise ValueError(f"concurrency must be a positive integer, got {concurrency}")
        self._serializer.registry.register(command_type)
        self._command_handlers[command_type] = handler
        if concurrency is not None:
            self._concurrency_config[command_type] = concurrency

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
        concurrency: int | None = None,
    ) -> None:
        if task_type in self._task_handlers:
            raise ValueError(f"Task handler already registered for {task_type.__name__}")
        if concurrency is not None and concurrency <= 0:
            raise ValueError(f"concurrency must be a positive integer, got {concurrency}")
        self._serializer.registry.register(task_type)
        self._task_handlers[task_type] = handler
        if concurrency is not None:
            self._concurrency_config[task_type] = concurrency

    # ------------------------------------------------------------------
    # Dispatch (async) – AsyncMessageDispatcher + AsyncQueryDispatcher
    # ------------------------------------------------------------------

    async def send(self, query: Query[T]) -> T:
        """Send *query* and await its reply via the shared Pub/Sub reply listener.

        A unique ``correlation_id`` is generated per call and registered as an
        ``asyncio.Future`` in ``_pending_queries``.  The shared
        ``_run_pubsub_reply_listener`` background task receives the
        ``PUBLISH``-ed reply and resolves the matching future, eliminating
        per-call subscribe/unsubscribe connection overhead.

        This reduces round-trips from 4 (XADD→XREADGROUP→XADD→XREAD) to 2
        (XADD→XREADGROUP→PUBLISH→push) and reuses a single Pub/Sub connection
        for all concurrent queries.

        Raises
        ------
        asyncio.TimeoutError
            When no reply arrives within *query_reply_timeout* seconds.
        RuntimeError
            When the handler raises an exception (type and message are preserved
            in the error message).
        """
        correlation_id = uuid.uuid4().hex
        reply_channel = self._reply_channel_key(correlation_id)
        stream_key = self._stream_key("query", type(query).__name__)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        self._pending_queries[correlation_id] = future
        try:
            data = self._serializer.dumps(query)
            await self._redis.xadd(
                stream_key,
                {
                    "data": data,
                    "correlation_id": correlation_id.encode(),
                    "reply_channel": reply_channel.encode(),
                },
                maxlen=self._max_stream_length,
                approximate=True,
            )

            try:
                raw_payload = await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=self._query_reply_timeout,
                )
            except TimeoutError:
                raise TimeoutError(
                    f"Query {type(query).__name__!r} timed out "
                    f"after {self._query_reply_timeout}s"
                ) from None
        finally:
            self._pending_queries.pop(correlation_id, None)

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Malformed reply on channel {reply_channel!r}: invalid JSON"
            ) from exc
        error_type = payload.get("error_type")
        if error_type:
            error_message = payload.get("error_message", "")
            raise RuntimeError(f"{error_type}: {error_message}")
        raw_data = payload.get("data")
        if raw_data is None:
            raise RuntimeError(
                f"Malformed reply on channel {reply_channel!r}: missing 'data' field"
            )
        return self._serializer.loads(  # type: ignore[no-any-return]
            raw_data if isinstance(raw_data, bytes) else raw_data.encode()
        )

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
    # Connection factory
    # ------------------------------------------------------------------

    def _create_redis_client(self) -> Any:
        """Create the appropriate Redis client based on the configured connection mode.

        Priority (from highest to lowest):
        1. ``connection_pool`` – wrap an externally-managed pool.
        2. ``sentinel_urls`` – connect via Redis Sentinel (HA failover).
        3. ``cluster_mode`` – connect via Redis Cluster.
        4. Default – single-node connection via *redis_url*.

        .. note::
            **Redis Streams + Cluster mode constraint**: Redis Streams consumer
            groups use ``XREADGROUP``, which only works within a single hash
            slot.  All stream keys generated by this bus share the same
            ``app_name`` prefix; when ``cluster_mode=True``, a hash tag
            ``{<app_name>}`` is automatically embedded so every stream key
            maps to the same slot.  Do not change ``app_name`` at runtime
            when cluster mode is active.
        """
        if self._connection_pool is not None:
            return aioredis.Redis(connection_pool=self._connection_pool, decode_responses=False)
        if self._sentinel_urls is not None:
            # sentinel_kwargs is for authenticating/configuring sentinel-node connections;
            # __init__ validation guarantees _sentinel_service_name is set.
            assert self._sentinel_service_name is not None
            sentinel = aioredis.Sentinel(
                self._sentinel_urls,
                sentinel_kwargs=self._sentinel_kwargs if self._sentinel_kwargs else None,
            )
            return sentinel.master_for(self._sentinel_service_name, decode_responses=False)
        if self._cluster_mode:
            return aioredis.RedisCluster.from_url(self._redis_url, decode_responses=False)
        return aioredis.from_url(self._redis_url, decode_responses=False)

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
            self._redis = self._create_redis_client()
        self._running = True

        # Per-message-type independent consumers (HOL Blocking 해소)
        # 각 Command/Task 타입이 독립 컨슈머를 가져 느린 핸들러가 다른 타입을 블로킹하지 않는다.
        for cmd_type, handler in self._command_handlers.items():
            stream_key = self._stream_key("command", cmd_type.__name__)
            concurrency = self._concurrency_config.get(cmd_type)
            semaphore = asyncio.Semaphore(concurrency) if concurrency is not None else None
            self._consumer_tasks.append(
                asyncio.create_task(
                    self._run_single_command_consumer(stream_key, handler, semaphore)
                )
            )
            self._consumer_tasks.append(
                asyncio.create_task(self._run_xautoclaim_loop([stream_key], self._consumer_group))
            )

        for task_type, task_handler in self._task_handlers.items():
            stream_key = self._stream_key("task", task_type.__name__)
            concurrency = self._concurrency_config.get(task_type)
            task_semaphore = asyncio.Semaphore(concurrency) if concurrency is not None else None
            self._consumer_tasks.append(
                asyncio.create_task(
                    self._run_single_task_consumer(stream_key, task_handler, task_semaphore)
                )
            )
            self._consumer_tasks.append(
                asyncio.create_task(self._run_xautoclaim_loop([stream_key], self._consumer_group))
            )

        # Single consumer loop for all queries (competitive consumption + reply)
        if self._query_handlers:
            self._consumer_tasks.append(asyncio.create_task(self._run_query_consumer()))

        # Shared Pub/Sub reply listener – always started so that send() works
        # even on buses that have no local query handlers (producer-only).
        self._pubsub = self._redis.pubsub()
        reply_pattern = (
            f"{{{self._app_name}}}:reply_ch:*"
            if self._cluster_mode
            else f"{self._app_name}:reply_ch:*"
        )
        try:
            await self._pubsub.psubscribe(reply_pattern)
        except Exception as exc:
            await self._pubsub.aclose()
            self._pubsub = None
            raise RuntimeError(
                f"Failed to subscribe to Pub/Sub reply pattern {reply_pattern!r}: {exc}"
            ) from exc
        self._consumer_tasks.append(
            asyncio.create_task(self._run_pubsub_reply_listener())
        )

        # Dedicated consumer loop per event subscriber (fan-out)
        for event_type, subscriber_group, event_handler in self._event_subscriptions:
            stream_key = self._stream_key("event", event_type.__name__)
            self._consumer_tasks.append(
                asyncio.create_task(
                    self._run_event_consumer(stream_key, subscriber_group, event_handler)
                )
            )
            self._consumer_tasks.append(
                asyncio.create_task(
                    self._run_xautoclaim_loop(
                        [stream_key], subscriber_group, event_handler=event_handler
                    )
                )
            )

    async def close(self) -> None:
        """Gracefully stop consumers and close the Redis connection.

        Sets the shutdown flag to prevent new messages from being fetched, then
        waits up to ``shutdown_timeout`` seconds for in-flight messages to
        complete before cancelling any remaining consumer tasks.

        Messages that did not finish within the timeout remain in the Redis
        Streams PEL (Pending Entry List) and will be reclaimed by XAUTOCLAIM
        when the next consumer starts.
        """
        self._running = False
        if self._consumer_tasks:
            done, pending = await asyncio.wait(
                self._consumer_tasks,
                timeout=self._shutdown_timeout,
            )
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    logger.warning(
                        "Consumer task raised an exception during shutdown: %s",
                        task.exception(),
                    )
            if pending:
                logger.warning(
                    "%d consumer task(s) did not finish within %.1fs shutdown timeout; "
                    "cancelling. Unacknowledged messages remain in the PEL for XAUTOCLAIM.",
                    len(pending),
                    self._shutdown_timeout,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        self._consumer_tasks.clear()
        if self._pubsub is not None:
            try:
                await self._pubsub.punsubscribe()
                await self._pubsub.aclose()
            except Exception as e:
                logger.warning("Error closing Pub/Sub connection: %s", e)
            self._pubsub = None
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
    # Observability
    # ------------------------------------------------------------------

    async def get_metrics_snapshot(self, include_pel: bool = True) -> BusMetricsSnapshot:
        """Collect and return a point-in-time operational metrics snapshot.

        All registered message types are included in the snapshot, even when
        no messages have been processed yet (counters will be zero).

        Parameters
        ----------
        include_pel:
            When ``True`` (default), query Redis for the current Pending
            Entries List (PEL) size of each stream/group.  Set to ``False``
            to skip Redis queries and return only in-memory counters — useful
            for low-latency scrape endpoints or when the bus has not been
            started yet.

        Returns
        -------
        BusMetricsSnapshot
            Immutable snapshot with per-stream counters, optional PEL sizes,
            and latency percentiles from the injected
            :class:`~message_bus.LatencyStats` (if any).
        """
        from datetime import UTC, datetime

        stream_metrics: list[StreamMetrics] = []

        # (stream_key, consumer_group, metrics_key) tuples for all registered handlers.
        # Event subscriptions use a composite metrics_key (stream_key:group) to avoid
        # double-counting when multiple subscriber groups share the same stream key.
        entries: list[tuple[str, str, str]] = []
        for cmd_type in self._command_handlers:
            sk = self._stream_key("command", cmd_type.__name__)
            entries.append((sk, self._consumer_group, sk))
        for task_type in self._task_handlers:
            sk = self._stream_key("task", task_type.__name__)
            entries.append((sk, self._consumer_group, sk))
        for event_type, subscriber_group, _ in self._event_subscriptions:
            sk = self._stream_key("event", event_type.__name__)
            entries.append((sk, subscriber_group, f"{sk}:{subscriber_group}"))
        for query_type in self._query_handlers:
            sk = self._stream_key("query", query_type.__name__)
            entries.append((sk, self._consumer_group, sk))

        for stream_key, group, metrics_key in entries:
            processed, failed = (0, 0)
            if self._metrics_collector is not None:
                processed, failed = self._metrics_collector.get_counts(metrics_key)

            pel_size = -1
            if include_pel and self._redis is not None:
                try:
                    pel_info = await self._redis.xpending(stream_key, group)
                    pel_size = int(pel_info.get("pending", 0)) if pel_info else 0
                except Exception:
                    pel_size = -2
                    logger.warning("Failed to query PEL for stream=%s group=%s", stream_key, group)

            stream_metrics.append(
                StreamMetrics(
                    stream_key=stream_key,
                    processed_total=processed,
                    failed_total=failed,
                    pel_size=pel_size,
                )
            )

        latency_percentiles = (
            self._latency_stats.all_percentiles() if self._latency_stats is not None else []
        )

        return BusMetricsSnapshot(
            collected_at=datetime.now(UTC),
            streams=stream_metrics,
            latency_percentiles=latency_percentiles,
        )

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict[str, Any]:
        """Return a health status snapshot of the bus.

        Checks
        ------
        * **redis_connected** – whether :meth:`ping` succeeds on the current
          Redis client.  ``False`` when the client is ``None`` (bus not started
          or already closed) or when the ping raises a connection error.
        * **consumer_loop_active** – whether background consumer tasks are
          alive.  Evaluates to ``True`` when no handlers are registered (no
          consumer tasks are needed).

        Returns
        -------
        dict with keys:

        * ``is_healthy`` (bool) – ``True`` iff both sub-checks pass.
        * ``redis_connected`` (bool)
        * ``consumer_loop_active`` (bool)
        """
        redis_connected = False
        redis_error: str | None = None
        if self._redis is not None:
            try:
                await self._redis.ping()
                redis_connected = True
            except (RedisConnectionError, RedisTimeoutError, RedisResponseError, OSError) as exc:
                redis_connected = False
                redis_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "health_check ping failed redis_url=%s consumer=%s error=%s",
                    self._redis_url,
                    self._consumer_name,
                    redis_error,
                )

        has_handlers = bool(
            self._command_handlers
            or self._task_handlers
            or self._event_subscriptions
            or self._query_handlers
        )
        if not self._running:
            consumer_loop_active = False
        elif not has_handlers:
            consumer_loop_active = True  # no consumers needed
        else:
            consumer_loop_active = bool(self._consumer_tasks) and all(
                not t.done() for t in self._consumer_tasks
            )

        return {
            "is_healthy": redis_connected and consumer_loop_active,
            "redis_connected": redis_connected,
            "consumer_loop_active": consumer_loop_active,
            "redis_error": redis_error,
        }

    async def is_healthy(self) -> bool:
        """Return ``True`` if :meth:`health_check` reports overall health."""
        result = await self.health_check()
        return bool(result["is_healthy"])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stream_key(self, prefix: str, name: str) -> str:
        cache_key = (prefix, name)
        cached = self._stream_key_cache.get(cache_key)
        if cached is not None:
            return cached
        if self._cluster_mode:
            result = f"{{{self._app_name}}}:{prefix}:{name}"
        else:
            result = f"{self._app_name}:{prefix}:{name}"
        self._stream_key_cache[cache_key] = result
        return result

    def _reply_channel_key(self, correlation_id: str) -> str:
        """Return the Pub/Sub channel name for a query reply."""
        if self._cluster_mode:
            return f"{{{self._app_name}}}:reply_ch:{correlation_id}"
        return f"{self._app_name}:reply_ch:{correlation_id}"

    async def _xadd(
        self, stream_key: str, message: Any, idempotency_key: str | None = None
    ) -> None:
        """Serialize *message* and append it to *stream_key*.

        ``metadata_mode="standard"`` (default): dedup fields plus
        lightweight enrichment — ``message_id``, ``idempotency_key``,
        ``source_id`` (cached hostname), ``message_type``, and
        ``timestamp`` (UNIX float string).

        ``metadata_mode="none"``: dedup fields only (``message_id`` and
        ``idempotency_key``).  Enrichment metadata is omitted for
        maximum producer throughput while preserving idempotency.
        """
        data = self._serializer.dumps(message)
        msg_id = uuid.uuid4().hex
        if self._metadata_mode == "none":
            fields: dict[str, Any] = {
                "data": data,
                "message_id": msg_id,
                "idempotency_key": idempotency_key or msg_id,
            }
        else:
            fields = {
                "data": data,
                "message_id": msg_id,
                "idempotency_key": idempotency_key or msg_id,
                "source_id": self._source_id,
                "message_type": type(message).__name__,
                "timestamp": str(time.time()),
            }
        await self._redis.xadd(
            stream_key,
            fields,
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

    async def _run_single_command_consumer(
        self,
        stream_key: str,
        handler: Callable[[Command], Awaitable[None]],
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        """Per-type command consumer: 독립 실행으로 HOL Blocking을 방지한다.

        *semaphore* 가 주어지면 각 메시지를 asyncio.Task 로 발사해 최대 N 개 동시 처리.
        없으면 순차 처리 (타입 간 격리만으로 HOL Blocking 해소).
        """
        await self._ensure_group(stream_key, self._consumer_group)

        concurrent_tasks: set[asyncio.Task[None]] = set()
        retry_delay = 0.1
        _graceful = False
        try:
            while self._running:
                try:
                    messages = await self._redis.xreadgroup(
                        groupname=self._consumer_group,
                        consumername=self._consumer_name,
                        streams={stream_key: ">"},
                        count=10,
                        block=self._block_ms,
                    )
                    if messages:
                        for _, stream_msgs in messages:
                            for message_id, fields in stream_msgs:
                                coro = self._process_command_message(
                                    stream_key, message_id, fields, handler, semaphore
                                )
                                if semaphore is not None:
                                    task = asyncio.create_task(coro)
                                    concurrent_tasks.add(task)
                                    task.add_done_callback(concurrent_tasks.discard)
                                else:
                                    await coro
                    else:
                        await asyncio.sleep(0)
                    retry_delay = 0.1
                except asyncio.CancelledError:
                    raise
                except (RedisConnectionError, RedisTimeoutError):
                    logger.warning(
                        "Redis connection error in command consumer for %s, reconnecting in %.1fs",
                        stream_key,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30.0)
                    await self._reconnect()
                except Exception as e:
                    if not await self._handle_nogroup_error(e, [stream_key], self._consumer_group):
                        logger.exception("Unexpected error in command consumer for %s", stream_key)
                    await asyncio.sleep(0.1)
            _graceful = True  # loop exited normally via _running = False
        except asyncio.CancelledError:
            pass
        finally:
            if concurrent_tasks:
                if not _graceful:
                    # Forced cancellation: cancel in-flight tasks so messages stay in PEL
                    for t in concurrent_tasks:
                        t.cancel()
                try:
                    await asyncio.gather(*concurrent_tasks, return_exceptions=True)
                except asyncio.CancelledError:
                    # We are being cancelled while draining inner tasks.
                    # Ensure every inner task is cancelled and schedule a
                    # background drain so they are not left orphaned.
                    for t in concurrent_tasks:
                        if not t.done():
                            t.cancel()
                    asyncio.ensure_future(asyncio.gather(*concurrent_tasks, return_exceptions=True))
                    raise

    async def _process_command_message(
        self,
        stream_key: str,
        message_id: Any,
        fields: dict[Any, Any],
        handler: Callable[[Command], Awaitable[None]],
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        """단일 Command 메시지를 처리하고 ACK한다. 실패 시 PEL에 남겨 XAUTOCLAIM이 재시도."""
        raw: bytes | None = fields.get(b"data") or fields.get("data")
        if raw is None:
            logger.warning("Command message missing 'data' field; ACKing id=%s", message_id)
            await self._redis.xack(stream_key, self._consumer_group, message_id)
            return

        ikey_raw = fields.get(b"idempotency_key") or fields.get("idempotency_key")
        ikey: str | None = None
        if ikey_raw is not None:
            ikey = ikey_raw.decode() if isinstance(ikey_raw, bytes) else ikey_raw
            if not await self._try_claim_dedup(ikey, scope=stream_key):
                logger.info(
                    "Skipping duplicate command (idempotency_key=%s) id=%s", ikey, message_id
                )
                await self._redis.xack(stream_key, self._consumer_group, message_id)
                return

        try:
            msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())
        except Exception:
            logger.exception("Failed to deserialize command message id=%s", message_id)
            if self._metrics_collector is not None:
                self._metrics_collector.record_failed(stream_key)
            await self._redis.xack(stream_key, self._consumer_group, message_id)
            return

        if not isinstance(msg, Command):
            logger.warning(
                "Non-Command payload on command stream (type=%s); ACKing id=%s",
                type(msg).__name__,
                message_id,
            )
            if self._metrics_collector is not None:
                self._metrics_collector.record_failed(stream_key)
            await self._redis.xack(stream_key, self._consumer_group, message_id)
            return

        try:
            if semaphore is not None:
                async with semaphore:
                    await handler(msg)
            else:
                await handler(msg)
        except asyncio.CancelledError:
            if ikey is not None:
                await self._release_dedup_key(ikey, scope=stream_key)
            raise
        except Exception:
            logger.exception("Error processing command message id=%s", message_id)
            if ikey is not None:
                await self._release_dedup_key(ikey, scope=stream_key)
            if self._metrics_collector is not None:
                self._metrics_collector.record_failed(stream_key)
            return  # Leave in PEL for XAUTOCLAIM retry

        if self._metrics_collector is not None:
            self._metrics_collector.record_processed(stream_key)
        await self._redis.xack(stream_key, self._consumer_group, message_id)

    async def _run_single_task_consumer(
        self,
        stream_key: str,
        handler: Callable[[Task], Awaitable[None]],
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        """Per-type task consumer: 독립 실행으로 HOL Blocking을 방지한다."""
        await self._ensure_group(stream_key, self._consumer_group)

        concurrent_tasks: set[asyncio.Task[None]] = set()
        retry_delay = 0.1
        _graceful = False
        try:
            while self._running:
                try:
                    messages = await self._redis.xreadgroup(
                        groupname=self._consumer_group,
                        consumername=self._consumer_name,
                        streams={stream_key: ">"},
                        count=10,
                        block=self._block_ms,
                    )
                    if messages:
                        for _, stream_msgs in messages:
                            for message_id, fields in stream_msgs:
                                coro = self._process_task_message(
                                    stream_key, message_id, fields, handler, semaphore
                                )
                                if semaphore is not None:
                                    task = asyncio.create_task(coro)
                                    concurrent_tasks.add(task)
                                    task.add_done_callback(concurrent_tasks.discard)
                                else:
                                    await coro
                    else:
                        await asyncio.sleep(0)
                    retry_delay = 0.1
                except asyncio.CancelledError:
                    raise
                except (RedisConnectionError, RedisTimeoutError):
                    logger.warning(
                        "Redis connection error in task consumer for %s, reconnecting in %.1fs",
                        stream_key,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30.0)
                    await self._reconnect()
                except Exception as e:
                    if not await self._handle_nogroup_error(e, [stream_key], self._consumer_group):
                        logger.exception("Unexpected error in task consumer for %s", stream_key)
                    await asyncio.sleep(0.1)
            _graceful = True  # loop exited normally via _running = False
        except asyncio.CancelledError:
            pass
        finally:
            if concurrent_tasks:
                if not _graceful:
                    # Forced cancellation: cancel in-flight tasks so messages stay in PEL
                    for t in concurrent_tasks:
                        t.cancel()
                try:
                    await asyncio.gather(*concurrent_tasks, return_exceptions=True)
                except asyncio.CancelledError:
                    # We are being cancelled while draining inner tasks.
                    # Ensure every inner task is cancelled and schedule a
                    # background drain so they are not left orphaned.
                    for t in concurrent_tasks:
                        if not t.done():
                            t.cancel()
                    asyncio.ensure_future(asyncio.gather(*concurrent_tasks, return_exceptions=True))
                    raise

    async def _process_task_message(
        self,
        stream_key: str,
        message_id: Any,
        fields: dict[Any, Any],
        handler: Callable[[Task], Awaitable[None]],
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        """단일 Task 메시지를 처리하고 ACK한다. 실패 시 PEL에 남겨 XAUTOCLAIM이 재시도."""
        raw: bytes | None = fields.get(b"data") or fields.get("data")
        if raw is None:
            logger.warning("Task message missing 'data' field; ACKing id=%s", message_id)
            await self._redis.xack(stream_key, self._consumer_group, message_id)
            return

        ikey_raw = fields.get(b"idempotency_key") or fields.get("idempotency_key")
        ikey: str | None = None
        if ikey_raw is not None:
            ikey = ikey_raw.decode() if isinstance(ikey_raw, bytes) else ikey_raw
            if not await self._try_claim_dedup(ikey, scope=stream_key):
                logger.info("Skipping duplicate task (idempotency_key=%s) id=%s", ikey, message_id)
                await self._redis.xack(stream_key, self._consumer_group, message_id)
                return

        try:
            msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())
        except Exception:
            logger.exception("Failed to deserialize task message id=%s", message_id)
            if self._metrics_collector is not None:
                self._metrics_collector.record_failed(stream_key)
            await self._redis.xack(stream_key, self._consumer_group, message_id)
            return

        if not isinstance(msg, Task):
            logger.warning(
                "Non-Task payload on task stream (type=%s); ACKing id=%s",
                type(msg).__name__,
                message_id,
            )
            if self._metrics_collector is not None:
                self._metrics_collector.record_failed(stream_key)
            await self._redis.xack(stream_key, self._consumer_group, message_id)
            return

        try:
            if semaphore is not None:
                async with semaphore:
                    await handler(msg)
            else:
                await handler(msg)
        except asyncio.CancelledError:
            if ikey is not None:
                await self._release_dedup_key(ikey, scope=stream_key)
            raise
        except Exception:
            logger.exception("Error processing task message id=%s", message_id)
            if ikey is not None:
                await self._release_dedup_key(ikey, scope=stream_key)
            if self._metrics_collector is not None:
                self._metrics_collector.record_failed(stream_key)
            return  # Leave in PEL for XAUTOCLAIM retry

        if self._metrics_collector is not None:
            self._metrics_collector.record_processed(stream_key)
        await self._redis.xack(stream_key, self._consumer_group, message_id)

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
                            if self._metrics_collector is not None:
                                self._metrics_collector.record_failed(stream_str)
                            raise
                        if self._metrics_collector is not None:
                            self._metrics_collector.record_processed(stream_str)
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
                            if self._metrics_collector is not None:
                                self._metrics_collector.record_failed(stream_str)
                            raise
                        if self._metrics_collector is not None:
                            self._metrics_collector.record_processed(stream_str)
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
                                        if self._metrics_collector is not None:
                                            self._metrics_collector.record_failed(
                                                f"{stream_key}:{subscriber_group}"
                                            )
                                        raise
                                    if self._metrics_collector is not None:
                                        self._metrics_collector.record_processed(
                                            f"{stream_key}:{subscriber_group}"
                                        )
                                    await self._redis.xack(stream_key, subscriber_group, message_id)
                                else:
                                    if evt_ikey is not None:
                                        await self._release_dedup_key(
                                            evt_ikey, scope=subscriber_group
                                        )
                                    if self._metrics_collector is not None:
                                        self._metrics_collector.record_failed(
                                            f"{stream_key}:{subscriber_group}"
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
            except Exception as e:
                if not await self._handle_nogroup_error(e, [stream_key], subscriber_group):
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
            except Exception as e:
                if not await self._handle_nogroup_error(e, streams, self._consumer_group):
                    logger.exception("Unexpected error in query consumer")
                await asyncio.sleep(0.1)

    async def _handle_query_batch(self, stream_str: str, batch: list[Any]) -> None:
        """Process a batch of query messages, publishing replies to their Pub/Sub channels."""
        for message_id, fields in batch:
            reply_channel: str | None = None
            try:
                raw = fields.get(b"data") or fields.get("data")
                reply_channel_raw = fields.get(b"reply_channel") or fields.get("reply_channel")

                # Decode reply_channel early so we can send error replies even on bad data
                if reply_channel_raw is not None:
                    reply_channel = (
                        reply_channel_raw.decode()
                        if isinstance(reply_channel_raw, bytes)
                        else reply_channel_raw
                    )

                if raw is None or reply_channel_raw is None:
                    logger.warning(
                        "Query message missing required fields "
                        "(data=%s, reply_channel=%s); skipping id=%s",
                        raw is not None,
                        reply_channel_raw is not None,
                        message_id,
                    )
                    if reply_channel:
                        await self._send_error_reply(
                            reply_channel, "ValueError", "Missing required query fields"
                        )
                    await self._redis.xack(stream_str, self._consumer_group, message_id)
                    continue

                msg = self._serializer.loads(raw if isinstance(raw, bytes) else raw.encode())

                if isinstance(msg, Query):
                    handler = self._query_handlers.get(type(msg))
                    if handler is not None:
                        _handler_ok = False
                        try:
                            result = await handler(msg)
                            result_data = self._serializer.dumps(result)
                            payload = json.dumps({"data": result_data.decode("utf-8")})
                            try:
                                await self._send_reply_and_ack(
                                    reply_channel, payload, stream_str, message_id
                                )
                                _handler_ok = True
                            except Exception as pipeline_exc:
                                logger.warning(
                                    "Query reply pipeline failed (reply_channel=%s): %s",
                                    reply_channel,
                                    pipeline_exc,
                                )
                                await self._send_error_reply(
                                    reply_channel,
                                    type(pipeline_exc).__name__,
                                    str(pipeline_exc),
                                )
                                await self._redis.xack(stream_str, self._consumer_group, message_id)
                        except Exception as exc:
                            logger.warning(
                                "Query handler %s raised %s (reply_channel=%s): %s",
                                type(msg).__name__,
                                type(exc).__name__,
                                reply_channel,
                                exc,
                            )
                            if self._metrics_collector is not None:
                                self._metrics_collector.record_failed(stream_str)
                            await self._send_error_reply(
                                reply_channel, type(exc).__name__, str(exc)
                            )
                            await self._redis.xack(stream_str, self._consumer_group, message_id)
                        if _handler_ok and self._metrics_collector is not None:
                            self._metrics_collector.record_processed(stream_str)
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
                if reply_channel:
                    await self._send_error_reply(
                        reply_channel, "RuntimeError", "Internal error processing query"
                    )
                # Always ACK to prevent PEL growth on unrecoverable errors
                try:
                    await self._redis.xack(stream_str, self._consumer_group, message_id)
                except Exception as e:
                    logger.warning("Failed to XACK query message id=%s: %s", message_id, e)

    async def _send_reply_and_ack(
        self,
        reply_channel: str,
        payload: str,
        stream_str: str,
        message_id: bytes,
    ) -> None:
        """Pipeline PUBLISH reply + XACK into a single Redis round trip."""
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.publish(reply_channel, payload)
            pipe.xack(stream_str, self._consumer_group, message_id)
            await pipe.execute()

    async def _run_pubsub_reply_listener(self) -> None:
        """Background coroutine: receive PUBLISH-ed query replies and dispatch to futures.

        Uses PSUBSCRIBE on ``{app_name}:reply_ch:*`` so a single connection
        serves all concurrent ``send()`` callers without per-call subscribe
        overhead.  Polls via ``get_message()`` so that ``_running=False``
        causes a clean exit on the next iteration.
        """
        try:
            while self._running:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.01
                )
                if message is None:
                    await asyncio.sleep(0)
                    continue
                msg_type = message.get("type")
                if msg_type not in ("pmessage", "message"):
                    continue  # skip subscribe/unsubscribe confirmations
                channel = message.get("channel", b"")
                if isinstance(channel, bytes):
                    channel = channel.decode()
                # Extract correlation_id: last `:` segment of the channel name
                correlation_id = channel.rsplit(":", 1)[-1]
                future = self._pending_queries.get(correlation_id)
                if future is not None and not future.done():
                    raw = message["data"]
                    future.set_result(raw if isinstance(raw, bytes) else raw.encode())
        except asyncio.CancelledError:
            return
        except (RedisConnectionError, RedisTimeoutError, OSError) as e:
            logger.warning("Pub/Sub reply listener disconnected: %s", e)
            # Fail all pending queries immediately so callers get ConnectionError
            # rather than waiting until their individual timeouts expire.
            conn_err = ConnectionError(f"Pub/Sub reply listener disconnected: {e}")
            for fut in list(self._pending_queries.values()):
                if not fut.done():
                    fut.set_exception(conn_err)

    async def _send_error_reply(
        self, reply_channel: str, error_type: str, error_message: str
    ) -> None:
        """Publish an error reply to *reply_channel*, logging failures instead of swallowing."""
        try:
            payload = json.dumps({"error_type": error_type, "error_message": error_message})
            await self._redis.publish(reply_channel, payload)
        except Exception:
            logger.exception(
                "Failed to publish error reply to %s (%s: %s); "
                "requester will receive TimeoutError instead of the original error",
                reply_channel,
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
            # Sleep in 100 ms chunks so shutdown is responsive even with a
            # large claim_idle_ms interval (e.g. 30 s default).
            remaining = sleep_secs
            while remaining > 0 and self._running:
                await asyncio.sleep(min(0.1, remaining))
                remaining -= 0.1
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
                if self._metrics_collector is not None:
                    self._metrics_collector.record_failed(f"{stream_key}:{group}")
                await self._redis.xack(stream_key, group, message_id)
                return
            try:
                await handler(event)
            except Exception:
                if reprocess_ikey is not None:
                    await self._release_dedup_key(reprocess_ikey, scope=group)
                if self._metrics_collector is not None:
                    self._metrics_collector.record_failed(f"{stream_key}:{group}")
                raise
            if self._metrics_collector is not None:
                self._metrics_collector.record_processed(f"{stream_key}:{group}")
            await self._redis.xack(stream_key, group, message_id)
        except Exception:
            logger.exception("Error reprocessing claimed event message id=%s", message_id)

    async def _ensure_all_groups(self) -> None:
        """Recreate all registered consumer groups.

        Called at startup and after reconnect to ensure groups exist on the
        current Redis master (e.g. after a failover that promoted a new master).
        """
        for t in self._command_handlers:
            await self._ensure_group(self._stream_key("command", t.__name__), self._consumer_group)
        for t in self._task_handlers:
            await self._ensure_group(self._stream_key("task", t.__name__), self._consumer_group)
        for t in self._query_handlers:
            await self._ensure_group(self._stream_key("query", t.__name__), self._consumer_group)
        for event_type, subscriber_group, _ in self._event_subscriptions:
            await self._ensure_group(
                self._stream_key("event", event_type.__name__), subscriber_group
            )

    async def _handle_nogroup_error(self, exc: Exception, streams: list[str], group: str) -> bool:
        """Handle a NOGROUP ResponseError by recreating consumer groups.

        Returns True if *exc* was a NOGROUP error (regardless of whether the
        group recreation succeeded), False otherwise.  Guards against the case
        where the redis package is unavailable and ``RedisResponseError`` has
        been aliased to the built-in ``Exception``.
        """
        if RedisResponseError is Exception:
            return False
        if not isinstance(exc, RedisResponseError) or "NOGROUP" not in str(exc):
            return False
        logger.warning(
            "NOGROUP error detected, recreating %d consumer groups in group=%s",
            len(streams),
            group,
        )
        try:
            for stream_key in streams:
                await self._ensure_group(stream_key, group)
        except Exception:
            logger.exception(
                "Failed to recreate consumer groups after NOGROUP error (group=%s)", group
            )
        return True

    def _connection_mode_description(self) -> str:
        """Return a human-readable description of the active connection mode."""
        if self._connection_pool is not None:
            return "connection_pool"
        if self._sentinel_urls is not None:
            return f"sentinel(service={self._sentinel_service_name})"
        if self._cluster_mode:
            return f"cluster(url={self._redis_url})"
        return f"standalone(url={self._redis_url})"

    async def _reconnect(self) -> None:
        """Replace the Redis client after a connection failure.

        Only transient network errors are suppressed; configuration or
        programming errors are re-raised so callers are not silently broken.
        """
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception as e:
                logger.warning("Error closing Redis connection during reconnect: %s", e)
        mode = self._connection_mode_description()
        try:
            self._redis = self._create_redis_client()
        except (RedisConnectionError, RedisTimeoutError, OSError):
            logger.exception("Failed to reconnect to Redis (mode=%s)", mode)
            return
        except Exception:
            logger.exception(
                "Non-transient error while reconnecting to Redis (mode=%s); re-raising", mode
            )
            raise
        try:
            await self._ensure_all_groups()
        except Exception:
            logger.exception(
                "Failed to recreate consumer groups after reconnect; "
                "individual consumers will recover via NOGROUP handling"
            )
