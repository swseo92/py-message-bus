"""E2E tests for AsyncRedisMessageBus against a real Redis server.

Requires Redis 7 running at redis://localhost:6379/0
(e.g. via `docker run -p 6379:6379 redis:7`).

Each test uses a unique app_name (uuid-based) so tests are fully isolated
from each other and from production streams.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass

import pytest
import redis.asyncio as aioredis

from message_bus import AsyncRedisMessageBus, BusMetricsCollector
from message_bus.ports import Command, Event, Query, Task
from message_bus.redis_dead_letter import RedisDeadLetterStore

# ---------------------------------------------------------------------------
# Mark all tests in this module as e2e
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.e2e

REDIS_URL = "redis://localhost:6379/0"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def unique_app() -> str:
    """Return a unique app_name prefix to isolate each test's Redis keys."""
    return f"e2e_{uuid.uuid4().hex[:12]}"


async def flush_keys(pattern: str) -> None:
    """Delete all Redis keys matching *pattern* (for cleanup)."""
    client = aioredis.from_url(REDIS_URL, decode_responses=False)
    try:
        keys = await client.keys(pattern)
        if keys:
            await client.delete(*keys)
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Message types (shared across tests)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PingCommand(Command):
    payload: str


@dataclass(frozen=True)
class SlowCommand(Command):
    delay: float
    label: str


@dataclass(frozen=True)
class FastCommand(Command):
    label: str


@dataclass(frozen=True)
class SampleEvent(Event):
    value: str


@dataclass(frozen=True)
class EchoQuery(Query[str]):
    text: str


@dataclass(frozen=True)
class SampleTask(Task):
    name: str


@dataclass(frozen=True)
class FailingCommand(Command):
    reason: str


# ---------------------------------------------------------------------------
# SWS2-45: XADD MAXLEN — stream length is capped near max_stream_length
# ---------------------------------------------------------------------------


class TestSws245MaxStreamLength:
    @pytest.mark.asyncio
    async def test_stream_length_capped(self):
        app = unique_app()
        max_len = 20
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            max_stream_length=max_len,
        )

        handled: list[str] = []

        async def handler(cmd: PingCommand) -> None:
            handled.append(cmd.payload)

        bus.register_command(PingCommand, handler)

        try:
            await bus.start()
            # Publish 3× max_len messages
            for i in range(max_len * 3):
                await bus.execute(PingCommand(payload=f"msg-{i}"))

            # Give the consumer time to process them
            await asyncio.sleep(1.5)

            # Query XLEN directly
            stream_key = f"{app}:command:PingCommand"
            client = aioredis.from_url(REDIS_URL, decode_responses=False)
            try:
                xlen = await client.xlen(stream_key)
            finally:
                await client.aclose()

            # Redis approximate trimming: length should be close to max_len
            # (may slightly exceed due to approximate trimming)
            assert xlen <= max_len * 2, (
                f"Stream length {xlen} should be near max_stream_length={max_len}"
            )
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-46: Message metadata enrichment + idempotency
# ---------------------------------------------------------------------------


class TestSws246MessageMetadata:
    @pytest.mark.asyncio
    async def test_metadata_fields_present_in_stream(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
        )
        handled: list[PingCommand] = []

        async def handler(cmd: PingCommand) -> None:
            handled.append(cmd)

        bus.register_command(PingCommand, handler)

        try:
            await bus.start()
            await bus.execute(PingCommand(payload="meta-test"))
            await asyncio.sleep(0.5)

            # Read the raw stream entry to inspect metadata
            stream_key = f"{app}:command:PingCommand"
            client = aioredis.from_url(REDIS_URL, decode_responses=True)
            try:
                entries = await client.xrange(stream_key, count=5)
            finally:
                await client.aclose()

            assert entries, "No entries found in stream"
            _, fields = entries[0]
            assert "message_id" in fields, "message_id metadata missing"
            assert "producer_id" in fields, "producer_id metadata missing"
            assert "idempotency_key" in fields, "idempotency_key metadata missing"
            assert "enqueue_timestamp" in fields, "enqueue_timestamp metadata missing"
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")

    @pytest.mark.asyncio
    async def test_duplicate_idempotency_key_processed_once(self):
        """Manually inject two messages with the same idempotency_key; only first is handled."""
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            idempotency_ttl=60,
        )
        handled_count = 0

        async def handler(cmd: PingCommand) -> None:
            nonlocal handled_count
            handled_count += 1

        bus.register_command(PingCommand, handler)
        stream_key = f"{app}:command:PingCommand"

        try:
            await bus.start()

            # Inject two entries with the same idempotency_key directly
            ikey = uuid.uuid4().hex

            from message_bus.async_redis_bus import AsyncJsonSerializer, TypeRegistry

            reg = TypeRegistry()
            reg.register(PingCommand)
            ser = AsyncJsonSerializer(reg)
            data = ser.dumps(PingCommand(payload="dup-test"))

            client = aioredis.from_url(REDIS_URL, decode_responses=False)
            try:
                for _ in range(2):
                    await client.xadd(
                        stream_key,
                        {
                            "data": data,
                            "message_id": uuid.uuid4().hex,
                            "producer_id": "test-producer",
                            "idempotency_key": ikey,
                            "enqueue_timestamp": "2026-01-01T00:00:00+00:00",
                        },
                    )
            finally:
                await client.aclose()

            await asyncio.sleep(1.0)
            assert handled_count == 1, f"Expected 1 handled (dedup), got {handled_count}"
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-47: Query reply via XREAD block
# ---------------------------------------------------------------------------


class TestSws247QueryReply:
    @pytest.mark.asyncio
    async def test_send_query_returns_handler_result(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            query_reply_timeout=10.0,
        )

        async def echo_handler(q: EchoQuery) -> str:
            return f"pong:{q.text}"

        bus.register_query(EchoQuery, echo_handler)

        try:
            await bus.start()
            result = await bus.send(EchoQuery(text="hello"))
            assert result == "pong:hello"
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")

    @pytest.mark.asyncio
    async def test_send_multiple_queries_concurrently(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            query_reply_timeout=10.0,
        )

        async def echo_handler(q: EchoQuery) -> str:
            return f"reply:{q.text}"

        bus.register_query(EchoQuery, echo_handler)

        try:
            await bus.start()
            results = await asyncio.gather(
                bus.send(EchoQuery(text="a")),
                bus.send(EchoQuery(text="b")),
                bus.send(EchoQuery(text="c")),
            )
            assert set(results) == {"reply:a", "reply:b", "reply:c"}
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-48: HOL Blocking 해소 — slow handler on one type doesn't block another
# ---------------------------------------------------------------------------


class TestSws248HolBlocking:
    @pytest.mark.asyncio
    async def test_fast_command_not_blocked_by_slow_command(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
        )

        slow_started = asyncio.Event()
        fast_done = asyncio.Event()
        slow_done = asyncio.Event()

        async def slow_handler(cmd: SlowCommand) -> None:
            slow_started.set()
            await asyncio.sleep(cmd.delay)
            slow_done.set()

        async def fast_handler(cmd: FastCommand) -> None:
            fast_done.set()

        bus.register_command(SlowCommand, slow_handler)
        bus.register_command(FastCommand, fast_handler)

        try:
            await bus.start()
            # Send slow command first, then fast
            await bus.execute(SlowCommand(delay=2.0, label="slow"))
            await asyncio.sleep(0.1)  # let slow consumer pick it up
            await bus.execute(FastCommand(label="fast"))

            # Fast command should complete well before the slow one finishes
            await asyncio.wait_for(fast_done.wait(), timeout=5.0)
            assert not slow_done.is_set(), "Slow command should still be running"
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-49: Reconnect / Consumer Group auto-recreation
# ---------------------------------------------------------------------------


class TestSws249ReconnectConsumerGroup:
    @pytest.mark.asyncio
    async def test_consumer_group_recreated_after_destroy(self):
        """Bus should auto-recreate consumer group if it's been deleted."""
        app = unique_app()
        group = f"{app}-grp"

        # First bus: register & start to create the stream + group
        bus1 = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=group,
            app_name=app,
        )

        async def handler(cmd: PingCommand) -> None:
            pass

        bus1.register_command(PingCommand, handler)

        try:
            await bus1.start()
            await bus1.execute(PingCommand(payload="first"))
            await asyncio.sleep(0.3)
        finally:
            await bus1.close()

        # Destroy the consumer group via redis-cli equivalent
        stream_key = f"{app}:command:PingCommand"
        client = aioredis.from_url(REDIS_URL, decode_responses=False)
        try:
            await client.xgroup_destroy(stream_key, group)
        finally:
            await client.aclose()

        # Second bus: should auto-recreate the consumer group on start
        bus2 = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=group,
            app_name=app,
        )
        handled: list[str] = []

        async def handler2(cmd: PingCommand) -> None:
            handled.append(cmd.payload)

        bus2.register_command(PingCommand, handler2)

        try:
            await bus2.start()
            await bus2.execute(PingCommand(payload="after-recreate"))
            await asyncio.sleep(0.8)
            assert "after-recreate" in handled, "Message not handled after group recreation"
        finally:
            await bus2.close()
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-50: Health Check
# ---------------------------------------------------------------------------


class TestSws250HealthCheck:
    @pytest.mark.asyncio
    async def test_health_before_start(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
        )
        result = await bus.health_check()
        # Before start: redis client is None → not connected
        assert result["redis_connected"] is False
        assert result["is_healthy"] is False

    @pytest.mark.asyncio
    async def test_health_after_start(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
        )

        try:
            await bus.start()
            result = await bus.health_check()
            assert result["redis_connected"] is True
            # No handlers registered → consumer_loop_active should be True
            # (no consumers needed)
            assert result["consumer_loop_active"] is True
            assert result["is_healthy"] is True
            assert await bus.is_healthy() is True
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")

    @pytest.mark.asyncio
    async def test_health_after_close(self):
        app = unique_app()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
        )
        await bus.start()
        await bus.close()
        result = await bus.health_check()
        # After close: redis client is None → not connected
        assert result["redis_connected"] is False
        assert result["is_healthy"] is False
        await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-51: Monitoring Metrics
# ---------------------------------------------------------------------------


class TestSws251Metrics:
    @pytest.mark.asyncio
    async def test_metrics_snapshot_after_processing(self):
        app = unique_app()
        collector = BusMetricsCollector()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            metrics_collector=collector,
        )

        processed = asyncio.Event()
        count = 0

        async def handler(cmd: PingCommand) -> None:
            nonlocal count
            count += 1
            if count >= 3:
                processed.set()

        bus.register_command(PingCommand, handler)

        try:
            await bus.start()
            for i in range(3):
                await bus.execute(PingCommand(payload=f"msg-{i}"))

            await asyncio.wait_for(processed.wait(), timeout=5.0)

            snap = await bus.get_metrics_snapshot(include_pel=True)
            assert snap is not None
            assert len(snap.streams) > 0

            stream_key = f"{app}:command:PingCommand"
            cmd_metrics = next((s for s in snap.streams if s.stream_key == stream_key), None)
            assert cmd_metrics is not None, f"No metrics for stream {stream_key}"
            assert cmd_metrics.processed_total == 3
            assert cmd_metrics.failed_total == 0
            assert snap.total_processed == 3
        finally:
            await bus.close()
            await flush_keys(f"{app}:*")

    @pytest.mark.asyncio
    async def test_metrics_snapshot_without_start(self):
        """get_metrics_snapshot can be called before start (include_pel=False)."""
        app = unique_app()
        collector = BusMetricsCollector()
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            metrics_collector=collector,
        )

        async def handler(cmd: PingCommand) -> None:
            pass

        bus.register_command(PingCommand, handler)
        snap = await bus.get_metrics_snapshot(include_pel=False)
        assert snap is not None
        assert len(snap.streams) == 1
        assert snap.streams[0].processed_total == 0
        assert snap.streams[0].pel_size == -1  # not queried


# ---------------------------------------------------------------------------
# SWS2-52: Graceful Shutdown — in-flight messages complete before close
# ---------------------------------------------------------------------------


class TestSws252GracefulShutdown:
    @pytest.mark.asyncio
    async def test_inflight_message_completes_during_shutdown(self):
        app = unique_app()
        shutdown_timeout = 3.0
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            shutdown_timeout=shutdown_timeout,
        )

        handler_started = asyncio.Event()
        handler_done = asyncio.Event()

        async def slow_handler(cmd: SlowCommand) -> None:
            handler_started.set()
            await asyncio.sleep(cmd.delay)
            handler_done.set()

        bus.register_command(SlowCommand, slow_handler)

        try:
            await bus.start()
            await bus.execute(SlowCommand(delay=1.0, label="inflight"))

            # Wait for handler to start, then initiate close
            await asyncio.wait_for(handler_started.wait(), timeout=5.0)
            await bus.close()

            # Handler should have completed within shutdown_timeout
            assert handler_done.is_set(), "In-flight handler did not complete before shutdown"
        finally:
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-53: Connection Pool
# ---------------------------------------------------------------------------


class TestSws253ConnectionPool:
    @pytest.mark.asyncio
    async def test_connection_pool_works(self):
        """Bus accepts an external ConnectionPool and processes messages normally."""
        app = unique_app()
        pool = aioredis.ConnectionPool.from_url(REDIS_URL, decode_responses=False)
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            connection_pool=pool,
        )

        done = asyncio.Event()

        async def handler(cmd: PingCommand) -> None:
            done.set()

        bus.register_command(PingCommand, handler)

        try:
            await bus.start()
            await bus.execute(PingCommand(payload="pool-test"))
            await asyncio.wait_for(done.wait(), timeout=5.0)
            assert done.is_set()
        finally:
            await bus.close()
            await pool.aclose()
            await flush_keys(f"{app}:*")

    @pytest.mark.asyncio
    async def test_health_check_with_connection_pool(self):
        app = unique_app()
        pool = aioredis.ConnectionPool.from_url(REDIS_URL, decode_responses=False)
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            connection_pool=pool,
        )
        try:
            await bus.start()
            result = await bus.health_check()
            assert result["redis_connected"] is True
        finally:
            await bus.close()
            await pool.aclose()
            await flush_keys(f"{app}:*")


# ---------------------------------------------------------------------------
# SWS2-54: Persistent DeadLetterStore (RedisDeadLetterStore)
# ---------------------------------------------------------------------------


class TestSws254PersistentDeadLetterStore:
    @pytest.mark.asyncio
    async def test_failed_message_captured_in_dlq(self):
        app = unique_app()
        dlq_key = f"{app}:dlq"
        dlq = RedisDeadLetterStore(redis_url=REDIS_URL, stream_key=dlq_key)

        # claim_idle_ms=500: XAUTOCLAIM runs every 0.5s and claims messages
        # idle for >500ms.  max_retry=0: delivery_count=1 exceeds max_retry
        # on the first XAUTOCLAIM pass, routing the message to the DLQ.
        bus = AsyncRedisMessageBus(
            redis_url=REDIS_URL,
            consumer_group=f"{app}-grp",
            app_name=app,
            dead_letter_store=dlq,
            max_retry=0,
            claim_idle_ms=500,  # short idle so XAUTOCLAIM triggers quickly
        )

        async def failing_handler(cmd: FailingCommand) -> None:
            raise ValueError(cmd.reason)

        bus.register_command(FailingCommand, failing_handler)

        try:
            await bus.start()
            await bus.execute(FailingCommand(reason="test-failure"))

            # Wait for handler to fail (leaves message in PEL), then for
            # XAUTOCLAIM to run and route to DLQ.
            # claim_idle_ms=500 → XAUTOCLAIM loop sleeps ~0.5s, then claims
            # messages idle >500ms.  Allow up to 10s total.
            for _ in range(20):
                await asyncio.sleep(0.5)
                if dlq.count() > 0:
                    break

            entries = dlq.list(limit=10)
            assert len(entries) > 0, "No entries in dead-letter store"
            entry = entries[0]
            assert "FailingCommand" in entry.message_type
            assert entry.error_type == "MaxRetriesExceededError"

            # get() by ID
            fetched = dlq.get(entry.id)
            assert fetched is not None
            assert fetched.id == entry.id

            # delete()
            dlq.delete(entry.id)
            assert dlq.get(entry.id) is None
        finally:
            await bus.close()
            dlq.close()
            await flush_keys(f"{app}:*")
