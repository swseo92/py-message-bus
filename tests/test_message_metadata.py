"""Tests for message metadata enrichment and idempotency (SWS2-46).

Covers:
1. _xadd enriches stream entries with {message_id, producer_id,
   idempotency_key, enqueue_timestamp}
2. Consumer-side idempotency check: duplicate idempotency_key processed once
3. Backward compatibility: legacy messages (data-only) still processed
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import pytest

pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402
import fakeredis.aioredis  # noqa: E402, F401

from message_bus import AsyncJsonSerializer, AsyncRedisMessageBus, TypeRegistry
from message_bus.ports import Command, Event

# ---------------------------------------------------------------------------
# Test message types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlaceOrderCommand(Command):
    order_id: str
    amount: Decimal


@dataclass(frozen=True)
class StockUpdatedEvent(Event):
    product_id: str
    quantity: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def type_registry() -> TypeRegistry:
    registry = TypeRegistry()
    registry.register(PlaceOrderCommand, StockUpdatedEvent)
    return registry


@pytest.fixture
def serializer(type_registry: TypeRegistry) -> AsyncJsonSerializer:
    return AsyncJsonSerializer(type_registry)


@pytest.fixture
def fake_redis_server():
    return fakeredis.FakeServer()


@pytest.fixture
def fake_redis(fake_redis_server):
    return fakeredis.aioredis.FakeRedis(server=fake_redis_server)


@pytest.fixture
async def bus(serializer, fake_redis):
    b = AsyncRedisMessageBus(
        redis_url="redis://localhost",
        consumer_group="test-group",
        app_name="test",
        serializer=serializer,
        consumer_name="test-producer",
        _block_ms=0,
        _redis_client=fake_redis,
    )
    yield b
    await b.close()


# ---------------------------------------------------------------------------
# Phase 1: Schema enrichment — _xadd must write metadata fields
# ---------------------------------------------------------------------------


class TestMessageSchemaEnrichment:
    """_xadd must produce {data, message_id, producer_id, idempotency_key, enqueue_timestamp}."""

    @pytest.mark.asyncio
    async def test_xadd_includes_message_id(self, bus, fake_redis):
        cmd = PlaceOrderCommand(order_id="ord-1", amount=Decimal("10.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        assert len(messages) == 1
        _, fields = messages[0]
        raw = fields.get(b"message_id") or fields.get("message_id")
        assert raw is not None, "message_id must be present"
        assert len(raw) > 0

    @pytest.mark.asyncio
    async def test_xadd_producer_id_matches_consumer_name(self, bus, fake_redis):
        cmd = PlaceOrderCommand(order_id="ord-2", amount=Decimal("20.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        raw = fields.get(b"producer_id") or fields.get("producer_id")
        assert raw is not None, "producer_id must be present"
        producer_id = raw.decode() if isinstance(raw, bytes) else raw
        assert producer_id == "test-producer"

    @pytest.mark.asyncio
    async def test_xadd_includes_idempotency_key(self, bus, fake_redis):
        cmd = PlaceOrderCommand(order_id="ord-3", amount=Decimal("30.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        raw = fields.get(b"idempotency_key") or fields.get("idempotency_key")
        assert raw is not None, "idempotency_key must be present"

    @pytest.mark.asyncio
    async def test_xadd_enqueue_timestamp_is_valid_utc(self, bus, fake_redis):
        before = datetime.now(UTC)
        cmd = PlaceOrderCommand(order_id="ord-4", amount=Decimal("40.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)
        after = datetime.now(UTC)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        raw = fields.get(b"enqueue_timestamp") or fields.get("enqueue_timestamp")
        assert raw is not None, "enqueue_timestamp must be present"
        ts_str = raw.decode() if isinstance(raw, bytes) else raw
        ts = datetime.fromisoformat(ts_str)
        assert before <= ts <= after

    @pytest.mark.asyncio
    async def test_idempotency_key_equals_message_id_by_default(self, bus, fake_redis):
        cmd = PlaceOrderCommand(order_id="ord-5", amount=Decimal("50.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        mid_raw = fields.get(b"message_id") or fields.get("message_id")
        ikey_raw = fields.get(b"idempotency_key") or fields.get("idempotency_key")
        mid = mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw
        ikey = ikey_raw.decode() if isinstance(ikey_raw, bytes) else ikey_raw
        assert mid == ikey, "Default idempotency_key must equal message_id"

    @pytest.mark.asyncio
    async def test_each_xadd_generates_unique_message_id(self, bus, fake_redis):
        cmd = PlaceOrderCommand(order_id="ord-6", amount=Decimal("60.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        assert len(messages) == 2
        _, f1 = messages[0]
        _, f2 = messages[1]
        mid1 = f1.get(b"message_id") or f1.get("message_id")
        mid2 = f2.get(b"message_id") or f2.get("message_id")
        assert mid1 != mid2, "Each _xadd must produce a unique message_id"


# ---------------------------------------------------------------------------
# Phase 2: Consumer-side idempotency
# ---------------------------------------------------------------------------


class TestConsumerIdempotency:
    """Duplicate idempotency_key must cause message to be skipped after first processing."""

    @pytest.mark.asyncio
    async def test_duplicate_idempotency_key_processed_once(self, bus, fake_redis):
        """Two messages sharing the same idempotency_key: handler called once."""
        results: list[str] = []

        async def handler(cmd: PlaceOrderCommand) -> None:
            results.append(cmd.order_id)

        bus.register_command(PlaceOrderCommand, handler)
        await bus.start()

        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        cmd = PlaceOrderCommand(order_id="dup-1", amount=Decimal("99.00"))
        data = bus._serializer.dumps(cmd)
        shared_key = "shared-idempotency-key"

        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "msg-id-001",
                "producer_id": "producer-1",
                "idempotency_key": shared_key,
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
        )
        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "msg-id-002",
                "producer_id": "producer-1",
                "idempotency_key": shared_key,  # same key — duplicate
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
        )

        await asyncio.sleep(0.15)
        await bus.close()

        assert results.count("dup-1") == 1, "Handler must be called exactly once for duplicate"

    @pytest.mark.asyncio
    async def test_unique_idempotency_keys_all_processed(self, bus, fake_redis):
        results: list[str] = []

        async def handler(cmd: PlaceOrderCommand) -> None:
            results.append(cmd.order_id)

        bus.register_command(PlaceOrderCommand, handler)
        await bus.start()

        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        for i in range(3):
            cmd = PlaceOrderCommand(order_id=f"ord-{i}", amount=Decimal("10.00"))
            data = bus._serializer.dumps(cmd)
            await fake_redis.xadd(
                stream_key,
                {
                    "data": data,
                    "message_id": f"msg-{i}",
                    "producer_id": "producer-1",
                    "idempotency_key": f"ikey-{i}",
                    "enqueue_timestamp": datetime.now(UTC).isoformat(),
                },
            )

        await asyncio.sleep(0.15)
        await bus.close()

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_legacy_message_without_metadata_still_processed(self, bus, fake_redis):
        """Backward compat: data-only entries (no message_id) must be processed."""
        results: list[str] = []

        async def handler(cmd: PlaceOrderCommand) -> None:
            results.append(cmd.order_id)

        bus.register_command(PlaceOrderCommand, handler)
        await bus.start()

        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        cmd = PlaceOrderCommand(order_id="legacy-1", amount=Decimal("99.00"))
        data = bus._serializer.dumps(cmd)
        await fake_redis.xadd(stream_key, {"data": data})  # legacy format

        await asyncio.sleep(0.15)
        await bus.close()

        assert "legacy-1" in results

    @pytest.mark.asyncio
    async def test_duplicate_event_idempotency_key_processed_once(
        self, bus, fake_redis, fake_redis_server
    ):
        """Duplicate event with same idempotency_key: subscriber handler called once."""
        results: list[str] = []

        async def handler(evt: StockUpdatedEvent) -> None:
            results.append(evt.product_id)

        bus.subscribe(StockUpdatedEvent, handler)
        await bus.start()

        stream_key = bus._stream_key("event", "StockUpdatedEvent")
        evt = StockUpdatedEvent(product_id="prod-1", quantity=5)
        data = bus._serializer.dumps(evt)
        shared_key = "event-shared-key"

        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "evt-msg-001",
                "producer_id": "producer-1",
                "idempotency_key": shared_key,
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
        )
        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "evt-msg-002",
                "producer_id": "producer-1",
                "idempotency_key": shared_key,  # duplicate
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
        )

        await asyncio.sleep(0.15)
        await bus.close()

        assert results.count("prod-1") == 1, "Event handler must be called once for duplicate"


# ---------------------------------------------------------------------------
# Phase 3: Retry-after-failure — dedup key released on handler error
# ---------------------------------------------------------------------------


async def _poll_until(condition, *, timeout: float = 2.0, interval: float = 0.02) -> bool:
    """Poll *condition()* every *interval* seconds until True or *timeout* expires."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False


class TestRetryAfterFailure:
    """Handler failures must release the dedup key so the message can be retried."""

    @pytest.mark.asyncio
    async def test_failed_handler_allows_retry(self, bus, fake_redis):
        """If the first handler invocation raises, the message must be retried."""
        call_count = 0
        results: list[str] = []

        async def handler(cmd: PlaceOrderCommand) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient failure")
            results.append(cmd.order_id)

        bus.register_command(PlaceOrderCommand, handler)
        await bus.start()

        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        cmd = PlaceOrderCommand(order_id="retry-1", amount=Decimal("10.00"))
        data = bus._serializer.dumps(cmd)
        ikey = "retry-ikey-001"

        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "msg-retry",
                "producer_id": "prod",
                "idempotency_key": ikey,
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
        )

        # Wait for first attempt (which fails and releases the key), then inject
        # the same message again to simulate XAUTOCLAIM re-delivery.
        await _poll_until(lambda: call_count >= 1)
        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "msg-retry-2",
                "producer_id": "prod",
                "idempotency_key": ikey,  # same key — must be retried, not skipped
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
        )

        succeeded = await _poll_until(lambda: len(results) >= 1)
        await bus.close()

        assert succeeded, "Handler must succeed on second delivery after key release"
        assert results == ["retry-1"]

    @pytest.mark.asyncio
    async def test_invalid_idempotency_ttl_raises(self):
        """idempotency_ttl <= 0 must raise ValueError at construction time."""
        import fakeredis as _fakeredis

        fake_redis = _fakeredis.aioredis.FakeRedis()
        with pytest.raises(ValueError, match="idempotency_ttl"):
            AsyncRedisMessageBus(
                redis_url="redis://localhost",
                consumer_group="test-group",
                idempotency_ttl=0,
                _redis_client=fake_redis,
            )
