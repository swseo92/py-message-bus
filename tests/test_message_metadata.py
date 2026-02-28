"""Tests for message metadata enrichment and idempotency (SWS2-46, SWS2-58).

Covers:
1. _xadd enriches stream entries with {message_id, source_id,
   idempotency_key, message_type, timestamp} in standard mode
2. metadata_mode="none" writes only {data} for maximum throughput
3. Consumer-side idempotency check: duplicate idempotency_key processed once
4. Backward compatibility: legacy messages (data-only) still processed
"""

import asyncio
from dataclasses import dataclass
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
    """_xadd (standard mode) must produce {data, message_id, source_id,
    idempotency_key, message_type, timestamp}."""

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
    async def test_xadd_source_id_is_hostname(self, bus, fake_redis):
        import socket as _socket

        cmd = PlaceOrderCommand(order_id="ord-2", amount=Decimal("20.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        raw = fields.get(b"source_id") or fields.get("source_id")
        assert raw is not None, "source_id must be present"
        source_id = raw.decode() if isinstance(raw, bytes) else raw
        assert source_id == _socket.gethostname()

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
    async def test_xadd_timestamp_is_unix_float(self, bus, fake_redis):
        import time as _time

        before = _time.time()
        cmd = PlaceOrderCommand(order_id="ord-4", amount=Decimal("40.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)
        after = _time.time()

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        raw = fields.get(b"timestamp") or fields.get("timestamp")
        assert raw is not None, "timestamp must be present"
        ts_str = raw.decode() if isinstance(raw, bytes) else raw
        ts = float(ts_str)
        assert before <= ts <= after

    @pytest.mark.asyncio
    async def test_xadd_includes_message_type(self, bus, fake_redis):
        cmd = PlaceOrderCommand(order_id="ord-4b", amount=Decimal("40.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]
        raw = fields.get(b"message_type") or fields.get("message_type")
        assert raw is not None, "message_type must be present"
        msg_type = raw.decode() if isinstance(raw, bytes) else raw
        assert msg_type == "PlaceOrderCommand"

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
                "idempotency_key": shared_key,
            },
        )
        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "msg-id-002",
                "idempotency_key": shared_key,  # same key — duplicate
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
                    "idempotency_key": f"ikey-{i}",
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
                "idempotency_key": shared_key,
            },
        )
        await fake_redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": "evt-msg-002",
                "idempotency_key": shared_key,  # duplicate
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
                "idempotency_key": ikey,
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
                "idempotency_key": ikey,  # same key — must be retried, not skipped
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


# ---------------------------------------------------------------------------
# Phase 4: metadata_mode parameter (SWS2-58)
# ---------------------------------------------------------------------------


class TestMetadataMode:
    """metadata_mode controls which fields are written to the Redis stream."""

    @pytest.fixture
    async def bus_none_mode(self, serializer, fake_redis):
        b = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-producer",
            metadata_mode="none",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        yield b
        await b.close()

    @pytest.mark.asyncio
    async def test_standard_mode_has_all_lightweight_fields(self, bus, fake_redis):
        """standard mode writes: message_id, idempotency_key, source_id, message_type, timestamp."""
        cmd = PlaceOrderCommand(order_id="std-1", amount=Decimal("10.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]

        def get_field(key: str) -> str | None:
            raw = fields.get(key.encode()) or fields.get(key)
            return (raw.decode() if isinstance(raw, bytes) else raw) if raw else None

        assert get_field("data") is not None
        assert get_field("message_id") is not None
        assert get_field("idempotency_key") is not None
        assert get_field("source_id") is not None
        assert get_field("message_type") == "PlaceOrderCommand"
        assert get_field("timestamp") is not None
        # timestamp must be parseable as a float
        float(get_field("timestamp"))  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_none_mode_omits_enrichment_fields(self, bus_none_mode, fake_redis):
        """none mode writes dedup fields only — no enrichment metadata."""
        cmd = PlaceOrderCommand(order_id="none-1", amount=Decimal("10.00"))
        stream_key = bus_none_mode._stream_key("command", "PlaceOrderCommand")
        await bus_none_mode._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, fields = messages[0]

        def has_field(key: str) -> bool:
            return bool(fields.get(key.encode()) or fields.get(key))

        assert has_field("data"), "data must always be present"
        assert has_field("message_id"), "none mode must still write message_id for dedup"
        assert has_field("idempotency_key"), "none mode must still write idempotency_key for dedup"
        assert not has_field("source_id"), "none mode must not write source_id"
        assert not has_field("message_type"), "none mode must not write message_type"
        assert not has_field("timestamp"), "none mode must not write timestamp"

    @pytest.mark.asyncio
    async def test_none_mode_messages_are_consumed(self, bus_none_mode, fake_redis):
        """none mode messages (data-only) must still be processed by consumers."""
        results: list[str] = []

        async def handler(cmd: PlaceOrderCommand) -> None:
            results.append(cmd.order_id)

        bus_none_mode.register_command(PlaceOrderCommand, handler)
        await bus_none_mode.start()

        cmd = PlaceOrderCommand(order_id="none-consume-1", amount=Decimal("5.00"))
        await bus_none_mode.execute(cmd)

        await asyncio.sleep(0.15)
        await bus_none_mode.close()

        assert "none-consume-1" in results

    @pytest.mark.asyncio
    async def test_source_id_is_cached_not_regenerated(self, bus, fake_redis):
        """source_id must be the same across multiple _xadd calls (cached hostname)."""
        cmd = PlaceOrderCommand(order_id="cache-1", amount=Decimal("1.00"))
        stream_key = bus._stream_key("command", "PlaceOrderCommand")
        await bus._xadd(stream_key, cmd)
        await bus._xadd(stream_key, cmd)

        messages = await fake_redis.xrange(stream_key)
        _, f1 = messages[0]
        _, f2 = messages[1]
        sid1 = f1.get(b"source_id") or f1.get("source_id")
        sid2 = f2.get(b"source_id") or f2.get("source_id")
        s1 = sid1.decode() if isinstance(sid1, bytes) else sid1
        s2 = sid2.decode() if isinstance(sid2, bytes) else sid2
        assert s1 == s2, "source_id must be identical across calls (cached)"
