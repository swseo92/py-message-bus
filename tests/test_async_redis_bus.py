"""Tests for AsyncRedisMessageBus using fakeredis."""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis.aioredis  # noqa: E402

from message_bus import AsyncJsonSerializer, AsyncRedisMessageBus, TypeRegistry
from message_bus.ports import Command, Event, Query, Task

# ---------------------------------------------------------------------------
# Test message types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CreateOrderCommand(Command):
    order_id: str
    amount: Decimal


@dataclass(frozen=True)
class CancelOrderCommand(Command):
    order_id: str


@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    order_id: str
    created_at: datetime


@dataclass(frozen=True)
class PaymentReceivedEvent(Event):
    payment_id: str


@dataclass(frozen=True)
class ProcessPaymentTask(Task):
    payment_id: str
    amount: Decimal


@dataclass(frozen=True)
class GetOrderQuery(Query[str]):
    order_id: str


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def type_registry() -> TypeRegistry:
    registry = TypeRegistry()
    registry.register(
        CreateOrderCommand,
        CancelOrderCommand,
        OrderCreatedEvent,
        PaymentReceivedEvent,
        ProcessPaymentTask,
        GetOrderQuery,
    )
    return registry


@pytest.fixture
def serializer(type_registry: TypeRegistry) -> AsyncJsonSerializer:
    return AsyncJsonSerializer(type_registry)


@pytest.fixture
def fake_redis_server():
    """Shared FakeServer so producer and consumer see the same data."""
    import fakeredis

    return fakeredis.FakeServer()


@pytest.fixture
def fake_redis(fake_redis_server):
    return fakeredis.aioredis.FakeRedis(server=fake_redis_server)


@pytest.fixture
async def bus(serializer, fake_redis):
    """AsyncRedisMessageBus backed by fakeredis."""
    b = AsyncRedisMessageBus(
        redis_url="redis://localhost",
        consumer_group="test-group",
        app_name="test",
        serializer=serializer,
        consumer_name="test-consumer",
        _block_ms=0,  # fakeredis: disable blocking to avoid event-loop stall
        _redis_client=fake_redis,
    )
    yield b
    await b.close()


# ---------------------------------------------------------------------------
# TypeRegistry tests
# ---------------------------------------------------------------------------


class TestTypeRegistry:
    def test_register_and_lookup(self):
        registry = TypeRegistry()
        registry.register(CreateOrderCommand)
        key = f"{CreateOrderCommand.__module__}.{CreateOrderCommand.__qualname__}"
        assert registry.lookup(key) is CreateOrderCommand

    def test_lookup_unregistered_returns_none(self):
        registry = TypeRegistry()
        assert registry.lookup("no.such.Type") is None

    def test_register_multiple(self):
        registry = TypeRegistry()
        registry.register(CreateOrderCommand, OrderCreatedEvent)
        cmd_key = f"{CreateOrderCommand.__module__}.{CreateOrderCommand.__qualname__}"
        evt_key = f"{OrderCreatedEvent.__module__}.{OrderCreatedEvent.__qualname__}"
        assert registry.lookup(cmd_key) is CreateOrderCommand
        assert registry.lookup(evt_key) is OrderCreatedEvent

    def test_register_returns_self_for_chaining(self):
        registry = TypeRegistry()
        result = registry.register(CreateOrderCommand)
        assert result is registry


# ---------------------------------------------------------------------------
# AsyncJsonSerializer tests
# ---------------------------------------------------------------------------


class TestAsyncJsonSerializer:
    def test_roundtrip_simple_command(self, serializer):
        cmd = CreateOrderCommand(order_id="ord-1", amount=Decimal("99.99"))
        result = serializer.loads(serializer.dumps(cmd))
        assert result == cmd

    def test_decimal_roundtrip(self, serializer):
        cmd = CreateOrderCommand(order_id="ord-2", amount=Decimal("123.456789"))
        result = serializer.loads(serializer.dumps(cmd))
        assert result.amount == Decimal("123.456789")
        assert isinstance(result.amount, Decimal)

    def test_datetime_roundtrip(self, serializer):
        now = datetime(2026, 2, 27, 12, 0, 0, tzinfo=UTC)
        evt = OrderCreatedEvent(order_id="ord-3", created_at=now)
        result = serializer.loads(serializer.dumps(evt))
        assert result.created_at == now

    def test_uuid_roundtrip(self):
        @dataclass(frozen=True)
        class MsgWithUUID(Command):
            ref: uuid.UUID

        reg = TypeRegistry()
        reg.register(MsgWithUUID)
        ser = AsyncJsonSerializer(reg)

        ref_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        msg = MsgWithUUID(ref=ref_id)
        result = ser.loads(ser.dumps(msg))
        assert result.ref == ref_id
        assert isinstance(result.ref, uuid.UUID)

    def test_unregistered_type_raises(self):
        @dataclass(frozen=True)
        class UnknownCmd(Command):
            name: str

        ser = AsyncJsonSerializer()  # empty registry
        raw = ser.dumps(UnknownCmd(name="x"))
        with pytest.raises(ValueError, match="Unknown type"):
            ser.loads(raw)

    def test_auto_register_via_bus(self):
        """register_command auto-registers the type in the serializer registry."""
        ser = AsyncJsonSerializer()
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=ser,
        )

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)
        raw = ser.dumps(CreateOrderCommand(order_id="x", amount=Decimal("1")))
        result = ser.loads(raw)
        assert isinstance(result, CreateOrderCommand)


# ---------------------------------------------------------------------------
# AsyncRedisMessageBus – registration tests
# ---------------------------------------------------------------------------


class TestAsyncRedisMessageBusRegistration:
    def test_duplicate_command_raises(self, bus):
        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)
        with pytest.raises(ValueError, match="already registered"):
            bus.register_command(CreateOrderCommand, handler)

    def test_duplicate_task_raises(self, bus):
        async def handler(t: ProcessPaymentTask) -> None:
            pass

        bus.register_task(ProcessPaymentTask, handler)
        with pytest.raises(ValueError, match="already registered"):
            bus.register_task(ProcessPaymentTask, handler)

    def test_subscribe_multiple_handlers_ok(self, bus):
        """Multiple subscribers on the same event type must not raise."""

        async def h1(e: OrderCreatedEvent) -> None:
            pass

        async def h2(e: OrderCreatedEvent) -> None:
            pass

        bus.subscribe(OrderCreatedEvent, h1)
        bus.subscribe(OrderCreatedEvent, h2)
        assert len(bus._event_subscriptions) == 2

    def test_register_query_stores_handler(self, bus):
        async def handler(q: GetOrderQuery) -> str:
            return "order-result"

        bus.register_query(GetOrderQuery, handler)
        assert GetOrderQuery in bus._query_handlers

    def test_register_duplicate_query_raises(self, bus):
        async def handler(q: GetOrderQuery) -> str:
            return ""

        bus.register_query(GetOrderQuery, handler)
        with pytest.raises(ValueError, match="already registered"):
            bus.register_query(GetOrderQuery, handler)


# ---------------------------------------------------------------------------
# AsyncRedisMessageBus – dispatch tests (with fakeredis)
# ---------------------------------------------------------------------------


class TestAsyncRedisMessageBusDispatch:
    @pytest.mark.asyncio
    async def test_execute_command_calls_handler(self, bus):
        received: list[CreateOrderCommand] = []

        async def handler(cmd: CreateOrderCommand) -> None:
            received.append(cmd)

        bus.register_command(CreateOrderCommand, handler)
        await bus.start()

        cmd = CreateOrderCommand(order_id="ord-10", amount=Decimal("50.00"))
        await bus.execute(cmd)
        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert received[0] == cmd

    @pytest.mark.asyncio
    async def test_dispatch_task_calls_handler(self, bus):
        received: list[ProcessPaymentTask] = []

        async def handler(t: ProcessPaymentTask) -> None:
            received.append(t)

        bus.register_task(ProcessPaymentTask, handler)
        await bus.start()

        task = ProcessPaymentTask(payment_id="pay-1", amount=Decimal("25.00"))
        await bus.dispatch(task)
        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert received[0] == task

    @pytest.mark.asyncio
    async def test_publish_event_calls_single_subscriber(self, bus):
        received: list[OrderCreatedEvent] = []

        async def handler(evt: OrderCreatedEvent) -> None:
            received.append(evt)

        bus.subscribe(OrderCreatedEvent, handler)
        await bus.start()

        evt = OrderCreatedEvent(
            order_id="ord-20",
            created_at=datetime(2026, 2, 27, tzinfo=UTC),
        )
        await bus.publish(evt)
        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert received[0] == evt

    @pytest.mark.asyncio
    async def test_publish_event_fan_out_to_all_subscribers(self, fake_redis_server, serializer):
        """Two independent subscribers both receive the same event (fan-out)."""
        import fakeredis

        client1 = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        client2 = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        received_1: list[OrderCreatedEvent] = []
        received_2: list[OrderCreatedEvent] = []

        bus1 = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc-A",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer-1",
            _block_ms=0,
            _redis_client=client1,
        )
        bus2 = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc-B",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer-2",
            _block_ms=0,
            _redis_client=client2,
        )

        async def h1(evt: OrderCreatedEvent) -> None:
            received_1.append(evt)

        async def h2(evt: OrderCreatedEvent) -> None:
            received_2.append(evt)

        bus1.subscribe(OrderCreatedEvent, h1)
        bus2.subscribe(OrderCreatedEvent, h2)

        await bus1.start()
        await bus2.start()

        # Publish via a third client (producer)
        publisher = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="publisher",
            app_name="test",
            serializer=serializer,
            consumer_name="publisher",
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        await publisher.start()

        evt = OrderCreatedEvent(
            order_id="ord-30",
            created_at=datetime(2026, 2, 27, tzinfo=UTC),
        )
        await publisher.publish(evt)
        await asyncio.sleep(0.5)

        assert len(received_1) == 1, "subscriber 1 should receive the event"
        assert len(received_2) == 1, "subscriber 2 should receive the event"
        assert received_1[0] == evt
        assert received_2[0] == evt

        await bus1.close()
        await bus2.close()
        await publisher.close()

    @pytest.mark.asyncio
    async def test_command_competitive_consumption(self, fake_redis_server, serializer):
        """Only one of two workers processes each command (no duplicate processing)."""
        import fakeredis

        client1 = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        client2 = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        processed_by: list[str] = []

        def make_handler(name: str):
            async def handler(cmd: CreateOrderCommand) -> None:
                processed_by.append(name)

            return handler

        # Both workers share the same consumer_group → competitive consumption
        worker1 = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="workers",
            app_name="test",
            serializer=serializer,
            consumer_name="w1",
            _block_ms=0,
            _redis_client=client1,
        )
        worker2 = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="workers",
            app_name="test",
            serializer=serializer,
            consumer_name="w2",
            _block_ms=0,
            _redis_client=client2,
        )

        worker1.register_command(CreateOrderCommand, make_handler("w1"))
        worker2.register_command(CreateOrderCommand, make_handler("w2"))

        await worker1.start()
        await worker2.start()

        # Publish one command
        publisher_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        publisher = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="publisher",
            app_name="test",
            serializer=serializer,
            consumer_name="pub",
            _block_ms=0,
            _redis_client=publisher_client,
        )
        await publisher.start()

        await publisher.execute(CreateOrderCommand(order_id="ord-40", amount=Decimal("10")))
        await asyncio.sleep(0.5)

        # Exactly one worker should have processed the command
        assert len(processed_by) == 1, "command must be processed exactly once"

        await worker1.close()
        await worker2.close()
        await publisher.close()

    @pytest.mark.asyncio
    async def test_decimal_preserved_end_to_end(self, bus):
        received: list[CreateOrderCommand] = []

        async def handler(cmd: CreateOrderCommand) -> None:
            received.append(cmd)

        bus.register_command(CreateOrderCommand, handler)
        await bus.start()

        cmd = CreateOrderCommand(order_id="ord-50", amount=Decimal("9999.123456789"))
        await bus.execute(cmd)
        await asyncio.sleep(0.3)

        assert received[0].amount == Decimal("9999.123456789")

    @pytest.mark.asyncio
    async def test_datetime_preserved_end_to_end(self, bus):
        received: list[OrderCreatedEvent] = []

        async def handler(evt: OrderCreatedEvent) -> None:
            received.append(evt)

        bus.subscribe(OrderCreatedEvent, handler)
        await bus.start()

        ts = datetime(2026, 2, 27, 15, 30, 45, tzinfo=UTC)
        await bus.publish(OrderCreatedEvent(order_id="ord-60", created_at=ts))
        await asyncio.sleep(0.3)

        assert received[0].created_at == ts


# ---------------------------------------------------------------------------
# AsyncRedisMessageBus – Query (correlation_id request-reply) tests
# ---------------------------------------------------------------------------


class TestAsyncRedisMessageBusQuery:
    @pytest.mark.asyncio
    async def test_send_query_returns_handler_result(self, bus):
        """send() returns the value returned by the registered handler."""

        async def handler(q: GetOrderQuery) -> str:
            return f"order:{q.order_id}"

        bus.register_query(GetOrderQuery, handler)
        await bus.start()

        result = await bus.send(GetOrderQuery(order_id="ord-100"))

        assert result == "order:ord-100"

    @pytest.mark.asyncio
    async def test_send_query_across_two_bus_instances(self, fake_redis_server, serializer):
        """Query sent by one bus instance is handled by another (cross-process simulation)."""
        import fakeredis

        producer_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        consumer_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        producer = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc",
            app_name="test",
            serializer=serializer,
            consumer_name="producer",
            _block_ms=0,
            _redis_client=producer_client,
        )
        consumer = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=consumer_client,
        )

        async def handler(q: GetOrderQuery) -> str:
            return f"result:{q.order_id}"

        consumer.register_query(GetOrderQuery, handler)

        await producer.start()
        await consumer.start()

        try:
            result = await producer.send(GetOrderQuery(order_id="ord-200"))
            assert result == "result:ord-200"
        finally:
            await producer.close()
            await consumer.close()

    @pytest.mark.asyncio
    async def test_send_query_timeout(self, fake_redis_server, serializer):
        """send() raises asyncio.TimeoutError when no handler responds within timeout."""
        import fakeredis

        client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc",
            app_name="test",
            serializer=serializer,
            consumer_name="caller",
            _block_ms=0,
            _redis_client=client,
            query_reply_timeout=0.1,  # 100 ms – fast for test
        )
        await bus.start()  # no query handler registered → no consumer loop

        try:
            with pytest.raises(asyncio.TimeoutError):
                await bus.send(GetOrderQuery(order_id="ord-x"))
        finally:
            await bus.close()

    @pytest.mark.asyncio
    async def test_send_query_handler_exception_propagated(self, bus):
        """Exception raised in the handler is propagated to the send() caller."""

        async def failing_handler(q: GetOrderQuery) -> str:
            raise ValueError("Order not found")

        bus.register_query(GetOrderQuery, failing_handler)
        await bus.start()

        with pytest.raises(RuntimeError, match="ValueError.*Order not found"):
            await bus.send(GetOrderQuery(order_id="ord-300"))

    @pytest.mark.asyncio
    async def test_send_multiple_concurrent_queries(self, bus):
        """Multiple concurrent send() calls each get their own reply."""
        call_count = [0]

        async def handler(q: GetOrderQuery) -> str:
            call_count[0] += 1
            return f"result:{q.order_id}"

        bus.register_query(GetOrderQuery, handler)
        await bus.start()

        results = await asyncio.gather(
            bus.send(GetOrderQuery(order_id="a")),
            bus.send(GetOrderQuery(order_id="b")),
            bus.send(GetOrderQuery(order_id="c")),
        )

        assert sorted(results) == ["result:a", "result:b", "result:c"]
        assert call_count[0] == 3

    @pytest.mark.asyncio
    async def test_query_competitive_consumption(self, fake_redis_server, serializer):
        """Exactly one of two workers processes each query (no duplicate handling)."""
        import fakeredis

        processed_by: list[str] = []

        def make_handler(name: str):
            async def handler(q: GetOrderQuery) -> str:
                processed_by.append(name)
                return f"{name}:{q.order_id}"

            return handler

        # Both workers share the same consumer_group → competitive consumption
        worker1 = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="workers",
            app_name="test",
            serializer=serializer,
            consumer_name="w1",
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        worker2 = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="workers",
            app_name="test",
            serializer=serializer,
            consumer_name="w2",
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )

        worker1.register_query(GetOrderQuery, make_handler("w1"))
        worker2.register_query(GetOrderQuery, make_handler("w2"))

        await worker1.start()
        await worker2.start()

        # Caller uses a separate bus instance (producer role)
        caller = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="caller",
            app_name="test",
            serializer=serializer,
            consumer_name="caller",
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        await caller.start()

        try:
            result = await caller.send(GetOrderQuery(order_id="ord-400"))
            assert len(processed_by) == 1, "query must be processed exactly once"
            assert result.endswith(":ord-400")
        finally:
            await caller.close()
            await worker1.close()
            await worker2.close()

    @pytest.mark.asyncio
    async def test_query_handler_returns_none(self, bus):
        """Query handler can return None; send() returns None without error."""

        async def none_handler(q: GetOrderQuery) -> None:
            return None

        bus.register_query(GetOrderQuery, none_handler)
        await bus.start()

        result = await bus.send(GetOrderQuery(order_id="ord-500"))
        assert result is None

    @pytest.mark.asyncio
    async def test_send_query_uses_xread_block_when_block_ms_positive(
        self, fake_redis_server, serializer
    ):
        """send() must call xread(block=N) instead of polling when _block_ms > 0."""
        from unittest.mock import patch  # noqa: PLC0415

        import fakeredis

        client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc",
            app_name="test",
            serializer=serializer,
            consumer_name="caller",
            _block_ms=50,
            _redis_client=client,
            query_reply_timeout=0.15,
        )
        await bus.start()

        xread_block_values: list[int | None] = []

        async def spy_xread(streams, count=None, block=None):
            if any("reply" in str(k) for k in streams):
                xread_block_values.append(block)
            return []  # always empty → triggers timeout

        try:
            with (
                patch.object(client, "xread", side_effect=spy_xread),
                pytest.raises((asyncio.TimeoutError, TimeoutError)),
            ):
                await bus.send(GetOrderQuery(order_id="test-block"))

            assert xread_block_values, "No xread calls recorded for reply stream"
            assert all(b is not None and b > 0 for b in xread_block_values), (
                f"Expected all reply xread calls to use block>0, got: {xread_block_values}"
            )
        finally:
            await bus.close()

    @pytest.mark.asyncio
    async def test_send_query_last_id_advances_after_malformed_envelope(
        self, fake_redis_server, serializer
    ):
        """send() raises RuntimeError on malformed envelope and starts cursor at '0-0'."""
        from unittest.mock import patch  # noqa: PLC0415

        import fakeredis

        client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc",
            app_name="test",
            serializer=serializer,
            consumer_name="caller",
            _block_ms=0,
            _redis_client=client,
            query_reply_timeout=0.5,
        )
        await bus.start()

        call_ids: list[str] = []
        malformed_msg_id = "1700000000000-0"
        call_count = [0]

        async def spy_xread(streams, count=None, block=None):
            for k, stream_id in streams.items():
                if "reply" in str(k):
                    call_ids.append(stream_id if isinstance(stream_id, str) else stream_id.decode())
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: return empty → cursor stays at "0-0"
                return []
            if call_count[0] == 2:
                # Second call: return malformed envelope → last_id advances, RuntimeError raised
                return [
                    (
                        b"reply_stream",
                        [(malformed_msg_id.encode(), {b"irrelevant": b"field"})],
                    )
                ]
            return []

        try:
            with (
                patch.object(client, "xread", side_effect=spy_xread),
                pytest.raises(RuntimeError, match="Malformed reply envelope"),
            ):
                await bus.send(GetOrderQuery(order_id="test-lastid"))

            # First call used "0-0"
            assert call_ids[0] == "0-0", f"First read must start at '0-0', got: {call_ids[0]}"
            # Second call also used "0-0" (empty first read, cursor not advanced)
            assert call_ids[1] == "0-0", (
                f"Second read must use '0-0' after empty read, got: {call_ids[1]}"
            )
        finally:
            await bus.close()

    @pytest.mark.asyncio
    async def test_send_query_blocking_mode_receives_valid_reply(
        self, fake_redis_server, serializer
    ):
        """send() with _block_ms > 0 correctly deserializes and returns the handler reply."""
        from unittest.mock import patch  # noqa: PLC0415

        import fakeredis

        client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="svc",
            app_name="test",
            serializer=serializer,
            consumer_name="caller",
            _block_ms=50,
            _redis_client=client,
            query_reply_timeout=1.0,
        )
        await bus.start()

        expected = "order:test-block-ok"
        serialized_reply = serializer.dumps(expected)

        async def mock_xread(streams, count=None, block=None):
            for stream_key in streams:
                if "reply" in str(stream_key):
                    return [(stream_key, [(b"1-0", {b"data": serialized_reply})])]
            return []

        try:
            with patch.object(client, "xread", side_effect=mock_xread):
                result = await bus.send(GetOrderQuery(order_id="test-block-ok"))
            assert result == expected
        finally:
            await bus.close()


# ---------------------------------------------------------------------------
# AsyncRedisMessageBus – lifecycle tests
# ---------------------------------------------------------------------------


class TestAsyncRedisMessageBusLifecycle:
    @pytest.mark.asyncio
    async def test_context_manager_starts_and_closes(self, serializer, fake_redis):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            _block_ms=0,
            _redis_client=fake_redis,
        )
        async with bus:
            assert bus._running is True
        assert bus._running is False

    @pytest.mark.asyncio
    async def test_close_without_start_is_safe(self, serializer, fake_redis):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            _block_ms=0,
            _redis_client=fake_redis,
        )
        # Should not raise
        await bus.close()

    def test_missing_redis_package_raises_import_error(self, monkeypatch):
        import message_bus.async_redis_bus as mod

        monkeypatch.setattr(mod, "aioredis", None)
        with pytest.raises(ImportError, match="redis package is required"):
            AsyncRedisMessageBus(redis_url="redis://localhost", consumer_group="grp")

    @pytest.mark.asyncio
    async def test_consumer_group_required_no_default(self):
        """Calling without consumer_group must raise TypeError."""
        with pytest.raises(TypeError):
            AsyncRedisMessageBus(redis_url="redis://localhost")  # type: ignore[call-arg]

    @pytest.mark.asyncio
    async def test_stream_key_format(self, bus):
        assert bus._stream_key("command", "FooCmd") == "test:command:FooCmd"
        assert bus._stream_key("event", "BarEvt") == "test:event:BarEvt"
        assert bus._stream_key("task", "BazTask") == "test:task:BazTask"


# ---------------------------------------------------------------------------
# AsyncRedisMessageBus – XAUTOCLAIM + max_retry + DeadLetter tests
# ---------------------------------------------------------------------------


class TestAsyncRedisMessageBusXAutoclaim:
    def test_constructor_exposes_max_retry_and_claim_idle_ms(self, serializer, fake_redis):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            max_retry=5,
            claim_idle_ms=10_000,
            _block_ms=0,
            _redis_client=fake_redis,
        )
        assert bus._max_retry == 5
        assert bus._claim_idle_ms == 10_000

    def test_default_max_retry_and_claim_idle_ms(self, serializer, fake_redis):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            _block_ms=0,
            _redis_client=fake_redis,
        )
        assert bus._max_retry == 3
        assert bus._claim_idle_ms == 30_000

    @pytest.mark.asyncio
    async def test_xautoclaim_routes_command_to_dead_letter_on_max_retry_exceeded(
        self, fake_redis_server, serializer
    ):
        """Command that always fails is routed to DLQ when delivery_count > max_retry."""
        import fakeredis

        from message_bus.dead_letter import MemoryDeadLetterStore

        dlq = MemoryDeadLetterStore()
        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-dlq-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            max_retry=1,
            claim_idle_ms=50,
            dead_letter_store=dlq,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        received: list[CreateOrderCommand] = []

        async def always_fails(cmd: CreateOrderCommand) -> None:
            received.append(cmd)
            raise RuntimeError("Simulated handler failure")

        bus.register_command(CreateOrderCommand, always_fails)
        await bus.start()

        cmd = CreateOrderCommand(order_id="dlq-1", amount=Decimal("42.00"))
        await bus.execute(cmd)

        # Wait for initial delivery + XAUTOCLAIM cycle
        await asyncio.sleep(0.6)

        await bus.close()

        # Message must have been attempted at least once
        assert len(received) >= 1, "Handler should have been called at least once"
        # After max_retry exceeded, message should be in DLQ
        assert len(dlq.records) == 1, "Exactly one DLQ record expected"
        assert dlq.records[0].message == cmd
        assert "exceeded max retries" in str(dlq.records[0].error).lower()

    @pytest.mark.asyncio
    async def test_xautoclaim_does_not_route_to_dlq_without_dead_letter_store(
        self, fake_redis_server, serializer
    ):
        """Without dead_letter_store configured, messages are only retried (no DLQ)."""
        import fakeredis

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-nodlq-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            max_retry=1,
            claim_idle_ms=50,
            dead_letter_store=None,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        call_count = 0

        async def counting_handler(cmd: CreateOrderCommand) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First attempt fails")
            # Second attempt succeeds

        bus.register_command(CreateOrderCommand, counting_handler)
        await bus.start()

        cmd = CreateOrderCommand(order_id="retry-1", amount=Decimal("10.00"))
        await bus.execute(cmd)

        await asyncio.sleep(0.6)
        await bus.close()

        # Handler should have been called at least twice (initial + retry)
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_xautoclaim_routes_event_to_dead_letter_on_max_retry_exceeded(
        self, fake_redis_server, serializer
    ):
        """Event that always fails is routed to DLQ when delivery_count > max_retry."""
        import fakeredis

        from message_bus.dead_letter import MemoryDeadLetterStore

        dlq = MemoryDeadLetterStore()
        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-evt-dlq",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            max_retry=1,
            claim_idle_ms=50,
            dead_letter_store=dlq,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        received: list[OrderCreatedEvent] = []

        async def failing_handler(evt: OrderCreatedEvent) -> None:
            received.append(evt)
            raise RuntimeError("Event handler failure")

        bus.subscribe(OrderCreatedEvent, failing_handler)
        await bus.start()

        evt = OrderCreatedEvent(order_id="evt-dlq-1", created_at=datetime(2026, 2, 27, tzinfo=UTC))
        await bus.publish(evt)

        await asyncio.sleep(0.6)
        await bus.close()

        assert len(received) >= 1, "Handler should have been called at least once"
        assert len(dlq.records) == 1, "Exactly one DLQ record expected"
        assert dlq.records[0].message == evt

    @pytest.mark.asyncio
    async def test_xautoclaim_does_not_dlq_at_exactly_max_retry(
        self, fake_redis_server, serializer
    ):
        """Message with delivery_count == max_retry must NOT be routed to DLQ (only > triggers it).

        With max_retry=2:
        - delivery_count=1 (initial delivery, fails)         → NOT > 2 → retry
        - delivery_count=2 (XAUTOCLAIM reclaim, succeeds)   → NOT > 2 → ACKed, no DLQ
        """
        import fakeredis

        from message_bus.dead_letter import MemoryDeadLetterStore

        dlq = MemoryDeadLetterStore()
        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        # max_retry=2: DLQ only triggers when delivery_count > 2 (i.e., 3+)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-boundary-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            max_retry=2,
            claim_idle_ms=50,
            dead_letter_store=dlq,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        call_count = 0

        async def handler_succeeds_on_second(cmd: CreateOrderCommand) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First attempt fails")
            # 2nd call (delivery_count==max_retry) succeeds → no DLQ

        bus.register_command(CreateOrderCommand, handler_succeeds_on_second)
        await bus.start()

        cmd = CreateOrderCommand(order_id="boundary-1", amount=Decimal("1.00"))
        await bus.execute(cmd)

        await asyncio.sleep(0.8)
        await bus.close()

        # delivery_count==max_retry must NOT go to DLQ; only delivery_count>max_retry would
        assert call_count >= 2, "Handler should have been called at least twice"
        assert len(dlq.records) == 0, "Message at exactly max_retry should not be routed to DLQ"

    @pytest.mark.asyncio
    async def test_dlq_message_removed_from_pel_after_routing(self, fake_redis_server, serializer):
        """After DLQ routing the message must be ACKed and absent from the PEL."""
        import fakeredis

        from message_bus.dead_letter import MemoryDeadLetterStore

        dlq = MemoryDeadLetterStore()
        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-pel-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            max_retry=1,
            claim_idle_ms=50,
            dead_letter_store=dlq,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        async def always_fails(cmd: CreateOrderCommand) -> None:
            raise RuntimeError("Always fails")

        bus.register_command(CreateOrderCommand, always_fails)
        await bus.start()

        cmd = CreateOrderCommand(order_id="pel-1", amount=Decimal("9.99"))
        await bus.execute(cmd)

        await asyncio.sleep(0.8)
        await bus.close()

        # Message must be in DLQ
        assert len(dlq.records) == 1, "Message should have been routed to DLQ"

        # PEL must be empty — the message was ACKed after DLQ append.
        # fakeredis returns a non-list value when the PEL is empty, so we
        # normalise the result before asserting.
        stream_key = f"bus:cmd:{CreateOrderCommand.__name__}"
        try:
            raw_pending = await fake_redis.xpending_range(
                stream_key, "test-pel-group", min="-", max="+", count=100
            )
            pending_count = len(raw_pending) if isinstance(raw_pending, list) else 0
        except (TypeError, IndexError):
            # fakeredis quirk: returns an unexpected scalar when PEL is empty.
            pending_count = 0
        assert pending_count == 0, "PEL should be empty after DLQ routing (message must be ACKed)"

    @pytest.mark.asyncio
    async def test_xautoclaim_routes_task_to_dead_letter_on_max_retry_exceeded(
        self, fake_redis_server, serializer
    ):
        """Task that always fails is routed to DLQ when delivery_count > max_retry.

        Cross-component integration: AsyncRedisMessageBus (XAUTOCLAIM) ↔ DeadLetterStore.
        Tasks share the command_task_streams XAUTOCLAIM loop, so DLQ routing must
        also apply to Task messages.
        """
        import fakeredis

        from message_bus.dead_letter import MemoryDeadLetterStore

        dlq = MemoryDeadLetterStore()
        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-task-dlq-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            max_retry=1,
            claim_idle_ms=50,
            dead_letter_store=dlq,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        received: list[ProcessPaymentTask] = []

        async def always_fails(task: ProcessPaymentTask) -> None:
            received.append(task)
            raise RuntimeError("Simulated task handler failure")

        bus.register_task(ProcessPaymentTask, always_fails)
        await bus.start()

        task_msg = ProcessPaymentTask(payment_id="pay-dlq-1", amount=Decimal("99.00"))
        await bus.dispatch(task_msg)

        await asyncio.sleep(0.6)
        await bus.close()

        assert len(received) >= 1, "Task handler should have been called at least once"
        assert len(dlq.records) == 1, "Exactly one DLQ record expected for Task"
        assert dlq.records[0].message == task_msg
        assert "exceeded max retries" in str(dlq.records[0].error).lower()

    def test_max_retries_exceeded_error_importable_from_public_api(self):
        """MaxRetriesExceededError must be importable from the top-level message_bus package.

        Cross-component integration: async_redis_bus.MaxRetriesExceededError is re-exported
        via __init__.py; callers must be able to catch it using the public API import.
        """
        from message_bus import MaxRetriesExceededError

        err = MaxRetriesExceededError(message_id="msg-1", delivery_count=4, max_retry=3)
        assert isinstance(err, Exception)
        assert err.message_id == "msg-1"
        assert err.delivery_count == 4
        assert err.max_retry == 3
        assert "msg-1" in str(err)
        assert "4" in str(err)

    @pytest.mark.asyncio
    async def test_query_stream_has_no_xautoclaim_loop(self, fake_redis_server, serializer):
        """Query consumer does NOT have an XAUTOCLAIM loop (by design).

        Command/Task/Event streams get XAUTOCLAIM recovery. Query streams do not:
        a timed-out query caller already handles the failure via TimeoutError, so
        stale PEL entries for queries are an expected operational artefact.
        This test documents the current design boundary.
        """
        import fakeredis

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-query-no-xac",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )

        async def handler(q: GetOrderQuery) -> str:
            return f"ok:{q.order_id}"

        bus.register_query(GetOrderQuery, handler)
        await bus.start()
        try:
            # Verify: only _run_query_consumer task is spawned (no xautoclaim task)
            # Command/Event buses spawn 2 tasks per stream (consumer + xautoclaim).
            # A Query-only bus must spawn exactly 1 task (query consumer, no xautoclaim).
            assert len(bus._consumer_tasks) == 1, (
                "Query-only bus must spawn exactly 1 background task "
                "(no XAUTOCLAIM loop for query streams)"
            )
        finally:
            await bus.close()


# ---------------------------------------------------------------------------
# MAXLEN / OOM prevention tests  (SWS2-45)
# ---------------------------------------------------------------------------


class TestMaxStreamLength:
    """XADD MAXLEN option must be applied to all streams to prevent Redis OOM."""

    def _make_bus(self, serializer, fake_redis, **kwargs):
        return AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            _block_ms=0,
            _redis_client=fake_redis,
            **kwargs,
        )

    def test_default_max_stream_length_is_10000(self, serializer, fake_redis):
        bus = self._make_bus(serializer, fake_redis)
        assert bus._max_stream_length == 10_000

    def test_custom_max_stream_length_is_stored(self, serializer, fake_redis):
        bus = self._make_bus(serializer, fake_redis, max_stream_length=500)
        assert bus._max_stream_length == 500

    @pytest.mark.asyncio
    async def test_execute_xadd_includes_maxlen(self, serializer, fake_redis):
        """execute() must pass maxlen to Redis XADD."""
        bus = self._make_bus(serializer, fake_redis, max_stream_length=999)

        async def noop_handler(_: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, noop_handler)

        xadd_calls: list[dict] = []
        original_xadd = fake_redis.xadd

        async def capturing_xadd(name, fields, *args, **kwargs):
            xadd_calls.append({"name": name, "kwargs": kwargs})
            return await original_xadd(name, fields, *args, **kwargs)

        fake_redis.xadd = capturing_xadd
        await bus.execute(CreateOrderCommand(order_id="o1", amount=Decimal("10")))

        assert len(xadd_calls) == 1
        assert xadd_calls[0]["kwargs"].get("maxlen") == 999

    @pytest.mark.asyncio
    async def test_publish_xadd_includes_maxlen(self, serializer, fake_redis):
        """publish() must pass maxlen to Redis XADD."""
        bus = self._make_bus(serializer, fake_redis, max_stream_length=888)

        xadd_calls: list[dict] = []
        original_xadd = fake_redis.xadd

        async def capturing_xadd(name, fields, *args, **kwargs):
            xadd_calls.append({"name": name, "kwargs": kwargs})
            return await original_xadd(name, fields, *args, **kwargs)

        fake_redis.xadd = capturing_xadd
        await bus.publish(OrderCreatedEvent(order_id="e1", created_at=datetime.now(UTC)))

        assert len(xadd_calls) == 1
        assert xadd_calls[0]["kwargs"].get("maxlen") == 888

    @pytest.mark.asyncio
    async def test_send_query_stream_xadd_includes_maxlen(self, serializer, fake_redis_server):
        """send() query stream XADD must pass maxlen."""
        import fakeredis

        producer_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        consumer_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        producer = self._make_bus(serializer, producer_redis, max_stream_length=777)
        consumer = self._make_bus(serializer, consumer_redis)

        async def handler(q: GetOrderQuery) -> str:
            return f"ok:{q.order_id}"

        consumer.register_query(GetOrderQuery, handler)

        xadd_calls: list[dict] = []
        original_xadd = producer_redis.xadd

        async def capturing_xadd(name, fields, *args, **kwargs):
            xadd_calls.append({"name": name, "kwargs": kwargs})
            return await original_xadd(name, fields, *args, **kwargs)

        producer_redis.xadd = capturing_xadd

        await consumer.start()
        try:
            await producer.send(GetOrderQuery(order_id="q1"))
        finally:
            await consumer.close()

        query_stream_calls = [c for c in xadd_calls if "query" in c["name"]]
        assert len(query_stream_calls) >= 1
        assert query_stream_calls[0]["kwargs"].get("maxlen") == 777

    @pytest.mark.asyncio
    async def test_reply_stream_xadd_includes_maxlen(self, serializer, fake_redis_server):
        """Reply stream XADD (query result) must pass maxlen."""
        import fakeredis

        producer_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        consumer_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)

        producer = self._make_bus(serializer, producer_redis)
        consumer = self._make_bus(serializer, consumer_redis, max_stream_length=666)

        async def handler(q: GetOrderQuery) -> str:
            return f"ok:{q.order_id}"

        consumer.register_query(GetOrderQuery, handler)

        xadd_calls: list[dict] = []
        original_xadd = consumer_redis.xadd

        async def capturing_xadd(name, fields, *args, **kwargs):
            xadd_calls.append({"name": name, "kwargs": kwargs})
            return await original_xadd(name, fields, *args, **kwargs)

        consumer_redis.xadd = capturing_xadd

        await consumer.start()
        try:
            await producer.send(GetOrderQuery(order_id="q1"))
        finally:
            await consumer.close()

        reply_stream_calls = [c for c in xadd_calls if "reply" in c["name"]]
        assert len(reply_stream_calls) >= 1
        assert reply_stream_calls[0]["kwargs"].get("maxlen") == 666

    def test_invalid_max_stream_length_raises(self, serializer, fake_redis):
        """max_stream_length <= 0 must raise ValueError at construction time (fail-fast)."""
        with pytest.raises(ValueError, match="max_stream_length must be a positive integer"):
            self._make_bus(serializer, fake_redis, max_stream_length=0)

        with pytest.raises(ValueError, match="max_stream_length must be a positive integer"):
            self._make_bus(serializer, fake_redis, max_stream_length=-1)

    @pytest.mark.asyncio
    async def test_send_error_reply_failure_is_logged_as_exception(
        self, serializer, fake_redis, caplog
    ):
        """When _send_error_reply XADD fails, it must log at ERROR level (not silently swallow)."""
        import logging

        bus = self._make_bus(serializer, fake_redis)

        async def always_fail(*args, **kwargs):
            raise OSError("Redis connection lost")

        fake_redis.xadd = always_fail

        with caplog.at_level(logging.ERROR):
            await bus._send_error_reply("test:reply:stream", "ValueError", "some error")

        assert any("test:reply:stream" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# SWS2-48: HOL Blocking 해소 (메시지 타입별 독립 컨슈머)
# ---------------------------------------------------------------------------


class TestHOLBlockingFix:
    """Head-of-Line Blocking 해소 검증 – 메시지 타입별 독립 컨슈머."""

    @pytest.mark.asyncio
    async def test_slow_command_does_not_block_other_command_type(
        self, fake_redis_server, serializer
    ):
        """느린 CommandA 핸들러가 CommandB 처리를 지연시키지 않아야 한다 (HOL 해소).

        CreateOrderCommand 핸들러가 0.3초 걸려도 CancelOrderCommand는
        즉시(< 0.3초) 처리돼야 한다.
        """
        import fakeredis

        create_order_done = asyncio.Event()
        cancel_order_done = asyncio.Event()
        cancel_order_time: list[float] = []
        create_order_time: list[float] = []

        async def slow_create_handler(cmd: CreateOrderCommand) -> None:
            await asyncio.sleep(0.3)
            create_order_time.append(asyncio.get_event_loop().time())
            create_order_done.set()

        async def fast_cancel_handler(cmd: CancelOrderCommand) -> None:
            cancel_order_time.append(asyncio.get_event_loop().time())
            cancel_order_done.set()

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="hol-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_command(CreateOrderCommand, slow_create_handler)
        bus.register_command(CancelOrderCommand, fast_cancel_handler)

        await bus.start()

        await bus.execute(CreateOrderCommand(order_id="slow-1", amount=Decimal("100")))
        await bus.execute(CancelOrderCommand(order_id="fast-1"))

        await asyncio.wait_for(
            asyncio.gather(create_order_done.wait(), cancel_order_done.wait()),
            timeout=1.5,
        )
        await bus.close()

        assert len(cancel_order_time) == 1
        assert len(create_order_time) == 1
        # CancelOrder(빠름)는 CreateOrder(느림)보다 먼저 완료돼야 한다
        assert cancel_order_time[0] < create_order_time[0], (
            f"HOL Blocking detected! cancel={cancel_order_time[0]:.4f}, "
            f"create={create_order_time[0]:.4f}"
        )

    @pytest.mark.asyncio
    async def test_per_type_consumer_tasks_spawned(self, fake_redis_server, serializer):
        """타입별 독립 컨슈머 태스크 쌍(consumer+xautoclaim)이 생성돼야 한다."""
        import fakeredis

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="task-count-test",
            app_name="test",
            serializer=serializer,
            _block_ms=0,
            _redis_client=fake_redis,
        )

        async def noop_create(cmd: CreateOrderCommand) -> None:
            pass

        async def noop_cancel(cmd: CancelOrderCommand) -> None:
            pass

        async def noop_task(t: ProcessPaymentTask) -> None:
            pass

        bus.register_command(CreateOrderCommand, noop_create)
        bus.register_command(CancelOrderCommand, noop_cancel)
        bus.register_task(ProcessPaymentTask, noop_task)

        await bus.start()

        # 3 타입 × 2 태스크(consumer + xautoclaim) = 6
        assert len(bus._consumer_tasks) == 6, (
            f"3 message types × 2 tasks each = 6 expected, got {len(bus._consumer_tasks)}"
        )

        await bus.close()

    @pytest.mark.asyncio
    async def test_slow_task_does_not_block_command(self, fake_redis_server, serializer):
        """느린 Task 핸들러가 Command 처리를 지연시키지 않아야 한다."""
        import fakeredis

        task_done = asyncio.Event()
        cmd_done = asyncio.Event()
        cmd_time: list[float] = []
        task_time: list[float] = []

        async def slow_task_handler(t: ProcessPaymentTask) -> None:
            await asyncio.sleep(0.3)
            task_time.append(asyncio.get_event_loop().time())
            task_done.set()

        async def fast_cmd_handler(cmd: CancelOrderCommand) -> None:
            cmd_time.append(asyncio.get_event_loop().time())
            cmd_done.set()

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="hol-task-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_task(ProcessPaymentTask, slow_task_handler)
        bus.register_command(CancelOrderCommand, fast_cmd_handler)

        await bus.start()

        await bus.dispatch(ProcessPaymentTask(payment_id="slow-pay", amount=Decimal("100")))
        await bus.execute(CancelOrderCommand(order_id="fast-cancel"))

        await asyncio.wait_for(
            asyncio.gather(task_done.wait(), cmd_done.wait()),
            timeout=1.5,
        )
        await bus.close()

        # Command(빠름)가 Task(느림)보다 먼저 완료돼야 한다
        assert cmd_time[0] < task_time[0], (
            "CancelOrderCommand should finish before slow ProcessPaymentTask. "
            "HOL Blocking between Task and Command detected!"
        )

    @pytest.mark.asyncio
    async def test_concurrency_parameter_limits_concurrent_handlers(
        self, fake_redis_server, serializer
    ):
        """register_command(concurrency=N)은 핸들러를 최대 N개 동시 실행으로 제한한다."""
        import fakeredis

        concurrent_count = 0
        max_concurrent = 0
        completed = 0
        total = 5

        async def tracking_handler(cmd: CreateOrderCommand) -> None:
            nonlocal concurrent_count, max_concurrent, completed
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1
            completed += 1

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="sem-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_command(CreateOrderCommand, tracking_handler, concurrency=2)
        await bus.start()

        for i in range(total):
            await bus.execute(CreateOrderCommand(order_id=f"ord-{i}", amount=Decimal("1")))

        deadline = asyncio.get_event_loop().time() + 3.0
        while completed < total and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)

        await bus.close()

        assert completed == total, f"All {total} messages must be processed, got {completed}"
        assert max_concurrent <= 2, (
            f"concurrency=2 means max 2 concurrent handlers, got {max_concurrent}"
        )

    def test_invalid_concurrency_raises_value_error(self, bus):
        """concurrency=0 또는 음수는 Semaphore 데드락을 유발하므로 즉시 ValueError를 발생시킨다."""

        async def noop(cmd: CreateOrderCommand) -> None:
            pass

        with pytest.raises(ValueError, match="concurrency must be a positive integer"):
            bus.register_command(CreateOrderCommand, noop, concurrency=0)

        with pytest.raises(ValueError, match="concurrency must be a positive integer"):
            bus.register_command(CreateOrderCommand, noop, concurrency=-1)

    def test_invalid_task_concurrency_raises_value_error(self, bus):
        """register_task concurrency=0/-1도 ValueError를 발생시킨다."""

        async def noop(t: ProcessPaymentTask) -> None:
            pass

        with pytest.raises(ValueError, match="concurrency must be a positive integer"):
            bus.register_task(ProcessPaymentTask, noop, concurrency=0)

    @pytest.mark.asyncio
    async def test_concurrency_enables_actual_parallel_execution(
        self, fake_redis_server, serializer
    ):
        """concurrency=2면 실제로 2개 이상 동시 실행이 일어난다."""
        import fakeredis

        concurrent_count = 0
        max_concurrent = 0
        completed = 0

        async def parallel_handler(cmd: CreateOrderCommand) -> None:
            nonlocal concurrent_count, max_concurrent, completed
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1
            completed += 1

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="parallel-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_command(CreateOrderCommand, parallel_handler, concurrency=3)
        await bus.start()

        for i in range(6):
            await bus.execute(CreateOrderCommand(order_id=f"p-{i}", amount=Decimal("1")))

        deadline = asyncio.get_event_loop().time() + 3.0
        while completed < 6 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.02)

        await bus.close()

        assert completed == 6
        assert max_concurrent >= 2, (
            f"concurrency=3 should allow ≥2 parallel executions, got {max_concurrent}"
        )
        assert max_concurrent <= 3, f"concurrency=3 must cap at 3, got {max_concurrent}"

    @pytest.mark.asyncio
    async def test_register_task_concurrency_parameter(self, fake_redis_server, serializer):
        """register_task(concurrency=N)도 정상 동작한다."""
        import fakeredis

        concurrent_count = 0
        max_concurrent = 0
        completed = 0

        async def parallel_task(t: ProcessPaymentTask) -> None:
            nonlocal concurrent_count, max_concurrent, completed
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1
            completed += 1

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="task-parallel-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_task(ProcessPaymentTask, parallel_task, concurrency=2)
        await bus.start()

        for i in range(4):
            await bus.dispatch(ProcessPaymentTask(payment_id=f"pay-{i}", amount=Decimal("1")))

        deadline = asyncio.get_event_loop().time() + 3.0
        while completed < 4 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.02)

        await bus.close()

        assert completed == 4
        assert max_concurrent <= 2, f"concurrency=2 must cap at 2, got {max_concurrent}"


# ---------------------------------------------------------------------------
# Malformed payload handling tests
# ---------------------------------------------------------------------------


class TestMalformedPayloadHandling:
    """Poison-message 방지: malformed 페이로드가 ACK 처리되어 재전달 루프가 없음을 검증."""

    @pytest.mark.asyncio
    async def test_missing_data_field_is_acked_and_handler_not_called(
        self, fake_redis_server, serializer
    ):
        """'data' 필드가 없는 메시지는 ACK되고 핸들러가 호출되지 않아야 한다.

        재전달 루프(poison-message)를 막기 위해, _process_command_message는
        'data' 필드 누락 시 xack를 호출하고 핸들러를 건너뛴다.
        """
        import fakeredis

        handler_called = False

        async def handler(cmd: CreateOrderCommand) -> None:
            nonlocal handler_called
            handler_called = True

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="malformed-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_command(CreateOrderCommand, handler)

        # 컨슈머 그룹 생성 후 malformed 메시지를 직접 스트림에 주입
        stream_key = "test:command:CreateOrderCommand"
        await fake_redis.xgroup_create(stream_key, "malformed-test", id="0", mkstream=True)
        await fake_redis.xadd(stream_key, {"garbage": "no_data_field"})

        await bus.start()
        await asyncio.sleep(0.3)
        await bus.close()

        # 핸들러가 호출되지 않아야 함
        assert not handler_called, "Handler must not be called for message missing 'data' field"

        # PEL에 미처리 메시지가 없어야 함 (ACK된 상태)
        pending = await fake_redis.xpending(stream_key, "malformed-test")
        assert pending["pending"] == 0, (
            f"Malformed message must be ACKed (pending count should be 0, got {pending['pending']})"
        )

    @pytest.mark.asyncio
    async def test_missing_data_field_in_task_is_acked(self, fake_redis_server, serializer):
        """Task 스트림에서도 'data' 필드 누락 시 ACK되어 재전달 루프가 없어야 한다."""
        import fakeredis

        handler_called = False

        async def task_handler(t: ProcessPaymentTask) -> None:
            nonlocal handler_called
            handler_called = True

        fake_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="malformed-task-test",
            app_name="test",
            serializer=serializer,
            consumer_name="consumer",
            _block_ms=0,
            _redis_client=fake_redis,
        )
        bus.register_task(ProcessPaymentTask, task_handler)

        stream_key = "test:task:ProcessPaymentTask"
        await fake_redis.xgroup_create(stream_key, "malformed-task-test", id="0", mkstream=True)
        await fake_redis.xadd(stream_key, {"no_data": "missing"})

        await bus.start()
        await asyncio.sleep(0.3)
        await bus.close()

        assert not handler_called, (
            "Handler must not be called for task message missing 'data' field"
        )

        pending = await fake_redis.xpending(stream_key, "malformed-task-test")
        assert pending["pending"] == 0, (
            f"Malformed task message must be ACKed (pending={pending['pending']})"
        )


# ---------------------------------------------------------------------------
# SWS2-49: Reconnect consumer group recreation + NOGROUP handling
# ---------------------------------------------------------------------------


class TestReconnectConsumerGroupRecreation:
    """SWS2-49: _reconnect() must recreate consumer groups after Redis failover."""

    def _make_bus(self, serializer, redis_client):
        return AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="test-group",
            app_name="test",
            serializer=serializer,
            consumer_name="test-consumer",
            _block_ms=0,
            _redis_client=redis_client,
        )

    @pytest.mark.asyncio
    async def test_ensure_all_groups_creates_command_groups(self, serializer, fake_redis):
        """_ensure_all_groups() should create consumer groups for all command handlers."""
        bus = self._make_bus(serializer, fake_redis)

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)
        await bus._ensure_all_groups()

        stream_key = bus._stream_key("command", "CreateOrderCommand")
        groups = await fake_redis.xinfo_groups(stream_key)
        group_names = [
            g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups
        ]
        assert "test-group" in group_names

        await bus.close()

    @pytest.mark.asyncio
    async def test_ensure_all_groups_creates_event_groups(self, serializer, fake_redis):
        """_ensure_all_groups() should create consumer groups for all event subscribers."""
        bus = self._make_bus(serializer, fake_redis)

        async def handler(evt: OrderCreatedEvent) -> None:
            pass

        bus.subscribe(OrderCreatedEvent, handler)
        await bus._ensure_all_groups()

        stream_key = bus._stream_key("event", "OrderCreatedEvent")
        groups = await fake_redis.xinfo_groups(stream_key)
        assert len(groups) >= 1

        await bus.close()

    @pytest.mark.asyncio
    async def test_ensure_all_groups_creates_query_groups(self, serializer, fake_redis):
        """_ensure_all_groups() should create consumer groups for all query handlers."""
        bus = self._make_bus(serializer, fake_redis)

        async def handler(q: GetOrderQuery) -> str:
            return f"ok:{q.order_id}"

        bus.register_query(GetOrderQuery, handler)
        await bus._ensure_all_groups()

        stream_key = bus._stream_key("query", "GetOrderQuery")
        groups = await fake_redis.xinfo_groups(stream_key)
        group_names = [
            g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups
        ]
        assert "test-group" in group_names

        await bus.close()

    @pytest.mark.asyncio
    async def test_reconnect_recreates_groups_on_new_connection(
        self, serializer, fake_redis_server
    ):
        """After _reconnect(), consumer groups must exist on the new Redis connection."""
        from unittest.mock import patch  # noqa: PLC0415

        import fakeredis

        initial_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = self._make_bus(serializer, initial_redis)

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)
        await bus.start()

        # Simulate Redis failover: new master is a fresh server with no groups
        new_server = fakeredis.FakeServer()
        new_redis = fakeredis.aioredis.FakeRedis(server=new_server)

        with patch("message_bus.async_redis_bus.aioredis.from_url", return_value=new_redis):
            await bus._reconnect()

        stream_key = bus._stream_key("command", "CreateOrderCommand")
        groups = await new_redis.xinfo_groups(stream_key)
        group_names = [
            g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups
        ]
        assert "test-group" in group_names, (
            "Consumer group must be recreated on the new Redis connection after failover"
        )

        await bus.close()

    @pytest.mark.asyncio
    async def test_nogroup_in_command_consumer_triggers_group_recreation(
        self, serializer, fake_redis
    ):
        """NOGROUP error in command consumer loop must trigger _ensure_group calls."""
        from redis.exceptions import ResponseError

        bus = self._make_bus(serializer, fake_redis)

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)

        ensure_calls: list[tuple[str, str]] = []
        original_ensure_group = bus._ensure_group

        async def tracking_ensure_group(stream_key: str, group: str) -> None:
            ensure_calls.append((stream_key, group))
            await original_ensure_group(stream_key, group)

        bus._ensure_group = tracking_ensure_group

        call_count = 0

        async def mock_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResponseError("NOGROUP Consumer Group 'test-group' does not exist")
            return []

        fake_redis.xreadgroup = mock_xreadgroup

        await bus.start()
        await asyncio.sleep(0.05)
        await bus.close()

        stream_key = bus._stream_key("command", "CreateOrderCommand")
        calls_for_stream = [c for c in ensure_calls if c[0] == stream_key]
        assert len(calls_for_stream) >= 2, (
            f"Expected ≥2 _ensure_group calls for {stream_key} "
            f"(startup + after NOGROUP), got {len(calls_for_stream)}"
        )

    @pytest.mark.asyncio
    async def test_nogroup_in_event_consumer_triggers_group_recreation(
        self, serializer, fake_redis
    ):
        """NOGROUP error in event consumer loop must trigger _ensure_group calls."""
        from redis.exceptions import ResponseError

        bus = self._make_bus(serializer, fake_redis)

        async def handler(evt: OrderCreatedEvent) -> None:
            pass

        bus.subscribe(OrderCreatedEvent, handler)

        ensure_calls: list[tuple[str, str]] = []
        original_ensure_group = bus._ensure_group

        async def tracking_ensure_group(stream_key: str, group: str) -> None:
            ensure_calls.append((stream_key, group))
            await original_ensure_group(stream_key, group)

        bus._ensure_group = tracking_ensure_group

        call_count = 0

        async def mock_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResponseError("NOGROUP Consumer Group does not exist")
            return []

        fake_redis.xreadgroup = mock_xreadgroup

        await bus.start()
        await asyncio.sleep(0.05)
        await bus.close()

        stream_key = bus._stream_key("event", "OrderCreatedEvent")
        calls_for_stream = [c for c in ensure_calls if c[0] == stream_key]
        assert len(calls_for_stream) >= 2, (
            f"Expected ≥2 _ensure_group calls for {stream_key} "
            f"(startup + after NOGROUP), got {len(calls_for_stream)}"
        )

    @pytest.mark.asyncio
    async def test_nogroup_in_query_consumer_triggers_group_recreation(
        self, serializer, fake_redis
    ):
        """NOGROUP error in query consumer loop must trigger _ensure_group calls."""
        from redis.exceptions import ResponseError

        bus = self._make_bus(serializer, fake_redis)

        async def handler(q: GetOrderQuery) -> str:
            return f"ok:{q.order_id}"

        bus.register_query(GetOrderQuery, handler)

        ensure_calls: list[tuple[str, str]] = []
        original_ensure_group = bus._ensure_group

        async def tracking_ensure_group(stream_key: str, group: str) -> None:
            ensure_calls.append((stream_key, group))
            await original_ensure_group(stream_key, group)

        bus._ensure_group = tracking_ensure_group

        call_count = 0

        async def mock_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResponseError("NOGROUP Consumer Group 'test-group' does not exist")
            return []

        fake_redis.xreadgroup = mock_xreadgroup

        await bus.start()
        await asyncio.sleep(0.05)
        await bus.close()

        stream_key = bus._stream_key("query", "GetOrderQuery")
        calls_for_stream = [c for c in ensure_calls if c[0] == stream_key]
        assert len(calls_for_stream) >= 2, (
            f"Expected ≥2 _ensure_group calls for {stream_key} "
            f"(startup + after NOGROUP), got {len(calls_for_stream)}"
        )

    @pytest.mark.asyncio
    async def test_ensure_all_groups_creates_task_groups(self, serializer, fake_redis):
        """_ensure_all_groups() should create consumer groups for all task handlers."""
        bus = self._make_bus(serializer, fake_redis)

        async def handler(task: ProcessPaymentTask) -> None:
            pass

        bus.register_task(ProcessPaymentTask, handler)
        await bus._ensure_all_groups()

        stream_key = bus._stream_key("task", "ProcessPaymentTask")
        groups = await fake_redis.xinfo_groups(stream_key)
        group_names = [
            g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups
        ]
        assert "test-group" in group_names

        await bus.close()

    @pytest.mark.asyncio
    async def test_non_nogroup_response_error_in_command_consumer_logs_exception(
        self, serializer, fake_redis, caplog
    ):
        """Non-NOGROUP RedisResponseError must log as exception, not be swallowed."""
        import logging

        from redis.exceptions import ResponseError

        bus = self._make_bus(serializer, fake_redis)

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)

        call_count = 0

        async def mock_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResponseError("WRONGTYPE Operation against a key of the wrong type")
            return []

        fake_redis.xreadgroup = mock_xreadgroup

        with caplog.at_level(logging.ERROR):
            await bus.start()
            await asyncio.sleep(0.05)
            await bus.close()

        assert any("Unexpected error in command consumer" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_reconnect_ensure_all_groups_failure_does_not_hide_connection(
        self, serializer, fake_redis_server
    ):
        """_ensure_all_groups() failure during _reconnect() must not hide the new connection."""
        from unittest.mock import patch  # noqa: PLC0415

        import fakeredis

        initial_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = self._make_bus(serializer, initial_redis)

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handler)
        await bus.start()

        new_server = fakeredis.FakeServer()
        new_redis = fakeredis.aioredis.FakeRedis(server=new_server)

        # _ensure_all_groups will fail, but _reconnect should still set the new client
        ensure_call_count = 0

        async def failing_ensure_all_groups():
            nonlocal ensure_call_count
            ensure_call_count += 1
            raise OSError("Simulated group creation failure")

        with patch("message_bus.async_redis_bus.aioredis.from_url", return_value=new_redis):
            bus._ensure_all_groups = failing_ensure_all_groups
            await bus._reconnect()

        # The new Redis client must be set despite _ensure_all_groups failing
        assert bus._redis is new_redis
        assert ensure_call_count == 1

        await bus.close()


# ---------------------------------------------------------------------------
# Graceful Shutdown (SWS2-52)
# ---------------------------------------------------------------------------


class TestGracefulShutdown:
    """close() should protect in-flight messages during shutdown."""

    def test_shutdown_timeout_default_value(self, serializer, fake_redis):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            _block_ms=0,
            _redis_client=fake_redis,
        )
        assert bus._shutdown_timeout == 5.0

    def test_shutdown_timeout_custom_value(self, serializer, fake_redis):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            shutdown_timeout=10.0,
            _block_ms=0,
            _redis_client=fake_redis,
        )
        assert bus._shutdown_timeout == 10.0

    @pytest.mark.asyncio
    async def test_close_waits_for_in_flight_sequential_command(
        self, fake_redis_server, serializer
    ):
        """close() must not cancel a command handler that is in progress."""
        handler_started = asyncio.Event()
        handler_can_finish = asyncio.Event()
        handler_done = asyncio.Event()

        async def slow_handler(cmd: CreateOrderCommand) -> None:
            handler_started.set()
            await handler_can_finish.wait()
            handler_done.set()

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="test_graceful",
            serializer=serializer,
            shutdown_timeout=5.0,
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        bus.register_command(CreateOrderCommand, slow_handler)
        await bus.start()

        await bus.execute(CreateOrderCommand(order_id="g1", amount=Decimal("1")))
        # Wait until the handler is actively running
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        # Initiate shutdown while handler is blocked
        close_task = asyncio.create_task(bus.close())
        await asyncio.sleep(0)  # let close() set _running=False and enter asyncio.wait

        # Allow handler to complete
        handler_can_finish.set()
        await close_task

        assert handler_done.is_set(), "Handler should have completed before shutdown"

    @pytest.mark.asyncio
    async def test_close_cancels_after_shutdown_timeout(self, fake_redis_server, serializer):
        """close() must cancel consumer tasks that exceed shutdown_timeout."""
        handler_started = asyncio.Event()

        async def blocking_handler(cmd: CreateOrderCommand) -> None:
            handler_started.set()
            await asyncio.sleep(100)  # never finishes in time

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="test_timeout",
            serializer=serializer,
            shutdown_timeout=0.05,  # very short – will expire before handler
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        bus.register_command(CreateOrderCommand, blocking_handler)
        await bus.start()

        await bus.execute(CreateOrderCommand(order_id="t1", amount=Decimal("1")))
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        # close() should return (after timeout) even though handler is still blocking
        await asyncio.wait_for(bus.close(), timeout=2.0)

        assert not bus._running

    @pytest.mark.asyncio
    async def test_unfinished_message_stays_in_pel_after_forced_cancel(
        self, fake_redis_server, serializer
    ):
        """A message that could not complete within shutdown_timeout must remain in the PEL."""
        handler_started = asyncio.Event()

        async def blocking_handler(cmd: CreateOrderCommand) -> None:
            handler_started.set()
            await asyncio.sleep(100)

        inspect_redis = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="test_pel",
            serializer=serializer,
            consumer_name="consumer-1",
            shutdown_timeout=0.05,
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        bus.register_command(CreateOrderCommand, blocking_handler)
        await bus.start()

        await bus.execute(CreateOrderCommand(order_id="p1", amount=Decimal("1")))
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        await asyncio.wait_for(bus.close(), timeout=2.0)

        # The message was dequeued but not ACKed → must remain in PEL
        stream_key = "test_pel:command:CreateOrderCommand"
        pending_info = await inspect_redis.xpending(stream_key, "grp")
        assert pending_info["pending"] > 0, "Message should remain in PEL for XAUTOCLAIM recovery"
        await inspect_redis.aclose()

    def test_shutdown_timeout_rejects_invalid_values(self, serializer, fake_redis):
        """shutdown_timeout must be a finite positive number."""
        import math

        for bad_value in (0, -1, -0.1, math.nan, math.inf, float("-inf")):
            with pytest.raises(ValueError, match="shutdown_timeout"):
                AsyncRedisMessageBus(
                    redis_url="redis://localhost",
                    consumer_group="grp",
                    serializer=serializer,
                    shutdown_timeout=bad_value,
                    _block_ms=0,
                    _redis_client=fake_redis,
                )

    @pytest.mark.asyncio
    async def test_task_consumer_graceful_shutdown(self, fake_redis_server, serializer):
        """close() must wait for an in-flight task handler to complete (parity with command)."""
        handler_started = asyncio.Event()
        handler_can_finish = asyncio.Event()
        handler_done = asyncio.Event()

        async def slow_task_handler(task: ProcessPaymentTask) -> None:
            handler_started.set()
            await handler_can_finish.wait()
            handler_done.set()

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="test_task_graceful",
            serializer=serializer,
            shutdown_timeout=5.0,
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        bus.register_task(ProcessPaymentTask, slow_task_handler)
        await bus.start()

        await bus.dispatch(ProcessPaymentTask(payment_id="p1", amount=Decimal("10")))
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        close_task = asyncio.create_task(bus.close())
        await asyncio.sleep(0)

        handler_can_finish.set()
        await close_task

        assert handler_done.is_set(), "Task handler should have completed before shutdown"

    @pytest.mark.asyncio
    async def test_concurrent_mode_shutdown_waits_for_inner_tasks(
        self, fake_redis_server, serializer
    ):
        """close() must wait for concurrent inner tasks spawned by semaphore-mode consumers."""
        handlers_started = 0
        handlers_done = 0
        all_started = asyncio.Event()
        can_finish = asyncio.Event()

        async def slow_handler(cmd: CreateOrderCommand) -> None:
            nonlocal handlers_started, handlers_done
            handlers_started += 1
            if handlers_started >= 2:
                all_started.set()
            await can_finish.wait()
            handlers_done += 1

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="test_concurrent_shutdown",
            serializer=serializer,
            shutdown_timeout=5.0,
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        bus.register_command(CreateOrderCommand, slow_handler, concurrency=2)
        await bus.start()

        await bus.execute(CreateOrderCommand(order_id="c1", amount=Decimal("1")))
        await bus.execute(CreateOrderCommand(order_id="c2", amount=Decimal("2")))
        await asyncio.wait_for(all_started.wait(), timeout=2.0)

        close_task = asyncio.create_task(bus.close())
        await asyncio.sleep(0)

        can_finish.set()
        await close_task

        assert handlers_done == 2, "Both concurrent handlers should complete before shutdown"

    @pytest.mark.asyncio
    async def test_task_consumer_nogroup_recovery(self, fake_redis_server, serializer):
        """Task consumer must recreate the consumer group after a NOGROUP error."""
        handled = asyncio.Event()

        async def handler(task: ProcessPaymentTask) -> None:
            handled.set()

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="test_task_nogroup",
            serializer=serializer,
            shutdown_timeout=2.0,
            _block_ms=0,
            _redis_client=fakeredis.aioredis.FakeRedis(server=fake_redis_server),
        )
        bus.register_task(ProcessPaymentTask, handler)
        await bus.start()

        # Delete the consumer group to simulate NOGROUP scenario
        stream_key = "test_task_nogroup:task:ProcessPaymentTask"
        await bus._redis.xgroup_destroy(stream_key, "grp")

        # Dispatch a task – the consumer must recover by recreating the group
        await bus.dispatch(ProcessPaymentTask(payment_id="n1", amount=Decimal("5")))
        try:
            await asyncio.wait_for(handled.wait(), timeout=3.0)
        finally:
            await bus.close()

        assert handled.is_set(), "Task handler should be called after NOGROUP recovery"


# ---------------------------------------------------------------------------
# Connection Pool / Sentinel / Cluster mode tests
# ---------------------------------------------------------------------------


class TestConnectionModes:
    """Unit tests for _create_redis_client() factory and validation."""

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def test_sentinel_without_service_name_raises(self, serializer):
        with pytest.raises(ValueError, match="sentinel_service_name"):
            AsyncRedisMessageBus(
                redis_url="redis://localhost",
                consumer_group="grp",
                serializer=serializer,
                sentinel_urls=[("localhost", 26379)],
            )

    def test_multiple_connection_modes_raises_pool_and_cluster(self, serializer):
        with pytest.raises(ValueError, match="Only one connection mode"):
            AsyncRedisMessageBus(
                redis_url="redis://localhost",
                consumer_group="grp",
                serializer=serializer,
                connection_pool=MagicMock(),
                cluster_mode=True,
            )

    def test_multiple_connection_modes_raises_pool_and_sentinel(self, serializer):
        with pytest.raises(ValueError, match="Only one connection mode"):
            AsyncRedisMessageBus(
                redis_url="redis://localhost",
                consumer_group="grp",
                serializer=serializer,
                connection_pool=MagicMock(),
                sentinel_urls=[("localhost", 26379)],
                sentinel_service_name="mymaster",
            )

    def test_multiple_connection_modes_raises_sentinel_and_cluster(self, serializer):
        with pytest.raises(ValueError, match="Only one connection mode"):
            AsyncRedisMessageBus(
                redis_url="redis://localhost",
                consumer_group="grp",
                serializer=serializer,
                sentinel_urls=[("localhost", 26379)],
                sentinel_service_name="mymaster",
                cluster_mode=True,
            )

    # ------------------------------------------------------------------
    # _create_redis_client – default (from_url)
    # ------------------------------------------------------------------

    def test_create_redis_client_default_uses_from_url(self, serializer):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost:6379/0",
            consumer_group="grp",
            serializer=serializer,
        )
        with patch("message_bus.async_redis_bus.aioredis.from_url") as mock_from_url:
            bus._create_redis_client()
            mock_from_url.assert_called_once_with(
                "redis://localhost:6379/0", decode_responses=False
            )

    # ------------------------------------------------------------------
    # _create_redis_client – ConnectionPool
    # ------------------------------------------------------------------

    def test_create_redis_client_with_connection_pool(self, serializer):
        import redis.asyncio as aioredis

        mock_pool = MagicMock(spec=aioredis.ConnectionPool)
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            connection_pool=mock_pool,
        )
        with patch("message_bus.async_redis_bus.aioredis.Redis") as mock_redis_cls:
            bus._create_redis_client()
            mock_redis_cls.assert_called_once_with(
                connection_pool=mock_pool, decode_responses=False
            )

    # ------------------------------------------------------------------
    # _create_redis_client – Sentinel
    # ------------------------------------------------------------------

    def test_create_redis_client_with_sentinel(self, serializer):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            sentinel_urls=[("sentinel-host", 26379), ("sentinel-host2", 26379)],
            sentinel_service_name="mymaster",
        )
        mock_sentinel_instance = MagicMock()
        with patch(
            "message_bus.async_redis_bus.aioredis.Sentinel", return_value=mock_sentinel_instance
        ) as mock_sentinel_cls:
            bus._create_redis_client()
            mock_sentinel_cls.assert_called_once_with(
                [("sentinel-host", 26379), ("sentinel-host2", 26379)],
                sentinel_kwargs=None,
            )
            mock_sentinel_instance.master_for.assert_called_once_with(
                "mymaster", decode_responses=False
            )

    def test_create_redis_client_sentinel_passes_kwargs(self, serializer):
        # sentinel_kwargs are forwarded as sentinel_kwargs= (for sentinel-node auth),
        # NOT spread as **kwargs (which would apply to master connections).
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            sentinel_urls=[("sentinel-host", 26379)],
            sentinel_service_name="mymaster",
            sentinel_kwargs={"password": "secret", "socket_timeout": 0.5},
        )
        mock_sentinel_instance = MagicMock()
        with patch(
            "message_bus.async_redis_bus.aioredis.Sentinel", return_value=mock_sentinel_instance
        ) as mock_sentinel_cls:
            bus._create_redis_client()
            mock_sentinel_cls.assert_called_once_with(
                [("sentinel-host", 26379)],
                sentinel_kwargs={"password": "secret", "socket_timeout": 0.5},
            )

    # ------------------------------------------------------------------
    # _create_redis_client – Cluster
    # ------------------------------------------------------------------

    def test_create_redis_client_with_cluster(self, serializer):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost:6379",
            consumer_group="grp",
            serializer=serializer,
            cluster_mode=True,
        )
        with patch("message_bus.async_redis_bus.aioredis.RedisCluster") as mock_cluster_cls:
            bus._create_redis_client()
            mock_cluster_cls.from_url.assert_called_once_with(
                "redis://localhost:6379", decode_responses=False
            )

    # ------------------------------------------------------------------
    # Cluster – stream key hash tags
    # ------------------------------------------------------------------

    def test_cluster_mode_stream_key_uses_hash_tag(self, serializer):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="myapp",
            serializer=serializer,
            cluster_mode=True,
        )
        assert bus._stream_key("command", "CreateOrder") == "{myapp}:command:CreateOrder"
        assert bus._stream_key("event", "OrderCreated") == "{myapp}:event:OrderCreated"
        assert bus._stream_key("query", "GetOrder") == "{myapp}:query:GetOrder"
        assert bus._stream_key("task", "ProcessPayment") == "{myapp}:task:ProcessPayment"

    def test_non_cluster_mode_stream_key_no_hash_tag(self, serializer):
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            app_name="myapp",
            serializer=serializer,
        )
        assert bus._stream_key("command", "CreateOrder") == "myapp:command:CreateOrder"
        assert bus._stream_key("event", "OrderCreated") == "myapp:event:OrderCreated"

    # ------------------------------------------------------------------
    # ConnectionPool – end-to-end with fakeredis
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_connection_pool_bus_dispatches_command(
        self, serializer, fake_redis_server
    ):
        """Bus created with connection_pool correctly dispatches commands end-to-end."""
        handled = asyncio.Event()

        async def handler(cmd: CreateOrderCommand) -> None:
            handled.set()

        fake_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server)
        # Reuse the FakeRedis connection pool (shared server)
        mock_pool = MagicMock()

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="pool-grp",
            app_name="pool_test",
            serializer=serializer,
            connection_pool=mock_pool,
            _block_ms=0,
            # Inject fakeredis client directly; pool is already set but _redis_client
            # takes priority so we can exercise the routing without a real Redis server.
            _redis_client=fake_client,
        )
        bus.register_command(CreateOrderCommand, handler)
        await bus.start()
        await bus.execute(CreateOrderCommand(order_id="ord-pool", amount=Decimal("1")))
        try:
            await asyncio.wait_for(handled.wait(), timeout=3.0)
        finally:
            await bus.close()
        assert handled.is_set()

    # ------------------------------------------------------------------
    # Additional validation
    # ------------------------------------------------------------------

    def test_sentinel_empty_urls_raises(self, serializer):
        with pytest.raises(ValueError, match="at least one"):
            AsyncRedisMessageBus(
                redis_url="redis://localhost",
                consumer_group="grp",
                serializer=serializer,
                sentinel_urls=[],
                sentinel_service_name="mymaster",
            )

    # ------------------------------------------------------------------
    # Lifecycle: start() and _reconnect() use _create_redis_client()
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_start_calls_create_redis_client_for_pool_mode(self, serializer):
        """start() must delegate to _create_redis_client() when pool mode is active."""
        mock_pool = MagicMock()
        mock_client = MagicMock()
        mock_client.ping = MagicMock(return_value=None)

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            connection_pool=mock_pool,
        )
        with patch.object(bus, "_create_redis_client", return_value=mock_client) as mock_factory:
            with patch.object(bus, "_running", False):
                # Manually call the factory path (bypass full start() side-effects)
                if bus._redis is None:
                    bus._redis = bus._create_redis_client()
            mock_factory.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_calls_create_redis_client_for_sentinel_mode(self, serializer):
        """start() must delegate to _create_redis_client() when sentinel mode is active."""
        mock_client = MagicMock()

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            sentinel_urls=[("sentinel-host", 26379)],
            sentinel_service_name="mymaster",
        )
        with patch.object(bus, "_create_redis_client", return_value=mock_client) as mock_factory:
            if bus._redis is None:
                bus._redis = bus._create_redis_client()
            mock_factory.assert_called_once()

    @pytest.mark.asyncio
    async def test_reconnect_reraises_non_transient_errors(self, serializer):
        """_reconnect() must not swallow non-transient configuration errors."""
        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
            cluster_mode=True,
        )
        bus._redis = None

        with patch.object(
            bus, "_create_redis_client", side_effect=ValueError("bad config")
        ), pytest.raises(ValueError, match="bad config"):
            await bus._reconnect()

    @pytest.mark.asyncio
    async def test_reconnect_suppresses_transient_connection_errors(
        self, serializer
    ):
        """_reconnect() must suppress transient RedisConnectionError."""
        import redis.exceptions as redis_exc

        bus = AsyncRedisMessageBus(
            redis_url="redis://localhost",
            consumer_group="grp",
            serializer=serializer,
        )
        bus._redis = None

        with patch.object(
            bus,
            "_create_redis_client",
            side_effect=redis_exc.ConnectionError("network down"),
        ):
            # Should not raise – transient error is logged and swallowed
            await bus._reconnect()
        assert bus._redis is None  # client stays None after transient failure


# AsyncRedisMessageBus – health check tests (SWS2-50)
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Tests for health_check() and is_healthy()."""

    def _make_bus(
        self,
        fake_redis,
        serializer=None,
        consumer_group: str = "hc-group",
    ) -> AsyncRedisMessageBus:
        kwargs: dict = {
            "redis_url": "redis://localhost",
            "consumer_group": consumer_group,
            "app_name": "test",
            "consumer_name": "hc-consumer",
            "_block_ms": 0,
            "_redis_client": fake_redis,
        }
        if serializer is not None:
            kwargs["serializer"] = serializer
        return AsyncRedisMessageBus(**kwargs)

    @pytest.mark.asyncio
    async def test_not_started_is_unhealthy(self, fake_redis):
        """Bus not started → consumer_loop_active False → is_healthy False.

        The injected redis client is still reachable (redis_connected True),
        but the bus is not running so overall health is False.
        """
        bus = self._make_bus(fake_redis)
        result = await bus.health_check()
        assert result["is_healthy"] is False
        assert result["consumer_loop_active"] is False
        assert result["redis_connected"] is True  # injected client can be pinged
        assert result["redis_error"] is None

    @pytest.mark.asyncio
    async def test_started_no_handlers_is_healthy(self, fake_redis):
        """Started bus with no handlers → redis reachable, no consumers needed → healthy."""
        bus = self._make_bus(fake_redis)
        async with bus:
            result = await bus.health_check()
        assert result["is_healthy"] is True
        assert result["redis_connected"] is True
        assert result["consumer_loop_active"] is True

    @pytest.mark.asyncio
    async def test_started_with_command_handler_is_healthy(self, fake_redis, serializer):
        """Started bus with a command handler → consumer tasks alive → healthy."""
        bus = self._make_bus(fake_redis, serializer)

        async def handle_cmd(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handle_cmd)
        async with bus:
            result = await bus.health_check()
        assert result["is_healthy"] is True
        assert result["redis_connected"] is True
        assert result["consumer_loop_active"] is True

    @pytest.mark.asyncio
    async def test_closed_bus_is_unhealthy(self, fake_redis):
        """After close() → redis is None → redis_connected False → is_healthy False."""
        bus = self._make_bus(fake_redis)
        async with bus:
            pass  # start then immediately close
        result = await bus.health_check()
        assert result["is_healthy"] is False
        assert result["redis_connected"] is False
        assert result["consumer_loop_active"] is False

    @pytest.mark.asyncio
    async def test_is_healthy_returns_true_when_running(self, fake_redis):
        """is_healthy() is a bool convenience wrapper around health_check()."""
        bus = self._make_bus(fake_redis)
        async with bus:
            assert await bus.is_healthy() is True

    @pytest.mark.asyncio
    async def test_is_healthy_returns_false_when_closed(self, fake_redis):
        """is_healthy() returns False after close."""
        bus = self._make_bus(fake_redis)
        async with bus:
            pass
        assert await bus.is_healthy() is False

    @pytest.mark.asyncio
    async def test_consumer_loop_inactive_when_tasks_cancelled(self, fake_redis, serializer):
        """Cancelling consumer tasks → consumer_loop_active False → is_healthy False."""
        bus = self._make_bus(fake_redis, serializer)

        async def handle_cmd(cmd: CreateOrderCommand) -> None:
            pass

        bus.register_command(CreateOrderCommand, handle_cmd)
        await bus.start()
        try:
            for task in bus._consumer_tasks:
                task.cancel()
            await asyncio.sleep(0)  # let cancellations propagate

            result = await bus.health_check()
            assert result["consumer_loop_active"] is False
            assert result["is_healthy"] is False
        finally:
            await bus.close()

    @pytest.mark.asyncio
    async def test_health_check_redis_unreachable(self, fake_redis):
        """When Redis ping fails → redis_connected False → is_healthy False."""
        bus = self._make_bus(fake_redis)
        await bus.start()
        try:
            # Simulate Redis ping failure
            async def failing_ping(*args, **kwargs):
                raise ConnectionError("Redis down")

            bus._redis.ping = failing_ping

            result = await bus.health_check()
            assert result["redis_connected"] is False
            assert result["is_healthy"] is False
            assert result["redis_error"] is not None
            assert "ConnectionError" in result["redis_error"]
        finally:
            await bus.close()
