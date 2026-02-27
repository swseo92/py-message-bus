"""Tests for AsyncRedisMessageBus using fakeredis."""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

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
