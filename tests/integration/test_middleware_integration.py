"""Integration test: middleware with realistic modular monolith setup."""

from __future__ import annotations

import time
from dataclasses import dataclass

import pytest

from message_bus import (
    AsyncLatencyMiddleware,
    AsyncLocalMessageBus,
    AsyncMiddlewareBus,
    LatencyMiddleware,
    LatencyStats,
    LocalMessageBus,
    MemoryStore,
    MiddlewareBus,
    RecordingBus,
)
from message_bus.ports import Command, Event, Query

# --- Shared messages (would be in shared/messages/ and shared/events/) ---


@dataclass(frozen=True)
class GetUserQuery(Query[dict]):
    user_id: str


@dataclass(frozen=True)
class CreateOrderCommand(Command):
    user_id: str
    items: list[str]


@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    order_id: str
    user_id: str
    items: list[str]


# --- Module services ---


class UserService:
    """Simulates user module service."""

    def get_user(self, query: GetUserQuery) -> dict:
        time.sleep(0.001)  # Simulate DB lookup
        return {"id": query.user_id, "name": "Test User", "active": True}


class OrderService:
    """Simulates order module service using MessageBus."""

    def __init__(self, bus):
        self._bus = bus
        self.created_orders = []

    def create_order(self, cmd: CreateOrderCommand) -> None:
        # Query user via bus (cross-module)
        user = self._bus.send(GetUserQuery(cmd.user_id))
        assert user["active"], "User must be active"

        order_id = f"ORD-{len(self.created_orders) + 1}"
        self.created_orders.append(order_id)

        # Publish event via bus (cross-module)
        self._bus.publish(
            OrderCreatedEvent(order_id=order_id, user_id=cmd.user_id, items=cmd.items)
        )


class InventoryService:
    """Simulates inventory module service."""

    def __init__(self):
        self.reservations = []

    def on_order_created(self, event: OrderCreatedEvent) -> None:
        time.sleep(0.001)  # Simulate inventory check
        self.reservations.append(
            {
                "order_id": event.order_id,
                "items": event.items,
            }
        )


# --- Integration tests ---


class TestMiddlewareIntegration:
    """Test middleware with realistic multi-module setup."""

    def test_full_order_flow_with_latency_tracking(self):
        """Full order creation flow: Query → Command → Event, all tracked by latency middleware."""
        # Bootstrap (like bootstrap/ in real app)
        stats = LatencyStats()
        threshold_calls = []

        def on_slow(msg_class, msg_type, duration_ms):
            threshold_calls.append((msg_class, msg_type, duration_ms))

        mw = LatencyMiddleware(stats, threshold_ms=500.0, on_threshold_exceeded=on_slow)
        inner_bus = LocalMessageBus()
        bus = MiddlewareBus(inner_bus, [mw])

        # Wire up modules
        user_svc = UserService()
        inventory_svc = InventoryService()
        order_svc = OrderService(bus)

        bus.register_query(GetUserQuery, user_svc.get_user)
        bus.register_command(CreateOrderCommand, order_svc.create_order)
        bus.subscribe(OrderCreatedEvent, inventory_svc.on_order_created)

        # Execute
        bus.execute(CreateOrderCommand(user_id="U-1", items=["item-a", "item-b"]))

        # Verify business logic
        assert len(order_svc.created_orders) == 1
        assert order_svc.created_orders[0] == "ORD-1"
        assert len(inventory_svc.reservations) == 1
        assert inventory_svc.reservations[0]["items"] == ["item-a", "item-b"]

        # Verify latency tracking
        all_p = stats.all_percentiles()
        tracked_types = {(p.message_class, p.message_type) for p in all_p}

        # Should have tracked: GetUserQuery, CreateOrderCommand, OrderCreatedEvent
        fq_user = f"{GetUserQuery.__module__}.{GetUserQuery.__qualname__}"
        fq_order_cmd = f"{CreateOrderCommand.__module__}.{CreateOrderCommand.__qualname__}"
        fq_order_evt = f"{OrderCreatedEvent.__module__}.{OrderCreatedEvent.__qualname__}"

        assert (fq_user, "query") in tracked_types
        assert (fq_order_cmd, "command") in tracked_types
        assert (fq_order_evt, "event") in tracked_types

        # All latencies should be > 0
        for p in all_p:
            assert p.p50_ms > 0
            assert p.count >= 1

    def test_multiple_orders_baseline_detection(self):
        """Run multiple orders, set baseline, increase latency, detect separation signal."""
        stats = LatencyStats()
        mw = LatencyMiddleware(stats, threshold_ms=5000.0)
        bus = MiddlewareBus(LocalMessageBus(), [mw])

        user_svc = UserService()
        bus.register_query(GetUserQuery, user_svc.get_user)

        # Run baseline queries
        for i in range(50):
            bus.send(GetUserQuery(user_id=f"U-{i}"))

        stats.set_baseline()

        # Simulate slower queries (add artificial delay)
        original_get_user = user_svc.get_user

        def slow_get_user(query):
            time.sleep(0.01)  # 10x slower
            return original_get_user(query)

        # Re-register with slow handler
        inner_bus = LocalMessageBus()
        bus2 = MiddlewareBus(inner_bus, [LatencyMiddleware(stats, threshold_ms=5000.0)])
        inner_bus._query_handlers.clear()  # Hacky but needed for test
        bus2.register_query(GetUserQuery, slow_get_user)

        for i in range(50):
            bus2.send(GetUserQuery(user_id=f"U-{i}"))

        # Check separation signals
        signals = stats.check_separation_signals(threshold_ratio=2.0)
        # Should detect significant increase
        assert len(signals) >= 1

    def test_recording_plus_middleware_composition(self):
        """RecordingBus + MiddlewareBus composition in realistic setup."""
        stats = LatencyStats()
        store = MemoryStore()
        mw = LatencyMiddleware(stats, threshold_ms=5000.0)

        # RecordingBus wrapping MiddlewareBus wrapping LocalMessageBus
        inner = LocalMessageBus()
        middleware_bus = MiddlewareBus(inner, [mw])
        bus = RecordingBus(middleware_bus, store)

        user_svc = UserService()
        bus.register_query(GetUserQuery, user_svc.get_user)

        result = bus.send(GetUserQuery(user_id="U-1"))

        # Business result correct
        assert result["id"] == "U-1"
        assert result["name"] == "Test User"

        # Recording captured
        assert len(store.records) == 1
        assert store.records[0].message_class == "GetUserQuery"

        # Latency captured
        fq_user = f"{GetUserQuery.__module__}.{GetUserQuery.__qualname__}"
        p = stats.percentiles(fq_user, "query")
        assert p is not None
        assert p.count == 1

    def test_context_manager_cleanup(self):
        """Bus cleanup via context manager works correctly."""
        stats = LatencyStats()
        mw = LatencyMiddleware(stats, threshold_ms=5000.0)

        with MiddlewareBus(LocalMessageBus(), [mw]) as bus:
            bus.register_query(GetUserQuery, lambda q: {"id": q.user_id})
            result = bus.send(GetUserQuery(user_id="U-1"))
            assert result["id"] == "U-1"

        # Bus should be closed (no exception)

    def test_error_in_handler_still_tracks_latency(self):
        """Handler errors are propagated; latency still recorded."""
        stats = LatencyStats()
        mw = LatencyMiddleware(stats, threshold_ms=5000.0)
        bus = MiddlewareBus(LocalMessageBus(), [mw])

        def bad_handler(q):
            raise RuntimeError("DB connection failed")

        bus.register_query(GetUserQuery, bad_handler)

        with pytest.raises(RuntimeError, match="DB connection failed"):
            bus.send(GetUserQuery(user_id="U-1"))

        # Latency still recorded despite error
        fq_user = f"{GetUserQuery.__module__}.{GetUserQuery.__qualname__}"
        p = stats.percentiles(fq_user, "query")
        assert p is not None
        assert p.count == 1

    def test_callback_error_does_not_mask_handler_result(self):
        """If threshold callback raises, handler result is still returned."""
        stats = LatencyStats()

        def bad_callback(msg_class, msg_type, duration_ms):
            raise RuntimeError("Callback exploded")

        # threshold_ms=0.0001 to ensure callback fires
        mw = LatencyMiddleware(stats, threshold_ms=0.0001, on_threshold_exceeded=bad_callback)
        bus = MiddlewareBus(LocalMessageBus(), [mw])

        bus.register_query(GetUserQuery, lambda q: {"id": q.user_id, "name": "OK"})

        # Should NOT raise despite callback error
        result = bus.send(GetUserQuery(user_id="U-1"))
        assert result["id"] == "U-1"
        assert result["name"] == "OK"

    def test_callback_error_does_not_mask_handler_error(self):
        """If both handler and callback raise, handler error is preserved."""
        stats = LatencyStats()

        def bad_callback(msg_class, msg_type, duration_ms):
            raise RuntimeError("Callback exploded")

        mw = LatencyMiddleware(stats, threshold_ms=0.0001, on_threshold_exceeded=bad_callback)
        bus = MiddlewareBus(LocalMessageBus(), [mw])

        def bad_handler(q):
            raise ValueError("Handler failed")

        bus.register_query(GetUserQuery, bad_handler)

        # Should raise the HANDLER error, not the callback error
        with pytest.raises(ValueError, match="Handler failed"):
            bus.send(GetUserQuery(user_id="U-1"))


class TestAsyncMiddlewareIntegration:
    """Async version of integration tests."""

    async def test_full_async_order_flow(self):
        """Full async order flow with latency tracking."""
        import asyncio

        stats = LatencyStats()
        mw = AsyncLatencyMiddleware(stats, threshold_ms=5000.0)
        bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

        async def get_user(q: GetUserQuery) -> dict:
            await asyncio.sleep(0.001)
            return {"id": q.user_id, "name": "Async User", "active": True}

        events_received = []

        async def on_order_created(e: OrderCreatedEvent) -> None:
            events_received.append(e)

        bus.register_query(GetUserQuery, get_user)
        bus.subscribe(OrderCreatedEvent, on_order_created)

        user = await bus.send(GetUserQuery(user_id="U-1"))
        assert user["name"] == "Async User"

        await bus.publish(OrderCreatedEvent(order_id="ORD-1", user_id="U-1", items=["x"]))
        assert len(events_received) == 1

        all_p = stats.all_percentiles()
        assert len(all_p) >= 2  # query + event
