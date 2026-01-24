"""Contract tests for asynchronous AsyncMessageBus implementations."""

import asyncio

import pytest

from tests.contract.messages import (
    CreateOrderCommand,
    GetOptionalQuery,
    GetUserQuery,
    OrderCreatedEvent,
    SendEmailTask,
)


class TestAsyncMessageBusQueryContract:
    """Async Query contract tests."""

    async def test_send_query_returns_handler_result(self, async_bus):
        """Query handler's return value is returned by send()."""

        async def handler(query: GetUserQuery) -> dict[str, str]:
            return {"id": query.user_id, "name": "Test User"}

        async_bus.register_query(GetUserQuery, handler)
        result = await async_bus.send(GetUserQuery(user_id="123"))

        assert result == {"id": "123", "name": "Test User"}

    async def test_send_unregistered_query_raises_lookup_error(self, async_bus):
        """Sending unregistered query raises LookupError."""
        with pytest.raises(LookupError):
            await async_bus.send(GetUserQuery(user_id="123"))

    async def test_register_duplicate_query_raises_value_error(self, async_bus):
        """Registering same query type twice raises ValueError."""

        async def handler(q: GetUserQuery) -> dict[str, str]:
            return {}

        async_bus.register_query(GetUserQuery, handler)

        with pytest.raises(ValueError):
            async_bus.register_query(GetUserQuery, handler)


class TestAsyncMessageBusCommandContract:
    """Async Command contract tests."""

    async def test_execute_command_calls_handler(self, async_bus):
        """Command handler is called when command is executed."""
        called_with = []

        async def handler(cmd: CreateOrderCommand) -> None:
            called_with.append(cmd)

        async_bus.register_command(CreateOrderCommand, handler)
        cmd = CreateOrderCommand(user_id="123", item="widget")
        await async_bus.execute(cmd)

        assert len(called_with) == 1
        assert called_with[0] == cmd

    async def test_execute_unregistered_command_raises_lookup_error(self, async_bus):
        """Executing unregistered command raises LookupError."""
        with pytest.raises(LookupError):
            await async_bus.execute(CreateOrderCommand(user_id="123", item="widget"))

    async def test_register_duplicate_command_raises_value_error(self, async_bus):
        """Registering same command type twice raises ValueError."""

        async def handler(cmd: CreateOrderCommand) -> None:
            pass

        async_bus.register_command(CreateOrderCommand, handler)

        with pytest.raises(ValueError):
            async_bus.register_command(CreateOrderCommand, handler)


class TestAsyncMessageBusEventContract:
    """Async Event contract tests."""

    async def test_publish_event_calls_all_subscribers(self, async_bus):
        """All subscribers are called when event is published."""
        results = []

        async def handler1(e: OrderCreatedEvent) -> None:
            results.append(1)

        async def handler2(e: OrderCreatedEvent) -> None:
            results.append(2)

        async_bus.subscribe(OrderCreatedEvent, handler1)
        async_bus.subscribe(OrderCreatedEvent, handler2)

        await async_bus.publish(OrderCreatedEvent(order_id="ord-1", user_id="123"))

        assert sorted(results) == [1, 2]

    async def test_publish_event_with_no_subscribers_is_silent(self, async_bus):
        """Publishing event with no subscribers does not raise."""
        # Should not raise
        await async_bus.publish(OrderCreatedEvent(order_id="ord-1", user_id="123"))

    async def test_publish_event_runs_handlers_concurrently(self, async_bus):
        """Event handlers run concurrently via asyncio.gather."""
        order = []

        async def slow_handler(e: OrderCreatedEvent) -> None:
            await asyncio.sleep(0.05)
            order.append("slow")

        async def fast_handler(e: OrderCreatedEvent) -> None:
            order.append("fast")

        async_bus.subscribe(OrderCreatedEvent, slow_handler)
        async_bus.subscribe(OrderCreatedEvent, fast_handler)

        await async_bus.publish(OrderCreatedEvent(order_id="ord-1", user_id="123"))

        # fast should complete before slow due to concurrent execution
        assert order == ["fast", "slow"]


class TestAsyncMessageBusTaskContract:
    """Async Task contract tests."""

    async def test_dispatch_task_calls_handler(self, async_bus):
        """Task handler is called when task is dispatched."""
        called_with = []

        async def handler(task: SendEmailTask) -> None:
            called_with.append(task)

        async_bus.register_task(SendEmailTask, handler)
        task = SendEmailTask(email="test@example.com", subject="Hello")
        await async_bus.dispatch(task)

        assert len(called_with) == 1
        assert called_with[0] == task

    async def test_dispatch_unregistered_task_raises_lookup_error(self, async_bus):
        """Dispatching unregistered task raises LookupError."""
        with pytest.raises(LookupError):
            await async_bus.dispatch(SendEmailTask(email="test@example.com", subject="Hello"))

    async def test_register_duplicate_task_raises_value_error(self, async_bus):
        """Registering same task type twice raises ValueError."""

        async def handler(task: SendEmailTask) -> None:
            pass

        async_bus.register_task(SendEmailTask, handler)

        with pytest.raises(ValueError):
            async_bus.register_task(SendEmailTask, handler)


class TestAsyncMessageBusExceptionPropagation:
    """Async exception propagation contract tests.

    These tests document CURRENT BEHAVIOR - exceptions propagate directly
    in AsyncLocalMessageBus.
    """

    async def test_query_handler_exception_propagates(self, async_bus):
        """Query handler exception is propagated to caller."""

        async def handler(q):
            raise ValueError("Query failed")

        async_bus.register_query(GetUserQuery, handler)

        with pytest.raises(ValueError, match="Query failed"):
            await async_bus.send(GetUserQuery(user_id="123"))

    async def test_command_handler_exception_propagates(self, async_bus):
        """Command handler exception is propagated to caller."""

        async def handler(c):
            raise RuntimeError("Command failed")

        async_bus.register_command(CreateOrderCommand, handler)

        with pytest.raises(RuntimeError, match="Command failed"):
            await async_bus.execute(CreateOrderCommand(user_id="1", item="x"))

    async def test_task_handler_exception_propagates(self, async_bus):
        """Task handler exception is propagated to caller."""

        async def handler(t):
            raise OSError("Task failed")

        async_bus.register_task(SendEmailTask, handler)

        with pytest.raises(OSError, match="Task failed"):
            await async_bus.dispatch(SendEmailTask(email="a@b.com", subject="Hi"))

    async def test_event_handler_exception_in_gather(self, async_bus):
        """Event handler exception propagates via asyncio.gather (default behavior)."""
        results = []

        async def handler1(e):
            raise ValueError("Handler1 failed")

        async def handler2(e):
            await asyncio.sleep(0)
            results.append("handler2")

        async_bus.subscribe(OrderCreatedEvent, handler1)
        async_bus.subscribe(OrderCreatedEvent, handler2)

        # asyncio.gather without return_exceptions=True raises first exception
        with pytest.raises(ValueError):
            await async_bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))


class TestAsyncMessageBusNoneReturn:
    """Tests for None return values in async bus."""

    async def test_query_handler_returns_none(self, async_bus):
        """Async query handler can return None."""

        async def handler(q):
            return None

        async_bus.register_query(GetOptionalQuery, handler)
        result = await async_bus.send(GetOptionalQuery(key="missing"))

        assert result is None


class TestAsyncMessageBusConcurrency:
    """Concurrency tests for async message bus."""

    async def test_multiple_concurrent_queries(self, async_bus):
        """Multiple queries can be sent concurrently."""
        call_count = [0]

        async def handler(q):
            call_count[0] += 1
            await asyncio.sleep(0.01)
            return {"id": q.user_id}

        async_bus.register_query(GetUserQuery, handler)

        # Send 5 queries concurrently
        queries = [GetUserQuery(user_id=str(i)) for i in range(5)]
        results = await asyncio.gather(*[async_bus.send(q) for q in queries])

        assert len(results) == 5
        assert call_count[0] == 5

    async def test_multiple_concurrent_commands(self, async_bus):
        """Multiple commands can be executed concurrently."""
        results = []

        async def handler(c):
            await asyncio.sleep(0.01)
            results.append(c.user_id)

        async_bus.register_command(CreateOrderCommand, handler)

        # Execute 5 commands concurrently
        commands = [CreateOrderCommand(user_id=str(i), item="x") for i in range(5)]
        await asyncio.gather(*[async_bus.execute(c) for c in commands])

        assert len(results) == 5

    async def test_concurrent_publish_to_same_event(self, async_bus):
        """Multiple publishes to same event type run concurrently."""
        handler_calls = []

        async def handler(e):
            handler_calls.append(e.order_id)
            await asyncio.sleep(0.01)

        async_bus.subscribe(OrderCreatedEvent, handler)

        # Publish 3 events concurrently
        events = [OrderCreatedEvent(order_id=str(i), user_id="1") for i in range(3)]
        await asyncio.gather(*[async_bus.publish(e) for e in events])

        assert len(handler_calls) == 3


class TestAsyncMessageBusStateManagement:
    """Tests for async handler registration state management."""

    async def test_same_handler_different_query_types(self, async_bus):
        """Same handler function can be registered for different query types."""

        async def generic_handler(q):
            return {"type": type(q).__name__}

        async_bus.register_query(GetUserQuery, generic_handler)
        async_bus.register_query(GetOptionalQuery, generic_handler)

        result1 = await async_bus.send(GetUserQuery(user_id="1"))
        result2 = await async_bus.send(GetOptionalQuery(key="k"))

        assert result1 == {"type": "GetUserQuery"}
        assert result2 == {"type": "GetOptionalQuery"}

    async def test_same_handler_subscribed_multiple_times_is_allowed(self, async_bus):
        """Same handler can subscribe to same event multiple times.

        This test documents CURRENT BEHAVIOR: AsyncLocalMessageBus allows
        the same handler to be subscribed multiple times. This mirrors
        the sync LocalMessageBus behavior.
        """
        call_count = [0]

        async def handler(e):
            call_count[0] += 1

        # Subscribe same handler twice
        async_bus.subscribe(OrderCreatedEvent, handler)
        async_bus.subscribe(OrderCreatedEvent, handler)

        await async_bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        # Handler called twice (once per subscription)
        assert call_count[0] == 2
