"""Contract tests for synchronous MessageBus implementations."""

import pytest

from tests.contract.messages import (
    CreateOrderCommand,
    GetOptionalQuery,
    GetUserQuery,
    OrderCreatedEvent,
    SendEmailTask,
)


class TestMessageBusQueryContract:
    """Query contract tests - applies to ALL MessageBus implementations."""

    def test_send_query_returns_handler_result(self, bus):
        """Query handler's return value is returned by send()."""

        def handler(query: GetUserQuery) -> dict[str, str]:
            return {"id": query.user_id, "name": "Test User"}

        bus.register_query(GetUserQuery, handler)
        result = bus.send(GetUserQuery(user_id="123"))

        assert result == {"id": "123", "name": "Test User"}

    def test_send_unregistered_query_raises_lookup_error(self, bus):
        """Sending unregistered query raises LookupError."""
        with pytest.raises(LookupError):
            bus.send(GetUserQuery(user_id="123"))

    def test_register_duplicate_query_raises_value_error(self, bus):
        """Registering same query type twice raises ValueError."""
        bus.register_query(GetUserQuery, lambda q: {})

        with pytest.raises(ValueError):
            bus.register_query(GetUserQuery, lambda q: {})


class TestMessageBusCommandContract:
    """Command contract tests - only for implementations that support direct command handling."""

    def test_execute_command_calls_handler(self, bus_full_contract):
        """Command handler is called when command is executed."""
        called_with = []

        def handler(cmd: CreateOrderCommand) -> None:
            called_with.append(cmd)

        bus_full_contract.register_command(CreateOrderCommand, handler)
        cmd = CreateOrderCommand(user_id="123", item="widget")
        bus_full_contract.execute(cmd)

        assert len(called_with) == 1
        assert called_with[0] == cmd

    def test_execute_unregistered_command_raises_lookup_error(self, bus_full_contract):
        """Executing unregistered command raises LookupError."""
        with pytest.raises(LookupError):
            bus_full_contract.execute(CreateOrderCommand(user_id="123", item="widget"))

    def test_register_duplicate_command_raises_value_error(self, bus_full_contract):
        """Registering same command type twice raises ValueError."""
        bus_full_contract.register_command(CreateOrderCommand, lambda c: None)

        with pytest.raises(ValueError):
            bus_full_contract.register_command(CreateOrderCommand, lambda c: None)


class TestMessageBusEventContract:
    """Event contract tests - only for implementations that support direct event handling."""

    def test_publish_event_calls_all_subscribers(self, bus_full_contract):
        """All subscribers are called when event is published."""
        results = []

        bus_full_contract.subscribe(OrderCreatedEvent, lambda e: results.append(1))
        bus_full_contract.subscribe(OrderCreatedEvent, lambda e: results.append(2))

        bus_full_contract.publish(OrderCreatedEvent(order_id="ord-1", user_id="123"))

        assert results == [1, 2]

    def test_publish_event_with_no_subscribers_is_silent(self, bus_full_contract):
        """Publishing event with no subscribers does not raise."""
        # Should not raise
        bus_full_contract.publish(OrderCreatedEvent(order_id="ord-1", user_id="123"))

    def test_subscribe_allows_multiple_handlers(self, bus_full_contract):
        """Multiple handlers can subscribe to same event type."""
        count = [0]

        for _ in range(3):
            bus_full_contract.subscribe(
                OrderCreatedEvent, lambda e: count.__setitem__(0, count[0] + 1)
            )

        bus_full_contract.publish(OrderCreatedEvent(order_id="ord-1", user_id="123"))

        assert count[0] == 3


class TestMessageBusTaskContract:
    """Task contract tests - only for implementations that support direct task handling."""

    def test_dispatch_task_calls_handler(self, bus_full_contract):
        """Task handler is called when task is dispatched."""
        called_with = []

        def handler(task: SendEmailTask) -> None:
            called_with.append(task)

        bus_full_contract.register_task(SendEmailTask, handler)
        task = SendEmailTask(email="test@example.com", subject="Hello")
        bus_full_contract.dispatch(task)

        assert len(called_with) == 1
        assert called_with[0] == task

    def test_dispatch_unregistered_task_raises_lookup_error(self, bus_full_contract):
        """Dispatching unregistered task raises LookupError."""
        with pytest.raises(LookupError):
            bus_full_contract.dispatch(SendEmailTask(email="test@example.com", subject="Hello"))

    def test_register_duplicate_task_raises_value_error(self, bus_full_contract):
        """Registering same task type twice raises ValueError."""
        bus_full_contract.register_task(SendEmailTask, lambda t: None)

        with pytest.raises(ValueError):
            bus_full_contract.register_task(SendEmailTask, lambda t: None)


class TestMessageBusExceptionPropagation:
    """Exception propagation contract tests.

    These tests document CURRENT BEHAVIOR - exceptions propagate directly
    without try/catch in LocalMessageBus.
    """

    def test_query_handler_exception_propagates(self, bus):
        """Query handler exception is propagated to caller."""
        def handler(q):
            raise ValueError("Query failed")

        bus.register_query(GetUserQuery, handler)

        with pytest.raises(ValueError, match="Query failed"):
            bus.send(GetUserQuery(user_id="123"))

    def test_command_handler_exception_propagates(self, bus_full_contract):
        """Command handler exception is propagated to caller."""
        def handler(c):
            raise RuntimeError("Command failed")

        bus_full_contract.register_command(CreateOrderCommand, handler)

        with pytest.raises(RuntimeError, match="Command failed"):
            bus_full_contract.execute(CreateOrderCommand(user_id="1", item="x"))

    def test_task_handler_exception_propagates(self, bus_full_contract):
        """Task handler exception is propagated to caller."""
        def handler(t):
            raise OSError("Task failed")

        bus_full_contract.register_task(SendEmailTask, handler)

        with pytest.raises(OSError, match="Task failed"):
            bus_full_contract.dispatch(SendEmailTask(email="a@b.com", subject="Hi"))

    def test_event_first_handler_exception_stops_propagation(self, bus_full_contract):
        """Event handler exception stops subsequent handlers (current behavior)."""
        results = []

        def handler1(e):
            raise ValueError("Handler1 failed")

        def handler2(e):
            results.append("handler2")

        bus_full_contract.subscribe(OrderCreatedEvent, handler1)
        bus_full_contract.subscribe(OrderCreatedEvent, handler2)

        with pytest.raises(ValueError):
            bus_full_contract.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        # handler2 was NOT called due to handler1 failure
        assert results == []


class TestMessageBusNoneReturn:
    """Tests for None return values."""

    def test_query_handler_returns_none(self, bus):
        """Query handler can return None."""
        def handler(q):
            return None

        bus.register_query(GetOptionalQuery, handler)
        result = bus.send(GetOptionalQuery(key="missing"))

        assert result is None


class TestMessageBusStateManagement:
    """Tests for handler registration state management."""

    def test_same_handler_different_query_types(self, bus):
        """Same handler function can be registered for different query types."""
        def generic_handler(q):
            return {"type": type(q).__name__}

        bus.register_query(GetUserQuery, generic_handler)
        bus.register_query(GetOptionalQuery, generic_handler)

        result1 = bus.send(GetUserQuery(user_id="1"))
        result2 = bus.send(GetOptionalQuery(key="k"))

        assert result1 == {"type": "GetUserQuery"}
        assert result2 == {"type": "GetOptionalQuery"}

    def test_same_handler_subscribed_multiple_times_is_allowed(self, bus_full_contract):
        """Same handler can subscribe to same event multiple times.

        This test documents CURRENT BEHAVIOR: LocalMessageBus allows
        the same handler to be subscribed multiple times, and it will
        be called once per subscription. This is intentional - subscribe()
        uses a list, not a set.
        """
        call_count = [0]

        def handler(e):
            call_count[0] += 1

        # Subscribe same handler twice
        bus_full_contract.subscribe(OrderCreatedEvent, handler)
        bus_full_contract.subscribe(OrderCreatedEvent, handler)

        bus_full_contract.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        # Handler called twice (once per subscription)
        assert call_count[0] == 2
