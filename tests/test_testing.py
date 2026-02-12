"""Tests for testing utilities (FakeMessageBus, AsyncFakeMessageBus)."""

from dataclasses import dataclass

import pytest

from message_bus import Command, Event, Query, Task
from message_bus.testing import AsyncFakeMessageBus, FakeMessageBus


# Test message types
@dataclass(frozen=True)
class GetUserQuery(Query[dict]):
    user_id: str


@dataclass(frozen=True)
class GetOrderQuery(Query[list]):
    order_id: str


@dataclass(frozen=True)
class CreateOrderCommand(Command):
    user_id: str
    items: list[str]


@dataclass(frozen=True)
class DeleteOrderCommand(Command):
    order_id: str


@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    order_id: str
    user_id: str


@dataclass(frozen=True)
class OrderDeletedEvent(Event):
    order_id: str


@dataclass(frozen=True)
class SendEmailTask(Task):
    email: str
    subject: str


class TestFakeMessageBusRecording:
    """Test that FakeMessageBus records all dispatched messages."""

    def test_send_records_query(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})

        bus.send(GetUserQuery(user_id="123"))

        assert len(bus.sent_queries) == 1
        assert isinstance(bus.sent_queries[0], GetUserQuery)
        assert bus.sent_queries[0].user_id == "123"

    def test_execute_records_command(self):
        bus = FakeMessageBus()

        bus.execute(CreateOrderCommand(user_id="123", items=["item1"]))

        assert len(bus.executed_commands) == 1
        assert isinstance(bus.executed_commands[0], CreateOrderCommand)
        assert bus.executed_commands[0].user_id == "123"

    def test_publish_records_event(self):
        bus = FakeMessageBus()

        bus.publish(OrderCreatedEvent(order_id="456", user_id="123"))

        assert len(bus.published_events) == 1
        assert isinstance(bus.published_events[0], OrderCreatedEvent)
        assert bus.published_events[0].order_id == "456"

    def test_dispatch_records_task(self):
        bus = FakeMessageBus()

        bus.dispatch(SendEmailTask(email="test@example.com", subject="Test"))

        assert len(bus.dispatched_tasks) == 1
        assert isinstance(bus.dispatched_tasks[0], SendEmailTask)
        assert bus.dispatched_tasks[0].email == "test@example.com"

    def test_multiple_dispatches_all_recorded(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "1"})

        bus.send(GetUserQuery(user_id="1"))
        bus.execute(CreateOrderCommand(user_id="1", items=["a"]))
        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        bus.dispatch(SendEmailTask(email="a@b.com", subject="Hi"))

        assert len(bus.sent_queries) == 1
        assert len(bus.executed_commands) == 1
        assert len(bus.published_events) == 1
        assert len(bus.dispatched_tasks) == 1


class TestFakeMessageBusQueryStubs:
    """Test query result stubbing."""

    def test_given_query_result_returns_stubbed_value(self):
        bus = FakeMessageBus()
        expected = {"id": "123", "name": "Alice"}
        bus.given_query_result(GetUserQuery, expected)

        result = bus.send(GetUserQuery(user_id="123"))

        assert result == expected

    def test_unstubbed_query_raises_lookup_error(self):
        bus = FakeMessageBus()

        with pytest.raises(LookupError, match="No stub result for query GetUserQuery"):
            bus.send(GetUserQuery(user_id="123"))

    def test_multiple_query_stubs(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})
        bus.given_query_result(GetOrderQuery, [{"order_id": "456"}])

        user = bus.send(GetUserQuery(user_id="123"))
        orders = bus.send(GetOrderQuery(order_id="456"))

        assert user == {"id": "123"}
        assert orders == [{"order_id": "456"}]

    def test_stub_can_be_overwritten(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})
        bus.given_query_result(GetUserQuery, {"id": "456"})

        result = bus.send(GetUserQuery(user_id="123"))

        assert result == {"id": "456"}


class TestFakeMessageBusAssertions:
    """Test assertion helper methods."""

    def test_assert_published_success(self):
        bus = FakeMessageBus()
        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        bus.assert_published(OrderCreatedEvent, count=1)

    def test_assert_published_failure_zero_found(self):
        bus = FakeMessageBus()

        with pytest.raises(
            AssertionError, match="Expected 1 Event of type OrderCreatedEvent, but found 0"
        ):
            bus.assert_published(OrderCreatedEvent, count=1)

    def test_assert_published_failure_wrong_count(self):
        bus = FakeMessageBus()
        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        bus.publish(OrderCreatedEvent(order_id="2", user_id="2"))

        with pytest.raises(
            AssertionError, match="Expected 1 Event of type OrderCreatedEvent, but found 2"
        ):
            bus.assert_published(OrderCreatedEvent, count=1)

    def test_assert_published_multiple_event_types(self):
        bus = FakeMessageBus()
        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        bus.publish(OrderDeletedEvent(order_id="2"))

        bus.assert_published(OrderCreatedEvent, count=1)
        bus.assert_published(OrderDeletedEvent, count=1)

    def test_assert_executed_success(self):
        bus = FakeMessageBus()
        bus.execute(CreateOrderCommand(user_id="1", items=["a"]))

        bus.assert_executed(CreateOrderCommand, count=1)

    def test_assert_executed_failure(self):
        bus = FakeMessageBus()

        with pytest.raises(
            AssertionError, match="Expected 1 Command of type CreateOrderCommand, but found 0"
        ):
            bus.assert_executed(CreateOrderCommand, count=1)

    def test_assert_executed_multiple_command_types(self):
        bus = FakeMessageBus()
        bus.execute(CreateOrderCommand(user_id="1", items=["a"]))
        bus.execute(DeleteOrderCommand(order_id="2"))

        bus.assert_executed(CreateOrderCommand, count=1)
        bus.assert_executed(DeleteOrderCommand, count=1)

    def test_assert_nothing_published_success(self):
        bus = FakeMessageBus()

        bus.assert_nothing_published()

    def test_assert_nothing_published_failure(self):
        bus = FakeMessageBus()
        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        bus.publish(OrderDeletedEvent(order_id="2"))

        with pytest.raises(
            AssertionError,
            match="Expected no events to be published, but found 2: "
            r"\['OrderCreatedEvent', 'OrderDeletedEvent'\]",
        ):
            bus.assert_nothing_published()


class TestFakeMessageBusReset:
    """Test reset() clears all state."""

    def test_reset_clears_all_recorded_messages(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})
        bus.send(GetUserQuery(user_id="123"))
        bus.execute(CreateOrderCommand(user_id="1", items=["a"]))
        bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        bus.dispatch(SendEmailTask(email="a@b.com", subject="Hi"))

        bus.reset()

        assert len(bus.sent_queries) == 0
        assert len(bus.executed_commands) == 0
        assert len(bus.published_events) == 0
        assert len(bus.dispatched_tasks) == 0

    def test_reset_clears_query_stubs(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})

        bus.reset()

        with pytest.raises(LookupError):
            bus.send(GetUserQuery(user_id="123"))

    def test_reset_allows_reuse(self):
        bus = FakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "1"})
        bus.send(GetUserQuery(user_id="1"))

        bus.reset()

        bus.given_query_result(GetUserQuery, {"id": "2"})
        result = bus.send(GetUserQuery(user_id="2"))

        assert result == {"id": "2"}
        assert len(bus.sent_queries) == 1


class TestFakeMessageBusRegistration:
    """Test that registration methods are no-ops (fake doesn't need handlers)."""

    def test_register_query_is_noop(self):
        bus = FakeMessageBus()
        bus.register_query(GetUserQuery, lambda q: {"id": q.user_id})
        # Should not raise

    def test_register_command_is_noop(self):
        bus = FakeMessageBus()
        bus.register_command(CreateOrderCommand, lambda c: None)
        # Should not raise

    def test_subscribe_is_noop(self):
        bus = FakeMessageBus()
        bus.subscribe(OrderCreatedEvent, lambda e: None)
        # Should not raise

    def test_register_task_is_noop(self):
        bus = FakeMessageBus()
        bus.register_task(SendEmailTask, lambda t: None)
        # Should not raise


# Async tests


class TestAsyncFakeMessageBusRecording:
    """Test that AsyncFakeMessageBus records all dispatched messages."""

    @pytest.mark.asyncio
    async def test_send_records_query(self):
        bus = AsyncFakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})

        await bus.send(GetUserQuery(user_id="123"))

        assert len(bus.sent_queries) == 1
        assert isinstance(bus.sent_queries[0], GetUserQuery)
        assert bus.sent_queries[0].user_id == "123"

    @pytest.mark.asyncio
    async def test_execute_records_command(self):
        bus = AsyncFakeMessageBus()

        await bus.execute(CreateOrderCommand(user_id="123", items=["item1"]))

        assert len(bus.executed_commands) == 1
        assert isinstance(bus.executed_commands[0], CreateOrderCommand)
        assert bus.executed_commands[0].user_id == "123"

    @pytest.mark.asyncio
    async def test_publish_records_event(self):
        bus = AsyncFakeMessageBus()

        await bus.publish(OrderCreatedEvent(order_id="456", user_id="123"))

        assert len(bus.published_events) == 1
        assert isinstance(bus.published_events[0], OrderCreatedEvent)
        assert bus.published_events[0].order_id == "456"

    @pytest.mark.asyncio
    async def test_dispatch_records_task(self):
        bus = AsyncFakeMessageBus()

        await bus.dispatch(SendEmailTask(email="test@example.com", subject="Test"))

        assert len(bus.dispatched_tasks) == 1
        assert isinstance(bus.dispatched_tasks[0], SendEmailTask)
        assert bus.dispatched_tasks[0].email == "test@example.com"

    @pytest.mark.asyncio
    async def test_multiple_dispatches_all_recorded(self):
        bus = AsyncFakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "1"})

        await bus.send(GetUserQuery(user_id="1"))
        await bus.execute(CreateOrderCommand(user_id="1", items=["a"]))
        await bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        await bus.dispatch(SendEmailTask(email="a@b.com", subject="Hi"))

        assert len(bus.sent_queries) == 1
        assert len(bus.executed_commands) == 1
        assert len(bus.published_events) == 1
        assert len(bus.dispatched_tasks) == 1


class TestAsyncFakeMessageBusQueryStubs:
    """Test async query result stubbing."""

    @pytest.mark.asyncio
    async def test_given_query_result_returns_stubbed_value(self):
        bus = AsyncFakeMessageBus()
        expected = {"id": "123", "name": "Alice"}
        bus.given_query_result(GetUserQuery, expected)

        result = await bus.send(GetUserQuery(user_id="123"))

        assert result == expected

    @pytest.mark.asyncio
    async def test_unstubbed_query_raises_lookup_error(self):
        bus = AsyncFakeMessageBus()

        with pytest.raises(LookupError, match="No stub result for query GetUserQuery"):
            await bus.send(GetUserQuery(user_id="123"))

    @pytest.mark.asyncio
    async def test_multiple_query_stubs(self):
        bus = AsyncFakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})
        bus.given_query_result(GetOrderQuery, [{"order_id": "456"}])

        user = await bus.send(GetUserQuery(user_id="123"))
        orders = await bus.send(GetOrderQuery(order_id="456"))

        assert user == {"id": "123"}
        assert orders == [{"order_id": "456"}]


class TestAsyncFakeMessageBusAssertions:
    """Test async assertion helper methods (sync assertions on async bus)."""

    @pytest.mark.asyncio
    async def test_assert_published_success(self):
        bus = AsyncFakeMessageBus()
        await bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        bus.assert_published(OrderCreatedEvent, count=1)

    @pytest.mark.asyncio
    async def test_assert_published_failure_zero_found(self):
        bus = AsyncFakeMessageBus()

        with pytest.raises(
            AssertionError, match="Expected 1 Event of type OrderCreatedEvent, but found 0"
        ):
            bus.assert_published(OrderCreatedEvent, count=1)

    @pytest.mark.asyncio
    async def test_assert_executed_success(self):
        bus = AsyncFakeMessageBus()
        await bus.execute(CreateOrderCommand(user_id="1", items=["a"]))

        bus.assert_executed(CreateOrderCommand, count=1)

    @pytest.mark.asyncio
    async def test_assert_nothing_published_success(self):
        bus = AsyncFakeMessageBus()

        bus.assert_nothing_published()

    @pytest.mark.asyncio
    async def test_assert_nothing_published_failure(self):
        bus = AsyncFakeMessageBus()
        await bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))

        with pytest.raises(AssertionError, match="Expected no events to be published, but found 1"):
            bus.assert_nothing_published()


class TestAsyncFakeMessageBusReset:
    """Test async reset() clears all state."""

    @pytest.mark.asyncio
    async def test_reset_clears_all_recorded_messages(self):
        bus = AsyncFakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})
        await bus.send(GetUserQuery(user_id="123"))
        await bus.execute(CreateOrderCommand(user_id="1", items=["a"]))
        await bus.publish(OrderCreatedEvent(order_id="1", user_id="1"))
        await bus.dispatch(SendEmailTask(email="a@b.com", subject="Hi"))

        bus.reset()

        assert len(bus.sent_queries) == 0
        assert len(bus.executed_commands) == 0
        assert len(bus.published_events) == 0
        assert len(bus.dispatched_tasks) == 0

    @pytest.mark.asyncio
    async def test_reset_clears_query_stubs(self):
        bus = AsyncFakeMessageBus()
        bus.given_query_result(GetUserQuery, {"id": "123"})

        bus.reset()

        with pytest.raises(LookupError):
            await bus.send(GetUserQuery(user_id="123"))


class TestAsyncFakeMessageBusRegistration:
    """Test that async registration methods are no-ops."""

    def test_register_query_is_noop(self):
        bus = AsyncFakeMessageBus()

        async def handler(q):
            return {"id": q.user_id}

        bus.register_query(GetUserQuery, handler)
        # Should not raise

    def test_register_command_is_noop(self):
        bus = AsyncFakeMessageBus()

        async def handler(c):
            pass

        bus.register_command(CreateOrderCommand, handler)
        # Should not raise

    def test_subscribe_is_noop(self):
        bus = AsyncFakeMessageBus()

        async def handler(e):
            pass

        bus.subscribe(OrderCreatedEvent, handler)
        # Should not raise

    def test_register_task_is_noop(self):
        bus = AsyncFakeMessageBus()

        async def handler(t):
            pass

        bus.register_task(SendEmailTask, handler)
        # Should not raise
