"""Testing utilities for message bus - fakes with assertion helpers.

Provides FakeMessageBus and AsyncFakeMessageBus for unit testing without real handlers.
Records all dispatched messages and allows stubbing query results.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from message_bus.ports import AsyncMessageBus, Command, Event, MessageBus, Query, Task

T = TypeVar("T")


class FakeMessageBus(MessageBus):
    """Fake message bus for testing.

    Records all dispatched messages and allows stubbing query results.
    Registration methods are no-ops since this is a fake for testing.

    Example:
        >>> bus = FakeMessageBus()
        >>> bus.given_query_result(GetUserQuery, {"id": "123"})
        >>> user = bus.send(GetUserQuery(user_id="123"))
        >>> bus.assert_published(OrderCreatedEvent, count=1)
    """

    __slots__ = (
        "sent_queries",
        "executed_commands",
        "published_events",
        "dispatched_tasks",
        "_query_stubs",
    )

    def __init__(self) -> None:
        """Create a new FakeMessageBus."""
        self.sent_queries: list[Query[Any]] = []
        self.executed_commands: list[Command] = []
        self.published_events: list[Event] = []
        self.dispatched_tasks: list[Task] = []
        self._query_stubs: dict[type[Query[Any]], Any] = {}

    # Dispatch methods - record messages

    def send(self, query: Query[T]) -> T:
        """Send a query and return the stubbed result."""
        self.sent_queries.append(query)
        query_type = type(query)
        if query_type not in self._query_stubs:
            raise LookupError(f"No stub result for query {query_type.__name__}")
        return cast(T, self._query_stubs[query_type])

    def execute(self, command: Command) -> None:
        """Execute a command (records it)."""
        self.executed_commands.append(command)

    def publish(self, event: Event) -> None:
        """Publish an event (records it)."""
        self.published_events.append(event)

    def dispatch(self, task: Task) -> None:
        """Dispatch a task (records it)."""
        self.dispatched_tasks.append(task)

    # Registration methods - no-ops (fake bus doesn't need real handlers)

    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    # Stub and assertion helpers

    def given_query_result(self, query_type: type[Query[T]], result: T) -> None:
        """Stub a query result for testing.

        Args:
            query_type: The query class to stub
            result: The result to return when this query is sent

        Example:
            >>> bus.given_query_result(GetUserQuery, {"id": "123"})
            >>> user = bus.send(GetUserQuery(user_id="123"))
        """
        self._query_stubs[query_type] = result

    def assert_published(self, event_type: type[Event], *, count: int = 1) -> None:
        """Assert that an event was published the expected number of times.

        Args:
            event_type: The event class to check
            count: Expected number of times (default: 1)

        Raises:
            AssertionError: If the count doesn't match
        """
        actual_count = sum(1 for e in self.published_events if isinstance(e, event_type))
        if actual_count != count:
            raise AssertionError(
                f"Expected {count} Event of type {event_type.__name__}, but found {actual_count}"
            )

    def assert_executed(self, command_type: type[Command], *, count: int = 1) -> None:
        """Assert that a command was executed the expected number of times.

        Args:
            command_type: The command class to check
            count: Expected number of times (default: 1)

        Raises:
            AssertionError: If the count doesn't match
        """
        actual_count = sum(1 for c in self.executed_commands if isinstance(c, command_type))
        if actual_count != count:
            raise AssertionError(
                f"Expected {count} Command of type {command_type.__name__}, "
                f"but found {actual_count}"
            )

    def assert_nothing_published(self) -> None:
        """Assert that no events were published.

        Raises:
            AssertionError: If any events were published
        """
        if self.published_events:
            event_names = [type(e).__name__ for e in self.published_events]
            raise AssertionError(
                f"Expected no events to be published, "
                f"but found {len(self.published_events)}: {event_names}"
            )

    def reset(self) -> None:
        """Clear all recorded messages and stubs.

        Useful for resetting state between test cases.
        """
        self.sent_queries.clear()
        self.executed_commands.clear()
        self.published_events.clear()
        self.dispatched_tasks.clear()
        self._query_stubs.clear()


class AsyncFakeMessageBus(AsyncMessageBus):
    """Async fake message bus for testing.

    Async version of FakeMessageBus. Same recording and stubbing behavior,
    but all dispatch methods are async.

    Example:
        >>> bus = AsyncFakeMessageBus()
        >>> bus.given_query_result(GetUserQuery, {"id": "123"})
        >>> user = await bus.send(GetUserQuery(user_id="123"))
        >>> bus.assert_published(OrderCreatedEvent, count=1)
    """

    __slots__ = (
        "sent_queries",
        "executed_commands",
        "published_events",
        "dispatched_tasks",
        "_query_stubs",
    )

    def __init__(self) -> None:
        """Create a new AsyncFakeMessageBus."""
        self.sent_queries: list[Query[Any]] = []
        self.executed_commands: list[Command] = []
        self.published_events: list[Event] = []
        self.dispatched_tasks: list[Task] = []
        self._query_stubs: dict[type[Query[Any]], Any] = {}

    # Async dispatch methods - record messages

    async def send(self, query: Query[T]) -> T:
        """Send a query and return the stubbed result."""
        self.sent_queries.append(query)
        query_type = type(query)
        if query_type not in self._query_stubs:
            raise LookupError(f"No stub result for query {query_type.__name__}")
        return cast(T, self._query_stubs[query_type])

    async def execute(self, command: Command) -> None:
        """Execute a command (records it)."""
        self.executed_commands.append(command)

    async def publish(self, event: Event) -> None:
        """Publish an event (records it)."""
        self.published_events.append(event)

    async def dispatch(self, task: Task) -> None:
        """Dispatch a task (records it)."""
        self.dispatched_tasks.append(task)

    # Registration methods - no-ops (fake bus doesn't need real handlers)

    def register_query(
        self, query_type: type[Query[T]], handler: Callable[[Query[T]], Awaitable[T]]
    ) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], Awaitable[None]]
    ) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    def subscribe(
        self, event_type: type[Event], handler: Callable[[Event], Awaitable[None]]
    ) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    def register_task(
        self, task_type: type[Task], handler: Callable[[Task], Awaitable[None]]
    ) -> None:
        """No-op - fake bus doesn't use real handlers."""
        pass

    # Stub and assertion helpers (same as sync version)

    def given_query_result(self, query_type: type[Query[T]], result: T) -> None:
        """Stub a query result for testing.

        Args:
            query_type: The query class to stub
            result: The result to return when this query is sent

        Example:
            >>> bus.given_query_result(GetUserQuery, {"id": "123"})
            >>> user = await bus.send(GetUserQuery(user_id="123"))
        """
        self._query_stubs[query_type] = result

    def assert_published(self, event_type: type[Event], *, count: int = 1) -> None:
        """Assert that an event was published the expected number of times.

        Args:
            event_type: The event class to check
            count: Expected number of times (default: 1)

        Raises:
            AssertionError: If the count doesn't match
        """
        actual_count = sum(1 for e in self.published_events if isinstance(e, event_type))
        if actual_count != count:
            raise AssertionError(
                f"Expected {count} Event of type {event_type.__name__}, but found {actual_count}"
            )

    def assert_executed(self, command_type: type[Command], *, count: int = 1) -> None:
        """Assert that a command was executed the expected number of times.

        Args:
            command_type: The command class to check
            count: Expected number of times (default: 1)

        Raises:
            AssertionError: If the count doesn't match
        """
        actual_count = sum(1 for c in self.executed_commands if isinstance(c, command_type))
        if actual_count != count:
            raise AssertionError(
                f"Expected {count} Command of type {command_type.__name__}, "
                f"but found {actual_count}"
            )

    def assert_nothing_published(self) -> None:
        """Assert that no events were published.

        Raises:
            AssertionError: If any events were published
        """
        if self.published_events:
            event_names = [type(e).__name__ for e in self.published_events]
            raise AssertionError(
                f"Expected no events to be published, "
                f"but found {len(self.published_events)}: {event_names}"
            )

    def reset(self) -> None:
        """Clear all recorded messages and stubs.

        Useful for resetting state between test cases.
        """
        self.sent_queries.clear()
        self.executed_commands.clear()
        self.published_events.clear()
        self.dispatched_tasks.clear()
        self._query_stubs.clear()
