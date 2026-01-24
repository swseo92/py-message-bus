"""Local (in-memory) message bus implementation."""

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from message_bus.ports import AsyncMessageBus, Command, Event, MessageBus, Query, Task

T = TypeVar("T")


class IntrospectionMixin:
    """Mixin providing introspection methods for message bus implementations."""

    _query_handlers: dict[type[Query[Any]], Any]
    _command_handlers: dict[type[Command], Any]
    _event_subscribers: dict[type[Event], list[Any]]
    _task_handlers: dict[type[Task], Any]

    def registered_queries(self) -> list[str]:
        """List all registered query types."""
        return [t.__name__ for t in self._query_handlers]

    def registered_commands(self) -> list[str]:
        """List all registered command types."""
        return [t.__name__ for t in self._command_handlers]

    def registered_events(self) -> dict[str, int]:
        """List all registered event types with subscriber counts."""
        return {t.__name__: len(handlers) for t, handlers in self._event_subscribers.items()}

    def registered_tasks(self) -> list[str]:
        """List all registered task types."""
        return [t.__name__ for t in self._task_handlers]


class LocalMessageBus(MessageBus, IntrospectionMixin):
    """
    In-memory message bus implementation.

    This is essentially a registry + router, not a real message queue.
    No serialization, no network, just direct function calls.

    Performance characteristics:
    - Registration: O(1) dict insertion
    - Dispatch: O(1) dict lookup + function call
    - Overhead vs direct call: ~80ns per call (negligible for most apps)

    Use cases:
    - Development and testing
    - Modular monolith (single process)
    - Backtest simulations (with sync handlers)

    For production with multiple services, use KafkaMessageBus or similar.
    """

    __slots__ = ("_query_handlers", "_command_handlers", "_event_subscribers", "_task_handlers")

    def __init__(self) -> None:
        self._query_handlers: dict[type[Query[Any]], Callable[[Query[Any]], Any]] = {}
        self._command_handlers: dict[type[Command], Callable[[Command], None]] = {}
        self._event_subscribers: dict[type[Event], list[Callable[[Event], None]]] = defaultdict(
            list
        )
        self._task_handlers: dict[type[Task], Callable[[Task], None]] = {}

    # Registration methods

    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        if query_type in self._query_handlers:
            raise ValueError(f"Query handler already registered for {query_type.__name__}")
        self._query_handlers[query_type] = handler

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        if command_type in self._command_handlers:
            raise ValueError(f"Command handler already registered for {command_type.__name__}")
        self._command_handlers[command_type] = handler

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        self._event_subscribers[event_type].append(handler)

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        if task_type in self._task_handlers:
            raise ValueError(f"Task handler already registered for {task_type.__name__}")
        self._task_handlers[task_type] = handler

    # Dispatch methods

    def send(self, query: Query[T]) -> T:
        query_type = type(query)
        handler = self._query_handlers.get(query_type)
        if handler is None:
            raise LookupError(f"No handler registered for query {query_type.__name__}")
        return cast(T, handler(query))

    def execute(self, command: Command) -> None:
        command_type = type(command)
        handler = self._command_handlers.get(command_type)
        if handler is None:
            raise LookupError(f"No handler registered for command {command_type.__name__}")
        handler(command)

    def publish(self, event: Event) -> None:
        event_type = type(event)
        handlers = self._event_subscribers.get(event_type)
        if handlers:
            for handler in handlers:
                handler(event)

    def dispatch(self, task: Task) -> None:
        task_type = type(task)
        handler = self._task_handlers.get(task_type)
        if handler is None:
            raise LookupError(f"No handler registered for task {task_type.__name__}")
        handler(task)


class AsyncLocalMessageBus(AsyncMessageBus, IntrospectionMixin):
    """
    Async in-memory message bus implementation.

    Similar to LocalMessageBus, but with async handlers and dispatch methods.
    Event handlers can run concurrently using asyncio.gather.

    Performance characteristics:
    - Registration: O(1) dict insertion
    - Dispatch: O(1) dict lookup + async function call
    - Event publishing: Parallel execution with asyncio.gather

    Use cases:
    - Async/await applications (FastAPI, aiohttp, etc.)
    - I/O-bound operations (database queries, API calls)
    - Concurrent event processing

    For production with multiple services, use an async-capable distributed bus.
    """

    __slots__ = ("_query_handlers", "_command_handlers", "_event_subscribers", "_task_handlers")

    def __init__(self) -> None:
        self._query_handlers: dict[type[Query[Any]], Callable[[Query[Any]], Awaitable[Any]]] = {}
        self._command_handlers: dict[type[Command], Callable[[Command], Awaitable[None]]] = {}
        self._event_subscribers: dict[type[Event], list[Callable[[Event], Awaitable[None]]]] = (
            defaultdict(list)
        )
        self._task_handlers: dict[type[Task], Callable[[Task], Awaitable[None]]] = {}

    # Registration methods

    def register_query(
        self, query_type: type[Query[T]], handler: Callable[[Query[T]], Awaitable[T]]
    ) -> None:
        if query_type in self._query_handlers:
            raise ValueError(f"Query handler already registered for {query_type.__name__}")
        self._query_handlers[query_type] = handler

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], Awaitable[None]]
    ) -> None:
        if command_type in self._command_handlers:
            raise ValueError(f"Command handler already registered for {command_type.__name__}")
        self._command_handlers[command_type] = handler

    def subscribe(
        self, event_type: type[Event], handler: Callable[[Event], Awaitable[None]]
    ) -> None:
        self._event_subscribers[event_type].append(handler)

    def register_task(
        self, task_type: type[Task], handler: Callable[[Task], Awaitable[None]]
    ) -> None:
        if task_type in self._task_handlers:
            raise ValueError(f"Task handler already registered for {task_type.__name__}")
        self._task_handlers[task_type] = handler

    # Dispatch methods

    async def send(self, query: Query[T]) -> T:
        query_type = type(query)
        handler = self._query_handlers.get(query_type)
        if handler is None:
            raise LookupError(f"No handler registered for query {query_type.__name__}")
        return cast(T, await handler(query))

    async def execute(self, command: Command) -> None:
        command_type = type(command)
        handler = self._command_handlers.get(command_type)
        if handler is None:
            raise LookupError(f"No handler registered for command {command_type.__name__}")
        await handler(command)

    async def publish(self, event: Event) -> None:
        event_type = type(event)
        handlers = self._event_subscribers.get(event_type)
        if handlers:
            await asyncio.gather(*[handler(event) for handler in handlers])

    async def dispatch(self, task: Task) -> None:
        task_type = type(task)
        handler = self._task_handlers.get(task_type)
        if handler is None:
            raise LookupError(f"No handler registered for task {task_type.__name__}")
        await handler(task)
