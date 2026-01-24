"""Local (in-memory) message bus implementation."""

from collections import defaultdict
from collections.abc import Callable
from typing import TypeVar

from message_bus.ports import Command, Event, MessageBus, Query

T = TypeVar("T")


class LocalMessageBus(MessageBus):
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

    __slots__ = ("_query_handlers", "_command_handlers", "_event_subscribers")

    def __init__(self) -> None:
        self._query_handlers: dict[type, Callable] = {}
        self._command_handlers: dict[type, Callable] = {}
        self._event_subscribers: dict[type, list[Callable]] = defaultdict(list)

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

    # Dispatch methods

    def send(self, query: Query[T]) -> T:
        query_type = type(query)
        handler = self._query_handlers.get(query_type)
        if handler is None:
            raise LookupError(f"No handler registered for query {query_type.__name__}")
        return handler(query)

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

    # Introspection (useful for debugging)

    def registered_queries(self) -> list[str]:
        """List all registered query types."""
        return [t.__name__ for t in self._query_handlers]

    def registered_commands(self) -> list[str]:
        """List all registered command types."""
        return [t.__name__ for t in self._command_handlers]

    def registered_events(self) -> dict[str, int]:
        """List all registered event types with subscriber counts."""
        return {t.__name__: len(handlers) for t, handlers in self._event_subscribers.items()}
