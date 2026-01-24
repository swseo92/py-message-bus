"""Message bus interfaces (ports)."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Query(Generic[T]):
    """Base class for queries. Returns a response of type T."""

    pass


@dataclass(frozen=True)
class Command:
    """Base class for commands. No response expected."""

    pass


@dataclass(frozen=True)
class Event:
    """Base class for events. Multicast to multiple handlers."""

    pass


@dataclass(frozen=True)
class Task:
    """Base class for distributed tasks. Only one worker processes each task."""

    pass


class MessageBus(ABC):
    """
    Message bus interface.

    Modules depend only on this interface, not on implementations.
    This allows swapping LocalMessageBus with KafkaMessageBus without changing module code.
    """

    # Registration methods (called at bootstrap time)

    @abstractmethod
    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        """Register a query handler. One handler per query type."""
        ...

    @abstractmethod
    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        """Register a command handler. One handler per command type."""
        ...

    @abstractmethod
    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        """Subscribe to an event. Multiple handlers per event type allowed."""
        ...

    @abstractmethod
    def register_task(
        self, task_type: type[Task], handler: Callable[[Task], None]
    ) -> None:
        """Register a task handler. In distributed mode, only one worker processes each task."""
        ...

    # Dispatch methods (called at runtime)

    @abstractmethod
    def send(self, query: Query[T]) -> T:
        """Send a query and return the response synchronously."""
        ...

    @abstractmethod
    def execute(self, command: Command) -> None:
        """Execute a command."""
        ...

    @abstractmethod
    def publish(self, event: Event) -> None:
        """Publish an event to all subscribers."""
        ...

    @abstractmethod
    def dispatch(self, task: Task) -> None:
        """Dispatch a task to one worker. In distributed mode, load-balanced across workers."""
        ...
