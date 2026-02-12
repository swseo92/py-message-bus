"""Dead letter queue for failed message dispatches.

Provides DeadLetterStore ABC and DeadLetterMiddleware for capturing failed
messages (events, commands, tasks) that can be replayed or analyzed later.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from message_bus.middleware import AsyncPassthroughMiddleware, PassthroughMiddleware
from message_bus.ports import Command, Event, Query, Task

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """A single failed message record.

    Stores the failed message, error details, and handler information.
    """

    message: Event | Command | Task
    error: Exception
    handler_name: str


class DeadLetterStore(ABC):
    """Abstract base class for storing failed messages."""

    @abstractmethod
    def append(self, message: Event | Command | Task, error: Exception, handler_name: str) -> None:
        """Append a failed message record. Must be non-blocking for middleware use."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Flush and close the store. Must be idempotent."""
        ...


class MemoryDeadLetterStore(DeadLetterStore):
    """In-memory dead letter store for testing. Stores records in a list."""

    __slots__ = ("records", "_closed")

    def __init__(self) -> None:
        self.records: list[DeadLetterRecord] = []
        self._closed: bool = False

    def append(self, message: Event | Command | Task, error: Exception, handler_name: str) -> None:
        if self._closed:
            logger.warning("MemoryDeadLetterStore.append() called after close, record dropped")
            return
        self.records.append(
            DeadLetterRecord(message=message, error=error, handler_name=handler_name)
        )

    def close(self) -> None:
        self._closed = True


# ---------------------------------------------------------------------------
# DeadLetterMiddleware (sync)
# ---------------------------------------------------------------------------


class DeadLetterMiddleware(PassthroughMiddleware):
    """Middleware that captures failed messages to a dead letter queue.

    Records failed dispatches without altering error propagation behavior.
    For events with ExceptionGroup, extracts individual handler failures.

    Args:
        store: DeadLetterStore instance for storing failed messages.
    """

    __slots__ = ("_store",)

    def __init__(self, store: DeadLetterStore) -> None:
        self._store = store

    def on_send(self, query: Query[Any], next_fn: Callable[[Query[Any]], Any]) -> Any:
        """Pass through query dispatch (queries not stored in DLQ)."""
        return next_fn(query)

    def on_execute(self, command: Command, next_fn: Callable[[Command], None]) -> None:
        """Execute command and record failures to DLQ."""
        try:
            next_fn(command)
        except Exception as exc:
            handler_name = type(command).__name__
            try:
                self._store.append(command, exc, handler_name)
            except Exception:
                logger.exception(
                    "Failed to record command failure to DLQ: %s",
                    handler_name,
                )
            raise

    def on_publish(self, event: Event, next_fn: Callable[[Event], None]) -> None:
        """Publish event and record individual handler failures to DLQ."""
        try:
            next_fn(event)
        except ExceptionGroup as eg:
            # Extract individual handler failures from ExceptionGroup
            event_type_name = type(event).__name__
            for i, exc in enumerate(eg.exceptions):
                handler_name = f"{event_type_name}_handler_{i}"
                try:
                    self._store.append(event, exc, handler_name)
                except Exception:
                    logger.exception(
                        "Failed to record event handler failure to DLQ: %s",
                        handler_name,
                    )
            raise
        except Exception as exc:
            # Single handler failure (only one subscriber)
            handler_name = type(event).__name__
            try:
                self._store.append(event, exc, handler_name)
            except Exception:
                logger.exception(
                    "Failed to record event failure to DLQ: %s",
                    handler_name,
                )
            raise

    def on_dispatch(self, task: Task, next_fn: Callable[[Task], None]) -> None:
        """Dispatch task and record failures to DLQ."""
        try:
            next_fn(task)
        except Exception as exc:
            handler_name = type(task).__name__
            try:
                self._store.append(task, exc, handler_name)
            except Exception:
                logger.exception(
                    "Failed to record task failure to DLQ: %s",
                    handler_name,
                )
            raise


# ---------------------------------------------------------------------------
# AsyncDeadLetterMiddleware
# ---------------------------------------------------------------------------


class AsyncDeadLetterMiddleware(AsyncPassthroughMiddleware):
    """Async middleware that captures failed messages to a dead letter queue.

    Same behavior as DeadLetterMiddleware but for async handlers.
    Uses sync DeadLetterStore (append is non-blocking).

    Args:
        store: DeadLetterStore instance for storing failed messages.
    """

    __slots__ = ("_store",)

    def __init__(self, store: DeadLetterStore) -> None:
        self._store = store

    async def on_send(
        self, query: Query[Any], next_fn: Callable[[Query[Any]], Awaitable[Any]]
    ) -> Any:
        """Pass through async query dispatch (queries not stored in DLQ)."""
        return await next_fn(query)

    async def on_execute(
        self, command: Command, next_fn: Callable[[Command], Awaitable[None]]
    ) -> None:
        """Execute async command and record failures to DLQ."""
        try:
            await next_fn(command)
        except Exception as exc:
            handler_name = type(command).__name__
            try:
                self._store.append(command, exc, handler_name)
            except Exception:
                logger.exception(
                    "Failed to record command failure to DLQ: %s",
                    handler_name,
                )
            raise

    async def on_publish(self, event: Event, next_fn: Callable[[Event], Awaitable[None]]) -> None:
        """Publish async event and record individual handler failures to DLQ."""
        try:
            await next_fn(event)
        except ExceptionGroup as eg:
            # Extract individual handler failures from ExceptionGroup
            event_type_name = type(event).__name__
            for i, exc in enumerate(eg.exceptions):
                handler_name = f"{event_type_name}_handler_{i}"
                try:
                    self._store.append(event, exc, handler_name)
                except Exception:
                    logger.exception(
                        "Failed to record event handler failure to DLQ: %s",
                        handler_name,
                    )
            raise
        except Exception as exc:
            # Single handler failure (only one subscriber)
            handler_name = type(event).__name__
            try:
                self._store.append(event, exc, handler_name)
            except Exception:
                logger.exception(
                    "Failed to record event failure to DLQ: %s",
                    handler_name,
                )
            raise

    async def on_dispatch(self, task: Task, next_fn: Callable[[Task], Awaitable[None]]) -> None:
        """Dispatch async task and record failures to DLQ."""
        try:
            await next_fn(task)
        except Exception as exc:
            handler_name = type(task).__name__
            try:
                self._store.append(task, exc, handler_name)
            except Exception:
                logger.exception(
                    "Failed to record task failure to DLQ: %s",
                    handler_name,
                )
            raise
