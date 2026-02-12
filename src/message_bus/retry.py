"""Retry middleware for message bus with exponential backoff.

Provides RetryMiddleware (sync) and AsyncRetryMiddleware (async) for
automatic retry of transient failures with exponential backoff strategy.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from message_bus.middleware import AsyncPassthroughMiddleware, PassthroughMiddleware
from message_bus.ports import Command, Query, Task

# ---------------------------------------------------------------------------
# RetryMiddleware (sync)
# ---------------------------------------------------------------------------


class RetryMiddleware(PassthroughMiddleware):
    """Retry handler execution on retryable exceptions with exponential backoff.

    Applies retry logic to send/execute/dispatch (Query/Command/Task).
    Events (publish) are excluded due to multicast semantics and ExceptionGroup
    complexity -- uses passthrough from PassthroughMiddleware.

    Backoff formula: backoff_base ** attempt (e.g., 2^1, 2^2, 2^3 seconds).
    Only exceptions in the retryable whitelist trigger retry. All others
    propagate immediately.

    Args:
        max_attempts: Maximum number of attempts (initial + retries).
                      Must be >= 1. Default 3.
        backoff_base: Exponential backoff base in seconds. Must be > 0.
                      Default 2.0 (2s, 4s, 8s...).
        max_backoff: Maximum backoff delay in seconds. Must be > 0.
                     Default 60.0. Caps exponential growth.
        retryable: Tuple of exception types that trigger retry.
                   Must be non-empty. Default (ConnectionError, TimeoutError).

    Example:
        retry = RetryMiddleware(max_attempts=3, backoff_base=2.0)
        bus = MiddlewareBus(LocalMessageBus(), [retry])
        # First failure at t=0, retry at t=2, retry at t=6 (2^1 + 2^2)
    """

    __slots__ = ("_max_attempts", "_backoff_base", "_max_backoff", "_retryable")

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_base: float = 2.0,
        max_backoff: float = 60.0,
        retryable: tuple[type[Exception], ...] = (ConnectionError, TimeoutError),
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if backoff_base <= 0:
            raise ValueError(f"backoff_base must be > 0, got {backoff_base}")
        if max_backoff <= 0:
            raise ValueError(f"max_backoff must be > 0, got {max_backoff}")
        if not retryable:
            raise ValueError("retryable tuple must not be empty")
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._max_backoff = max_backoff
        self._retryable = retryable

    def _retry(
        self,
        message: Query[Any] | Command | Task,
        next_fn: Callable[..., Any],
    ) -> Any:
        """Common retry logic for send/execute/dispatch.

        Attempts up to max_attempts times. Sleeps with exponential backoff
        between retries. Only retries exceptions in retryable whitelist.
        """
        last_exception: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                return next_fn(message)
            except self._retryable as exc:
                last_exception = exc
                if attempt < self._max_attempts:
                    # Backoff: 2^1, 2^2, 2^3, ... capped at max_backoff
                    delay = min(self._backoff_base**attempt, self._max_backoff)
                    time.sleep(delay)
                # else: last attempt failed, raise after loop

        # All attempts exhausted, raise last exception
        if last_exception is not None:
            raise last_exception
        # Unreachable (loop always sets last_exception or returns)
        raise RuntimeError("Retry loop exited without result or exception")  # pragma: no cover

    def on_send(self, query: Query[Any], next_fn: Callable[[Query[Any]], Any]) -> Any:
        return self._retry(query, next_fn)

    def on_execute(self, command: Command, next_fn: Callable[[Command], None]) -> None:
        self._retry(command, next_fn)

    def on_dispatch(self, task: Task, next_fn: Callable[[Task], None]) -> None:
        self._retry(task, next_fn)

    # on_publish: inherited passthrough from PassthroughMiddleware (no retry for events)


# ---------------------------------------------------------------------------
# AsyncRetryMiddleware
# ---------------------------------------------------------------------------


class AsyncRetryMiddleware(AsyncPassthroughMiddleware):
    """Async version of RetryMiddleware. Uses asyncio.sleep() for backoff.

    Same retry logic as RetryMiddleware but with async dispatch.
    Events (publish) use passthrough (no retry).
    """

    __slots__ = ("_max_attempts", "_backoff_base", "_max_backoff", "_retryable")

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_base: float = 2.0,
        max_backoff: float = 60.0,
        retryable: tuple[type[Exception], ...] = (ConnectionError, TimeoutError),
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if backoff_base <= 0:
            raise ValueError(f"backoff_base must be > 0, got {backoff_base}")
        if max_backoff <= 0:
            raise ValueError(f"max_backoff must be > 0, got {max_backoff}")
        if not retryable:
            raise ValueError("retryable tuple must not be empty")
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._max_backoff = max_backoff
        self._retryable = retryable

    async def _retry(
        self,
        message: Query[Any] | Command | Task,
        next_fn: Callable[..., Awaitable[Any]],
    ) -> Any:
        """Async retry logic with asyncio.sleep() backoff."""
        last_exception: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                return await next_fn(message)
            except self._retryable as exc:
                last_exception = exc
                if attempt < self._max_attempts:
                    delay = min(self._backoff_base**attempt, self._max_backoff)
                    await asyncio.sleep(delay)

        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Retry loop exited without result or exception")  # pragma: no cover

    async def on_send(
        self,
        query: Query[Any],
        next_fn: Callable[[Query[Any]], Awaitable[Any]],
    ) -> Any:
        return await self._retry(query, next_fn)

    async def on_execute(
        self,
        command: Command,
        next_fn: Callable[[Command], Awaitable[None]],
    ) -> None:
        await self._retry(command, next_fn)

    async def on_dispatch(
        self,
        task: Task,
        next_fn: Callable[[Task], Awaitable[None]],
    ) -> None:
        await self._retry(task, next_fn)

    # on_publish: inherited passthrough from AsyncPassthroughMiddleware
