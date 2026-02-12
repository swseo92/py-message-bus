"""Timeout middleware for message bus handlers.

Provides TimeoutMiddleware (sync) and AsyncTimeoutMiddleware (async) for
enforcing timeout limits on handler execution.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any

from message_bus.middleware import AsyncPassthroughMiddleware, PassthroughMiddleware
from message_bus.ports import Command, Event, Query, Task

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TimeoutMiddleware (sync)
# ---------------------------------------------------------------------------


class TimeoutMiddleware(PassthroughMiddleware):
    """Enforce timeout limits on sync handler execution.

    Uses ThreadPoolExecutor with Future.result(timeout=) to enforce timeouts.
    Raises TimeoutError when handler execution exceeds the configured timeout.

    **CRITICAL WARNING - HANDLERS CONTINUE RUNNING AFTER TIMEOUT:**

    Timed-out sync handlers CONTINUE RUNNING in background threads even after
    TimeoutError is raised. This is a fundamental ThreadPoolExecutor limitation
    - Python threads cannot be forcibly cancelled. The handler will complete its
    work (consuming resources, holding locks, making side effects) even though
    the caller has moved on.

    For true cancellation with immediate handler termination, use
    AsyncTimeoutMiddleware with async handlers (asyncio.wait_for cancels tasks).

    Args:
        default_timeout: Default timeout in seconds. Default 5.0.

    Example:
        timeout_mw = TimeoutMiddleware(default_timeout=2.0)
        bus = MiddlewareBus(LocalMessageBus(), [timeout_mw])
        # Handlers exceeding 2 seconds will raise TimeoutError
    """

    __slots__ = ("_default_timeout", "_executor")

    def __init__(self, default_timeout: float = 5.0) -> None:
        if default_timeout <= 0:
            raise ValueError(f"default_timeout must be > 0, got {default_timeout}")
        self._default_timeout = default_timeout
        self._executor = ThreadPoolExecutor(max_workers=None)

    def _with_timeout(
        self,
        message: Query[Any] | Command | Event | Task,
        next_fn: Callable[..., Any],
    ) -> Any:
        """Execute next_fn with timeout enforcement."""
        future = self._executor.submit(next_fn, message)
        try:
            return future.result(timeout=self._default_timeout)
        except FuturesTimeoutError as e:
            future.cancel()
            msg_type = type(message).__qualname__
            logger.warning("Handler for %s timed out after %.1fs", msg_type, self._default_timeout)
            raise TimeoutError(
                f"Handler for {msg_type} exceeded timeout of {self._default_timeout}s"
            ) from e

    def on_send(self, query: Query[Any], next_fn: Callable[[Query[Any]], Any]) -> Any:
        return self._with_timeout(query, next_fn)

    def on_execute(self, command: Command, next_fn: Callable[[Command], None]) -> None:
        self._with_timeout(command, next_fn)

    def on_publish(self, event: Event, next_fn: Callable[[Event], None]) -> None:
        self._with_timeout(event, next_fn)

    def on_dispatch(self, task: Task, next_fn: Callable[[Task], None]) -> None:
        self._with_timeout(task, next_fn)

    def close(self) -> None:
        """Shutdown the thread pool executor."""
        self._executor.shutdown(wait=False, cancel_futures=True)


# ---------------------------------------------------------------------------
# AsyncTimeoutMiddleware
# ---------------------------------------------------------------------------


class AsyncTimeoutMiddleware(AsyncPassthroughMiddleware):
    """Enforce timeout limits on async handler execution.

    Uses asyncio.wait_for() to enforce timeouts. Raises asyncio.TimeoutError
    when handler execution exceeds the configured timeout.

    Args:
        default_timeout: Default timeout in seconds. Default 5.0.

    Example:
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=2.0)
        bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [timeout_mw])
        # Handlers exceeding 2 seconds will raise asyncio.TimeoutError
    """

    __slots__ = ("_default_timeout",)

    def __init__(self, default_timeout: float = 5.0) -> None:
        if default_timeout <= 0:
            raise ValueError(f"default_timeout must be > 0, got {default_timeout}")
        self._default_timeout = default_timeout

    async def _with_timeout(
        self,
        message: Query[Any] | Command | Event | Task,
        next_fn: Callable[..., Awaitable[Any]],
    ) -> Any:
        """Execute next_fn with timeout enforcement."""
        try:
            return await asyncio.wait_for(next_fn(message), timeout=self._default_timeout)
        except TimeoutError as e:
            msg_type = type(message).__qualname__
            logger.warning("Handler for %s timed out after %.1fs", msg_type, self._default_timeout)
            raise TimeoutError(
                f"Handler for {msg_type} exceeded timeout of {self._default_timeout}s"
            ) from e

    async def on_send(
        self,
        query: Query[Any],
        next_fn: Callable[[Query[Any]], Awaitable[Any]],
    ) -> Any:
        return await self._with_timeout(query, next_fn)

    async def on_execute(
        self,
        command: Command,
        next_fn: Callable[[Command], Awaitable[None]],
    ) -> None:
        await self._with_timeout(command, next_fn)

    async def on_publish(
        self,
        event: Event,
        next_fn: Callable[[Event], Awaitable[None]],
    ) -> None:
        await self._with_timeout(event, next_fn)

    async def on_dispatch(
        self,
        task: Task,
        next_fn: Callable[[Task], Awaitable[None]],
    ) -> None:
        await self._with_timeout(task, next_fn)
