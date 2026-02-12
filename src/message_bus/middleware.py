"""Composable middleware system for message bus instrumentation.

Provides ABC-based middleware interfaces and decorator buses that apply
middleware chains to dispatches. Registration methods are pure delegation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from message_bus.ports import (
    AsyncMessageBus,
    Command,
    Event,
    MessageBus,
    Query,
    Task,
)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Sync Middleware ABC
# ---------------------------------------------------------------------------


class Middleware(ABC):
    """Abstract base class for message bus middleware.

    Middleware intercepts dispatch calls. Each hook wraps a 'next' callable
    representing the next middleware or the actual handler.

    Hook names match the bus dispatch method names:
    - on_send: intercept query dispatch
    - on_execute: intercept command dispatch
    - on_publish: intercept event dispatch
    - on_dispatch: intercept task dispatch

    Subclass and override the hooks you need. For pass-through defaults,
    extend PassthroughMiddleware instead.
    """

    __slots__ = ()

    @abstractmethod
    def on_send(self, query: Query[Any], next_fn: Callable[[Query[Any]], Any]) -> Any:
        """Intercept query dispatch. Must call next_fn(query) and return its result."""
        ...

    @abstractmethod
    def on_execute(self, command: Command, next_fn: Callable[[Command], None]) -> None:
        """Intercept command dispatch. Must call next_fn(command)."""
        ...

    @abstractmethod
    def on_publish(self, event: Event, next_fn: Callable[[Event], None]) -> None:
        """Intercept event dispatch. Must call next_fn(event)."""
        ...

    @abstractmethod
    def on_dispatch(self, task: Task, next_fn: Callable[[Task], None]) -> None:
        """Intercept task dispatch. Must call next_fn(task)."""
        ...


class PassthroughMiddleware(Middleware):
    """Concrete middleware with pass-through defaults for all hooks.

    Subclass and override only the hooks you need.
    """

    __slots__ = ()

    def on_send(self, query: Query[Any], next_fn: Callable[[Query[Any]], Any]) -> Any:
        return next_fn(query)

    def on_execute(self, command: Command, next_fn: Callable[[Command], None]) -> None:
        next_fn(command)

    def on_publish(self, event: Event, next_fn: Callable[[Event], None]) -> None:
        next_fn(event)

    def on_dispatch(self, task: Task, next_fn: Callable[[Task], None]) -> None:
        next_fn(task)


# ---------------------------------------------------------------------------
# Async Middleware ABC
# ---------------------------------------------------------------------------


class AsyncMiddleware(ABC):
    """Abstract base class for async message bus middleware.

    Same as Middleware but all hooks are async. Hook names match the bus
    dispatch method names.
    """

    __slots__ = ()

    @abstractmethod
    async def on_send(
        self, query: Query[Any], next_fn: Callable[[Query[Any]], Awaitable[Any]]
    ) -> Any:
        """Intercept async query dispatch."""
        ...

    @abstractmethod
    async def on_execute(
        self, command: Command, next_fn: Callable[[Command], Awaitable[None]]
    ) -> None:
        """Intercept async command dispatch."""
        ...

    @abstractmethod
    async def on_publish(self, event: Event, next_fn: Callable[[Event], Awaitable[None]]) -> None:
        """Intercept async event dispatch."""
        ...

    @abstractmethod
    async def on_dispatch(self, task: Task, next_fn: Callable[[Task], Awaitable[None]]) -> None:
        """Intercept async task dispatch."""
        ...


class AsyncPassthroughMiddleware(AsyncMiddleware):
    """Concrete async middleware with pass-through defaults for all hooks.

    Subclass and override only the hooks you need.
    """

    __slots__ = ()

    async def on_send(
        self, query: Query[Any], next_fn: Callable[[Query[Any]], Awaitable[Any]]
    ) -> Any:
        return await next_fn(query)

    async def on_execute(
        self, command: Command, next_fn: Callable[[Command], Awaitable[None]]
    ) -> None:
        await next_fn(command)

    async def on_publish(self, event: Event, next_fn: Callable[[Event], Awaitable[None]]) -> None:
        await next_fn(event)

    async def on_dispatch(self, task: Task, next_fn: Callable[[Task], Awaitable[None]]) -> None:
        await next_fn(task)


# ---------------------------------------------------------------------------
# Chain builder helpers (module-level to avoid closure overhead)
# ---------------------------------------------------------------------------


def _build_send_chain(
    middlewares: tuple[Middleware, ...],
    final: Callable[[Query[Any]], Any],
) -> Callable[[Query[Any]], Any]:
    """Pre-build the send chain. Called once at __init__ time."""
    fn = final
    for mw in reversed(middlewares):
        fn = _make_send_link(mw, fn)
    return fn


def _make_send_link(
    mw: Middleware, next_fn: Callable[[Query[Any]], Any]
) -> Callable[[Query[Any]], Any]:
    def chained(q: Query[Any]) -> Any:
        return mw.on_send(q, next_fn)

    return chained


def _build_execute_chain(
    middlewares: tuple[Middleware, ...],
    final: Callable[[Command], None],
) -> Callable[[Command], None]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_execute_link(mw, fn)
    return fn


def _make_execute_link(
    mw: Middleware, next_fn: Callable[[Command], None]
) -> Callable[[Command], None]:
    def chained(c: Command) -> None:
        mw.on_execute(c, next_fn)

    return chained


def _build_publish_chain(
    middlewares: tuple[Middleware, ...],
    final: Callable[[Event], None],
) -> Callable[[Event], None]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_publish_link(mw, fn)
    return fn


def _make_publish_link(mw: Middleware, next_fn: Callable[[Event], None]) -> Callable[[Event], None]:
    def chained(e: Event) -> None:
        mw.on_publish(e, next_fn)

    return chained


def _build_dispatch_chain(
    middlewares: tuple[Middleware, ...],
    final: Callable[[Task], None],
) -> Callable[[Task], None]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_dispatch_link(mw, fn)
    return fn


def _make_dispatch_link(mw: Middleware, next_fn: Callable[[Task], None]) -> Callable[[Task], None]:
    def chained(t: Task) -> None:
        mw.on_dispatch(t, next_fn)

    return chained


# ---------------------------------------------------------------------------
# Async chain builder helpers
# ---------------------------------------------------------------------------


def _build_async_send_chain(
    middlewares: tuple[AsyncMiddleware, ...],
    final: Callable[[Query[Any]], Awaitable[Any]],
) -> Callable[[Query[Any]], Awaitable[Any]]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_async_send_link(mw, fn)
    return fn


def _make_async_send_link(
    mw: AsyncMiddleware, next_fn: Callable[[Query[Any]], Awaitable[Any]]
) -> Callable[[Query[Any]], Awaitable[Any]]:
    async def chained(q: Query[Any]) -> Any:
        return await mw.on_send(q, next_fn)

    return chained


def _build_async_execute_chain(
    middlewares: tuple[AsyncMiddleware, ...],
    final: Callable[[Command], Awaitable[None]],
) -> Callable[[Command], Awaitable[None]]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_async_execute_link(mw, fn)
    return fn


def _make_async_execute_link(
    mw: AsyncMiddleware, next_fn: Callable[[Command], Awaitable[None]]
) -> Callable[[Command], Awaitable[None]]:
    async def chained(c: Command) -> None:
        await mw.on_execute(c, next_fn)

    return chained


def _build_async_publish_chain(
    middlewares: tuple[AsyncMiddleware, ...],
    final: Callable[[Event], Awaitable[None]],
) -> Callable[[Event], Awaitable[None]]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_async_publish_link(mw, fn)
    return fn


def _make_async_publish_link(
    mw: AsyncMiddleware, next_fn: Callable[[Event], Awaitable[None]]
) -> Callable[[Event], Awaitable[None]]:
    async def chained(e: Event) -> None:
        await mw.on_publish(e, next_fn)

    return chained


def _build_async_dispatch_chain(
    middlewares: tuple[AsyncMiddleware, ...],
    final: Callable[[Task], Awaitable[None]],
) -> Callable[[Task], Awaitable[None]]:
    fn = final
    for mw in reversed(middlewares):
        fn = _make_async_dispatch_link(mw, fn)
    return fn


def _make_async_dispatch_link(
    mw: AsyncMiddleware, next_fn: Callable[[Task], Awaitable[None]]
) -> Callable[[Task], Awaitable[None]]:
    async def chained(t: Task) -> None:
        await mw.on_dispatch(t, next_fn)

    return chained


# ---------------------------------------------------------------------------
# _MiddlewareMixin -- shared logic for sync and async middleware buses
# ---------------------------------------------------------------------------


class _MiddlewareMixin:
    """Shared logic for sync and async middleware buses.

    Provides close(), context manager support, and introspection delegation.
    Follows the same pattern as _RecordingMixin in recording.py.
    """

    __slots__ = ("_closed",)

    # These are declared in concrete classes' __slots__
    _inner: Any
    _middlewares: tuple[Middleware, ...] | tuple[AsyncMiddleware, ...]
    _closed: bool

    def close(self) -> None:
        """Close middleware and inner bus if they have close(). Idempotent."""
        if self._closed:
            return
        self._closed = True
        # Close all middleware first (outer to inner)
        for mw in self._middlewares:
            if hasattr(mw, "close"):
                mw.close()
        # Then close inner bus
        inner = getattr(self, "_inner", None)
        if inner is not None and hasattr(inner, "close"):
            inner.close()

    # Introspection delegation (same pattern as _RecordingMixin)
    # Uses explicit typed intermediate variables for mypy strict.

    def registered_queries(self) -> list[str]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_queries"):
            result: list[str] = self._inner.registered_queries()
            return result
        raise AttributeError("Inner bus does not support introspection")

    def registered_commands(self) -> list[str]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_commands"):
            result: list[str] = self._inner.registered_commands()
            return result
        raise AttributeError("Inner bus does not support introspection")

    def registered_events(self) -> dict[str, int]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_events"):
            result: dict[str, int] = self._inner.registered_events()
            return result
        raise AttributeError("Inner bus does not support introspection")

    def registered_tasks(self) -> list[str]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_tasks"):
            result: list[str] = self._inner.registered_tasks()
            return result
        raise AttributeError("Inner bus does not support introspection")


# ---------------------------------------------------------------------------
# MiddlewareBus (sync)
# ---------------------------------------------------------------------------


class MiddlewareBus(_MiddlewareMixin, MessageBus):
    """Decorator bus that applies middleware chain to dispatches.

    Wraps any MessageBus implementation. Middleware is applied in order:
    first middleware in the list is the outermost (first to intercept,
    last to complete).

    Middleware chains are pre-built at __init__ time as stored callables.
    Each dispatch method simply calls the pre-built chain with zero
    per-dispatch allocation overhead.

    Example:
        stats = LatencyStats()
        latency = LatencyMiddleware(stats)
        bus = MiddlewareBus(LocalMessageBus(), [latency])
        # Can compose with RecordingBus:
        recorded_bus = RecordingBus(bus, store)

    Registration methods are pure delegation (no middleware applied).
    Only dispatch methods (send/execute/publish/dispatch) go through middleware.
    """

    __slots__ = (
        "_inner",
        "_middlewares",
        "_send_chain",
        "_execute_chain",
        "_publish_chain",
        "_dispatch_chain",
        "_closed",
    )

    def __init__(self, inner: MessageBus, middlewares: list[Middleware]) -> None:
        if not middlewares:
            raise ValueError("middlewares list must not be empty")
        self._inner = inner
        self._middlewares = tuple(middlewares)
        self._closed = False

        # Pre-build chains at init time (zero allocation on hot path)
        self._send_chain: Callable[[Query[Any]], Any] = _build_send_chain(
            self._middlewares, self._inner.send
        )
        self._execute_chain: Callable[[Command], None] = _build_execute_chain(
            self._middlewares, self._inner.execute
        )
        self._publish_chain: Callable[[Event], None] = _build_publish_chain(
            self._middlewares, self._inner.publish
        )
        self._dispatch_chain: Callable[[Task], None] = _build_dispatch_chain(
            self._middlewares, self._inner.dispatch
        )

    # Dispatch methods -- call pre-built chains

    def send(self, query: Query[T]) -> T:
        return cast(T, self._send_chain(query))

    def execute(self, command: Command) -> None:
        self._execute_chain(command)

    def publish(self, event: Event) -> None:
        self._publish_chain(event)

    def dispatch(self, task: Task) -> None:
        self._dispatch_chain(task)

    # Registration: pure delegation (no middleware)

    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        self._inner.register_query(query_type, handler)

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        self._inner.register_command(command_type, handler)

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        self._inner.subscribe(event_type, handler)

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        self._inner.register_task(task_type, handler)

    # Context manager

    def __enter__(self) -> MiddlewareBus:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# AsyncMiddlewareBus
# ---------------------------------------------------------------------------


class AsyncMiddlewareBus(_MiddlewareMixin, AsyncMessageBus):
    """Async decorator bus that applies async middleware chain to dispatches.

    Wraps any AsyncMessageBus implementation. Middleware chains are pre-built
    at __init__ time. Same pattern as MiddlewareBus but with async dispatch.

    Registration methods are sync (same as AsyncRecordingBus and AsyncLocalMessageBus).
    """

    __slots__ = (
        "_inner",
        "_middlewares",
        "_send_chain",
        "_execute_chain",
        "_publish_chain",
        "_dispatch_chain",
        "_closed",
    )

    def __init__(self, inner: AsyncMessageBus, middlewares: list[AsyncMiddleware]) -> None:
        if not middlewares:
            raise ValueError("middlewares list must not be empty")
        self._inner = inner
        self._middlewares = tuple(middlewares)
        self._closed = False

        # Pre-build async chains at init time
        self._send_chain: Callable[[Query[Any]], Awaitable[Any]] = _build_async_send_chain(
            self._middlewares, self._inner.send
        )
        self._execute_chain: Callable[[Command], Awaitable[None]] = _build_async_execute_chain(
            self._middlewares, self._inner.execute
        )
        self._publish_chain: Callable[[Event], Awaitable[None]] = _build_async_publish_chain(
            self._middlewares, self._inner.publish
        )
        self._dispatch_chain: Callable[[Task], Awaitable[None]] = _build_async_dispatch_chain(
            self._middlewares, self._inner.dispatch
        )

    # Dispatch methods -- call pre-built async chains

    async def send(self, query: Query[T]) -> T:
        return cast(T, await self._send_chain(query))

    async def execute(self, command: Command) -> None:
        await self._execute_chain(command)

    async def publish(self, event: Event) -> None:
        await self._publish_chain(event)

    async def dispatch(self, task: Task) -> None:
        await self._dispatch_chain(task)

    # Registration: sync delegation (same as AsyncRecordingBus)

    def register_query(
        self,
        query_type: type[Query[T]],
        handler: Callable[[Query[T]], Awaitable[T]],
    ) -> None:
        self._inner.register_query(query_type, handler)

    def register_command(
        self,
        command_type: type[Command],
        handler: Callable[[Command], Awaitable[None]],
    ) -> None:
        self._inner.register_command(command_type, handler)

    def subscribe(
        self,
        event_type: type[Event],
        handler: Callable[[Event], Awaitable[None]],
    ) -> None:
        self._inner.subscribe(event_type, handler)

    def register_task(
        self,
        task_type: type[Task],
        handler: Callable[[Task], Awaitable[None]],
    ) -> None:
        self._inner.register_task(task_type, handler)

    # Async context manager

    async def __aenter__(self) -> AsyncMiddlewareBus:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
