"""Tests for middleware.py."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import Mock

import pytest

from message_bus import AsyncLocalMessageBus, LocalMessageBus
from message_bus.middleware import (
    AsyncMiddleware,
    AsyncMiddlewareBus,
    AsyncPassthroughMiddleware,
    Middleware,
    MiddlewareBus,
    PassthroughMiddleware,
)
from message_bus.ports import Command, Event, Query, Task


# Test message types
@dataclass(frozen=True)
class SampleQuery(Query[str]):
    """Sample query message for testing."""

    value: int


@dataclass(frozen=True)
class SampleCommand(Command):
    """Sample command message for testing."""

    action: str


@dataclass(frozen=True)
class SampleEvent(Event):
    """Sample event message for testing."""

    data: str


@dataclass(frozen=True)
class SampleTask(Task):
    """Sample task message for testing."""

    task_id: int


# ---------------------------------------------------------------------------
# TestMiddlewareABC
# ---------------------------------------------------------------------------


def test_middleware_is_abstract_cannot_instantiate() -> None:
    """Middleware ABC cannot be instantiated directly."""
    with pytest.raises(TypeError, match="abstract"):
        Middleware()  # type: ignore


def test_async_middleware_is_abstract_cannot_instantiate() -> None:
    """AsyncMiddleware ABC cannot be instantiated directly."""
    with pytest.raises(TypeError, match="abstract"):
        AsyncMiddleware()  # type: ignore


# ---------------------------------------------------------------------------
# TestPassthroughMiddleware
# ---------------------------------------------------------------------------


def test_passthrough_send() -> None:
    """PassthroughMiddleware passes through query dispatch."""
    mw = PassthroughMiddleware()
    next_fn = Mock(return_value="result")

    result = mw.on_send(SampleQuery(value=42), next_fn)

    assert result == "result"
    next_fn.assert_called_once_with(SampleQuery(value=42))


def test_passthrough_execute() -> None:
    """PassthroughMiddleware passes through command dispatch."""
    mw = PassthroughMiddleware()
    next_fn = Mock()

    mw.on_execute(SampleCommand(action="test"), next_fn)

    next_fn.assert_called_once_with(SampleCommand(action="test"))


def test_passthrough_publish() -> None:
    """PassthroughMiddleware passes through event dispatch."""
    mw = PassthroughMiddleware()
    next_fn = Mock()

    mw.on_publish(SampleEvent(data="test"), next_fn)

    next_fn.assert_called_once_with(SampleEvent(data="test"))


def test_passthrough_dispatch() -> None:
    """PassthroughMiddleware passes through task dispatch."""
    mw = PassthroughMiddleware()
    next_fn = Mock()

    mw.on_dispatch(SampleTask(task_id=123), next_fn)

    next_fn.assert_called_once_with(SampleTask(task_id=123))


# ---------------------------------------------------------------------------
# TestAsyncPassthroughMiddleware
# ---------------------------------------------------------------------------


async def test_async_passthrough_send() -> None:
    """AsyncPassthroughMiddleware passes through query dispatch."""
    mw = AsyncPassthroughMiddleware()

    async def next_fn(q: Query[str]) -> str:
        return f"result-{q.value}"

    result = await mw.on_send(SampleQuery(value=42), next_fn)

    assert result == "result-42"


async def test_async_passthrough_execute() -> None:
    """AsyncPassthroughMiddleware passes through command dispatch."""
    mw = AsyncPassthroughMiddleware()
    executed = []

    async def next_fn(c: Command) -> None:
        executed.append(c.action)

    await mw.on_execute(SampleCommand(action="test"), next_fn)

    assert executed == ["test"]


async def test_async_passthrough_publish() -> None:
    """AsyncPassthroughMiddleware passes through event dispatch."""
    mw = AsyncPassthroughMiddleware()
    received = []

    async def next_fn(e: Event) -> None:
        received.append(e.data)

    await mw.on_publish(SampleEvent(data="test"), next_fn)

    assert received == ["test"]


async def test_async_passthrough_dispatch() -> None:
    """AsyncPassthroughMiddleware passes through task dispatch."""
    mw = AsyncPassthroughMiddleware()
    processed = []

    async def next_fn(t: Task) -> None:
        processed.append(t.task_id)

    await mw.on_dispatch(SampleTask(task_id=123), next_fn)

    assert processed == [123]


# ---------------------------------------------------------------------------
# TestMiddlewareBus
# ---------------------------------------------------------------------------


def test_single_middleware_applied_to_send() -> None:
    """Single middleware intercepts query dispatch."""
    inner = LocalMessageBus()
    calls = []

    class LoggingMiddleware(PassthroughMiddleware):
        def on_send(self, query: Query, next_fn):
            calls.append(f"before-{query.value}")
            result = next_fn(query)
            calls.append(f"after-{result}")
            return result

    bus = MiddlewareBus(inner, [LoggingMiddleware()])
    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")

    result = bus.send(SampleQuery(value=42))

    assert result == "result-42"
    assert calls == ["before-42", "after-result-42"]


def test_single_middleware_applied_to_execute() -> None:
    """Single middleware intercepts command dispatch."""
    inner = LocalMessageBus()
    calls = []

    class LoggingMiddleware(PassthroughMiddleware):
        def on_execute(self, command: Command, next_fn):
            calls.append(f"before-{command.action}")
            next_fn(command)
            calls.append("after")

    bus = MiddlewareBus(inner, [LoggingMiddleware()])
    bus.register_command(SampleCommand, lambda c: None)

    bus.execute(SampleCommand(action="test"))

    assert calls == ["before-test", "after"]


def test_single_middleware_applied_to_publish() -> None:
    """Single middleware intercepts event dispatch."""
    inner = LocalMessageBus()
    calls = []

    class LoggingMiddleware(PassthroughMiddleware):
        def on_publish(self, event: Event, next_fn):
            calls.append(f"before-{event.data}")
            next_fn(event)
            calls.append("after")

    bus = MiddlewareBus(inner, [LoggingMiddleware()])
    bus.subscribe(SampleEvent, lambda e: None)

    bus.publish(SampleEvent(data="test"))

    assert calls == ["before-test", "after"]


def test_single_middleware_applied_to_dispatch() -> None:
    """Single middleware intercepts task dispatch."""
    inner = LocalMessageBus()
    calls = []

    class LoggingMiddleware(PassthroughMiddleware):
        def on_dispatch(self, task: Task, next_fn):
            calls.append(f"before-{task.task_id}")
            next_fn(task)
            calls.append("after")

    bus = MiddlewareBus(inner, [LoggingMiddleware()])
    bus.register_task(SampleTask, lambda t: None)

    bus.dispatch(SampleTask(task_id=123))

    assert calls == ["before-123", "after"]


def test_multiple_middlewares_chain_order() -> None:
    """Multiple middlewares execute in list order (first is outermost)."""
    inner = LocalMessageBus()
    calls = []

    class FirstMiddleware(PassthroughMiddleware):
        def on_send(self, query: Query, next_fn):
            calls.append("first-before")
            result = next_fn(query)
            calls.append("first-after")
            return result

    class SecondMiddleware(PassthroughMiddleware):
        def on_send(self, query: Query, next_fn):
            calls.append("second-before")
            result = next_fn(query)
            calls.append("second-after")
            return result

    bus = MiddlewareBus(inner, [FirstMiddleware(), SecondMiddleware()])
    bus.register_query(SampleQuery, lambda q: "result")

    bus.send(SampleQuery(value=1))

    # First middleware wraps second middleware wraps handler
    assert calls == ["first-before", "second-before", "second-after", "first-after"]


def test_registration_delegates_to_inner() -> None:
    """MiddlewareBus delegates registration to inner bus."""
    inner = LocalMessageBus()
    mw = PassthroughMiddleware()
    bus = MiddlewareBus(inner, [mw])

    bus.register_query(SampleQuery, lambda q: "result")
    bus.register_command(SampleCommand, lambda c: None)
    bus.subscribe(SampleEvent, lambda e: None)
    bus.register_task(SampleTask, lambda t: None)

    # Check via inner bus introspection
    assert "SampleQuery" in inner.registered_queries()
    assert "SampleCommand" in inner.registered_commands()
    assert "SampleEvent" in inner.registered_events()
    assert "SampleTask" in inner.registered_tasks()


def test_introspection_delegates_to_inner() -> None:
    """MiddlewareBus delegates introspection to inner bus."""
    inner = LocalMessageBus()
    mw = PassthroughMiddleware()
    bus = MiddlewareBus(inner, [mw])

    bus.register_query(SampleQuery, lambda q: "result")
    bus.register_command(SampleCommand, lambda c: None)
    bus.subscribe(SampleEvent, lambda e: None)
    bus.register_task(SampleTask, lambda t: None)

    assert "SampleQuery" in bus.registered_queries()
    assert "SampleCommand" in bus.registered_commands()
    assert "SampleEvent" in bus.registered_events()
    assert "SampleTask" in bus.registered_tasks()


def test_context_manager() -> None:
    """MiddlewareBus works as context manager."""
    inner = LocalMessageBus()
    mw = PassthroughMiddleware()

    with MiddlewareBus(inner, [mw]) as bus:
        bus.register_query(SampleQuery, lambda q: "result")
        result = bus.send(SampleQuery(value=1))
        assert result == "result"

    # Close should have been called
    # LocalMessageBus doesn't have close, so no effect here


def test_empty_middlewares_raises_value_error() -> None:
    """MiddlewareBus raises ValueError for empty middleware list."""
    inner = LocalMessageBus()

    with pytest.raises(ValueError, match="must not be empty"):
        MiddlewareBus(inner, [])


def test_close_delegates_to_inner() -> None:
    """MiddlewareBus.close() delegates to inner bus if it has close()."""
    inner = Mock(spec=["send", "execute", "publish", "dispatch", "close"])
    mw = PassthroughMiddleware()
    bus = MiddlewareBus(inner, [mw])

    bus.close()

    inner.close.assert_called_once()


def test_chains_are_pre_built_at_init() -> None:
    """Middleware chains are pre-built at __init__ time."""
    inner = LocalMessageBus()
    call_count = 0

    class CountingMiddleware(PassthroughMiddleware):
        def __init__(self):
            nonlocal call_count
            call_count += 1
            super().__init__()

    # Create bus with middleware
    bus = MiddlewareBus(inner, [CountingMiddleware()])
    initial_count = call_count

    # Multiple dispatches should not create new middleware instances
    bus.register_query(SampleQuery, lambda q: "result")
    bus.send(SampleQuery(value=1))
    bus.send(SampleQuery(value=2))

    assert call_count == initial_count  # No additional middleware instances


# ---------------------------------------------------------------------------
# TestAsyncMiddlewareBus
# ---------------------------------------------------------------------------


async def test_async_single_middleware_applied_to_send() -> None:
    """Single async middleware intercepts query dispatch."""
    inner = AsyncLocalMessageBus()
    calls = []

    class LoggingMiddleware(AsyncPassthroughMiddleware):
        async def on_send(self, query: Query, next_fn):
            calls.append(f"before-{query.value}")
            result = await next_fn(query)
            calls.append(f"after-{result}")
            return result

    bus = AsyncMiddlewareBus(inner, [LoggingMiddleware()])

    async def handler(q: Query[str]) -> str:
        return f"result-{q.value}"

    bus.register_query(SampleQuery, handler)

    result = await bus.send(SampleQuery(value=42))

    assert result == "result-42"
    assert calls == ["before-42", "after-result-42"]


async def test_async_single_middleware_applied_to_execute() -> None:
    """Single async middleware intercepts command dispatch."""
    inner = AsyncLocalMessageBus()
    calls = []

    class LoggingMiddleware(AsyncPassthroughMiddleware):
        async def on_execute(self, command: Command, next_fn):
            calls.append(f"before-{command.action}")
            await next_fn(command)
            calls.append("after")

    bus = AsyncMiddlewareBus(inner, [LoggingMiddleware()])

    async def handler(c: Command) -> None:
        pass

    bus.register_command(SampleCommand, handler)

    await bus.execute(SampleCommand(action="test"))

    assert calls == ["before-test", "after"]


async def test_async_single_middleware_applied_to_publish() -> None:
    """Single async middleware intercepts event dispatch."""
    inner = AsyncLocalMessageBus()
    calls = []

    class LoggingMiddleware(AsyncPassthroughMiddleware):
        async def on_publish(self, event: Event, next_fn):
            calls.append(f"before-{event.data}")
            await next_fn(event)
            calls.append("after")

    bus = AsyncMiddlewareBus(inner, [LoggingMiddleware()])

    async def handler(e: Event) -> None:
        pass

    bus.subscribe(SampleEvent, handler)

    await bus.publish(SampleEvent(data="test"))

    assert calls == ["before-test", "after"]


async def test_async_single_middleware_applied_to_dispatch() -> None:
    """Single async middleware intercepts task dispatch."""
    inner = AsyncLocalMessageBus()
    calls = []

    class LoggingMiddleware(AsyncPassthroughMiddleware):
        async def on_dispatch(self, task: Task, next_fn):
            calls.append(f"before-{task.task_id}")
            await next_fn(task)
            calls.append("after")

    bus = AsyncMiddlewareBus(inner, [LoggingMiddleware()])

    async def handler(t: Task) -> None:
        pass

    bus.register_task(SampleTask, handler)

    await bus.dispatch(SampleTask(task_id=123))

    assert calls == ["before-123", "after"]


async def test_async_multiple_middlewares_chain_order() -> None:
    """Multiple async middlewares execute in list order (first is outermost)."""
    inner = AsyncLocalMessageBus()
    calls = []

    class FirstMiddleware(AsyncPassthroughMiddleware):
        async def on_send(self, query: Query, next_fn):
            calls.append("first-before")
            result = await next_fn(query)
            calls.append("first-after")
            return result

    class SecondMiddleware(AsyncPassthroughMiddleware):
        async def on_send(self, query: Query, next_fn):
            calls.append("second-before")
            result = await next_fn(query)
            calls.append("second-after")
            return result

    bus = AsyncMiddlewareBus(inner, [FirstMiddleware(), SecondMiddleware()])

    async def handler(q: Query[str]) -> str:
        return "result"

    bus.register_query(SampleQuery, handler)

    await bus.send(SampleQuery(value=1))

    # First middleware wraps second middleware wraps handler
    assert calls == ["first-before", "second-before", "second-after", "first-after"]


async def test_async_registration_delegates_to_inner() -> None:
    """AsyncMiddlewareBus delegates registration to inner bus."""
    inner = AsyncLocalMessageBus()
    mw = AsyncPassthroughMiddleware()
    bus = AsyncMiddlewareBus(inner, [mw])

    async def query_handler(q: Query[str]) -> str:
        return "result"

    async def command_handler(c: Command) -> None:
        pass

    async def event_handler(e: Event) -> None:
        pass

    async def task_handler(t: Task) -> None:
        pass

    bus.register_query(SampleQuery, query_handler)
    bus.register_command(SampleCommand, command_handler)
    bus.subscribe(SampleEvent, event_handler)
    bus.register_task(SampleTask, task_handler)

    # Check via inner bus introspection
    assert "SampleQuery" in inner.registered_queries()
    assert "SampleCommand" in inner.registered_commands()
    assert "SampleEvent" in inner.registered_events()
    assert "SampleTask" in inner.registered_tasks()


async def test_async_introspection_delegates_to_inner() -> None:
    """AsyncMiddlewareBus delegates introspection to inner bus."""
    inner = AsyncLocalMessageBus()
    mw = AsyncPassthroughMiddleware()
    bus = AsyncMiddlewareBus(inner, [mw])

    async def query_handler(q: Query[str]) -> str:
        return "result"

    async def command_handler(c: Command) -> None:
        pass

    async def event_handler(e: Event) -> None:
        pass

    async def task_handler(t: Task) -> None:
        pass

    bus.register_query(SampleQuery, query_handler)
    bus.register_command(SampleCommand, command_handler)
    bus.subscribe(SampleEvent, event_handler)
    bus.register_task(SampleTask, task_handler)

    assert "SampleQuery" in bus.registered_queries()
    assert "SampleCommand" in bus.registered_commands()
    assert "SampleEvent" in bus.registered_events()
    assert "SampleTask" in bus.registered_tasks()


async def test_async_context_manager() -> None:
    """AsyncMiddlewareBus works as async context manager."""
    inner = AsyncLocalMessageBus()
    mw = AsyncPassthroughMiddleware()

    async def handler(q: Query[str]) -> str:
        return "result"

    async with AsyncMiddlewareBus(inner, [mw]) as bus:
        bus.register_query(SampleQuery, handler)
        result = await bus.send(SampleQuery(value=1))
        assert result == "result"

    # Close should have been called
    # AsyncLocalMessageBus doesn't have close, so no effect here


async def test_async_empty_middlewares_raises_value_error() -> None:
    """AsyncMiddlewareBus raises ValueError for empty middleware list."""
    inner = AsyncLocalMessageBus()

    with pytest.raises(ValueError, match="must not be empty"):
        AsyncMiddlewareBus(inner, [])


async def test_async_close_delegates_to_inner() -> None:
    """AsyncMiddlewareBus.close() delegates to inner bus if it has close()."""
    inner = Mock(spec=["send", "execute", "publish", "dispatch", "close"])
    mw = AsyncPassthroughMiddleware()
    bus = AsyncMiddlewareBus(inner, [mw])

    bus.close()

    inner.close.assert_called_once()
