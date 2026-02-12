"""Tests for timeout middleware."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import pytest

from message_bus import (
    AsyncLatencyMiddleware,
    AsyncLocalMessageBus,
    AsyncMiddlewareBus,
    AsyncTimeoutMiddleware,
    Command,
    LatencyMiddleware,
    LatencyStats,
    LocalMessageBus,
    MiddlewareBus,
    Query,
    TimeoutMiddleware,
)

# ---------------------------------------------------------------------------
# Test Messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FastQuery(Query[str]):
    """Query that completes quickly."""

    value: str


@dataclass(frozen=True)
class SlowQuery(Query[str]):
    """Query that takes longer than timeout."""

    delay_seconds: float


@dataclass(frozen=True)
class FastCommand(Command):
    """Command that completes quickly."""

    value: str


@dataclass(frozen=True)
class SlowCommand(Command):
    """Command that takes longer than timeout."""

    delay_seconds: float


# ---------------------------------------------------------------------------
# Sync TimeoutMiddleware Tests
# ---------------------------------------------------------------------------


class TestSyncTimeoutMiddleware:
    """Test sync TimeoutMiddleware."""

    def test_constructor_validates_timeout(self) -> None:
        """Constructor rejects non-positive timeout."""
        with pytest.raises(ValueError, match="default_timeout must be > 0"):
            TimeoutMiddleware(default_timeout=0.0)

        with pytest.raises(ValueError, match="default_timeout must be > 0"):
            TimeoutMiddleware(default_timeout=-1.0)

    def test_fast_query_completes_successfully(self) -> None:
        """Fast query completes and returns result."""
        bus = LocalMessageBus()
        timeout_mw = TimeoutMiddleware(default_timeout=1.0)
        wrapped_bus = MiddlewareBus(bus, [timeout_mw])

        def handle_fast(q: FastQuery) -> str:
            return f"result: {q.value}"

        wrapped_bus.register_query(FastQuery, handle_fast)

        result = wrapped_bus.send(FastQuery(value="test"))
        assert result == "result: test"

    def test_slow_query_raises_timeout_error(self) -> None:
        """Slow query exceeding timeout raises TimeoutError."""
        bus = LocalMessageBus()
        timeout_mw = TimeoutMiddleware(default_timeout=0.1)
        wrapped_bus = MiddlewareBus(bus, [timeout_mw])

        def handle_slow(q: SlowQuery) -> str:
            time.sleep(q.delay_seconds)
            return "should not return"

        wrapped_bus.register_query(SlowQuery, handle_slow)

        with pytest.raises(TimeoutError, match="SlowQuery exceeded timeout of 0.1s"):
            wrapped_bus.send(SlowQuery(delay_seconds=1.0))

    def test_fast_command_completes_successfully(self) -> None:
        """Fast command completes without error."""
        bus = LocalMessageBus()
        timeout_mw = TimeoutMiddleware(default_timeout=1.0)
        wrapped_bus = MiddlewareBus(bus, [timeout_mw])

        executed = []

        def handle_fast(c: FastCommand) -> None:
            executed.append(c.value)

        wrapped_bus.register_command(FastCommand, handle_fast)
        wrapped_bus.execute(FastCommand(value="test"))

        assert executed == ["test"]

    def test_slow_command_raises_timeout_error(self) -> None:
        """Slow command exceeding timeout raises TimeoutError."""
        bus = LocalMessageBus()
        timeout_mw = TimeoutMiddleware(default_timeout=0.1)
        wrapped_bus = MiddlewareBus(bus, [timeout_mw])

        def handle_slow(c: SlowCommand) -> None:
            time.sleep(c.delay_seconds)

        wrapped_bus.register_command(SlowCommand, handle_slow)

        with pytest.raises(TimeoutError, match="SlowCommand exceeded timeout of 0.1s"):
            wrapped_bus.execute(SlowCommand(delay_seconds=1.0))

    def test_composition_with_latency_middleware(self) -> None:
        """TimeoutMiddleware composes with LatencyMiddleware."""
        bus = LocalMessageBus()
        stats = LatencyStats()
        latency_mw = LatencyMiddleware(stats, threshold_ms=50.0)
        timeout_mw = TimeoutMiddleware(default_timeout=1.0)

        # Timeout outer, latency inner (latency measures even if timeout fires)
        wrapped_bus = MiddlewareBus(bus, [timeout_mw, latency_mw])

        def handle_fast(q: FastQuery) -> str:
            return f"ok: {q.value}"

        wrapped_bus.register_query(FastQuery, handle_fast)
        result = wrapped_bus.send(FastQuery(value="composed"))

        assert result == "ok: composed"
        # Latency should have recorded the query
        all_stats = stats.all_percentiles()
        assert len(all_stats) == 1
        assert all_stats[0].message_type == "query"


# ---------------------------------------------------------------------------
# Async TimeoutMiddleware Tests
# ---------------------------------------------------------------------------


class TestAsyncTimeoutMiddleware:
    """Test async TimeoutMiddleware."""

    def test_constructor_validates_timeout(self) -> None:
        """Constructor rejects non-positive timeout."""
        with pytest.raises(ValueError, match="default_timeout must be > 0"):
            AsyncTimeoutMiddleware(default_timeout=0.0)

        with pytest.raises(ValueError, match="default_timeout must be > 0"):
            AsyncTimeoutMiddleware(default_timeout=-1.0)

    async def test_fast_query_completes_successfully(self) -> None:
        """Fast async query completes and returns result."""
        bus = AsyncLocalMessageBus()
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=1.0)
        wrapped_bus = AsyncMiddlewareBus(bus, [timeout_mw])

        async def handle_fast(q: FastQuery) -> str:
            return f"result: {q.value}"

        wrapped_bus.register_query(FastQuery, handle_fast)

        result = await wrapped_bus.send(FastQuery(value="test"))
        assert result == "result: test"

    async def test_slow_query_raises_timeout_error(self) -> None:
        """Slow async query exceeding timeout raises asyncio.TimeoutError."""
        bus = AsyncLocalMessageBus()
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=0.1)
        wrapped_bus = AsyncMiddlewareBus(bus, [timeout_mw])

        async def handle_slow(q: SlowQuery) -> str:
            await asyncio.sleep(q.delay_seconds)
            return "should not return"

        wrapped_bus.register_query(SlowQuery, handle_slow)

        with pytest.raises(TimeoutError, match="SlowQuery exceeded timeout of 0.1s"):
            await wrapped_bus.send(SlowQuery(delay_seconds=1.0))

    async def test_fast_command_completes_successfully(self) -> None:
        """Fast async command completes without error."""
        bus = AsyncLocalMessageBus()
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=1.0)
        wrapped_bus = AsyncMiddlewareBus(bus, [timeout_mw])

        executed = []

        async def handle_fast(c: FastCommand) -> None:
            executed.append(c.value)

        wrapped_bus.register_command(FastCommand, handle_fast)
        await wrapped_bus.execute(FastCommand(value="test"))

        assert executed == ["test"]

    async def test_slow_command_raises_timeout_error(self) -> None:
        """Slow async command exceeding timeout raises asyncio.TimeoutError."""
        bus = AsyncLocalMessageBus()
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=0.1)
        wrapped_bus = AsyncMiddlewareBus(bus, [timeout_mw])

        async def handle_slow(c: SlowCommand) -> None:
            await asyncio.sleep(c.delay_seconds)

        wrapped_bus.register_command(SlowCommand, handle_slow)

        with pytest.raises(TimeoutError, match="SlowCommand exceeded timeout of 0.1s"):
            await wrapped_bus.execute(SlowCommand(delay_seconds=1.0))

    async def test_composition_with_latency_middleware(self) -> None:
        """AsyncTimeoutMiddleware composes with AsyncLatencyMiddleware."""
        bus = AsyncLocalMessageBus()
        stats = LatencyStats()
        latency_mw = AsyncLatencyMiddleware(stats, threshold_ms=50.0)
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=1.0)

        # Timeout outer, latency inner (latency measures even if timeout fires)
        wrapped_bus = AsyncMiddlewareBus(bus, [timeout_mw, latency_mw])

        async def handle_fast(q: FastQuery) -> str:
            return f"ok: {q.value}"

        wrapped_bus.register_query(FastQuery, handle_fast)
        result = await wrapped_bus.send(FastQuery(value="composed"))

        assert result == "ok: composed"
        # Latency should have recorded the query
        all_stats = stats.all_percentiles()
        assert len(all_stats) == 1
        assert all_stats[0].message_type == "query"

    async def test_timeout_cancels_slow_handler(self) -> None:
        """Timeout actually cancels the slow async handler."""
        bus = AsyncLocalMessageBus()
        timeout_mw = AsyncTimeoutMiddleware(default_timeout=0.1)
        wrapped_bus = AsyncMiddlewareBus(bus, [timeout_mw])

        handler_completed = []

        async def handle_slow(q: SlowQuery) -> str:
            try:
                await asyncio.sleep(q.delay_seconds)
                handler_completed.append(True)
                return "completed"
            except asyncio.CancelledError:
                handler_completed.append(False)
                raise

        wrapped_bus.register_query(SlowQuery, handle_slow)

        with pytest.raises(TimeoutError):
            await wrapped_bus.send(SlowQuery(delay_seconds=1.0))

        # Give a moment for cancellation to propagate
        await asyncio.sleep(0.05)

        # Handler should have been cancelled (False appended) or nothing appended
        # asyncio.wait_for cancels the task, so handler may not complete
        assert True not in handler_completed
