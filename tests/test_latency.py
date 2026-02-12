"""Tests for latency.py."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import pytest

from message_bus import (
    AsyncLocalMessageBus,
    AsyncRecordingBus,
    LocalMessageBus,
    MemoryStore,
    RecordingBus,
)
from message_bus.latency import (
    AsyncLatencyMiddleware,
    LatencyMiddleware,
    LatencyPercentiles,
    LatencyStats,
    SeparationSignal,
)
from message_bus.middleware import AsyncMiddlewareBus, MiddlewareBus
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


# Fully-qualified class names used by LatencyMiddleware for stats keys.
_FQ_QUERY = f"{SampleQuery.__module__}.{SampleQuery.__qualname__}"
_FQ_COMMAND = f"{SampleCommand.__module__}.{SampleCommand.__qualname__}"
_FQ_EVENT = f"{SampleEvent.__module__}.{SampleEvent.__qualname__}"
_FQ_TASK = f"{SampleTask.__module__}.{SampleTask.__qualname__}"


# ---------------------------------------------------------------------------
# TestLatencyPercentiles
# ---------------------------------------------------------------------------


def test_latency_percentiles_frozen_dataclass() -> None:
    """LatencyPercentiles is a frozen dataclass."""
    p = LatencyPercentiles(
        message_class="TestQuery",
        message_type="query",
        count=100,
        p50_ms=10.0,
        p95_ms=50.0,
        p99_ms=100.0,
        min_ms=1.0,
        max_ms=200.0,
    )

    with pytest.raises(AttributeError):
        p.count = 200


def test_latency_percentiles_slots() -> None:
    """LatencyPercentiles uses __slots__."""
    p = LatencyPercentiles(
        message_class="TestQuery",
        message_type="query",
        count=100,
        p50_ms=10.0,
        p95_ms=50.0,
        p99_ms=100.0,
        min_ms=1.0,
        max_ms=200.0,
    )

    assert not hasattr(p, "__dict__")


# ---------------------------------------------------------------------------
# TestSeparationSignal
# ---------------------------------------------------------------------------


def test_separation_signal_frozen_dataclass() -> None:
    """SeparationSignal is a frozen dataclass."""
    s = SeparationSignal(
        message_class="TestQuery",
        message_type="query",
        current_p95_ms=100.0,
        baseline_p95_ms=50.0,
        increase_ratio=2.0,
    )

    with pytest.raises(AttributeError):
        s.increase_ratio = 3.0


def test_separation_signal_slots() -> None:
    """SeparationSignal uses __slots__."""
    s = SeparationSignal(
        message_class="TestQuery",
        message_type="query",
        current_p95_ms=100.0,
        baseline_p95_ms=50.0,
        increase_ratio=2.0,
    )

    assert not hasattr(s, "__dict__")


# ---------------------------------------------------------------------------
# TestLatencyStats
# ---------------------------------------------------------------------------


def test_record_and_percentiles_basic() -> None:
    """LatencyStats records measurements and computes percentiles."""
    stats = LatencyStats()

    # Record some measurements
    for i in range(100):
        stats.record("TestQuery", "query", i * 1_000_000)  # 0-99ms in nanoseconds

    p = stats.percentiles("TestQuery", "query")

    assert p is not None
    assert p.message_class == "TestQuery"
    assert p.message_type == "query"
    assert p.count == 100
    assert 45 <= p.p50_ms <= 55  # ~50th percentile
    assert 90 <= p.p95_ms <= 99  # ~95th percentile
    assert p.min_ms == 0.0
    assert 98 <= p.max_ms <= 99


def test_percentiles_returns_none_for_unknown_key() -> None:
    """LatencyStats.percentiles() returns None for unknown key."""
    stats = LatencyStats()

    p = stats.percentiles("UnknownQuery", "query")

    assert p is None


def test_bounded_memory_evicts_oldest_via_deque() -> None:
    """LatencyStats evicts oldest samples when max_samples is reached."""
    stats = LatencyStats(max_samples=100)

    # Record 200 samples (should keep only last 100)
    for i in range(200):
        stats.record("TestQuery", "query", i * 1_000_000)

    p = stats.percentiles("TestQuery", "query")

    assert p is not None
    assert p.count == 100
    # min should be 100ms (index 100), not 0ms (index 0)
    assert 99 <= p.min_ms <= 101


def test_reset_clears_data_and_baselines() -> None:
    """LatencyStats.reset() clears all data and baselines."""
    stats = LatencyStats()

    stats.record("TestQuery", "query", 50_000_000)
    stats.set_baseline()

    stats.reset()

    p = stats.percentiles("TestQuery", "query")
    assert p is None

    signals = stats.check_separation_signals()
    assert len(signals) == 0


def test_all_percentiles_returns_all_types() -> None:
    """LatencyStats.all_percentiles() returns summaries for all types."""
    stats = LatencyStats()

    stats.record("QueryA", "query", 10_000_000)
    stats.record("QueryB", "query", 20_000_000)
    stats.record("CommandA", "command", 30_000_000)

    all_p = stats.all_percentiles()

    assert len(all_p) == 3
    classes = {p.message_class for p in all_p}
    assert classes == {"QueryA", "QueryB", "CommandA"}


def test_all_percentiles_no_deadlock() -> None:
    """LatencyStats.all_percentiles() does not deadlock."""
    stats = LatencyStats()

    for i in range(100):
        stats.record(f"Query{i % 10}", "query", i * 1_000_000)

    # Should complete without deadlock
    all_p = stats.all_percentiles()

    assert len(all_p) == 10


def test_thread_safety_concurrent_records() -> None:
    """LatencyStats handles concurrent record() calls safely."""
    stats = LatencyStats()
    errors = []

    def worker() -> None:
        try:
            for i in range(1000):
                stats.record("TestQuery", "query", i * 1_000_000)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0

    p = stats.percentiles("TestQuery", "query")
    assert p is not None
    # 10 threads * 1000 records = 10000 total (but max_samples is 10000 by default)
    assert p.count <= 10_000


def test_max_samples_validation() -> None:
    """LatencyStats raises ValueError for invalid max_samples."""
    with pytest.raises(ValueError, match="max_samples must be >= 1"):
        LatencyStats(max_samples=0)

    with pytest.raises(ValueError, match="max_samples must be >= 1"):
        LatencyStats(max_samples=-1)


def test_percentile_accuracy_known_data() -> None:
    """LatencyStats computes accurate percentiles for known data."""
    stats = LatencyStats()

    # Record 100 samples: 0, 1, 2, ..., 99 milliseconds
    for i in range(100):
        stats.record("TestQuery", "query", i * 1_000_000)

    p = stats.percentiles("TestQuery", "query")

    assert p is not None
    # For 100 samples (0-99):
    # p50 should be around index 49.5 -> 49.5ms
    # p95 should be around index 94.05 -> 94.05ms
    # p99 should be around index 98.01 -> 98.01ms
    assert 49 <= p.p50_ms <= 50
    assert 94 <= p.p95_ms <= 95
    assert 98 <= p.p99_ms <= 99
    assert p.min_ms == 0.0
    assert p.max_ms == 99.0


# ---------------------------------------------------------------------------
# TestLatencyStatsBaseline
# ---------------------------------------------------------------------------


def test_set_baseline_and_detect_increase() -> None:
    """LatencyStats detects latency increase vs baseline."""
    stats = LatencyStats()

    # Record baseline data: p95 = 50ms
    for _ in range(100):
        stats.record("TestQuery", "query", 50_000_000)

    stats.set_baseline()

    # Record new data with doubled latency: p95 = 100ms
    for _ in range(100):
        stats.record("TestQuery", "query", 100_000_000)

    signals = stats.check_separation_signals(threshold_ratio=2.0)

    assert len(signals) == 1
    assert signals[0].message_class == "TestQuery"
    assert signals[0].message_type == "query"
    assert signals[0].increase_ratio >= 2.0


def test_no_signal_below_threshold() -> None:
    """LatencyStats does not signal when increase is below threshold."""
    stats = LatencyStats()

    # Record baseline data
    for _ in range(100):
        stats.record("TestQuery", "query", 50_000_000)

    stats.set_baseline()

    # Record new data with 1.5x latency (below 2.0 threshold)
    for _ in range(100):
        stats.record("TestQuery", "query", 75_000_000)

    signals = stats.check_separation_signals(threshold_ratio=2.0)

    assert len(signals) == 0


def test_no_baseline_returns_empty() -> None:
    """LatencyStats returns empty list when no baseline is set."""
    stats = LatencyStats()

    stats.record("TestQuery", "query", 50_000_000)

    signals = stats.check_separation_signals()

    assert len(signals) == 0


def test_zero_baseline_skipped() -> None:
    """LatencyStats skips handlers with zero baseline."""
    stats = LatencyStats()

    # Record zero latency baseline (edge case)
    for _ in range(100):
        stats.record("TestQuery", "query", 0)

    stats.set_baseline()

    # Record non-zero latency
    for _ in range(100):
        stats.record("TestQuery", "query", 50_000_000)

    signals = stats.check_separation_signals()

    # Zero baseline should be skipped (division by zero)
    assert len(signals) == 0


def test_reset_clears_baselines() -> None:
    """LatencyStats.reset() clears baselines."""
    stats = LatencyStats()

    stats.record("TestQuery", "query", 50_000_000)
    stats.set_baseline()

    stats.reset()

    # Record new data with increased latency
    stats.record("TestQuery", "query", 100_000_000)

    signals = stats.check_separation_signals()

    # No baseline, so no signal
    assert len(signals) == 0


# ---------------------------------------------------------------------------
# TestLatencyMiddleware
# ---------------------------------------------------------------------------


def test_measures_query_latency() -> None:
    """LatencyMiddleware measures query latency."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")
    bus.send(SampleQuery(value=42))

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1
    assert p.p50_ms > 0


def test_measures_command_latency() -> None:
    """LatencyMiddleware measures command latency."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    bus.register_command(SampleCommand, lambda c: None)
    bus.execute(SampleCommand(action="test"))

    p = stats.percentiles(_FQ_COMMAND, "command")
    assert p is not None
    assert p.count == 1


def test_measures_event_latency() -> None:
    """LatencyMiddleware measures event latency."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    bus.subscribe(SampleEvent, lambda e: None)
    bus.publish(SampleEvent(data="test"))

    p = stats.percentiles(_FQ_EVENT, "event")
    assert p is not None
    assert p.count == 1


def test_measures_task_latency() -> None:
    """LatencyMiddleware measures task latency."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    bus.register_task(SampleTask, lambda t: None)
    bus.dispatch(SampleTask(task_id=123))

    p = stats.percentiles(_FQ_TASK, "task")
    assert p is not None
    assert p.count == 1


def test_threshold_callback_fires_on_slow_handler() -> None:
    """LatencyMiddleware invokes callback when threshold exceeded."""
    stats = LatencyStats()
    calls = []

    def callback(msg_class: str, msg_type: str, duration_ms: float) -> None:
        calls.append((msg_class, msg_type, duration_ms))

    mw = LatencyMiddleware(stats, threshold_ms=10.0, on_threshold_exceeded=callback)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    def slow_handler(q: Query[str]) -> str:
        time.sleep(0.02)  # 20ms
        return "result"

    bus.register_query(SampleQuery, slow_handler)
    bus.send(SampleQuery(value=1))

    assert len(calls) == 1
    assert calls[0][0] == _FQ_QUERY
    assert calls[0][1] == "query"
    assert calls[0][2] >= 10.0


def test_threshold_callback_silent_on_fast_handler() -> None:
    """LatencyMiddleware does not invoke callback when threshold not exceeded."""
    stats = LatencyStats()
    calls = []

    def callback(msg_class: str, msg_type: str, duration_ms: float) -> None:
        calls.append((msg_class, msg_type, duration_ms))

    mw = LatencyMiddleware(stats, threshold_ms=1000.0, on_threshold_exceeded=callback)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    bus.register_query(SampleQuery, lambda q: "result")
    bus.send(SampleQuery(value=1))

    assert len(calls) == 0


def test_exception_propagation_still_records() -> None:
    """LatencyMiddleware records latency even when handler raises."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    def failing_handler(q: Query[str]) -> str:
        raise ValueError("Test error")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(ValueError, match="Test error"):
        bus.send(SampleQuery(value=1))

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1


def test_threshold_validation() -> None:
    """LatencyMiddleware raises ValueError for invalid threshold."""
    stats = LatencyStats()

    with pytest.raises(ValueError, match="threshold_ms must be > 0"):
        LatencyMiddleware(stats, threshold_ms=0.0)

    with pytest.raises(ValueError, match="threshold_ms must be > 0"):
        LatencyMiddleware(stats, threshold_ms=-1.0)


def test_default_warning_logs(caplog: pytest.LogCaptureFixture) -> None:
    """LatencyMiddleware logs warnings by default when threshold exceeded."""
    import logging

    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=10.0)  # No custom callback
    bus = MiddlewareBus(LocalMessageBus(), [mw])

    def slow_handler(q: Query[str]) -> str:
        time.sleep(0.02)  # 20ms
        return "result"

    bus.register_query(SampleQuery, slow_handler)

    with caplog.at_level(logging.WARNING, logger="message_bus.latency"):
        bus.send(SampleQuery(value=1))

    assert len(caplog.records) >= 1
    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_records) == 1
    assert "Slow query handler" in warning_records[0].message
    assert _FQ_QUERY in warning_records[0].message


# ---------------------------------------------------------------------------
# TestAsyncLatencyMiddleware
# ---------------------------------------------------------------------------


async def test_measures_async_query_latency() -> None:
    """AsyncLatencyMiddleware measures async query latency."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

    async def handler(q: Query[str]) -> str:
        return f"result-{q.value}"

    bus.register_query(SampleQuery, handler)
    await bus.send(SampleQuery(value=42))

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1
    assert p.p50_ms > 0


async def test_measures_async_command_latency() -> None:
    """AsyncLatencyMiddleware measures async command latency."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

    async def handler(c: Command) -> None:
        pass

    bus.register_command(SampleCommand, handler)
    await bus.execute(SampleCommand(action="test"))

    p = stats.percentiles(_FQ_COMMAND, "command")
    assert p is not None
    assert p.count == 1


async def test_measures_async_event_latency() -> None:
    """AsyncLatencyMiddleware measures async event latency."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

    async def handler(e: Event) -> None:
        pass

    bus.subscribe(SampleEvent, handler)
    await bus.publish(SampleEvent(data="test"))

    p = stats.percentiles(_FQ_EVENT, "event")
    assert p is not None
    assert p.count == 1


async def test_measures_async_task_latency() -> None:
    """AsyncLatencyMiddleware measures async task latency."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

    async def handler(t: Task) -> None:
        pass

    bus.register_task(SampleTask, handler)
    await bus.dispatch(SampleTask(task_id=123))

    p = stats.percentiles(_FQ_TASK, "task")
    assert p is not None
    assert p.count == 1


async def test_async_threshold_callback_fires_on_slow_handler() -> None:
    """AsyncLatencyMiddleware invokes callback when threshold exceeded."""
    stats = LatencyStats()
    calls = []

    def callback(msg_class: str, msg_type: str, duration_ms: float) -> None:
        calls.append((msg_class, msg_type, duration_ms))

    mw = AsyncLatencyMiddleware(stats, threshold_ms=10.0, on_threshold_exceeded=callback)
    bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

    async def slow_handler(q: Query[str]) -> str:
        await asyncio.sleep(0.02)  # 20ms
        return "result"

    import asyncio

    bus.register_query(SampleQuery, slow_handler)
    await bus.send(SampleQuery(value=1))

    assert len(calls) == 1
    assert calls[0][0] == _FQ_QUERY
    assert calls[0][1] == "query"
    assert calls[0][2] >= 10.0


async def test_async_exception_propagation_still_records() -> None:
    """AsyncLatencyMiddleware records latency even when handler raises."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])

    async def failing_handler(q: Query[str]) -> str:
        raise ValueError("Test error")

    bus.register_query(SampleQuery, failing_handler)

    with pytest.raises(ValueError, match="Test error"):
        await bus.send(SampleQuery(value=1))

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1


async def test_async_threshold_validation() -> None:
    """AsyncLatencyMiddleware raises ValueError for invalid threshold."""
    stats = LatencyStats()

    with pytest.raises(ValueError, match="threshold_ms must be > 0"):
        AsyncLatencyMiddleware(stats, threshold_ms=0.0)

    with pytest.raises(ValueError, match="threshold_ms must be > 0"):
        AsyncLatencyMiddleware(stats, threshold_ms=-1.0)


# ---------------------------------------------------------------------------
# TestComposition
# ---------------------------------------------------------------------------


def test_recording_bus_wrapping_middleware_bus() -> None:
    """RecordingBus can wrap MiddlewareBus."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    inner = MiddlewareBus(LocalMessageBus(), [mw])
    store = MemoryStore()
    bus = RecordingBus(inner, store)

    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")
    result = bus.send(SampleQuery(value=42))

    # Both middleware and recording should work
    assert result == "result-42"
    assert len(store.records) == 1
    assert store.records[0].message_class == "SampleQuery"

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1


def test_middleware_bus_wrapping_recording_bus() -> None:
    """MiddlewareBus can wrap RecordingBus."""
    stats = LatencyStats()
    mw = LatencyMiddleware(stats, threshold_ms=1000.0)
    store = MemoryStore()
    inner = RecordingBus(LocalMessageBus(), store)
    bus = MiddlewareBus(inner, [mw])

    bus.register_query(SampleQuery, lambda q: f"result-{q.value}")
    result = bus.send(SampleQuery(value=42))

    # Both middleware and recording should work
    assert result == "result-42"
    assert len(store.records) == 1
    assert store.records[0].message_class == "SampleQuery"

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1


async def test_async_recording_bus_wrapping_middleware_bus() -> None:
    """AsyncRecordingBus can wrap AsyncMiddlewareBus."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    inner = AsyncMiddlewareBus(AsyncLocalMessageBus(), [mw])
    store = MemoryStore()
    bus = AsyncRecordingBus(inner, store)

    async def handler(q: Query[str]) -> str:
        return f"result-{q.value}"

    bus.register_query(SampleQuery, handler)
    result = await bus.send(SampleQuery(value=42))

    # Both middleware and recording should work
    assert result == "result-42"
    assert len(store.records) == 1
    assert store.records[0].message_class == "SampleQuery"

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1


async def test_async_middleware_bus_wrapping_recording_bus() -> None:
    """AsyncMiddlewareBus can wrap AsyncRecordingBus."""
    stats = LatencyStats()
    mw = AsyncLatencyMiddleware(stats, threshold_ms=1000.0)
    store = MemoryStore()
    inner = AsyncRecordingBus(AsyncLocalMessageBus(), store)
    bus = AsyncMiddlewareBus(inner, [mw])

    async def handler(q: Query[str]) -> str:
        return f"result-{q.value}"

    bus.register_query(SampleQuery, handler)
    result = await bus.send(SampleQuery(value=42))

    # Both middleware and recording should work
    assert result == "result-42"
    assert len(store.records) == 1
    assert store.records[0].message_class == "SampleQuery"

    p = stats.percentiles(_FQ_QUERY, "query")
    assert p is not None
    assert p.count == 1
