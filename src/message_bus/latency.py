"""Latency instrumentation middleware for message bus.

Provides LatencyMiddleware (sync) and AsyncLatencyMiddleware (async) for
measuring handler execution latency, plus LatencyStats for bounded-memory
percentile statistics and module separation signal detection.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from message_bus.middleware import AsyncPassthroughMiddleware, PassthroughMiddleware
from message_bus.ports import Command, Event, Query, Task

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LatencyPercentiles:
    """Percentile summary for a (message_class, message_type) key."""

    message_class: str
    message_type: str
    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float


@dataclass(frozen=True, slots=True)
class SeparationSignal:
    """Signal that a handler's latency may be coupled to another module's load.

    Emitted when a handler's current p95 exceeds its baseline p95 by the
    configured threshold ratio.
    """

    message_class: str
    message_type: str
    current_p95_ms: float
    baseline_p95_ms: float
    increase_ratio: float  # current / baseline


# ---------------------------------------------------------------------------
# LatencyStats -- Thread-safe statistics collector
# ---------------------------------------------------------------------------


class LatencyStats:
    """Thread-safe latency statistics collector with bounded memory.

    Stores the last `max_samples` measurements per (message_class, message_type)
    key using `collections.deque(maxlen=N)`. Older samples are automatically
    evicted when the deque is full (O(1) bounded append).

    Memory bound: max_samples * num_unique_message_types * 8 bytes (int64).
    Default: 10,000 samples * ~20 types = ~1.6MB worst case.

    Thread Safety:
        All methods are thread-safe. Protected by a single `threading.Lock`.
        Lock granularity is global (not per-key) for simplicity -- sufficient
        for monolith scale. The lock is held briefly for append (O(1)) and
        released before sort (cold path).
    """

    __slots__ = ("_max_samples", "_data", "_baselines", "_lock")

    def __init__(self, max_samples: int = 10_000) -> None:
        if max_samples < 1:
            raise ValueError(f"max_samples must be >= 1, got {max_samples}")
        self._max_samples = max_samples
        self._data: dict[tuple[str, str], deque[int]] = {}
        self._baselines: dict[tuple[str, str], float] = {}
        self._lock = threading.Lock()

    def record(self, message_class: str, message_type: str, duration_ns: int) -> None:
        """Record a latency measurement. O(1) amortized (deque append)."""
        key = (message_class, message_type)
        with self._lock:
            samples = self._data.get(key)
            if samples is None:
                samples = deque(maxlen=self._max_samples)
                self._data[key] = samples
            samples.append(duration_ns)

    def percentiles(self, message_class: str, message_type: str) -> LatencyPercentiles | None:
        """Get percentile summary for a specific message class.

        Returns None if no data recorded for this key.
        Copies data under lock, then sorts outside lock (cold path).
        """
        key = (message_class, message_type)
        with self._lock:
            samples = self._data.get(key)
            if not samples:
                return None
            snapshot = list(samples)

        return self._percentiles_from_snapshot(key[0], key[1], snapshot)

    def all_percentiles(self) -> list[LatencyPercentiles]:
        """Get percentile summaries for all recorded message classes.

        Acquires lock once to copy all data, then computes percentiles
        outside the lock. No deadlock risk (does not call self.percentiles()).
        """
        with self._lock:
            snapshots: list[tuple[str, str, list[int]]] = [
                (key[0], key[1], list(samples)) for key, samples in self._data.items() if samples
            ]

        results: list[LatencyPercentiles] = []
        for msg_class, msg_type, snapshot in snapshots:
            results.append(self._percentiles_from_snapshot(msg_class, msg_type, snapshot))
        return results

    def reset(self) -> None:
        """Clear all recorded data and baselines."""
        with self._lock:
            self._data.clear()
            self._baselines.clear()

    def set_baseline(self) -> None:
        """Snapshot current p95 values as baseline for signal detection.

        Call after warm-up period (e.g., first 1000 dispatches).
        Acquires lock once, computes p95 for each key from copied data.
        """
        with self._lock:
            snapshots: dict[tuple[str, str], list[int]] = {
                key: list(samples) for key, samples in self._data.items() if samples
            }

        baselines: dict[tuple[str, str], float] = {}
        for key, snapshot in snapshots.items():
            snapshot.sort()
            baselines[key] = self._percentile_value(snapshot, 95)

        with self._lock:
            self._baselines = baselines

    def check_separation_signals(
        self,
        threshold_ratio: float = 2.0,
    ) -> list[SeparationSignal]:
        """Detect handlers whose p95 latency increased significantly vs baseline.

        Returns list of SeparationSignal for handlers exceeding threshold_ratio.
        An increase suggests the handler may be coupled to load from another module.

        Args:
            threshold_ratio: Minimum current/baseline ratio to trigger signal.
                             Default 2.0 (p95 doubled since baseline).
        """
        with self._lock:
            baselines_copy = dict(self._baselines)
            snapshots: dict[tuple[str, str], list[int]] = {
                key: list(samples)
                for key, samples in self._data.items()
                if samples and key in baselines_copy
            }

        signals: list[SeparationSignal] = []
        for key, snapshot in snapshots.items():
            baseline = baselines_copy.get(key)
            if baseline is None or baseline == 0:
                continue
            snapshot.sort()
            current_p95 = self._percentile_value(snapshot, 95)
            ratio = current_p95 / baseline
            if ratio >= threshold_ratio:
                signals.append(
                    SeparationSignal(
                        message_class=key[0],
                        message_type=key[1],
                        current_p95_ms=current_p95 / 1_000_000,
                        baseline_p95_ms=baseline / 1_000_000,
                        increase_ratio=round(ratio, 2),
                    )
                )
        return signals

    def _percentiles_from_snapshot(
        self, message_class: str, message_type: str, snapshot: list[int]
    ) -> LatencyPercentiles:
        """Compute percentile summary from a snapshot (already copied, no lock needed)."""
        snapshot.sort()
        count = len(snapshot)
        return LatencyPercentiles(
            message_class=message_class,
            message_type=message_type,
            count=count,
            p50_ms=self._percentile_value(snapshot, 50) / 1_000_000,
            p95_ms=self._percentile_value(snapshot, 95) / 1_000_000,
            p99_ms=self._percentile_value(snapshot, 99) / 1_000_000,
            min_ms=snapshot[0] / 1_000_000,
            max_ms=snapshot[-1] / 1_000_000,
        )

    @staticmethod
    def _percentile_value(sorted_data: list[int], pct: int) -> float:
        """Nearest-rank percentile from sorted data."""
        n = len(sorted_data)
        if n == 0:
            return 0.0
        rank = (pct / 100) * (n - 1)
        lower = int(rank)
        upper = min(lower + 1, n - 1)
        frac = rank - lower
        return sorted_data[lower] * (1 - frac) + sorted_data[upper] * frac


# ---------------------------------------------------------------------------
# LatencyMiddleware (sync)
# ---------------------------------------------------------------------------


class LatencyMiddleware(PassthroughMiddleware):
    """Measure handler execution latency with threshold-based callbacks.

    Overhead: ~50ns per dispatch (one time.perf_counter_ns() call pair).
    Logging/callback only invoked when threshold exceeded.

    Args:
        stats: LatencyStats instance for collecting measurements.
        threshold_ms: Latency threshold in milliseconds. Callback invoked only
                      when exceeded. Default 100ms.
        on_threshold_exceeded: Callback(message_class: str, message_type: str,
                               duration_ms: float). Default: logger.warning().
    """

    __slots__ = ("_stats", "_threshold_ns", "_on_exceeded")

    def __init__(
        self,
        stats: LatencyStats,
        threshold_ms: float = 100.0,
        on_threshold_exceeded: Callable[[str, str, float], None] | None = None,
    ) -> None:
        if threshold_ms <= 0:
            raise ValueError(f"threshold_ms must be > 0, got {threshold_ms}")
        self._stats = stats
        self._threshold_ns = int(threshold_ms * 1_000_000)
        self._on_exceeded = on_threshold_exceeded or self._default_warning

    @staticmethod
    def _default_warning(message_class: str, message_type: str, duration_ms: float) -> None:
        logger.warning(
            "Slow %s handler: %s took %.2fms",
            message_type,
            message_class,
            duration_ms,
        )

    def _measure(
        self,
        message: Query[Any] | Command | Event | Task,
        message_type: str,
        next_fn: Callable[..., Any],
    ) -> Any:
        t0 = time.perf_counter_ns()
        try:
            return next_fn(message)
        finally:
            duration_ns = time.perf_counter_ns() - t0
            msg_class = type(message).__name__
            self._stats.record(msg_class, message_type, duration_ns)
            if duration_ns > self._threshold_ns:
                duration_ms = duration_ns / 1_000_000
                self._on_exceeded(msg_class, message_type, duration_ms)

    def on_send(self, query: Query[Any], next_fn: Callable[[Query[Any]], Any]) -> Any:
        return self._measure(query, "query", next_fn)

    def on_execute(self, command: Command, next_fn: Callable[[Command], None]) -> None:
        self._measure(command, "command", next_fn)

    def on_publish(self, event: Event, next_fn: Callable[[Event], None]) -> None:
        self._measure(event, "event", next_fn)

    def on_dispatch(self, task: Task, next_fn: Callable[[Task], None]) -> None:
        self._measure(task, "task", next_fn)


# ---------------------------------------------------------------------------
# AsyncLatencyMiddleware
# ---------------------------------------------------------------------------


class AsyncLatencyMiddleware(AsyncPassthroughMiddleware):
    """Async version of LatencyMiddleware. Uses same LatencyStats (thread-safe).

    Uses time.perf_counter_ns() for measurement (consistent with sync version
    and RecordingBus). The sync callback on_threshold_exceeded is fine here
    because it only fires on the cold path (threshold exceeded).
    """

    __slots__ = ("_stats", "_threshold_ns", "_on_exceeded")

    def __init__(
        self,
        stats: LatencyStats,
        threshold_ms: float = 100.0,
        on_threshold_exceeded: Callable[[str, str, float], None] | None = None,
    ) -> None:
        if threshold_ms <= 0:
            raise ValueError(f"threshold_ms must be > 0, got {threshold_ms}")
        self._stats = stats
        self._threshold_ns = int(threshold_ms * 1_000_000)
        self._on_exceeded = on_threshold_exceeded or LatencyMiddleware._default_warning

    async def _measure(
        self,
        message: Query[Any] | Command | Event | Task,
        message_type: str,
        next_fn: Callable[..., Awaitable[Any]],
    ) -> Any:
        t0 = time.perf_counter_ns()
        try:
            return await next_fn(message)
        finally:
            duration_ns = time.perf_counter_ns() - t0
            msg_class = type(message).__name__
            self._stats.record(msg_class, message_type, duration_ns)
            if duration_ns > self._threshold_ns:
                duration_ms = duration_ns / 1_000_000
                self._on_exceeded(msg_class, message_type, duration_ms)

    async def on_send(
        self,
        query: Query[Any],
        next_fn: Callable[[Query[Any]], Awaitable[Any]],
    ) -> Any:
        return await self._measure(query, "query", next_fn)

    async def on_execute(
        self,
        command: Command,
        next_fn: Callable[[Command], Awaitable[None]],
    ) -> None:
        await self._measure(command, "command", next_fn)

    async def on_publish(
        self,
        event: Event,
        next_fn: Callable[[Event], Awaitable[None]],
    ) -> None:
        await self._measure(event, "event", next_fn)

    async def on_dispatch(
        self,
        task: Task,
        next_fn: Callable[[Task], Awaitable[None]],
    ) -> None:
        await self._measure(task, "task", next_fn)
