"""Operational metrics for AsyncRedisMessageBus.

Provides zero-Redis-overhead, Prometheus/OpenTelemetry-compatible metric
collection for :class:`~message_bus.AsyncRedisMessageBus`.

All throughput and failure counters are kept in-memory (no Redis I/O on the
hot path).  PEL sizes are queried from Redis on-demand when
:meth:`~message_bus.AsyncRedisMessageBus.get_metrics_snapshot` is called
with ``include_pel=True``.

Example – bridging to Prometheus::

    from prometheus_client import Counter, Gauge
    from message_bus import AsyncRedisMessageBus, BusMetricsCollector

    processed = Counter("bus_processed_total", "Messages processed", ["stream"])
    failed    = Counter("bus_failed_total",    "Messages failed",    ["stream"])
    pel_gauge = Gauge(  "bus_pel_size",        "PEL size",           ["stream"])

    collector = BusMetricsCollector()
    bus = AsyncRedisMessageBus(..., metrics_collector=collector)

    # Call from your /metrics scrape handler:
    async def update_prometheus() -> None:
        snap = await bus.get_metrics_snapshot()
        for s in snap.streams:
            processed.labels(stream=s.stream_key).inc(s.processed_total)
            failed.labels(stream=s.stream_key).inc(s.failed_total)
            if s.pel_size >= 0:
                pel_gauge.labels(stream=s.stream_key).set(s.pel_size)

Example – bridging to OpenTelemetry::

    from opentelemetry import metrics as otel_metrics
    from message_bus import AsyncRedisMessageBus, BusMetricsCollector

    meter = otel_metrics.get_meter("message_bus")
    processed_counter = meter.create_counter("bus.processed")

    async def observe() -> None:
        snap = await bus.get_metrics_snapshot()
        for s in snap.streams:
            processed_counter.add(s.processed_total, {"stream": s.stream_key})
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_bus.latency import LatencyPercentiles


@dataclass(frozen=True, slots=True)
class StreamMetrics:
    """Operational metrics for a single Redis stream.

    One :class:`StreamMetrics` is produced per registered message type (and
    per event subscriber) in
    :meth:`~message_bus.AsyncRedisMessageBus.get_metrics_snapshot`.
    """

    stream_key: str
    """Redis stream key (e.g. ``message_bus:command:CreateOrderCommand``)."""

    processed_total: int
    """Messages successfully handled and ACKed in this process.

    Counter resets on process restart.  For events with multiple subscribers,
    reflects the total number of successful handler invocations across all
    subscriber consumer groups that share this stream key.
    """

    failed_total: int
    """Messages whose handler raised an exception in this process.

    Counter resets on process restart.  Failed messages remain in the PEL
    and will be retried via ``XAUTOCLAIM``.
    """

    pel_size: int
    """Current Pending Entries List (PEL) size queried from Redis.

    ``-1`` when PEL was not requested (``include_pel=False`` passed to
    :meth:`~message_bus.AsyncRedisMessageBus.get_metrics_snapshot`).
    ``-2`` when the PEL query failed (Redis error); a warning is logged.
    A non-negative value indicates the number of un-ACKed messages
    awaiting retry or DLQ routing.
    """


@dataclass(frozen=True, slots=True)
class BusMetricsSnapshot:
    """Point-in-time operational metrics for :class:`~message_bus.AsyncRedisMessageBus`.

    Returned by :meth:`~message_bus.AsyncRedisMessageBus.get_metrics_snapshot`.
    """

    collected_at: datetime
    """UTC timestamp when the snapshot was collected."""

    streams: list[StreamMetrics]
    """Per-stream metrics.  One entry per registered message type / subscriber."""

    latency_percentiles: list[LatencyPercentiles]
    """Latency percentiles per message class and type.

    Populated from the :class:`~message_bus.LatencyStats` instance injected
    via the ``latency_stats`` parameter of
    :class:`~message_bus.AsyncRedisMessageBus`.  Empty list when no
    ``latency_stats`` was provided.
    """

    @property
    def total_processed(self) -> int:
        """Sum of :attr:`StreamMetrics.processed_total` across all streams."""
        return sum(s.processed_total for s in self.streams)

    @property
    def total_failed(self) -> int:
        """Sum of :attr:`StreamMetrics.failed_total` across all streams."""
        return sum(s.failed_total for s in self.streams)

    @property
    def total_pel_size(self) -> int:
        """Sum of non-negative :attr:`StreamMetrics.pel_size` across all streams.

        Returns ``0`` when PEL was not queried (all sizes are ``-1``) or
        when all PEL queries failed (all sizes are ``-2``).
        """
        return sum(s.pel_size for s in self.streams if s.pel_size >= 0)


class BusMetricsCollector:
    """Thread-safe in-memory counter for :class:`~message_bus.AsyncRedisMessageBus` metrics.

    Tracks :attr:`~StreamMetrics.processed_total` and
    :attr:`~StreamMetrics.failed_total` per Redis stream key.  Counters are
    kept in-memory (no Redis overhead) and reset on process restart.

    Pass an instance to :class:`~message_bus.AsyncRedisMessageBus` via the
    ``metrics_collector`` constructor parameter::

        collector = BusMetricsCollector()
        bus = AsyncRedisMessageBus(..., metrics_collector=collector)

    Then retrieve a snapshot via
    :meth:`~message_bus.AsyncRedisMessageBus.get_metrics_snapshot`.

    Thread safety
    -------------
    All methods are protected by a single :class:`threading.Lock`.  The lock
    is held only for a single integer increment or dict lookup (O(1)), making
    contention negligible in typical asyncio workloads.
    """

    __slots__ = ("_processed", "_failed", "_lock")

    def __init__(self) -> None:
        self._processed: dict[str, int] = {}
        self._failed: dict[str, int] = {}
        self._lock = threading.Lock()

    def record_processed(self, stream_key: str) -> None:
        """Increment the processed counter for *stream_key*."""
        with self._lock:
            self._processed[stream_key] = self._processed.get(stream_key, 0) + 1

    def record_failed(self, stream_key: str) -> None:
        """Increment the failed counter for *stream_key*."""
        with self._lock:
            self._failed[stream_key] = self._failed.get(stream_key, 0) + 1

    def get_counts(self, stream_key: str) -> tuple[int, int]:
        """Return ``(processed, failed)`` for *stream_key*.

        Returns ``(0, 0)`` for stream keys that have never been seen.
        """
        with self._lock:
            return (
                self._processed.get(stream_key, 0),
                self._failed.get(stream_key, 0),
            )

    def all_stream_keys(self) -> frozenset[str]:
        """Return the set of stream keys with at least one recorded event."""
        with self._lock:
            return frozenset(self._processed) | frozenset(self._failed)

    def reset(self) -> None:
        """Clear all counters.  Useful for testing or rolling metric windows."""
        with self._lock:
            self._processed.clear()
            self._failed.clear()
