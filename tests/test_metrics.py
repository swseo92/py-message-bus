"""Tests for BusMetricsCollector, BusMetricsSnapshot, and AsyncRedisMessageBus metrics."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis.aioredis  # noqa: E402

from message_bus import AsyncRedisMessageBus, LatencyStats
from message_bus.metrics import BusMetricsCollector, BusMetricsSnapshot, StreamMetrics
from message_bus.ports import Command, Event, Query, Task

# ---------------------------------------------------------------------------
# Test message types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MetricCommand(Command):
    value: str


@dataclass(frozen=True)
class MetricEvent(Event):
    value: str


@dataclass(frozen=True)
class MetricQuery(Query[str]):
    value: str


@dataclass(frozen=True)
class MetricTask(Task):
    value: str


# ---------------------------------------------------------------------------
# Unit tests: BusMetricsCollector
# ---------------------------------------------------------------------------


class TestBusMetricsCollector:
    def test_initial_counts_are_zero(self) -> None:
        collector = BusMetricsCollector()
        assert collector.get_counts("stream:A") == (0, 0)

    def test_record_processed_increments_counter(self) -> None:
        collector = BusMetricsCollector()
        collector.record_processed("stream:A")
        collector.record_processed("stream:A")
        assert collector.get_counts("stream:A") == (2, 0)

    def test_record_failed_increments_counter(self) -> None:
        collector = BusMetricsCollector()
        collector.record_failed("stream:A")
        assert collector.get_counts("stream:A") == (0, 1)

    def test_counters_are_independent_per_stream_key(self) -> None:
        collector = BusMetricsCollector()
        collector.record_processed("stream:A")
        collector.record_failed("stream:B")
        assert collector.get_counts("stream:A") == (1, 0)
        assert collector.get_counts("stream:B") == (0, 1)

    def test_all_stream_keys_returns_seen_keys(self) -> None:
        collector = BusMetricsCollector()
        collector.record_processed("stream:A")
        collector.record_failed("stream:B")
        assert collector.all_stream_keys() == frozenset({"stream:A", "stream:B"})

    def test_all_stream_keys_empty_initially(self) -> None:
        collector = BusMetricsCollector()
        assert collector.all_stream_keys() == frozenset()

    def test_reset_clears_all_counters(self) -> None:
        collector = BusMetricsCollector()
        collector.record_processed("stream:A")
        collector.record_failed("stream:A")
        collector.reset()
        assert collector.get_counts("stream:A") == (0, 0)
        assert collector.all_stream_keys() == frozenset()

    def test_mixed_processed_and_failed(self) -> None:
        collector = BusMetricsCollector()
        collector.record_processed("stream:A")
        collector.record_processed("stream:A")
        collector.record_failed("stream:A")
        assert collector.get_counts("stream:A") == (2, 1)


# ---------------------------------------------------------------------------
# Unit tests: BusMetricsSnapshot
# ---------------------------------------------------------------------------


class TestBusMetricsSnapshot:
    def _make_snapshot(
        self, streams: list[StreamMetrics], latency_percentiles: list | None = None
    ) -> BusMetricsSnapshot:
        return BusMetricsSnapshot(
            collected_at=datetime.now(UTC),
            streams=streams,
            latency_percentiles=latency_percentiles or [],
        )

    def test_total_processed_sums_all_streams(self) -> None:
        snapshot = self._make_snapshot(
            [
                StreamMetrics("a", processed_total=5, failed_total=1, pel_size=0),
                StreamMetrics("b", processed_total=3, failed_total=2, pel_size=0),
            ]
        )
        assert snapshot.total_processed == 8

    def test_total_failed_sums_all_streams(self) -> None:
        snapshot = self._make_snapshot(
            [
                StreamMetrics("a", processed_total=5, failed_total=1, pel_size=0),
                StreamMetrics("b", processed_total=3, failed_total=2, pel_size=0),
            ]
        )
        assert snapshot.total_failed == 3

    def test_total_pel_size_sums_non_negative(self) -> None:
        snapshot = self._make_snapshot(
            [
                StreamMetrics("a", processed_total=0, failed_total=0, pel_size=5),
                StreamMetrics("b", processed_total=0, failed_total=0, pel_size=3),
            ]
        )
        assert snapshot.total_pel_size == 8

    def test_total_pel_size_excludes_minus_one(self) -> None:
        snapshot = self._make_snapshot(
            [
                StreamMetrics("a", processed_total=0, failed_total=0, pel_size=5),
                StreamMetrics("b", processed_total=0, failed_total=0, pel_size=-1),
            ]
        )
        assert snapshot.total_pel_size == 5

    def test_total_pel_size_all_minus_one_returns_zero(self) -> None:
        snapshot = self._make_snapshot(
            [
                StreamMetrics("a", processed_total=0, failed_total=0, pel_size=-1),
            ]
        )
        assert snapshot.total_pel_size == 0

    def test_empty_streams(self) -> None:
        snapshot = self._make_snapshot([])
        assert snapshot.total_processed == 0
        assert snapshot.total_failed == 0
        assert snapshot.total_pel_size == 0


# ---------------------------------------------------------------------------
# Integration tests: AsyncRedisMessageBus + metrics
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis() -> fakeredis.aioredis.FakeRedis:
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def metrics_collector() -> BusMetricsCollector:
    return BusMetricsCollector()


def make_bus(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector | None = None,
    latency_stats: LatencyStats | None = None,
) -> AsyncRedisMessageBus:
    return AsyncRedisMessageBus(
        redis_url="redis://localhost",
        consumer_group="test-group",
        _redis_client=fake_redis,
        _block_ms=0,
        metrics_collector=metrics_collector,
        latency_stats=latency_stats,
    )


@pytest.mark.asyncio
async def test_processed_counter_increments_on_command_success(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    handled: list[str] = []

    async def handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        handled.append(cmd.value)

    bus.register_command(MetricCommand, handler)  # type: ignore[arg-type]
    async with bus:
        await bus.execute(MetricCommand(value="x"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    stream_key = "message_bus:command:MetricCommand"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 1
    assert failed == 0


@pytest.mark.asyncio
async def test_failed_counter_increments_on_command_failure(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def failing_handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        raise RuntimeError("boom")

    bus.register_command(MetricCommand, failing_handler)  # type: ignore[arg-type]
    async with bus:
        await bus.execute(MetricCommand(value="x"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    stream_key = "message_bus:command:MetricCommand"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 0
    assert failed == 1


@pytest.mark.asyncio
async def test_processed_counter_increments_on_task_success(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def handler(task: MetricTask) -> None:  # type: ignore[misc]
        pass

    bus.register_task(MetricTask, handler)  # type: ignore[arg-type]
    async with bus:
        await bus.dispatch(MetricTask(value="t"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    stream_key = "message_bus:task:MetricTask"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 1
    assert failed == 0


@pytest.mark.asyncio
async def test_failed_counter_increments_on_task_failure(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def failing_task(task: MetricTask) -> None:  # type: ignore[misc]
        raise RuntimeError("task boom")

    bus.register_task(MetricTask, failing_task)  # type: ignore[arg-type]
    async with bus:
        await bus.dispatch(MetricTask(value="t"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    stream_key = "message_bus:task:MetricTask"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 0
    assert failed == 1


@pytest.mark.asyncio
async def test_processed_counter_increments_on_event_success(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def handler(event: MetricEvent) -> None:  # type: ignore[misc]
        pass

    bus.subscribe(MetricEvent, handler)  # type: ignore[arg-type]
    async with bus:
        await bus.publish(MetricEvent(value="e"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    stream_key = "message_bus:event:MetricEvent"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 1
    assert failed == 0


@pytest.mark.asyncio
async def test_failed_counter_increments_on_event_failure(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def failing_handler(event: MetricEvent) -> None:  # type: ignore[misc]
        raise RuntimeError("event boom")

    bus.subscribe(MetricEvent, failing_handler)  # type: ignore[arg-type]
    async with bus:
        await bus.publish(MetricEvent(value="e"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    stream_key = "message_bus:event:MetricEvent"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 0
    assert failed == 1


@pytest.mark.asyncio
async def test_processed_counter_increments_on_query_success(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def handler(query: MetricQuery) -> str:  # type: ignore[misc]
        return f"result:{query.value}"

    bus.register_query(MetricQuery, handler)  # type: ignore[arg-type]
    async with bus:
        result = await bus.send(MetricQuery(value="q"))
        assert result == "result:q"

    stream_key = "message_bus:query:MetricQuery"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 1
    assert failed == 0


@pytest.mark.asyncio
async def test_failed_counter_increments_on_query_failure(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def failing_query(query: MetricQuery) -> str:  # type: ignore[misc]
        raise ValueError("query boom")

    bus.register_query(MetricQuery, failing_query)  # type: ignore[arg-type]
    async with bus:
        with pytest.raises(RuntimeError, match="query boom"):
            await bus.send(MetricQuery(value="q"))

    stream_key = "message_bus:query:MetricQuery"
    processed, failed = metrics_collector.get_counts(stream_key)
    assert processed == 0
    assert failed == 1


@pytest.mark.asyncio
async def test_get_metrics_snapshot_lists_registered_streams(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        pass

    bus.register_command(MetricCommand, handler)  # type: ignore[arg-type]
    async with bus:
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    assert len(snapshot.streams) == 1
    assert snapshot.streams[0].stream_key == "message_bus:command:MetricCommand"
    assert snapshot.streams[0].pel_size == -1  # not queried


@pytest.mark.asyncio
async def test_get_metrics_snapshot_processed_reflects_counts(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        pass

    bus.register_command(MetricCommand, handler)  # type: ignore[arg-type]
    async with bus:
        await bus.execute(MetricCommand(value="1"))
        await bus.execute(MetricCommand(value="2"))
        for _ in range(30):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    assert snapshot.total_processed == 2
    assert snapshot.total_failed == 0


@pytest.mark.asyncio
async def test_get_metrics_snapshot_with_pel_returns_pel_size(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    """PEL size should be 0 after successful ACK."""
    bus = make_bus(fake_redis, metrics_collector)

    async def handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        pass

    bus.register_command(MetricCommand, handler)  # type: ignore[arg-type]
    async with bus:
        await bus.execute(MetricCommand(value="x"))
        for _ in range(30):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        snapshot = await bus.get_metrics_snapshot(include_pel=True)

    assert len(snapshot.streams) == 1
    assert snapshot.streams[0].pel_size == 0  # message was ACKed


@pytest.mark.asyncio
async def test_get_metrics_snapshot_includes_latency_percentiles(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    stats = LatencyStats()
    stats.record("SomeClass", "command", 1_000_000)  # 1ms
    stats.record("SomeClass", "command", 2_000_000)  # 2ms

    bus = make_bus(fake_redis, metrics_collector, latency_stats=stats)
    async with bus:
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    assert len(snapshot.latency_percentiles) == 1
    assert snapshot.latency_percentiles[0].message_class == "SomeClass"
    assert snapshot.latency_percentiles[0].message_type == "command"
    assert snapshot.latency_percentiles[0].count == 2


@pytest.mark.asyncio
async def test_get_metrics_snapshot_no_latency_stats_empty_list(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    bus = make_bus(fake_redis)
    async with bus:
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    assert snapshot.latency_percentiles == []


@pytest.mark.asyncio
async def test_get_metrics_snapshot_no_collector_zero_counts(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """Bus without metrics_collector returns 0-count streams (not an error)."""
    bus = make_bus(fake_redis, metrics_collector=None)

    async def handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        pass

    bus.register_command(MetricCommand, handler)  # type: ignore[arg-type]
    async with bus:
        await bus.execute(MetricCommand(value="x"))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    # Without a collector, counts are always 0
    assert snapshot.total_processed == 0
    assert snapshot.total_failed == 0
    # Stream entry still present (from handler registration)
    assert len(snapshot.streams) == 1


@pytest.mark.asyncio
async def test_get_metrics_snapshot_multiple_message_types(
    fake_redis: fakeredis.aioredis.FakeRedis,
    metrics_collector: BusMetricsCollector,
) -> None:
    bus = make_bus(fake_redis, metrics_collector)

    async def cmd_handler(cmd: MetricCommand) -> None:  # type: ignore[misc]
        pass

    async def event_handler(evt: MetricEvent) -> None:  # type: ignore[misc]
        pass

    bus.register_command(MetricCommand, cmd_handler)  # type: ignore[arg-type]
    bus.subscribe(MetricEvent, event_handler)  # type: ignore[arg-type]

    async with bus:
        await bus.execute(MetricCommand(value="c"))
        await bus.publish(MetricEvent(value="e"))
        for _ in range(30):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    assert len(snapshot.streams) == 2
    assert snapshot.total_processed == 2


@pytest.mark.asyncio
async def test_snapshot_collected_at_is_utc(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    bus = make_bus(fake_redis)
    async with bus:
        snapshot = await bus.get_metrics_snapshot(include_pel=False)

    assert snapshot.collected_at.tzinfo is not None
    assert snapshot.collected_at.tzinfo == UTC
