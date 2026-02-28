"""AsyncRedisMessageBus performance benchmark.

Run with:
    /home/agent-maestro-seo/project/py-message-bus/.venv/bin/python \
        benchmarks/bench_async_redis.py

Each benchmark section is isolated with a unique app_name prefix and
cleans up its Redis keys at the end.
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import statistics

# ---------------------------------------------------------------------------
# Add src to path so we can import message_bus without installing
# ---------------------------------------------------------------------------
import sys
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from message_bus.async_redis_bus import AsyncJsonSerializer, AsyncRedisMessageBus, TypeRegistry
from message_bus.ports import Command, Event, Query, Task

REDIS_URL = "redis://localhost:6379/0"
N_COMMAND = 1000
N_EVENT = 1000
N_TASK = 1000
N_QUERY = 100
N_LATENCY = 100
ROUNDS = 10  # minimum rounds for statistical reliability (SWS2-60)
CI_Z = 1.96  # z-score for 95% confidence interval


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class BenchCommand(Command):  # type: ignore[misc]
    value: int = 0


@dataclasses.dataclass(frozen=True)
class BenchEvent(Event):  # type: ignore[misc]
    value: int = 0


@dataclasses.dataclass(frozen=True)
class BenchTask(Task):  # type: ignore[misc]
    value: int = 0


@dataclasses.dataclass(frozen=True)
class BenchQuery(Query[int]):  # type: ignore[misc]
    value: int = 0


@dataclasses.dataclass(frozen=True)
class ComplexMessage(Command):
    amount: Decimal = Decimal("0")
    ts: datetime = dataclasses.field(default_factory=lambda: datetime.now(UTC))
    uid: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    tags: list[str] = dataclasses.field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    if not (0 <= p <= 100):
        raise ValueError(f"Percentile must be in [0, 100], got {p}")
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


@dataclasses.dataclass
class BenchStats:
    """Statistical summary across benchmark rounds (SWS2-60)."""

    mean: float
    stddev: float
    p95: float
    p99: float
    cv: float  # coefficient of variation (stddev/mean × 100)
    ci95: float  # 95% confidence interval half-width


def compute_stats(samples: list[float]) -> BenchStats:
    """Compute mean, stddev, p95, p99, CV, and 95% CI from a list of samples.

    Edge cases:
    - Empty samples: all fields are 0.0 (sentinel; not meaningful).
    - Single sample: stddev/cv/ci95 are 0.0 due to insufficient data,
      NOT because the measurement is perfectly stable.
    """
    if not samples:
        return BenchStats(mean=0.0, stddev=0.0, p95=0.0, p99=0.0, cv=0.0, ci95=0.0)
    if len(samples) == 1:
        v = samples[0]
        # stddev/cv/ci95 = 0.0: insufficient data for variance, not zero variance
        return BenchStats(mean=v, stddev=0.0, p95=v, p99=v, cv=0.0, ci95=0.0)
    mean = statistics.mean(samples)
    stddev = statistics.stdev(samples)
    cv = stddev / mean * 100 if mean > 0 else 0.0
    ci95 = CI_Z * stddev / math.sqrt(len(samples))
    return BenchStats(
        mean=mean,
        stddev=stddev,
        p95=percentile(samples, 95),
        p99=percentile(samples, 99),
        cv=cv,
        ci95=ci95,
    )


def print_table(title: str, headers: list[str], rows: list[list[str]]) -> None:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_row = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    print(f"\n### {title}")
    print(sep)
    print(header_row)
    print(sep)
    for row in rows:
        print(
            "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
        )  # noqa: E501
    print(sep)


async def cleanup_keys(redis_client: Any, app_name: str) -> None:
    """Delete all keys with the given app_name prefix."""
    pattern = f"{app_name}:*"
    keys = await redis_client.keys(pattern)
    if keys:
        await redis_client.delete(*keys)


async def make_bus(app_name: str, **kwargs: Any) -> AsyncRedisMessageBus:
    return AsyncRedisMessageBus(
        redis_url=REDIS_URL,
        consumer_group=f"{app_name}-group",
        app_name=app_name,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# 1. Throughput benchmarks
# ---------------------------------------------------------------------------


async def bench_command_throughput() -> float:
    """Measure Command execute() throughput (msg/sec) — producer side only."""
    app_name = f"bench-cmd-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name)
    async with bus:
        cmd = BenchCommand(value=42)
        # Warm up
        for _ in range(10):
            await bus.execute(cmd)

        t0 = time.perf_counter()
        for _ in range(N_COMMAND):
            await bus.execute(cmd)
        elapsed = time.perf_counter() - t0

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return N_COMMAND / elapsed


async def bench_event_throughput(n_subscribers: int) -> float:
    """Measure Event publish() throughput (msg/sec) — producer side only."""
    app_name = f"bench-evt{n_subscribers}-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name)

    async def noop_handler(e: Event) -> None:
        pass

    for _ in range(n_subscribers):
        bus.subscribe(BenchEvent, noop_handler)  # type: ignore[arg-type]

    async with bus:
        ev = BenchEvent(value=1)
        for _ in range(10):
            await bus.publish(ev)

        t0 = time.perf_counter()
        for _ in range(N_EVENT):
            await bus.publish(ev)
        elapsed = time.perf_counter() - t0

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return N_EVENT / elapsed


async def bench_task_throughput() -> float:
    """Measure Task dispatch() throughput (msg/sec) — producer side only."""
    app_name = f"bench-tsk-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name)
    async with bus:
        task_msg = BenchTask(value=7)
        for _ in range(10):
            await bus.dispatch(task_msg)

        t0 = time.perf_counter()
        for _ in range(N_TASK):
            await bus.dispatch(task_msg)
        elapsed = time.perf_counter() - t0

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return N_TASK / elapsed


async def bench_query_throughput() -> float:
    """Measure Query send() round-trip throughput (msg/sec)."""
    app_name = f"bench-qry-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name, query_reply_timeout=10.0)

    async def handle_query(q: BenchQuery) -> int:  # type: ignore[type-arg]
        return q.value * 2

    bus.register_query(BenchQuery, handle_query)  # type: ignore[arg-type]

    async with bus:
        # Warm up
        await bus.send(BenchQuery(value=1))

        t0 = time.perf_counter()
        for i in range(N_QUERY):
            await bus.send(BenchQuery(value=i))
        elapsed = time.perf_counter() - t0

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return N_QUERY / elapsed


# ---------------------------------------------------------------------------
# 2. Latency benchmarks
# ---------------------------------------------------------------------------


async def bench_command_latency() -> tuple[float, float, float]:
    """Measure per-message Command execute() latency (ms): p50, p95, p99."""
    app_name = f"bench-cmd-lat-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name)
    latencies: list[float] = []

    async with bus:
        cmd = BenchCommand(value=42)
        for _ in range(N_LATENCY):
            t0 = time.perf_counter()
            await bus.execute(cmd)
            latencies.append((time.perf_counter() - t0) * 1000)

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return (
        percentile(latencies, 50),
        percentile(latencies, 95),
        percentile(latencies, 99),
    )


async def bench_event_latency() -> tuple[float, float, float]:
    """Measure per-message Event publish() latency (ms): p50, p95, p99."""
    app_name = f"bench-evt-lat-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name)
    bus.subscribe(BenchEvent, lambda e: asyncio.sleep(0))  # type: ignore[arg-type, return-value]
    latencies: list[float] = []

    async with bus:
        ev = BenchEvent(value=1)
        for _ in range(N_LATENCY):
            t0 = time.perf_counter()
            await bus.publish(ev)
            latencies.append((time.perf_counter() - t0) * 1000)

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return (
        percentile(latencies, 50),
        percentile(latencies, 95),
        percentile(latencies, 99),
    )


async def bench_query_latency() -> tuple[float, float, float]:
    """Measure Query send() round-trip latency (ms): p50, p95, p99."""
    app_name = f"bench-qry-lat-{uuid.uuid4().hex[:6]}"
    bus = await make_bus(app_name, query_reply_timeout=10.0)

    async def handle_query(q: BenchQuery) -> int:  # type: ignore[type-arg]
        return q.value * 2

    bus.register_query(BenchQuery, handle_query)  # type: ignore[arg-type]
    latencies: list[float] = []

    async with bus:
        for i in range(N_LATENCY):
            t0 = time.perf_counter()
            await bus.send(BenchQuery(value=i))
            latencies.append((time.perf_counter() - t0) * 1000)

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()
    return (
        percentile(latencies, 50),
        percentile(latencies, 95),
        percentile(latencies, 99),
    )


# ---------------------------------------------------------------------------
# 3. Serialization overhead
# ---------------------------------------------------------------------------


def bench_serialization() -> list[dict[str, Any]]:
    """Measure dumps + loads time for simple and complex messages (μs)."""
    registry = TypeRegistry()
    registry.register(BenchCommand, ComplexMessage)
    ser = AsyncJsonSerializer(registry)

    results = []

    # Simple message
    simple_msg = BenchCommand(value=42)
    n_iter = 10_000

    t0 = time.perf_counter()
    for _ in range(n_iter):
        raw = ser.dumps(simple_msg)
    dumps_simple_us = (time.perf_counter() - t0) / n_iter * 1_000_000

    t0 = time.perf_counter()
    for _ in range(n_iter):
        ser.loads(raw)
    loads_simple_us = (time.perf_counter() - t0) / n_iter * 1_000_000

    results.append(
        {
            "type": "Simple (BenchCommand)",
            "dumps_us": dumps_simple_us,
            "loads_us": loads_simple_us,
            "raw_size_bytes": len(raw),
        }
    )

    # Complex message with Decimal, datetime, UUID
    complex_msg = ComplexMessage(
        amount=Decimal("9.99"),
        ts=datetime.now(UTC),
        uid=uuid.uuid4(),
        tags=["alpha", "beta", "gamma"],
    )

    t0 = time.perf_counter()
    for _ in range(n_iter):
        raw_c = ser.dumps(complex_msg)
    dumps_complex_us = (time.perf_counter() - t0) / n_iter * 1_000_000

    t0 = time.perf_counter()
    for _ in range(n_iter):
        ser.loads(raw_c)
    loads_complex_us = (time.perf_counter() - t0) / n_iter * 1_000_000

    results.append(
        {
            "type": "Complex (Decimal+datetime+UUID)",
            "dumps_us": dumps_complex_us,
            "loads_us": loads_complex_us,
            "raw_size_bytes": len(raw_c),
        }
    )

    return results


# ---------------------------------------------------------------------------
# 4. Consumer processing efficiency
# ---------------------------------------------------------------------------


async def bench_consumer_throughput(concurrency: int) -> float:
    """
    Measure consumer-side throughput: produce N commands, then consume all.
    Returns msg/sec consumed.
    """
    app_name = f"bench-con-c{concurrency}-{uuid.uuid4().hex[:6]}"
    n_messages = 500
    consumed: list[float] = []
    done_event = asyncio.Event()

    # Producer bus (no handlers)
    producer = await make_bus(app_name)
    async with producer:
        for i in range(n_messages):
            await producer.execute(BenchCommand(value=i))

    # Consumer bus
    consumer = await make_bus(
        app_name,
        _block_ms=0,  # non-blocking poll for speed in bench
    )

    async def handler(cmd: Command) -> None:
        consumed.append(time.perf_counter())
        if len(consumed) >= n_messages:
            done_event.set()

    consumer.register_command(  # type: ignore[arg-type]
        BenchCommand,
        handler,
        concurrency=concurrency if concurrency > 1 else None,
    )

    t0 = time.perf_counter()
    async with consumer:
        try:
            await asyncio.wait_for(done_event.wait(), timeout=30.0)
        except TimeoutError:
            print(
                f"  [WARN] Throughput benchmark timed out after 30 s "
                f"({len(consumed)}/{n_messages} messages consumed). "
                "Results reflect partial consumption."
            )

    elapsed = (consumed[-1] - t0) if consumed else 1.0
    actual_consumed = len(consumed)

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()

    return actual_consumed / elapsed


async def bench_block_ms_latency() -> list[dict[str, Any]]:
    """Measure how block_ms affects Query round-trip latency."""
    results = []
    for block_ms in [0, 10, 50, 100, 200]:
        app_name = f"bench-blk{block_ms}-{uuid.uuid4().hex[:6]}"
        bus = await make_bus(app_name, query_reply_timeout=10.0, _block_ms=block_ms)

        async def handle_query(q: BenchQuery) -> int:  # type: ignore[type-arg]
            return q.value

        bus.register_query(BenchQuery, handle_query)  # type: ignore[arg-type]

        latencies: list[float] = []
        n = 20  # fewer iterations since each blocks
        async with bus:
            for i in range(n):
                t0 = time.perf_counter()
                await bus.send(BenchQuery(value=i))
                latencies.append((time.perf_counter() - t0) * 1000)

        r = aioredis.from_url(REDIS_URL)
        await cleanup_keys(r, app_name)
        await r.aclose()

        results.append(
            {
                "block_ms": block_ms,
                "p50_ms": percentile(latencies, 50),
                "p95_ms": percentile(latencies, 95),
            }
        )

    return results


# ---------------------------------------------------------------------------
# 5. Metadata / idempotency overhead
# ---------------------------------------------------------------------------


async def bench_metadata_overhead() -> tuple[float, float]:
    """
    Compare raw Redis XADD throughput vs _xadd (with metadata enrichment).
    Returns (raw_xadd_msg_per_sec, full_xadd_msg_per_sec).
    """
    app_name = f"bench-meta-{uuid.uuid4().hex[:6]}"
    n = 1000

    r = aioredis.from_url(REDIS_URL)
    stream_key = f"{app_name}:raw"
    simple_payload = b'{"__type__":"bench","value":42}'

    # Raw XADD (no enrichment)
    t0 = time.perf_counter()
    for _ in range(n):
        await r.xadd(stream_key, {"data": simple_payload}, maxlen=10_000, approximate=True)
    raw_elapsed = time.perf_counter() - t0
    raw_rate = n / raw_elapsed

    # Full _xadd (with metadata: message_id, producer_id, idempotency_key, timestamp)
    bus = await make_bus(app_name)
    cmd = BenchCommand(value=42)
    async with bus:
        t0 = time.perf_counter()
        for _ in range(n):
            await bus._xadd(f"{app_name}:full", cmd)  # noqa: SLF001
        full_elapsed = time.perf_counter() - t0
    full_rate = n / full_elapsed

    # Idempotency overhead: SET NX per message
    dedup_key_base = f"{app_name}:dedup:test"
    t0 = time.perf_counter()
    for i in range(n):
        await r.set(f"{dedup_key_base}:{i}", "1", nx=True, ex=86400)
    dedup_elapsed = time.perf_counter() - t0
    dedup_rate = n / dedup_elapsed

    await cleanup_keys(r, app_name)
    await r.aclose()

    return raw_rate, full_rate, dedup_rate


# ---------------------------------------------------------------------------
# 6. Serialization vs dispatch ratio
# ---------------------------------------------------------------------------


async def bench_serialization_vs_dispatch() -> dict[str, float]:
    """Estimate what % of dispatch time is pure serialization."""
    app_name = f"bench-ratio-{uuid.uuid4().hex[:6]}"
    n = 500
    cmd = BenchCommand(value=42)

    registry = TypeRegistry()
    registry.register(BenchCommand)
    ser = AsyncJsonSerializer(registry)

    # Pure serialization time
    t0 = time.perf_counter()
    for _ in range(n):
        ser.dumps(cmd)
    ser_elapsed_ms = (time.perf_counter() - t0) / n * 1000

    # Full dispatch time (XADD to Redis)
    bus = await make_bus(app_name)
    async with bus:
        t0 = time.perf_counter()
        for _ in range(n):
            await bus.execute(cmd)
        dispatch_elapsed_ms = (time.perf_counter() - t0) / n * 1000

    r = aioredis.from_url(REDIS_URL)
    await cleanup_keys(r, app_name)
    await r.aclose()

    ser_pct = (ser_elapsed_ms / dispatch_elapsed_ms * 100) if dispatch_elapsed_ms > 0 else 0
    return {
        "ser_ms": ser_elapsed_ms,
        "dispatch_ms": dispatch_elapsed_ms,
        "ser_pct": ser_pct,
    }


# ---------------------------------------------------------------------------
# Round-runner helpers (SWS2-60)
# ---------------------------------------------------------------------------


async def _bench_rounds(bench_fn: Any, label: str, unit: str = "msg/s") -> BenchStats:
    """Run an async benchmark function ROUNDS times and return statistics."""
    samples: list[float] = []
    for i in range(ROUNDS):
        v = await bench_fn()
        samples.append(v)
        print(f"  [{i + 1:2d}/{ROUNDS}] {label}: {v:,.0f} {unit}")
    stats = compute_stats(samples)
    print(
        f"         → mean={stats.mean:,.0f}  ±{stats.stddev:,.0f}"
        f"  CV={stats.cv:.1f}%  CI95=±{stats.ci95:,.0f}"
    )
    return stats


async def _bench_latency_rounds(
    bench_fn: Any, label: str
) -> tuple[BenchStats, BenchStats, BenchStats]:
    """Run a latency benchmark ROUNDS times; return (p50_stats, p95_stats, p99_stats)."""
    p50_s: list[float] = []
    p95_s: list[float] = []
    p99_s: list[float] = []
    for i in range(ROUNDS):
        p50, p95, p99 = await bench_fn()
        p50_s.append(p50)
        p95_s.append(p95)
        p99_s.append(p99)
        print(
            f"  [{i + 1:2d}/{ROUNDS}] {label}:  p50={p50:.3f}ms  p95={p95:.3f}ms  p99={p99:.3f}ms"
        )
    return compute_stats(p50_s), compute_stats(p95_s), compute_stats(p99_s)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    print("=" * 70)
    print("  AsyncRedisMessageBus Performance Profile")
    print(f"  Redis: {REDIS_URL}")
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  Rounds per benchmark: {ROUNDS}")
    print("=" * 70)

    # ------------------------------------------------------------------
    print(f"\n[1/6] Throughput benchmarks ({ROUNDS} rounds each)...")
    cmd_stats = await _bench_rounds(bench_command_throughput, "Command execute()")
    print()
    evt1_stats = await _bench_rounds(lambda: bench_event_throughput(1), "Event 1 sub")
    print()
    evt3_stats = await _bench_rounds(lambda: bench_event_throughput(3), "Event 3 sub")
    print()
    evt5_stats = await _bench_rounds(lambda: bench_event_throughput(5), "Event 5 sub")
    print()
    task_stats = await _bench_rounds(bench_task_throughput, "Task dispatch()")
    print()
    query_stats = await _bench_rounds(bench_query_throughput, "Query send() RTT")

    print_table(
        f"Throughput — {ROUNDS} rounds (msg/s)",
        ["Operation", "mean", "±stddev", "p95", "p99", "CV%", "CI95±", "notes"],
        [
            [
                "Command execute()",
                f"{cmd_stats.mean:,.0f}",
                f"{cmd_stats.stddev:,.0f}",
                f"{cmd_stats.p95:,.0f}",
                f"{cmd_stats.p99:,.0f}",
                f"{cmd_stats.cv:.1f}%",
                f"{cmd_stats.ci95:,.0f}",
                f"n={N_COMMAND}",
            ],
            [
                "Event publish() 1 sub",
                f"{evt1_stats.mean:,.0f}",
                f"{evt1_stats.stddev:,.0f}",
                f"{evt1_stats.p95:,.0f}",
                f"{evt1_stats.p99:,.0f}",
                f"{evt1_stats.cv:.1f}%",
                f"{evt1_stats.ci95:,.0f}",
                f"n={N_EVENT}",
            ],
            [
                "Event publish() 3 sub",
                f"{evt3_stats.mean:,.0f}",
                f"{evt3_stats.stddev:,.0f}",
                f"{evt3_stats.p95:,.0f}",
                f"{evt3_stats.p99:,.0f}",
                f"{evt3_stats.cv:.1f}%",
                f"{evt3_stats.ci95:,.0f}",
                f"n={N_EVENT}",
            ],
            [
                "Event publish() 5 sub",
                f"{evt5_stats.mean:,.0f}",
                f"{evt5_stats.stddev:,.0f}",
                f"{evt5_stats.p95:,.0f}",
                f"{evt5_stats.p99:,.0f}",
                f"{evt5_stats.cv:.1f}%",
                f"{evt5_stats.ci95:,.0f}",
                f"n={N_EVENT}",
            ],
            [
                "Task dispatch()",
                f"{task_stats.mean:,.0f}",
                f"{task_stats.stddev:,.0f}",
                f"{task_stats.p95:,.0f}",
                f"{task_stats.p99:,.0f}",
                f"{task_stats.cv:.1f}%",
                f"{task_stats.ci95:,.0f}",
                f"n={N_TASK}",
            ],
            [
                "Query send() (RTT)",
                f"{query_stats.mean:,.1f}",
                f"{query_stats.stddev:,.1f}",
                f"{query_stats.p95:,.1f}",
                f"{query_stats.p99:,.1f}",
                f"{query_stats.cv:.1f}%",
                f"{query_stats.ci95:,.1f}",
                f"n={N_QUERY}",
            ],
        ],
    )

    # ------------------------------------------------------------------
    print(f"\n[2/6] Latency benchmarks ({ROUNDS} rounds each)...")
    cmd_p50_stats, cmd_p95_stats, cmd_p99_stats = await _bench_latency_rounds(
        bench_command_latency, "Command execute()"
    )
    print()
    evt_p50_stats, evt_p95_stats, evt_p99_stats = await _bench_latency_rounds(
        bench_event_latency, "Event publish()"
    )
    print()
    qry_p50_stats, qry_p95_stats, qry_p99_stats = await _bench_latency_rounds(
        bench_query_latency, "Query send() (RTT)"
    )

    print_table(
        f"Latency — mean across {ROUNDS} rounds (ms)",
        ["Operation", "p50 mean", "p50 ±σ", "p95 mean", "p95 ±σ", "p99 mean", "p99 ±σ"],
        [
            [
                "Command execute()",
                f"{cmd_p50_stats.mean:.3f}",
                f"{cmd_p50_stats.stddev:.3f}",
                f"{cmd_p95_stats.mean:.3f}",
                f"{cmd_p95_stats.stddev:.3f}",
                f"{cmd_p99_stats.mean:.3f}",
                f"{cmd_p99_stats.stddev:.3f}",
            ],
            [
                "Event publish()",
                f"{evt_p50_stats.mean:.3f}",
                f"{evt_p50_stats.stddev:.3f}",
                f"{evt_p95_stats.mean:.3f}",
                f"{evt_p95_stats.stddev:.3f}",
                f"{evt_p99_stats.mean:.3f}",
                f"{evt_p99_stats.stddev:.3f}",
            ],
            [
                "Query send() (RTT)",
                f"{qry_p50_stats.mean:.3f}",
                f"{qry_p50_stats.stddev:.3f}",
                f"{qry_p95_stats.mean:.3f}",
                f"{qry_p95_stats.stddev:.3f}",
                f"{qry_p99_stats.mean:.3f}",
                f"{qry_p99_stats.stddev:.3f}",
            ],
        ],
    )

    # ------------------------------------------------------------------
    print(f"\n[3/6] Serialization overhead ({ROUNDS} rounds)...")
    ser_pct_samples: list[float] = []
    last_ser_results: list[dict[str, Any]] = []
    for i in range(ROUNDS):
        sr = bench_serialization()
        ratio = await bench_serialization_vs_dispatch()
        ser_pct_samples.append(ratio["ser_pct"])
        last_ser_results = sr
        print(
            f"  [{i + 1:2d}/{ROUNDS}] ser%={ratio['ser_pct']:.1f}%"
            f"  ser={ratio['ser_ms'] * 1000:.2f}μs  dispatch={ratio['dispatch_ms'] * 1000:.2f}μs"
        )
    ser_pct_stats = compute_stats(ser_pct_samples)

    print_table(
        "Serialization Overhead",
        ["Message Type", "dumps (μs)", "loads (μs)", "size (B)", "ser% mean", "ser% ±σ"],
        [
            [
                r["type"],
                f"{r['dumps_us']:.2f}",
                f"{r['loads_us']:.2f}",
                str(r["raw_size_bytes"]),
                f"{ser_pct_stats.mean:.1f}%" if idx == 0 else "n/a",
                f"{ser_pct_stats.stddev:.1f}%" if idx == 0 else "n/a",
            ]
            for idx, r in enumerate(last_ser_results)
        ],
    )

    # ------------------------------------------------------------------
    print(f"\n[4/6] Consumer processing efficiency ({ROUNDS} rounds)...")
    con1_stats = await _bench_rounds(lambda: bench_consumer_throughput(1), "concurrency=1")
    print()
    con5_stats = await _bench_rounds(lambda: bench_consumer_throughput(5), "concurrency=5")
    speedup = con5_stats.mean / con1_stats.mean if con1_stats.mean > 0 else 0

    print_table(
        f"Consumer Throughput — {ROUNDS} rounds (consumed msg/s)",
        ["Configuration", "mean", "±stddev", "p95", "p99", "CV%", "speedup"],
        [
            [
                "concurrency=1 (sequential)",
                f"{con1_stats.mean:,.0f}",
                f"{con1_stats.stddev:,.0f}",
                f"{con1_stats.p95:,.0f}",
                f"{con1_stats.p99:,.0f}",
                f"{con1_stats.cv:.1f}%",
                "1.0x",
            ],
            [
                "concurrency=5 (parallel)",
                f"{con5_stats.mean:,.0f}",
                f"{con5_stats.stddev:,.0f}",
                f"{con5_stats.p95:,.0f}",
                f"{con5_stats.p99:,.0f}",
                f"{con5_stats.cv:.1f}%",
                f"{speedup:.1f}x",
            ],
        ],
    )

    # ------------------------------------------------------------------
    print(f"\n[5/6] block_ms vs Query latency ({ROUNDS} rounds each)...")
    block_results_all: dict[int, list[dict[str, float]]] = {}
    for i in range(ROUNDS):
        round_results = await bench_block_ms_latency()
        for r in round_results:
            bms = int(r["block_ms"])
            block_results_all.setdefault(bms, []).append(r)
        print(f"  [{i + 1:2d}/{ROUNDS}] done")

    block_stat_rows: list[list[str]] = []
    for bms, rounds_data in sorted(block_results_all.items()):
        p50_s = compute_stats([r["p50_ms"] for r in rounds_data])
        p95_s = compute_stats([r["p95_ms"] for r in rounds_data])
        block_stat_rows.append(
            [
                str(bms),
                f"{p50_s.mean:.2f}",
                f"{p50_s.stddev:.2f}",
                f"{p95_s.mean:.2f}",
                f"{p95_s.stddev:.2f}",
            ]
        )

    print_table(
        f"block_ms vs Query Latency — {ROUNDS} rounds",
        ["block_ms", "p50 mean (ms)", "p50 ±σ", "p95 mean (ms)", "p95 ±σ"],
        block_stat_rows,
    )

    # ------------------------------------------------------------------
    print(f"\n[6/6] Metadata / idempotency overhead ({ROUNDS} rounds)...")
    raw_samples: list[float] = []
    full_samples: list[float] = []
    dedup_samples: list[float] = []
    for i in range(ROUNDS):
        raw_r, full_r, dedup_r = await bench_metadata_overhead()
        raw_samples.append(raw_r)
        full_samples.append(full_r)
        dedup_samples.append(dedup_r)
        ovhd = (raw_r - full_r) / raw_r * 100 if raw_r > 0 else 0
        print(
            f"  [{i + 1:2d}/{ROUNDS}] raw={raw_r:,.0f}  full={full_r:,.0f}"
            f"  overhead={ovhd:.1f}%  dedup={dedup_r:,.0f}"
        )

    raw_stats = compute_stats(raw_samples)
    full_stats = compute_stats(full_samples)
    dedup_stats = compute_stats(dedup_samples)
    overhead_pct = (
        (raw_stats.mean - full_stats.mean) / raw_stats.mean * 100 if raw_stats.mean > 0 else 0
    )

    print_table(
        f"Metadata / Idempotency Overhead — {ROUNDS} rounds",
        ["Operation", "mean msg/s", "±stddev", "p95", "p99", "CV%"],
        [
            [
                "Raw XADD (1 field)",
                f"{raw_stats.mean:,.0f}",
                f"{raw_stats.stddev:,.0f}",
                f"{raw_stats.p95:,.0f}",
                f"{raw_stats.p99:,.0f}",
                f"{raw_stats.cv:.1f}%",
            ],
            [
                "_xadd with metadata (5 fields)",
                f"{full_stats.mean:,.0f}",
                f"{full_stats.stddev:,.0f}",
                f"{full_stats.p95:,.0f}",
                f"{full_stats.p99:,.0f}",
                f"{full_stats.cv:.1f}%",
            ],
            [
                "SET NX (idempotency check)",
                f"{dedup_stats.mean:,.0f}",
                f"{dedup_stats.stddev:,.0f}",
                f"{dedup_stats.p95:,.0f}",
                f"{dedup_stats.p99:,.0f}",
                f"{dedup_stats.cv:.1f}%",
            ],
        ],
    )

    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  BOTTLENECK ANALYSIS")
    print("=" * 70)

    query_latency_ms = qry_p50_stats.mean
    dedup_overhead = (1 / dedup_stats.mean * 1000) if dedup_stats.mean > 0 else 0
    slowdown = f"{cmd_stats.mean / query_stats.mean:.0f}"
    q_p50 = f"{query_latency_ms:.1f}"
    q_p99 = f"{qry_p99_stats.mean:.1f}"

    print(f"""
### Bottleneck Analysis (based on {ROUNDS}-round means)

1. [Query latency — {q_p50}ms p50 mean, {q_p99}ms p99 mean]
   Origin: Query requires 4 serial Redis round-trips:
     (a) XADD to query stream
     (b) XREADGROUP by consumer (block=100ms poll loop)
     (c) XADD reply to per-request reply stream
     (d) XREAD poll by caller + DELETE reply stream
   Impact: {query_stats.mean:.1f} vs {cmd_stats.mean:,.0f} msg/sec (~{slowdown}x slower).
   Optimization:
     - Use Redis Pub/Sub for reply notification instead of polling XREAD.
     - Reduce _block_ms (currently 100ms) — even 10ms cuts median latency.
     - Pipeline XADD + XREAD in a single connection using asyncio.gather.

2. [Idempotency SET NX — adds {dedup_overhead:.2f}ms overhead per command]
   Origin: Every consumed message triggers a SET NX EX to claim the dedup key,
   plus DELETE on failure. At {dedup_stats.mean:,.0f} ops/sec this adds a Redis RTT
   to every processed message on the consumer side.
   Impact: Consumer throughput bounded by 2x Redis RTTs (XREADGROUP + SET NX).
   Optimization:
     - Make idempotency opt-in per message type rather than always-on.
     - Batch dedup checks using a Redis pipeline or Lua script.
     - Use a local in-memory bloom filter as a fast approximate pre-check.

3. [Metadata enrichment adds {overhead_pct:.1f}% overhead to XADD]
   Origin: _xadd writes 5 fields including datetime.now(UTC).isoformat() per msg.
   Impact: {overhead_pct:.1f}% throughput reduction on the producer side.
   Optimization:
     - Make metadata fields opt-in (e.g. include_metadata=False by default).
     - Cache consumer_name bytes (currently re-encoded as string each call).
     - Use Unix timestamp (float) instead of ISO 8601 string.

### Summary
| Bottleneck | Impact | Priority |
|------------|--------|----------|
| Query polling (4 RTTs) | {slowdown}x lower throughput vs Command | HIGH |
| Idempotency SET NX | {dedup_overhead:.2f}ms extra per consumed msg | MEDIUM |
| Metadata enrichment | {overhead_pct:.1f}% throughput reduction | LOW |
""")

    # ------------------------------------------------------------------
    # Save report
    # ------------------------------------------------------------------
    report_dir = Path(__file__).parent.parent / ".omc" / "scientist" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"{timestamp}_async_redis_bus_perf.md"

    def _tput_row(label: str, s: BenchStats, n: int, fmt: str = ",.0f") -> str:
        return (
            f"| {label} | {s.mean:{fmt}} | {s.stddev:{fmt}}"
            f" | {s.p95:{fmt}} | {s.p99:{fmt}}"
            f" | {s.cv:.1f}% | {s.ci95:{fmt}} | n={n} |"
        )

    def _lat_row(label: str, p50: BenchStats, p95: BenchStats, p99: BenchStats) -> str:
        return (
            f"| {label} | {p50.mean:.3f} | {p50.stddev:.3f}"
            f" | {p95.mean:.3f} | {p95.stddev:.3f}"
            f" | {p99.mean:.3f} | {p99.stddev:.3f} |"
        )

    def _meta_row(label: str, s: BenchStats) -> str:
        return (
            f"| {label} | {s.mean:,.0f} | {s.stddev:,.0f}"
            f" | {s.p95:,.0f} | {s.p99:,.0f} | {s.cv:.1f}% |"
        )

    with open(report_path, "w") as f:
        f.write("# AsyncRedisMessageBus Performance Profile\n\n")
        f.write(f"**Generated**: {datetime.now(UTC).isoformat()}\n")
        f.write(f"**Redis**: {REDIS_URL}\n")
        f.write(f"**Python**: {sys.version.split()[0]}\n")
        f.write(f"**Rounds per benchmark**: {ROUNDS}\n\n")

        f.write(f"## Throughput — {ROUNDS} rounds (msg/s)\n\n")
        f.write("| Operation | mean | ±stddev | p95 | p99 | CV% | CI95± | notes |\n")
        f.write("|-----------|------|---------|-----|-----|-----|-------|-------|\n")
        f.write(_tput_row("Command execute()", cmd_stats, N_COMMAND) + "\n")
        f.write(_tput_row("Event publish() 1 sub", evt1_stats, N_EVENT) + "\n")
        f.write(_tput_row("Event publish() 3 sub", evt3_stats, N_EVENT) + "\n")
        f.write(_tput_row("Event publish() 5 sub", evt5_stats, N_EVENT) + "\n")
        f.write(_tput_row("Task dispatch()", task_stats, N_TASK) + "\n")
        f.write(_tput_row("Query send() (RTT)", query_stats, N_QUERY, ",.1f") + "\n")

        f.write(f"\n## Latency — mean across {ROUNDS} rounds (ms)\n\n")
        f.write("| Operation | p50 mean | p50 ±σ | p95 mean | p95 ±σ | p99 mean | p99 ±σ |\n")
        f.write("|-----------|----------|--------|----------|--------|----------|--------|\n")
        f.write(_lat_row("Command execute()", cmd_p50_stats, cmd_p95_stats, cmd_p99_stats) + "\n")
        f.write(_lat_row("Event publish()", evt_p50_stats, evt_p95_stats, evt_p99_stats) + "\n")
        f.write(_lat_row("Query send() (RTT)", qry_p50_stats, qry_p95_stats, qry_p99_stats) + "\n")

        f.write(f"\n## Consumer Throughput — {ROUNDS} rounds\n\n")
        f.write("| Configuration | mean | ±stddev | p95 | p99 | CV% | speedup |\n")
        f.write("|---------------|------|---------|-----|-----|-----|---------|\n")
        f.write(
            f"| concurrency=1 (sequential) | {con1_stats.mean:,.0f}"
            f" | {con1_stats.stddev:,.0f} | {con1_stats.p95:,.0f}"
            f" | {con1_stats.p99:,.0f} | {con1_stats.cv:.1f}% | 1.0x |\n"
        )
        f.write(
            f"| concurrency=5 (parallel) | {con5_stats.mean:,.0f}"
            f" | {con5_stats.stddev:,.0f} | {con5_stats.p95:,.0f}"
            f" | {con5_stats.p99:,.0f} | {con5_stats.cv:.1f}% | {speedup:.1f}x |\n"
        )

        f.write(f"\n## Metadata / Idempotency Overhead — {ROUNDS} rounds\n\n")
        f.write("| Operation | mean msg/s | ±stddev | p95 | p99 | CV% |\n")
        f.write("|-----------|-----------|---------|-----|-----|-----|\n")
        f.write(_meta_row("Raw XADD (1 field)", raw_stats) + "\n")
        f.write(_meta_row("_xadd with metadata (5 fields)", full_stats) + "\n")
        f.write(_meta_row("SET NX (idempotency check)", dedup_stats) + "\n")

        f.write("\n## Bottleneck Analysis\n\n")
        f.write(f"### 1. Query latency ({q_p50}ms p50 mean)\n")
        f.write(
            "- **Origin**: 4 serial Redis RTTs"
            " (XADD → XREADGROUP → XADD reply → XREAD poll + DELETE)\n"
        )
        f.write(f"- **Impact**: {slowdown}x lower throughput vs Command\n")
        f.write("- **Optimization**: Pub/Sub for reply; reduce _block_ms; pipeline\n\n")
        f.write(f"### 2. Idempotency SET NX (+{dedup_overhead:.2f}ms per msg)\n")
        f.write("- **Origin**: Every consumed message triggers SET NX EX\n")
        f.write("- **Impact**: Throughput bounded by 2x Redis RTTs before handler\n")
        f.write("- **Optimization**: Make opt-in; batch dedup; bloom filter pre-check\n\n")
        f.write(f"### 3. Metadata enrichment ({overhead_pct:.1f}% vs raw XADD)\n")
        f.write("- **Origin**: 5 fields including isoformat() per message\n")
        f.write(f"- **Impact**: {overhead_pct:.1f}% producer throughput reduction\n")
        f.write("- **Optimization**: Make opt-in; Unix float timestamp; cache bytes\n")

    print(f"\nReport saved to: {report_path}")


if __name__ == "__main__":
    asyncio.run(main())
