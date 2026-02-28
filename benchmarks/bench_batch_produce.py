"""Batch produce API benchmark: execute_many vs execute() in a loop.

Measures producer-side throughput improvement when using
execute_many / publish_many / dispatch_many versus individual calls.

Run with:
    uv run python benchmarks/bench_batch_produce.py

Requires a running Redis server at redis://localhost:6379/0.
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import statistics
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from message_bus.async_redis_bus import AsyncJsonSerializer, AsyncRedisMessageBus, TypeRegistry
from message_bus.ports import Command, Event, Task

REDIS_URL = "redis://localhost:6379/0"
BATCH_SIZES = [1, 10, 50, 100, 500, 1000]
ROUNDS = 10
CI_Z = 1.96


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class BatchCommand(Command):  # type: ignore[misc]
    value: int = 0


@dataclasses.dataclass(frozen=True)
class BatchEvent(Event):  # type: ignore[misc]
    value: int = 0


@dataclasses.dataclass(frozen=True)
class BatchTask(Task):  # type: ignore[misc]
    value: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class BenchStats:
    mean: float
    stddev: float
    p95: float
    p99: float
    cv: float
    ci95: float


def percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def compute_stats(samples: list[float]) -> BenchStats:
    if not samples:
        return BenchStats(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    if len(samples) == 1:
        v = samples[0]
        return BenchStats(v, 0.0, v, v, 0.0, 0.0)
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
            "| "
            + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row))
            + " |"
        )
    print(sep)


async def cleanup_keys(redis_client: Any, app_name: str) -> None:
    pattern = f"{app_name}:*"
    keys = await redis_client.keys(pattern)
    if keys:
        await redis_client.delete(*keys)


async def make_bus(app_name: str) -> AsyncRedisMessageBus:
    registry = TypeRegistry()
    registry.register(BatchCommand, BatchEvent, BatchTask)
    serializer = AsyncJsonSerializer(registry)
    return AsyncRedisMessageBus(
        redis_url=REDIS_URL,
        consumer_group=f"{app_name}-group",
        app_name=app_name,
        serializer=serializer,
    )


# ---------------------------------------------------------------------------
# Individual vs batch throughput per batch size
# ---------------------------------------------------------------------------


async def bench_individual(n: int, app_name: str) -> float:
    """N individual execute() calls → throughput (msg/s)."""
    bus = await make_bus(app_name)
    cmd = BatchCommand(value=42)
    async with bus:
        # warm up
        for _ in range(5):
            await bus.execute(cmd)
        t0 = time.perf_counter()
        for _ in range(n):
            await bus.execute(cmd)
        elapsed = time.perf_counter() - t0
    return n / elapsed


async def bench_batch(n: int, app_name: str) -> float:
    """Single execute_many(N) call → throughput (msg/s)."""
    bus = await make_bus(app_name)
    cmds = [BatchCommand(value=i) for i in range(n)]
    async with bus:
        # warm up with small batch
        await bus.execute_many([BatchCommand(value=0)] * 5)
        t0 = time.perf_counter()
        await bus.execute_many(cmds)
        elapsed = time.perf_counter() - t0
    return n / elapsed


async def bench_individual_event(n: int, app_name: str) -> float:
    """N individual publish() calls → throughput (msg/s)."""
    bus = await make_bus(app_name)
    evt = BatchEvent(value=42)
    async with bus:
        for _ in range(5):
            await bus.publish(evt)
        t0 = time.perf_counter()
        for _ in range(n):
            await bus.publish(evt)
        elapsed = time.perf_counter() - t0
    return n / elapsed


async def bench_batch_event(n: int, app_name: str) -> float:
    """Single publish_many(N) call → throughput (msg/s)."""
    bus = await make_bus(app_name)
    evts = [BatchEvent(value=i) for i in range(n)]
    async with bus:
        await bus.publish_many([BatchEvent(value=0)] * 5)
        t0 = time.perf_counter()
        await bus.publish_many(evts)
        elapsed = time.perf_counter() - t0
    return n / elapsed


async def _bench_rounds(bench_fn: Any, label: str, **kwargs: Any) -> BenchStats:
    samples = []
    for _ in range(ROUNDS):
        app_name = f"bench-{uuid.uuid4().hex[:8]}"
        tput = await bench_fn(**kwargs, app_name=app_name)
        samples.append(tput)
        r = aioredis.from_url(REDIS_URL)
        await cleanup_keys(r, app_name)
        await r.aclose()
    stats = compute_stats(samples)
    print(
        f"  {label}: mean={stats.mean:,.0f} msg/s  ±{stats.ci95:,.0f}  CV={stats.cv:.1f}%"
    )
    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    print("=" * 60)
    print("Batch Produce API Benchmark (SWS2-64)")
    print(f"Rounds per configuration: {ROUNDS}")
    print(f"Batch sizes: {BATCH_SIZES}")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # Command: execute() vs execute_many()
    # -----------------------------------------------------------------------
    print("\n[Command] execute() vs execute_many()")
    cmd_rows: list[list[str]] = []
    for n in BATCH_SIZES:
        print(f"\n  N={n}")
        ind = await _bench_rounds(bench_individual, f"individual N={n}", n=n)
        bat = await _bench_rounds(bench_batch, f"batch N={n}    ", n=n)
        speedup = bat.mean / ind.mean if ind.mean > 0 else 0.0
        cmd_rows.append([
            str(n),
            f"{ind.mean:,.0f}",
            f"±{ind.ci95:,.0f}",
            f"{bat.mean:,.0f}",
            f"±{bat.ci95:,.0f}",
            f"{speedup:.2f}x",
        ])

    print_table(
        "Command throughput: execute() vs execute_many() (msg/s)",
        ["N", "individual mean", "±CI95", "batch mean", "±CI95", "speedup"],
        cmd_rows,
    )

    # -----------------------------------------------------------------------
    # Event: publish() vs publish_many()
    # -----------------------------------------------------------------------
    print("\n[Event] publish() vs publish_many()")
    evt_rows: list[list[str]] = []
    for n in BATCH_SIZES:
        print(f"\n  N={n}")
        ind = await _bench_rounds(bench_individual_event, f"individual N={n}", n=n)
        bat = await _bench_rounds(bench_batch_event, f"batch N={n}    ", n=n)
        speedup = bat.mean / ind.mean if ind.mean > 0 else 0.0
        evt_rows.append([
            str(n),
            f"{ind.mean:,.0f}",
            f"±{ind.ci95:,.0f}",
            f"{bat.mean:,.0f}",
            f"±{bat.ci95:,.0f}",
            f"{speedup:.2f}x",
        ])

    print_table(
        "Event throughput: publish() vs publish_many() (msg/s)",
        ["N", "individual mean", "±CI95", "batch mean", "±CI95", "speedup"],
        evt_rows,
    )


if __name__ == "__main__":
    asyncio.run(main())
