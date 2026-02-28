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
import json
import statistics
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import redis.asyncio as aioredis

# ---------------------------------------------------------------------------
# Add src to path so we can import message_bus without installing
# ---------------------------------------------------------------------------
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from message_bus.async_redis_bus import AsyncJsonSerializer, AsyncRedisMessageBus, TypeRegistry
from message_bus.ports import Command, Event, Query, Task

REDIS_URL = "redis://localhost:6379/0"
N_COMMAND  = 1000
N_EVENT    = 1000
N_TASK     = 1000
N_QUERY    = 100
N_LATENCY  = 100


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
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


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
        print("| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)) + " |")
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

    results.append({
        "type": "Simple (BenchCommand)",
        "dumps_us": dumps_simple_us,
        "loads_us": loads_simple_us,
        "raw_size_bytes": len(raw),
    })

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

    results.append({
        "type": "Complex (Decimal+datetime+UUID)",
        "dumps_us": dumps_complex_us,
        "loads_us": loads_complex_us,
        "raw_size_bytes": len(raw_c),
    })

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

    consumer.register_command(BenchCommand, handler, concurrency=concurrency if concurrency > 1 else None)  # type: ignore[arg-type]

    t0 = time.perf_counter()
    async with consumer:
        try:
            await asyncio.wait_for(done_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass

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

        results.append({
            "block_ms": block_ms,
            "p50_ms": percentile(latencies, 50),
            "p95_ms": percentile(latencies, 95),
        })

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
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print("=" * 70)
    print("  AsyncRedisMessageBus Performance Profile")
    print(f"  Redis: {REDIS_URL}")
    print(f"  Python: {sys.version.split()[0]}")
    print("=" * 70)

    # ------------------------------------------------------------------
    print("\n[1/6] Throughput benchmarks...")
    cmd_tput = await bench_command_throughput()
    print(f"  Command execute():   {cmd_tput:,.0f} msg/sec")

    evt1_tput = await bench_event_throughput(1)
    print(f"  Event publish() 1 sub: {evt1_tput:,.0f} msg/sec")
    evt3_tput = await bench_event_throughput(3)
    print(f"  Event publish() 3 sub: {evt3_tput:,.0f} msg/sec")
    evt5_tput = await bench_event_throughput(5)
    print(f"  Event publish() 5 sub: {evt5_tput:,.0f} msg/sec")

    task_tput = await bench_task_throughput()
    print(f"  Task dispatch():     {task_tput:,.0f} msg/sec")
    query_tput = await bench_query_throughput()
    print(f"  Query send() (RTT):  {query_tput:,.1f} msg/sec")

    print_table(
        "Throughput",
        ["Operation", "msg/sec", "notes"],
        [
            ["Command execute()", f"{cmd_tput:,.0f}", f"n={N_COMMAND}, producer only"],
            ["Event publish() 1 subscriber", f"{evt1_tput:,.0f}", f"n={N_EVENT}"],
            ["Event publish() 3 subscribers", f"{evt3_tput:,.0f}", f"n={N_EVENT}"],
            ["Event publish() 5 subscribers", f"{evt5_tput:,.0f}", f"n={N_EVENT}"],
            ["Task dispatch()", f"{task_tput:,.0f}", f"n={N_TASK}, producer only"],
            ["Query send() round-trip", f"{query_tput:,.1f}", f"n={N_QUERY}, full RTT"],
        ],
    )

    # ------------------------------------------------------------------
    print("\n[2/6] Latency benchmarks...")
    cmd_p50, cmd_p95, cmd_p99 = await bench_command_latency()
    print(f"  Command: p50={cmd_p50:.3f}ms p95={cmd_p95:.3f}ms p99={cmd_p99:.3f}ms")
    evt_p50, evt_p95, evt_p99 = await bench_event_latency()
    print(f"  Event:   p50={evt_p50:.3f}ms p95={evt_p95:.3f}ms p99={evt_p99:.3f}ms")
    qry_p50, qry_p95, qry_p99 = await bench_query_latency()
    print(f"  Query:   p50={qry_p50:.3f}ms p95={qry_p95:.3f}ms p99={qry_p99:.3f}ms")

    print_table(
        "Latency (ms)",
        ["Operation", "p50", "p95", "p99"],
        [
            ["Command execute()", f"{cmd_p50:.3f}", f"{cmd_p95:.3f}", f"{cmd_p99:.3f}"],
            ["Event publish()", f"{evt_p50:.3f}", f"{evt_p95:.3f}", f"{evt_p99:.3f}"],
            ["Query send() (RTT)", f"{qry_p50:.3f}", f"{qry_p95:.3f}", f"{qry_p99:.3f}"],
        ],
    )

    # ------------------------------------------------------------------
    print("\n[3/6] Serialization overhead...")
    ser_results = bench_serialization()

    # Calculate % of dispatch
    ratio = await bench_serialization_vs_dispatch()
    print(f"  Serialization: {ratio['ser_ms']*1000:.2f}μs/msg")
    print(f"  Full dispatch: {ratio['dispatch_ms']*1000:.2f}μs/msg")
    print(f"  Ser% of total: {ratio['ser_pct']:.1f}%")

    print_table(
        "Serialization Overhead",
        ["Message Type", "dumps (μs)", "loads (μs)", "size (bytes)", "% of dispatch"],
        [
            [
                r["type"],
                f"{r['dumps_us']:.2f}",
                f"{r['loads_us']:.2f}",
                str(r["raw_size_bytes"]),
                f"{ratio['ser_pct']:.1f}%" if i == 0 else "n/a",
            ]
            for i, r in enumerate(ser_results)
        ],
    )

    # ------------------------------------------------------------------
    print("\n[4/6] Consumer processing efficiency...")
    con1_tput = await bench_consumer_throughput(1)
    print(f"  concurrency=1: {con1_tput:,.0f} msg/sec consumed")
    con5_tput = await bench_consumer_throughput(5)
    print(f"  concurrency=5: {con5_tput:,.0f} msg/sec consumed")

    speedup = con5_tput / con1_tput if con1_tput > 0 else 0
    print_table(
        "Consumer Throughput (consumed msg/sec)",
        ["Configuration", "msg/sec consumed", "speedup vs seq"],
        [
            ["concurrency=1 (sequential)", f"{con1_tput:,.0f}", "1.0x"],
            [f"concurrency=5 (parallel)", f"{con5_tput:,.0f}", f"{speedup:.1f}x"],
        ],
    )

    # ------------------------------------------------------------------
    print("\n[5/6] block_ms vs Query latency...")
    block_results = await bench_block_ms_latency()
    print_table(
        "block_ms vs Query Latency",
        ["block_ms", "p50 (ms)", "p95 (ms)"],
        [
            [str(r["block_ms"]), f"{r['p50_ms']:.2f}", f"{r['p95_ms']:.2f}"]
            for r in block_results
        ],
    )

    # ------------------------------------------------------------------
    print("\n[6/6] Metadata / idempotency overhead...")
    raw_rate, full_rate, dedup_rate = await bench_metadata_overhead()
    overhead_pct = (raw_rate - full_rate) / raw_rate * 100 if raw_rate > 0 else 0
    print(f"  Raw XADD:             {raw_rate:,.0f} msg/sec")
    print(f"  Full _xadd (meta):    {full_rate:,.0f} msg/sec")
    print(f"  Metadata overhead:    {overhead_pct:.1f}%")
    print(f"  SET NX (dedup):       {dedup_rate:,.0f} ops/sec")

    print_table(
        "Metadata / Idempotency Overhead",
        ["Operation", "msg/sec", "overhead vs raw XADD"],
        [
            ["Raw XADD (1 field)", f"{raw_rate:,.0f}", "—"],
            ["_xadd with metadata (5 fields)", f"{full_rate:,.0f}", f"+{overhead_pct:.1f}% slower"],
            ["SET NX (idempotency check)", f"{dedup_rate:,.0f}", "per-message extra RTT"],
        ],
    )

    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  BOTTLENECK ANALYSIS")
    print("=" * 70)

    query_latency_ms = qry_p50
    cmd_latency_ms = cmd_p50
    ser_pct = ratio["ser_pct"]
    dedup_overhead = (1 / dedup_rate * 1000) if dedup_rate > 0 else 0

    print(f"""
### Bottleneck Analysis

1. [Query round-trip latency — {query_latency_ms:.1f}ms p50, {qry_p99:.1f}ms p99]
   Origin: Query requires 4 serial Redis round-trips:
     (a) XADD to query stream
     (b) XREADGROUP by consumer (block={100}ms poll loop)
     (c) XADD reply to per-request reply stream
     (d) XREAD poll by caller + DELETE reply stream
   Impact: {query_tput:.1f} msg/sec vs {cmd_tput:,.0f} msg/sec for Commands (~{cmd_tput/query_tput:.0f}x slower).
   Optimization:
     - Use Redis Pub/Sub for reply notification instead of polling XREAD.
     - Reduce _block_ms (currently {100}ms) — even 10ms cuts median latency significantly.
     - Pipeline XADD + XREAD in a single connection using asyncio.gather where possible.

2. [Per-message idempotency SET NX — adds {dedup_overhead:.2f}ms overhead per command]
   Origin: Every consumed message triggers a SET NX EX to claim the dedup key,
   plus DELETE on failure. At {dedup_rate:,.0f} ops/sec this adds a full Redis RTT
   to every processed message on the consumer side.
   Impact: Consumer throughput is bounded by 2x Redis RTTs (XREADGROUP + SET NX)
   before any handler logic runs.
   Optimization:
     - Make idempotency opt-in per message type rather than always-on.
     - Batch dedup checks using a Redis pipeline or Lua script.
     - Use a local in-memory bloom filter (approximate dedup) as a fast pre-check.

3. [Metadata enrichment adds {overhead_pct:.1f}% overhead to XADD — {5} fields vs 1 field]
   Origin: _xadd writes 5 fields (data, message_id, producer_id, idempotency_key,
   enqueue_timestamp) including a datetime.now(UTC).isoformat() call per message.
   Impact: {overhead_pct:.1f}% throughput reduction on the producer side; timestamp
   generation and string formatting are pure CPU cost paid on every dispatch.
   Optimization:
     - Make metadata fields opt-in (e.g. include_metadata=False by default).
     - Cache consumer_name bytes (currently re-encoded as string each call).
     - Use Unix timestamp (float) instead of ISO 8601 string to reduce encoding cost.

### Summary
| Bottleneck | Impact | Priority |
|------------|--------|----------|
| Query polling (4 RTTs per send) | {cmd_tput/query_tput:.0f}x lower throughput vs Command | HIGH |
| Idempotency SET NX per message | {dedup_overhead:.2f}ms extra per consumed msg | MEDIUM |
| Metadata enrichment overhead | {overhead_pct:.1f}% throughput reduction | LOW |
""")

    # ------------------------------------------------------------------
    # Save report
    # ------------------------------------------------------------------
    import os
    report_dir = Path(__file__).parent.parent / ".omc" / "scientist" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"{timestamp}_async_redis_bus_perf.md"

    with open(report_path, "w") as f:
        f.write(f"""# AsyncRedisMessageBus Performance Profile

**Generated**: {datetime.now(UTC).isoformat()}
**Redis**: {REDIS_URL}
**Python**: {sys.version.split()[0]}

## Throughput

| Operation | msg/sec | notes |
|-----------|---------|-------|
| Command execute() | {cmd_tput:,.0f} | n={N_COMMAND}, producer only |
| Event publish() 1 subscriber | {evt1_tput:,.0f} | n={N_EVENT} |
| Event publish() 3 subscribers | {evt3_tput:,.0f} | n={N_EVENT} |
| Event publish() 5 subscribers | {evt5_tput:,.0f} | n={N_EVENT} |
| Task dispatch() | {task_tput:,.0f} | n={N_TASK}, producer only |
| Query send() round-trip | {query_tput:.1f} | n={N_QUERY}, full RTT |

## Latency (ms)

| Operation | p50 | p95 | p99 |
|-----------|-----|-----|-----|
| Command execute() | {cmd_p50:.3f} | {cmd_p95:.3f} | {cmd_p99:.3f} |
| Event publish() | {evt_p50:.3f} | {evt_p95:.3f} | {evt_p99:.3f} |
| Query send() (RTT) | {qry_p50:.3f} | {qry_p95:.3f} | {qry_p99:.3f} |

## Serialization Overhead

| Message Type | dumps (μs) | loads (μs) | size (bytes) | % of dispatch |
|--------------|-----------|-----------|--------------|---------------|
| Simple (BenchCommand) | {ser_results[0]['dumps_us']:.2f} | {ser_results[0]['loads_us']:.2f} | {ser_results[0]['raw_size_bytes']} | {ratio['ser_pct']:.1f}% |
| Complex (Decimal+datetime+UUID) | {ser_results[1]['dumps_us']:.2f} | {ser_results[1]['loads_us']:.2f} | {ser_results[1]['raw_size_bytes']} | n/a |

## Consumer Throughput

| Configuration | msg/sec consumed | speedup |
|---------------|-----------------|---------|
| concurrency=1 (sequential) | {con1_tput:,.0f} | 1.0x |
| concurrency=5 (parallel) | {con5_tput:,.0f} | {speedup:.1f}x |

## block_ms vs Query Latency

| block_ms | p50 (ms) | p95 (ms) |
|----------|---------|---------|
""")
        for r in block_results:
            f.write(f"| {r['block_ms']} | {r['p50_ms']:.2f} | {r['p95_ms']:.2f} |\n")

        f.write(f"""
## Metadata / Idempotency Overhead

| Operation | msg/sec | overhead |
|-----------|---------|---------|
| Raw XADD (1 field) | {raw_rate:,.0f} | — |
| _xadd with metadata (5 fields) | {full_rate:,.0f} | +{overhead_pct:.1f}% slower |
| SET NX (idempotency check) | {dedup_rate:,.0f} | per-message extra RTT |

## Bottleneck Analysis

### 1. Query round-trip latency ({query_latency_ms:.1f}ms p50)
- **Origin**: 4 serial Redis round-trips per Query (XADD → XREADGROUP → XADD reply → XREAD poll + DELETE)
- **Impact**: {cmd_tput/query_tput:.0f}x lower throughput compared to Command
- **Optimization**: Use Pub/Sub for reply notification; reduce _block_ms; pipeline where possible

### 2. Per-message idempotency SET NX (+{dedup_overhead:.2f}ms per consumed message)
- **Origin**: Every consumed message triggers SET NX EX to claim the dedup key
- **Impact**: Consumer throughput bounded by 2x Redis RTTs before handler logic
- **Optimization**: Make idempotency opt-in; batch dedup checks; use local bloom filter pre-check

### 3. Metadata enrichment overhead ({overhead_pct:.1f}% vs raw XADD)
- **Origin**: _xadd writes 5 fields including datetime.now(UTC).isoformat() per message
- **Impact**: {overhead_pct:.1f}% producer throughput reduction; CPU cost from timestamp formatting
- **Optimization**: Make metadata opt-in; use Unix float timestamp; cache consumer_name bytes
""")

    print(f"\nReport saved to: {report_path}")


if __name__ == "__main__":
    asyncio.run(main())
