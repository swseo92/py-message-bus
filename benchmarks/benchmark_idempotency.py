"""Benchmark: SWS2-57 — per-type idempotency smart defaults + Lua atomic ops.

Measures:
  1. Consumer throughput with idempotency OFF vs ON
  2. RTT savings from XADD+SET NX pipelining (publish path)
  3. Lua ack_and_dedup vs separate SET NX + XACK (consume path)

Usage:
    uv run python benchmarks/benchmark_idempotency.py

Requirements:
    - Running Redis at localhost:6379  (or set REDIS_URL env var)
    - pip install redis>=5.0.0
"""

import asyncio
import os
import time
import uuid
from dataclasses import dataclass

from message_bus import AsyncRedisMessageBus
from message_bus.ports import Command, Task

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
CONSUMER_GROUP = f"bench-{uuid.uuid4().hex[:6]}"
N_MESSAGES = 500
WARMUP = 50


@dataclass(frozen=True)
class BenchCommand(Command):
    value: int


@dataclass(frozen=True)
class BenchTask(Task):
    value: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _drain(
    bus: AsyncRedisMessageBus, target: list[int], n: int, timeout: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout
    while len(target) < n:
        if time.monotonic() > deadline:
            print(f"  [warn] timed out waiting for {n} messages, got {len(target)}")
            break
        await asyncio.sleep(0.01)


async def _bench_consumer_throughput(idempotency_on: bool) -> float:
    """Return messages/sec for command consumer with given idempotency setting."""
    label = "ON " if idempotency_on else "OFF"
    received: list[int] = []

    bus = AsyncRedisMessageBus(
        redis_url=REDIS_URL,
        consumer_group=CONSUMER_GROUP + f"-cmd-{label.strip()}",
        app_name=f"bench-{uuid.uuid4().hex[:4]}",
        command_idempotency=idempotency_on,
        _block_ms=10,
    )

    async def handler(cmd: BenchCommand) -> None:
        received.append(cmd.value)

    bus.register_command(BenchCommand, handler)
    async with bus:
        # Warmup
        for i in range(WARMUP):
            await bus.execute(BenchCommand(value=i))
        await _drain(bus, received, WARMUP)
        received.clear()

        # Timed run
        t0 = time.monotonic()
        for i in range(N_MESSAGES):
            await bus.execute(BenchCommand(value=i))
        await _drain(bus, received, N_MESSAGES)
        elapsed = time.monotonic() - t0

    tps = len(received) / elapsed if elapsed > 0 else 0.0
    n_recv = len(received)
    print(f"  Command idempotency {label}: {tps:,.0f} msg/s  ({n_recv} msgs in {elapsed:.3f}s)")
    return tps


async def _bench_publish_pipeline(with_pipeline: bool) -> float:
    """Return XADD ops/sec with and without SET NX pipelining."""
    label = "pipeline" if with_pipeline else "no-pipeline"
    bus = AsyncRedisMessageBus(
        redis_url=REDIS_URL,
        consumer_group=CONSUMER_GROUP + f"-pub-{label}",
        app_name=f"bench-pub-{uuid.uuid4().hex[:4]}",
        command_idempotency=with_pipeline,
        _block_ms=0,
    )
    await bus.start()
    try:
        # Warmup
        for i in range(WARMUP):
            await bus.execute(BenchCommand(value=i))

        t0 = time.monotonic()
        for i in range(N_MESSAGES):
            await bus.execute(BenchCommand(value=i))
        elapsed = time.monotonic() - t0
    finally:
        await bus.close()

    tps = N_MESSAGES / elapsed if elapsed > 0 else 0.0
    print(f"  Publish {label:12s}: {tps:,.0f} XADD/s  ({N_MESSAGES} msgs in {elapsed:.3f}s)")
    return tps


async def _bench_lua_vs_separate(use_lua: bool) -> float:
    """Return dedup checks/sec: Lua script vs separate SET NX + XACK."""
    import redis.asyncio as aioredis

    r = aioredis.from_url(REDIS_URL, decode_responses=False)
    stream_key = f"bench:dedup:{uuid.uuid4().hex[:6]}"
    group = "bench-grp"

    # Seed stream with messages
    for _ in range(N_MESSAGES + WARMUP):
        await r.xadd(stream_key, {"data": "x"})
    try:
        await r.xgroup_create(stream_key, group, id="0", mkstream=False)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    msgs = await r.xreadgroup(group, "bench-consumer", {stream_key: ">"}, count=N_MESSAGES + WARMUP)
    all_ids = [mid for _, batch in msgs for mid, _ in batch]

    from message_bus.async_redis_bus import _LUA_ACK_AND_DEDUP

    ttl = 86400

    # Warmup
    for mid in all_ids[:WARMUP]:
        dk = f"bench:dk:{uuid.uuid4().hex}"
        if use_lua:
            await r.eval(_LUA_ACK_AND_DEDUP, 3, dk, stream_key, group, mid, ttl)
        else:
            await r.set(dk, "1", nx=True, ex=ttl)
            await r.xack(stream_key, group, mid)

    # Timed
    t0 = time.monotonic()
    for mid in all_ids[WARMUP:]:
        dk = f"bench:dk:{uuid.uuid4().hex}"
        if use_lua:
            await r.eval(_LUA_ACK_AND_DEDUP, 3, dk, stream_key, group, mid, ttl)
        else:
            await r.set(dk, "1", nx=True, ex=ttl)
            await r.xack(stream_key, group, mid)
    elapsed = time.monotonic() - t0

    await r.aclose()
    tps = N_MESSAGES / elapsed if elapsed > 0 else 0.0
    label = "Lua (1 RTT)" if use_lua else "separate  "
    print(f"  Dedup {label}: {tps:,.0f} ops/s  ({N_MESSAGES} ops in {elapsed:.3f}s)")
    return tps


async def main() -> None:
    print(f"\n{'=' * 60}")
    print(f"SWS2-57 Benchmark  (N={N_MESSAGES}, warmup={WARMUP})")
    print(f"Redis: {REDIS_URL}")
    print(f"{'=' * 60}\n")

    print("[1] Consumer throughput: idempotency OFF vs ON")
    tps_off = await _bench_consumer_throughput(idempotency_on=False)
    tps_on = await _bench_consumer_throughput(idempotency_on=True)
    if tps_off > 0:
        delta = (tps_off - tps_on) / tps_off * 100
        print(f"  → Overhead of idempotency ON: {delta:+.1f}%\n")

    print("[2] Publish path: XADD+SET NX pipeline vs plain XADD")
    tps_plain = await _bench_publish_pipeline(with_pipeline=False)
    tps_pipeline = await _bench_publish_pipeline(with_pipeline=True)
    if tps_plain > 0:
        delta = (tps_pipeline - tps_plain) / tps_plain * 100
        print(f"  → Pipeline overhead vs plain XADD: {delta:+.1f}%\n")

    print("[3] Consume dedup: Lua (1 RTT) vs separate SET NX + XACK (2 RTT)")
    tps_separate = await _bench_lua_vs_separate(use_lua=False)
    tps_lua = await _bench_lua_vs_separate(use_lua=True)
    if tps_separate > 0:
        delta = (tps_lua - tps_separate) / tps_separate * 100
        print(f"  → Lua speedup over separate calls: {delta:+.1f}%\n")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
