"""Benchmark: metadata_mode overhead validation (SWS2-58).

Compares producer throughput across three profiles:
  - legacy:    original datetime.now(UTC).isoformat() (pre-SWS2-58)
  - standard:  lightweight fields — time.time() float, cached source_id
  - none:      dedup fields only (message_id + idempotency_key), no enrichment

Goals (SWS2-58):
  1. standard overhead vs none ≤ 5%  (enrichment cost is negligible)
  2. standard throughput ≥ 12,000 msg/s

Note: shared-server noise makes round-to-round variance high.
      Results are averaged over multiple rounds. If the 5% goal is not
      consistently met, consider setting ``metadata_mode`` default to
      ``"none"`` and making ``"standard"`` opt-in.

Usage:
    uv run python benchmarks/bench_metadata_mode.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

sys.path.insert(0, "src")

import fakeredis.aioredis  # noqa: E402

from message_bus import AsyncJsonSerializer, AsyncRedisMessageBus, TypeRegistry  # noqa: E402
from message_bus.ports import Command  # noqa: E402

# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderCommand(Command):
    order_id: str
    amount: Decimal


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

ITERATIONS = 5_000
WARMUP = 500
ROUNDS = 5


async def _measure(bus: AsyncRedisMessageBus, iterations: int) -> float:
    """Return msgs/sec for *iterations* _xadd calls."""
    stream_key = bus._stream_key("command", "OrderCommand")
    cmd = OrderCommand(order_id="bench-1", amount=Decimal("99.99"))

    for _ in range(WARMUP):
        await bus._xadd(stream_key, cmd)

    start = time.perf_counter()
    for _ in range(iterations):
        await bus._xadd(stream_key, cmd)
    elapsed = time.perf_counter() - start
    return iterations / elapsed


async def _measure_legacy(bus: AsyncRedisMessageBus, iterations: int) -> float:
    """Simulate legacy behavior: datetime.now(UTC).isoformat() per message."""
    import uuid as _uuid

    stream_key = bus._stream_key("command", "OrderCommand")
    cmd = OrderCommand(order_id="bench-1", amount=Decimal("99.99"))
    data = bus._serializer.dumps(cmd)

    for _ in range(WARMUP):
        msg_id = _uuid.uuid4().hex
        await bus._redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": msg_id,
                "producer_id": bus._consumer_name,
                "idempotency_key": msg_id,
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
            maxlen=bus._max_stream_length,
            approximate=True,
        )

    start = time.perf_counter()
    for _ in range(iterations):
        msg_id = _uuid.uuid4().hex
        await bus._redis.xadd(
            stream_key,
            {
                "data": data,
                "message_id": msg_id,
                "producer_id": bus._consumer_name,
                "idempotency_key": msg_id,
                "enqueue_timestamp": datetime.now(UTC).isoformat(),
            },
            maxlen=bus._max_stream_length,
            approximate=True,
        )
    elapsed = time.perf_counter() - start
    return iterations / elapsed


def _make_bus(metadata_mode: str) -> AsyncRedisMessageBus:
    registry = TypeRegistry()
    registry.register(OrderCommand)
    serializer = AsyncJsonSerializer(registry)
    fake_redis = fakeredis.aioredis.FakeRedis()
    return AsyncRedisMessageBus(
        redis_url="redis://localhost",
        consumer_group="bench-group",
        app_name="bench",
        serializer=serializer,
        consumer_name="bench-producer",
        metadata_mode=metadata_mode,  # type: ignore[arg-type]
        _block_ms=0,
        _redis_client=fake_redis,
    )


async def run_benchmark() -> None:
    print(f"\n{'=' * 65}")
    print("Metadata Mode Overhead Benchmark (SWS2-58)")
    print(f"Iterations: {ITERATIONS:,}  |  Warmup: {WARMUP:,}  |  Rounds: {ROUNDS}")
    print("=" * 65)

    bus_standard = _make_bus("standard")
    bus_none = _make_bus("none")
    # Legacy bus uses same instance — we call _measure_legacy directly
    bus_legacy = _make_bus("standard")

    try:
        legacy_results: list[float] = []
        standard_results: list[float] = []
        none_results: list[float] = []

        for i in range(ROUNDS):
            lg = await _measure_legacy(bus_legacy, ITERATIONS)
            s = await _measure(bus_standard, ITERATIONS)
            n = await _measure(bus_none, ITERATIONS)
            legacy_results.append(lg)
            standard_results.append(s)
            none_results.append(n)
            print(
                f"Round {i + 1}: legacy={lg:>8,.0f}  standard={s:>8,.0f}"
                f"  none={n:>8,.0f}  msg/s"
            )

        avg_legacy = sum(legacy_results) / ROUNDS
        avg_standard = sum(standard_results) / ROUNDS
        avg_none = sum(none_results) / ROUNDS

        overhead_vs_none = (avg_none - avg_standard) / avg_none * 100
        improvement_vs_legacy = (avg_standard - avg_legacy) / avg_legacy * 100

        print(f"\n{'─' * 65}")
        print(f"{'Mode':<12} {'Avg msg/s':>14}  {'vs none':>10}  {'vs legacy':>10}")
        print(f"{'─' * 65}")
        print(f"{'legacy':<12} {avg_legacy:>14,.0f}  {'n/a':>10}  {'baseline':>10}")
        print(
            f"{'standard':<12} {avg_standard:>14,.0f}  "
            f"{overhead_vs_none:>9.2f}%  {improvement_vs_legacy:>+9.1f}%"
        )
        print(f"{'none':<12} {avg_none:>14,.0f}  {'0.00%':>10}  {'n/a':>10}")
        print(f"{'─' * 65}")

        TARGET_MSGS = 12_000
        TARGET_OVERHEAD = 5.0

        passed = True
        if avg_standard >= TARGET_MSGS:
            print(f"✓ Throughput : {avg_standard:,.0f} >= {TARGET_MSGS:,} msg/s")
        else:
            print(f"✗ Throughput : {avg_standard:,.0f} < {TARGET_MSGS:,} msg/s  [MISS]")
            passed = False

        if overhead_vs_none <= TARGET_OVERHEAD:
            print(f"✓ Overhead   : {overhead_vs_none:.2f}% <= {TARGET_OVERHEAD:.1f}% (vs none)")
        else:
            print(
                f"✗ Overhead   : {overhead_vs_none:.2f}% > {TARGET_OVERHEAD:.1f}%  [MISS]"
                f"\n  → Consider setting default to 'none' per SWS2-58 재논의 rule"
            )
            passed = False

        if improvement_vs_legacy > 0:
            print(
                f"✓ Improvement: standard is {improvement_vs_legacy:+.1f}% faster than legacy"
            )

        print(f"\n{'PASS' if passed else 'TARGETS NOT MET — see 재논의 note above'}")
        sys.exit(0 if passed else 1)

    finally:
        await bus_standard.close()
        await bus_none.close()
        await bus_legacy.close()


if __name__ == "__main__":
    asyncio.run(run_benchmark())
