"""Tests for benchmark harness statistical reliability (SWS2-60).

Validates the correctness of statistical helper functions and benchmark
constants without requiring a live Redis connection.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import benchmark modules via path insertion (they are scripts, not packages)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent / "benchmarks"))

import bench_async_redis as bar  # noqa: E402
import bench_metadata_mode as bmm  # noqa: E402

# ---------------------------------------------------------------------------
# percentile()
# ---------------------------------------------------------------------------


class TestPercentile:
    def test_p50_uniform_distribution(self) -> None:
        data = [float(x) for x in range(1, 101)]  # 1.0 … 100.0
        result = bar.percentile(data, 50)
        assert result == pytest.approx(50.5, abs=1.0)

    def test_p95_uniform_distribution(self) -> None:
        data = [float(x) for x in range(1, 101)]
        result = bar.percentile(data, 95)
        assert result == pytest.approx(95.5, abs=1.0)

    def test_p99_uniform_distribution(self) -> None:
        data = [float(x) for x in range(1, 101)]
        result = bar.percentile(data, 99)
        assert result == pytest.approx(99.0, abs=1.5)

    def test_p100_returns_max(self) -> None:
        data = [1.0, 5.0, 3.0, 2.0, 4.0]
        assert bar.percentile(data, 100) == 5.0

    def test_p0_returns_min(self) -> None:
        data = [3.0, 1.0, 2.0]
        assert bar.percentile(data, 0) == 1.0

    def test_empty_returns_zero(self) -> None:
        assert bar.percentile([], 50) == 0.0

    def test_single_element(self) -> None:
        assert bar.percentile([42.0], 99) == 42.0

    def test_two_elements_median(self) -> None:
        result = bar.percentile([10.0, 20.0], 50)
        assert result == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# compute_stats()
# ---------------------------------------------------------------------------


class TestComputeStats:
    def test_mean_is_correct(self) -> None:
        stats = bar.compute_stats([10.0, 20.0, 30.0])
        assert stats.mean == pytest.approx(20.0)

    def test_stddev_is_correct(self) -> None:
        # stdev([10, 20, 30]) = 10 (sample stddev)
        stats = bar.compute_stats([10.0, 20.0, 30.0])
        assert stats.stddev == pytest.approx(10.0)

    def test_p95_within_range(self) -> None:
        data = [float(x) for x in range(1, 101)]
        stats = bar.compute_stats(data)
        assert stats.p95 >= 94.0

    def test_p99_within_range(self) -> None:
        data = [float(x) for x in range(1, 101)]
        stats = bar.compute_stats(data)
        assert stats.p99 >= 98.0

    def test_cv_formula(self) -> None:
        stats = bar.compute_stats([100.0, 110.0, 90.0])
        expected_cv = stats.stddev / stats.mean * 100
        assert stats.cv == pytest.approx(expected_cv)

    def test_ci95_formula(self) -> None:
        samples = [100.0, 110.0, 90.0, 105.0, 95.0]
        stats = bar.compute_stats(samples)
        import statistics as _stats

        expected_stddev = _stats.stdev(samples)
        expected_ci95 = 1.96 * expected_stddev / math.sqrt(len(samples))
        assert stats.ci95 == pytest.approx(expected_ci95)

    def test_single_sample_zeroed(self) -> None:
        stats = bar.compute_stats([42.0])
        assert stats.mean == 42.0
        assert stats.stddev == 0.0
        assert stats.cv == 0.0
        assert stats.ci95 == 0.0

    def test_empty_samples_zeroed(self) -> None:
        stats = bar.compute_stats([])
        assert stats.mean == 0.0
        assert stats.stddev == 0.0
        assert stats.p95 == 0.0
        assert stats.p99 == 0.0

    def test_all_equal_samples_zero_variance(self) -> None:
        stats = bar.compute_stats([500.0] * 10)
        assert stats.mean == pytest.approx(500.0)
        assert stats.stddev == pytest.approx(0.0, abs=1e-9)
        assert stats.cv == pytest.approx(0.0, abs=1e-9)

    def test_p99_le_max(self) -> None:
        samples = [float(x) for x in range(1, 21)]
        stats = bar.compute_stats(samples)
        assert stats.p99 <= max(samples)

    def test_p95_le_p99(self) -> None:
        samples = [float(x) for x in range(1, 21)]
        stats = bar.compute_stats(samples)
        assert stats.p95 <= stats.p99


# ---------------------------------------------------------------------------
# BenchStats fields
# ---------------------------------------------------------------------------


class TestBenchStatsFields:
    """BenchStats must expose all required statistical metrics (SWS2-60)."""

    def test_has_mean(self) -> None:
        stats = bar.BenchStats(mean=1.0, stddev=0.1, p95=1.1, p99=1.2, cv=10.0, ci95=0.05)
        assert hasattr(stats, "mean")

    def test_has_stddev(self) -> None:
        stats = bar.BenchStats(mean=1.0, stddev=0.1, p95=1.1, p99=1.2, cv=10.0, ci95=0.05)
        assert hasattr(stats, "stddev")

    def test_has_p95(self) -> None:
        stats = bar.BenchStats(mean=1.0, stddev=0.1, p95=1.1, p99=1.2, cv=10.0, ci95=0.05)
        assert hasattr(stats, "p95")

    def test_has_p99(self) -> None:
        stats = bar.BenchStats(mean=1.0, stddev=0.1, p95=1.1, p99=1.2, cv=10.0, ci95=0.05)
        assert hasattr(stats, "p99")

    def test_has_cv(self) -> None:
        stats = bar.BenchStats(mean=1.0, stddev=0.1, p95=1.1, p99=1.2, cv=10.0, ci95=0.05)
        assert hasattr(stats, "cv")

    def test_has_ci95(self) -> None:
        stats = bar.BenchStats(mean=1.0, stddev=0.1, p95=1.1, p99=1.2, cv=10.0, ci95=0.05)
        assert hasattr(stats, "ci95")


# ---------------------------------------------------------------------------
# Benchmark constants — statistical reliability requirements (SWS2-60)
# ---------------------------------------------------------------------------


class TestBenchmarkConstants:
    def test_bench_async_redis_rounds_ge_10(self) -> None:
        assert bar.ROUNDS >= 10, (
            f"bench_async_redis.ROUNDS={bar.ROUNDS} must be >= 10 "
            "for statistical reliability (SWS2-60)"
        )

    def test_bench_metadata_mode_rounds_ge_10(self) -> None:
        assert bmm.ROUNDS >= 10, (
            f"bench_metadata_mode.ROUNDS={bmm.ROUNDS} must be >= 10 "
            "for statistical reliability (SWS2-60)"
        )

    def test_bench_async_redis_has_ci_z(self) -> None:
        assert hasattr(bar, "CI_Z"), "bench_async_redis must define CI_Z for confidence intervals"
        assert pytest.approx(1.96) == bar.CI_Z

    def test_bench_metadata_mode_has_ci_z(self) -> None:
        assert hasattr(bmm, "CI_Z"), "bench_metadata_mode must define CI_Z for confidence intervals"
        assert pytest.approx(1.96) == bmm.CI_Z

    def test_bench_async_redis_n_latency_positive(self) -> None:
        assert bar.N_LATENCY > 0

    def test_bench_async_redis_n_command_positive(self) -> None:
        assert bar.N_COMMAND > 0

    def test_bench_metadata_mode_iterations_positive(self) -> None:
        assert bmm.ITERATIONS > 0

    def test_bench_metadata_mode_warmup_positive(self) -> None:
        assert bmm.WARMUP > 0
