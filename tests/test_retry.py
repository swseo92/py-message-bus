"""Tests for RetryMiddleware and AsyncRetryMiddleware."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import Mock, patch

import pytest

from message_bus import (
    AsyncLocalMessageBus,
    AsyncMiddlewareBus,
    AsyncRetryMiddleware,
    Command,
    LocalMessageBus,
    MiddlewareBus,
    Query,
    RetryMiddleware,
    Task,
)
from message_bus.latency import LatencyMiddleware, LatencyStats

# ---------------------------------------------------------------------------
# Test messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GetValueQuery(Query[int]):
    """Test query."""

    key: str


@dataclass(frozen=True)
class UpdateCommand(Command):
    """Test command."""

    value: int


@dataclass(frozen=True)
class ProcessTask(Task):
    """Test task."""

    data: str


# ---------------------------------------------------------------------------
# RetryMiddleware (sync) tests
# ---------------------------------------------------------------------------


class TestRetryMiddlewareValidation:
    """Test parameter validation."""

    def test_max_attempts_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            RetryMiddleware(max_attempts=0)

    def test_backoff_base_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="backoff_base must be > 0"):
            RetryMiddleware(backoff_base=0.0)

        with pytest.raises(ValueError, match="backoff_base must be > 0"):
            RetryMiddleware(backoff_base=-1.0)

    def test_retryable_must_not_be_empty(self) -> None:
        with pytest.raises(ValueError, match="retryable tuple must not be empty"):
            RetryMiddleware(retryable=())

    def test_max_backoff_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_backoff must be > 0"):
            RetryMiddleware(max_backoff=0.0)

        with pytest.raises(ValueError, match="max_backoff must be > 0"):
            RetryMiddleware(max_backoff=-1.0)


class TestRetryMiddlewareQuery:
    """Test retry logic for queries (send)."""

    def test_success_on_first_attempt(self) -> None:
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=3)
        wrapped = MiddlewareBus(bus, [retry])

        handler = Mock(return_value=42)
        wrapped.register_query(GetValueQuery, handler)

        result = wrapped.send(GetValueQuery(key="test"))

        assert result == 42
        assert handler.call_count == 1

    def test_retry_once_then_succeed(self) -> None:
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=3, backoff_base=0.01)  # Fast backoff for tests
        wrapped = MiddlewareBus(bus, [retry])

        # Fail once, then succeed
        handler = Mock(side_effect=[ConnectionError("DB offline"), 99])
        wrapped.register_query(GetValueQuery, handler)

        with patch("time.sleep") as mock_sleep:
            result = wrapped.send(GetValueQuery(key="test"))

        assert result == 99
        assert handler.call_count == 2
        # Verify backoff: 0.01^1 = 0.01
        mock_sleep.assert_called_once_with(0.01)

    def test_max_attempts_exceeded_raises_last_exception(self) -> None:
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=3, backoff_base=0.01)
        wrapped = MiddlewareBus(bus, [retry])

        handler = Mock(side_effect=TimeoutError("Network timeout"))
        wrapped.register_query(GetValueQuery, handler)

        with (
            patch("time.sleep") as mock_sleep,
            pytest.raises(TimeoutError, match="Network timeout"),
        ):
            wrapped.send(GetValueQuery(key="test"))

        assert handler.call_count == 3
        # Backoff delays: 0.01^1, 0.01^2 (no delay after last attempt)
        assert mock_sleep.call_count == 2

    def test_non_retryable_exception_propagates_immediately(self) -> None:
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=3, retryable=(ConnectionError,))
        wrapped = MiddlewareBus(bus, [retry])

        handler = Mock(side_effect=ValueError("Invalid key"))
        wrapped.register_query(GetValueQuery, handler)

        with pytest.raises(ValueError, match="Invalid key"):
            wrapped.send(GetValueQuery(key="test"))

        # No retry for non-retryable exception
        assert handler.call_count == 1


class TestRetryMiddlewareCommand:
    """Test retry logic for commands (execute)."""

    def test_retry_command_with_backoff(self) -> None:
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=2, backoff_base=2.0)
        wrapped = MiddlewareBus(bus, [retry])

        # Fail once, then succeed
        handler = Mock(side_effect=[ConnectionError("Transient"), None])
        wrapped.register_command(UpdateCommand, handler)

        with patch("time.sleep") as mock_sleep:
            wrapped.execute(UpdateCommand(value=100))

        assert handler.call_count == 2
        # Backoff: 2^1 = 2.0
        mock_sleep.assert_called_once_with(2.0)


class TestRetryMiddlewareMaxBackoff:
    """Test max_backoff capping behavior."""

    def test_backoff_capped_at_max_backoff(self) -> None:
        """Verify delay is capped when exponential backoff exceeds max_backoff."""
        bus = LocalMessageBus()
        # backoff_base=10, max_backoff=50: delays 10, 100->50, 1000->50
        retry = RetryMiddleware(max_attempts=4, backoff_base=10.0, max_backoff=50.0)
        wrapped = MiddlewareBus(bus, [retry])

        # Fail 3 times, succeed on 4th
        handler = Mock(
            side_effect=[
                TimeoutError("Retry 1"),
                TimeoutError("Retry 2"),
                TimeoutError("Retry 3"),
                42,
            ]
        )
        wrapped.register_query(GetValueQuery, handler)

        with patch("time.sleep") as mock_sleep:
            result = wrapped.send(GetValueQuery(key="test"))

        assert result == 42
        assert handler.call_count == 4
        # Delays: 10^1=10 (not capped), 10^2=100 (capped to 50), 10^3=1000 (capped to 50)
        assert mock_sleep.call_count == 3
        assert mock_sleep.call_args_list[0][0][0] == 10.0
        assert mock_sleep.call_args_list[1][0][0] == 50.0  # Capped
        assert mock_sleep.call_args_list[2][0][0] == 50.0  # Capped

    def test_custom_max_backoff(self) -> None:
        """Verify custom max_backoff value works correctly."""
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=3, backoff_base=5.0, max_backoff=15.0)
        wrapped = MiddlewareBus(bus, [retry])

        handler = Mock(side_effect=[ConnectionError("Fail 1"), ConnectionError("Fail 2"), 99])
        wrapped.register_query(GetValueQuery, handler)

        with patch("time.sleep") as mock_sleep:
            result = wrapped.send(GetValueQuery(key="test"))

        assert result == 99
        # Delays: 5^1=5 (not capped), 5^2=25 (capped to 15)
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 5.0
        assert mock_sleep.call_args_list[1][0][0] == 15.0  # Capped

    def test_max_backoff_equals_computed_delay(self) -> None:
        """Boundary: max_backoff exactly equals computed delay."""
        bus = LocalMessageBus()
        # 2^1 = 2.0, max_backoff = 2.0 -> delay should be exactly 2.0
        retry = RetryMiddleware(max_attempts=2, backoff_base=2.0, max_backoff=2.0)
        wrapped = MiddlewareBus(bus, [retry])
        handler = Mock(side_effect=[TimeoutError("fail"), 42])
        wrapped.register_query(GetValueQuery, handler)
        with patch("time.sleep") as mock_sleep:
            result = wrapped.send(GetValueQuery(key="test"))
        assert result == 42
        mock_sleep.assert_called_once_with(2.0)

    def test_very_small_max_backoff(self) -> None:
        """Very small max_backoff caps all delays."""
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=3, backoff_base=2.0, max_backoff=0.001)
        wrapped = MiddlewareBus(bus, [retry])
        handler = Mock(side_effect=[TimeoutError("1"), TimeoutError("2"), 42])
        wrapped.register_query(GetValueQuery, handler)
        with patch("time.sleep") as mock_sleep:
            result = wrapped.send(GetValueQuery(key="test"))
        assert result == 42
        assert mock_sleep.call_count == 2
        for call in mock_sleep.call_args_list:
            assert call[0][0] == 0.001


class TestRetryMiddlewareTask:
    """Test retry logic for tasks (dispatch)."""

    def test_retry_task_exponential_backoff(self) -> None:
        bus = LocalMessageBus()
        retry = RetryMiddleware(max_attempts=4, backoff_base=3.0)
        wrapped = MiddlewareBus(bus, [retry])

        # Fail 3 times, succeed on 4th
        handler = Mock(
            side_effect=[
                TimeoutError("Retry 1"),
                TimeoutError("Retry 2"),
                TimeoutError("Retry 3"),
                None,
            ]
        )
        wrapped.register_task(ProcessTask, handler)

        with patch("time.sleep") as mock_sleep:
            wrapped.dispatch(ProcessTask(data="test"))

        assert handler.call_count == 4
        # Backoff: 3^1=3, 3^2=9, 3^3=27
        assert mock_sleep.call_count == 3
        assert mock_sleep.call_args_list[0][0][0] == 3.0
        assert mock_sleep.call_args_list[1][0][0] == 9.0
        assert mock_sleep.call_args_list[2][0][0] == 27.0


class TestRetryMiddlewareChaining:
    """Test retry middleware composition with other middleware."""

    def test_retry_with_latency_middleware(self) -> None:
        """Verify RetryMiddleware composes with LatencyMiddleware."""
        bus = LocalMessageBus()
        stats = LatencyStats()
        latency = LatencyMiddleware(stats, threshold_ms=1000.0)
        retry = RetryMiddleware(max_attempts=2, backoff_base=0.01)

        # Chain: retry outer, latency inner
        wrapped = MiddlewareBus(bus, [retry, latency])

        handler = Mock(side_effect=[ConnectionError("Fail"), 42])
        wrapped.register_query(GetValueQuery, handler)

        with patch("time.sleep"):
            result = wrapped.send(GetValueQuery(key="test"))

        assert result == 42
        assert handler.call_count == 2

        # LatencyStats should record both attempts
        percentiles = stats.percentiles("tests.test_retry.GetValueQuery", "query")
        assert percentiles is not None
        assert percentiles.count == 2  # Both attempts measured


# ---------------------------------------------------------------------------
# AsyncRetryMiddleware tests
# ---------------------------------------------------------------------------


class TestAsyncRetryMiddlewareValidation:
    """Test async parameter validation."""

    def test_max_attempts_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            AsyncRetryMiddleware(max_attempts=0)

    def test_backoff_base_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="backoff_base must be > 0"):
            AsyncRetryMiddleware(backoff_base=0.0)

    def test_retryable_must_not_be_empty(self) -> None:
        with pytest.raises(ValueError, match="retryable tuple must not be empty"):
            AsyncRetryMiddleware(retryable=())

    def test_max_backoff_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_backoff must be > 0"):
            AsyncRetryMiddleware(max_backoff=0.0)

        with pytest.raises(ValueError, match="max_backoff must be > 0"):
            AsyncRetryMiddleware(max_backoff=-1.0)


class TestAsyncRetryMiddlewareQuery:
    """Test async retry logic for queries."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self) -> None:
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        async def handler(q: GetValueQuery) -> int:
            return 42

        wrapped.register_query(GetValueQuery, handler)
        result = await wrapped.send(GetValueQuery(key="test"))
        assert result == 42

    @pytest.mark.asyncio
    async def test_retry_once_then_succeed(self) -> None:
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3, backoff_base=0.01)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        attempt = 0

        async def handler(q: GetValueQuery) -> int:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise ConnectionError("DB offline")
            return 99

        wrapped.register_query(GetValueQuery, handler)

        with patch("asyncio.sleep") as mock_sleep:
            result = await wrapped.send(GetValueQuery(key="test"))

        assert result == 99
        assert attempt == 2
        mock_sleep.assert_called_once_with(0.01)

    @pytest.mark.asyncio
    async def test_max_attempts_exceeded_raises_last_exception(self) -> None:
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3, backoff_base=0.01)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        async def handler(q: GetValueQuery) -> int:
            raise TimeoutError("Network timeout")

        wrapped.register_query(GetValueQuery, handler)

        with (
            patch("asyncio.sleep") as mock_sleep,
            pytest.raises(TimeoutError, match="Network timeout"),
        ):
            await wrapped.send(GetValueQuery(key="test"))

        # 2 retries after initial attempt (no sleep after last)
        assert mock_sleep.call_count == 2


class TestAsyncRetryMiddlewareCommand:
    """Test async retry logic for commands."""

    @pytest.mark.asyncio
    async def test_retry_command_exponential_backoff(self) -> None:
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3, backoff_base=2.0)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        attempt = 0

        async def handler(c: UpdateCommand) -> None:
            nonlocal attempt
            attempt += 1
            if attempt < 3:
                raise ConnectionError(f"Attempt {attempt}")

        wrapped.register_command(UpdateCommand, handler)

        with patch("asyncio.sleep") as mock_sleep:
            await wrapped.execute(UpdateCommand(value=100))

        assert attempt == 3
        # Backoff: 2^1=2, 2^2=4
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 2.0
        assert mock_sleep.call_args_list[1][0][0] == 4.0


class TestAsyncRetryMiddlewareMaxBackoff:
    """Test async max_backoff capping behavior."""

    @pytest.mark.asyncio
    async def test_backoff_capped_at_max_backoff(self) -> None:
        """Verify async delay is capped when exponential backoff exceeds max_backoff."""
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=4, backoff_base=10.0, max_backoff=50.0)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        attempt = 0

        async def handler(q: GetValueQuery) -> int:
            nonlocal attempt
            attempt += 1
            if attempt < 4:
                raise TimeoutError(f"Retry {attempt}")
            return 42

        wrapped.register_query(GetValueQuery, handler)

        with patch("asyncio.sleep") as mock_sleep:
            result = await wrapped.send(GetValueQuery(key="test"))

        assert result == 42
        assert attempt == 4
        # Delays: 10^1=10, 10^2=100 (capped to 50), 10^3=1000 (capped to 50)
        assert mock_sleep.call_count == 3
        assert mock_sleep.call_args_list[0][0][0] == 10.0
        assert mock_sleep.call_args_list[1][0][0] == 50.0  # Capped
        assert mock_sleep.call_args_list[2][0][0] == 50.0  # Capped

    @pytest.mark.asyncio
    async def test_custom_max_backoff(self) -> None:
        """Verify custom max_backoff value works in async context."""
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3, backoff_base=5.0, max_backoff=15.0)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        attempt = 0

        async def handler(c: UpdateCommand) -> None:
            nonlocal attempt
            attempt += 1
            if attempt < 3:
                raise ConnectionError(f"Attempt {attempt}")

        wrapped.register_command(UpdateCommand, handler)

        with patch("asyncio.sleep") as mock_sleep:
            await wrapped.execute(UpdateCommand(value=100))

        assert attempt == 3
        # Delays: 5^1=5, 5^2=25 (capped to 15)
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 5.0
        assert mock_sleep.call_args_list[1][0][0] == 15.0  # Capped

    @pytest.mark.asyncio
    async def test_max_backoff_equals_computed_delay(self) -> None:
        """Boundary: max_backoff exactly equals computed delay (async)."""
        bus = AsyncLocalMessageBus()
        # 2^1 = 2.0, max_backoff = 2.0 -> delay should be exactly 2.0
        retry = AsyncRetryMiddleware(max_attempts=2, backoff_base=2.0, max_backoff=2.0)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        attempt = 0

        async def handler(q: GetValueQuery) -> int:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise TimeoutError("fail")
            return 42

        wrapped.register_query(GetValueQuery, handler)
        with patch("asyncio.sleep") as mock_sleep:
            result = await wrapped.send(GetValueQuery(key="test"))
        assert result == 42
        mock_sleep.assert_called_once_with(2.0)

    @pytest.mark.asyncio
    async def test_very_small_max_backoff(self) -> None:
        """Very small max_backoff caps all delays (async)."""
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3, backoff_base=2.0, max_backoff=0.001)
        wrapped = AsyncMiddlewareBus(bus, [retry])

        attempt = 0

        async def handler(q: GetValueQuery) -> int:
            nonlocal attempt
            attempt += 1
            if attempt < 3:
                raise TimeoutError(f"fail {attempt}")
            return 42

        wrapped.register_query(GetValueQuery, handler)
        with patch("asyncio.sleep") as mock_sleep:
            result = await wrapped.send(GetValueQuery(key="test"))
        assert result == 42
        assert mock_sleep.call_count == 2
        for call in mock_sleep.call_args_list:
            assert call[0][0] == 0.001


class TestAsyncRetryMiddlewareTask:
    """Test async retry logic for tasks."""

    @pytest.mark.asyncio
    async def test_non_retryable_exception_propagates_immediately(self) -> None:
        bus = AsyncLocalMessageBus()
        retry = AsyncRetryMiddleware(max_attempts=3, retryable=(ConnectionError,))
        wrapped = AsyncMiddlewareBus(bus, [retry])

        async def handler(t: ProcessTask) -> None:
            raise ValueError("Invalid data")

        wrapped.register_task(ProcessTask, handler)

        with pytest.raises(ValueError, match="Invalid data"):
            await wrapped.dispatch(ProcessTask(data="test"))
