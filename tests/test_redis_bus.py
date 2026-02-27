"""Tests for Redis message bus."""

import time
from dataclasses import dataclass

import pytest

# Skip all tests if redis not available
pytest.importorskip("redis")

from message_bus.ports import Command, Event, Query, Task
from message_bus.redis_bus import JsonSerializer, RedisMessageBus, RedisWorker, _fields_for_xadd


@dataclass(frozen=True)
class GetUserQuery(Query[str]):
    user_id: int


@dataclass(frozen=True)
class CreateUserCommand(Command):
    name: str


@dataclass(frozen=True)
class UserCreatedEvent(Event):
    user_id: int
    name: str


@dataclass(frozen=True)
class ProcessDataTask(Task):
    data: str


def _redis_available(redis_url: str = "redis://localhost:6379/1") -> bool:
    """Check if Redis is available."""
    import redis
    from redis.exceptions import ConnectionError

    try:
        r = redis.from_url(redis_url)  # type: ignore
        r.ping()
        r.close()
        return True
    except (ConnectionError, OSError):
        return False


@pytest.fixture
def unique_app_name():
    """Generate unique app name for each test to avoid Redis key conflicts."""
    timestamp = str(time.time()).replace(".", "")
    return f"test_app_{timestamp}"


@pytest.fixture
def redis_url():
    """Redis URL for testing."""
    return "redis://localhost:6379/1"  # Use DB 1 for tests


@pytest.fixture
def bus(redis_url, unique_app_name):
    """Create a RedisMessageBus with unique app name."""
    if not _redis_available(redis_url):
        pytest.skip("Redis not available")
    bus = RedisMessageBus(redis_url=redis_url, app_name=unique_app_name)
    yield bus
    bus.close()


@pytest.fixture
def worker(redis_url, unique_app_name):
    """Create a RedisWorker with unique app name."""
    if not _redis_available(redis_url):
        pytest.skip("Redis not available")
    worker = RedisWorker(redis_url=redis_url, app_name=unique_app_name)
    yield worker
    worker.close()


class TestRedisMessageBusBasic:
    """Basic tests for RedisMessageBus without worker processes."""

    def test_register_query_duplicate_raises(self, bus):
        def handler(query: GetUserQuery) -> str:
            return "user"

        bus.register_query(GetUserQuery, handler)

        with pytest.raises(ValueError, match="already registered"):
            bus.register_query(GetUserQuery, handler)

    def test_register_command_duplicate_raises(self, bus):
        def handler(command: CreateUserCommand) -> None:
            pass

        bus.register_command(CreateUserCommand, handler)

        with pytest.raises(ValueError, match="already registered"):
            bus.register_command(CreateUserCommand, handler)

    def test_register_task_duplicate_raises(self, bus):
        def handler(task: ProcessDataTask) -> None:
            pass

        bus.register_task(ProcessDataTask, handler)

        with pytest.raises(ValueError, match="already registered"):
            bus.register_task(ProcessDataTask, handler)

    def test_json_serializer_roundtrip(self):
        """Test JsonSerializer roundtrip."""
        serializer = JsonSerializer()

        obj = GetUserQuery(user_id=42)
        data = serializer.dumps(obj)
        result = serializer.loads(data)

        # JsonSerializer returns dict representation of dataclass
        assert "user_id" in str(result) or "user_id" in str(data)
        assert "42" in str(result)

    def test_invalid_timeout_raises(self):
        """Test that invalid timeout raises ValueError."""
        with pytest.raises(ValueError, match="query_timeout_sec must be > 0"):
            RedisMessageBus(query_timeout_sec=0)

        with pytest.raises(ValueError, match="query_timeout_sec must be > 0"):
            RedisMessageBus(query_timeout_sec=-1.0)


class TestRedisMessageBusIntegration:
    """Integration tests with Redis."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_redis(self, redis_url):
        """Skip tests if Redis is not available."""
        import redis
        from redis.exceptions import ConnectionError

        try:
            r = redis.from_url(redis_url)
            r.ping()
            r.close()
        except (ConnectionError, OSError):
            pytest.skip("Redis not available at {redis_url}")

    def test_send_query_returns_response(self, bus):
        """Test synchronous query/response pattern."""

        def handler(query: GetUserQuery) -> str:
            return f"User {query.user_id}"

        bus.register_query(GetUserQuery, handler)

        # Small delay for handler registration
        time.sleep(0.2)

        # Send query
        result = bus.send(GetUserQuery(user_id=42))
        assert result == "User 42"

    def test_send_unregistered_query_raises_timeout(self, bus):
        """Test that unregistered query times out."""
        # Use short timeout for faster test
        bus_fast = RedisMessageBus(
            redis_url="redis://localhost:6379/1",
            app_name=f"timeout_test_{time.time()}",
            query_timeout_sec=0.5,
        )

        query = GetUserQuery(user_id=1)

        with pytest.raises(TimeoutError, match="timed out"):
            bus_fast.send(query)

        bus_fast.close()

    def test_command_executed(self, bus):
        """Test command execution."""
        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        bus.register_command(CreateUserCommand, handler)

        # Small delay for handler registration
        time.sleep(0.2)

        # Execute command
        bus.execute(CreateUserCommand(name="Alice"))

        # Wait for processing
        time.sleep(0.3)

        assert "Alice" in processed

    def test_event_published(self, bus):
        """Test event publishing."""
        received = []

        def handler(event: UserCreatedEvent) -> None:
            received.append(event.name)

        bus.subscribe(UserCreatedEvent, handler)

        # Small delay for subscription
        time.sleep(0.2)

        # Publish event
        bus.publish(UserCreatedEvent(user_id=1, name="Bob"))

        # Wait for processing
        time.sleep(0.3)

        assert "Bob" in received

    def test_task_dispatched(self, bus):
        """Test task dispatching."""
        processed = []

        def handler(task: ProcessDataTask) -> None:
            processed.append(task.data)

        bus.register_task(ProcessDataTask, handler)

        # Small delay for handler registration
        time.sleep(0.2)

        # Dispatch task
        bus.dispatch(ProcessDataTask(data="test_data"))

        # Wait for processing
        time.sleep(0.3)

        assert "test_data" in processed

    def test_multiple_event_subscribers(self, bus):
        """Test that multiple subscribers receive events."""
        received_1 = []
        received_2 = []

        def handler1(event: UserCreatedEvent) -> None:
            received_1.append(event.name)

        def handler2(event: UserCreatedEvent) -> None:
            received_2.append(f"{event.name}_2")

        bus.subscribe(UserCreatedEvent, handler1)
        bus.subscribe(UserCreatedEvent, handler2)

        # Small delay for subscriptions
        time.sleep(0.2)

        # Publish event
        bus.publish(UserCreatedEvent(user_id=1, name="Charlie"))

        # Wait for processing
        time.sleep(0.3)

        assert "Charlie" in received_1
        assert "Charlie_2" in received_2


class TestRedisWorkerStandalone:
    """Tests for standalone RedisWorker."""

    def test_worker_processes_command(self, worker):
        """Test that worker processes commands."""
        processed = []

        def handler(command: CreateUserCommand) -> None:
            processed.append(command.name)

        worker.register_command(CreateUserCommand, handler)

        # Start worker in background
        import threading

        worker_thread = threading.Thread(target=worker.run, daemon=True)
        worker_thread.start()

        time.sleep(0.2)

        # Send command via Redis
        from redis import Redis

        redis_client = Redis.from_url("redis://localhost:6379/1", decode_responses=False)
        from message_bus.redis_bus import _serialize_message

        stream_key = f"{worker._app_name}:command:CreateUserCommand"
        fields = _serialize_message(CreateUserCommand(name="David"), worker._serializer)
        redis_client.xadd(stream_key, fields)

        # Wait for processing
        time.sleep(0.5)

        worker.stop()
        worker_thread.join(timeout=1.0)

        assert "David" in processed

    def test_worker_context_manager(self, worker):
        """Test worker as context manager."""
        with worker:
            assert not worker._running

        # After exit, worker should be stopped
        assert not worker._running


class TestRedisResourceCleanup:
    """Test resource cleanup and connection management."""

    def test_bus_close_cleans_resources(self, redis_url, unique_app_name):
        bus = RedisMessageBus(redis_url=redis_url, app_name=unique_app_name)
        bus.close()
        # Should not raise
        assert not bus._running

    def test_worker_close_cleans_resources(self, redis_url, unique_app_name):
        worker = RedisWorker(redis_url=redis_url, app_name=unique_app_name)
        worker.close()
        # Should not raise
        assert not worker._running

    def test_bus_context_manager(self, redis_url, unique_app_name):
        """Test bus as context manager."""
        with RedisMessageBus(redis_url=redis_url, app_name=unique_app_name) as bus:
            assert bus._running

        # After exit, bus should be stopped
        assert not bus._running


class TestRedisStreamKeys:
    """Test Redis stream key generation."""

    def test_stream_key_format(self, redis_url, unique_app_name):
        """Test that stream keys follow expected format."""
        bus = RedisMessageBus(redis_url=redis_url, app_name=unique_app_name)

        # Check stream key format
        query_key = bus._stream_key("query", "TestQuery")
        assert query_key == f"{unique_app_name}:query:TestQuery"

        command_key = bus._stream_key("command", "TestCommand")
        assert command_key == f"{unique_app_name}:command:TestCommand"

        event_key = bus._stream_key("event", "TestEvent")
        assert event_key == f"{unique_app_name}:event:TestEvent"

        task_key = bus._stream_key("task", "TestTask")
        assert task_key == f"{unique_app_name}:task:TestTask"

        bus.close()


class TestFieldsForXadd:
    """Unit tests for _fields_for_xadd() conversion helper (no Redis needed)."""

    def test_bytes_keys_and_values_are_decoded(self) -> None:
        fields = {b"key": b"value"}
        result = _fields_for_xadd(fields)
        assert result == {"key": "value"}

    def test_str_keys_and_values_are_passed_through(self) -> None:
        result = _fields_for_xadd({b"k": b"v"})
        assert result == {"k": "v"}
        # str keys/values also accepted
        result2 = _fields_for_xadd({"k": "v"})  # type: ignore[arg-type]
        assert result2 == {"k": "v"}

    def test_mixed_bytes_and_str(self) -> None:
        fields = {b"bytes_key": b"bytes_val", "str_key": "str_val"}  # type: ignore[dict-item]
        result = _fields_for_xadd(fields)
        assert result == {"bytes_key": "bytes_val", "str_key": "str_val"}

    def test_non_bytes_non_str_key_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Expected bytes or str key"):
            _fields_for_xadd({123: b"value"})  # type: ignore[dict-item]

    def test_non_bytes_non_str_value_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Expected bytes or str value"):
            _fields_for_xadd({b"key": 42})  # type: ignore[dict-item]

    def test_invalid_utf8_bytes_value_raises_value_error(self) -> None:
        invalid_utf8 = b"\xff\xfe"
        with pytest.raises(ValueError, match="Cannot decode value for key"):
            _fields_for_xadd({b"key": invalid_utf8})

    def test_invalid_utf8_bytes_key_raises_value_error(self) -> None:
        invalid_utf8_key = b"\xff\xfe"
        with pytest.raises(ValueError, match="Cannot decode key"):
            _fields_for_xadd({invalid_utf8_key: b"value"})

    def test_empty_dict_returns_empty_dict(self) -> None:
        assert _fields_for_xadd({}) == {}

    def test_all_string_fields_preserved(self) -> None:
        fields = {b"data": b"hello", b"type": b"TestCommand"}
        result = _fields_for_xadd(fields)
        assert result["data"] == "hello"
        assert result["type"] == "TestCommand"


class TestFieldsForXaddCallShape:
    """Tests that xadd is called with stringified fields from _fields_for_xadd."""

    def test_serialize_message_produces_bytes_fields(self) -> None:
        """_serialize_message output fed into _fields_for_xadd yields str fields."""
        from message_bus.redis_bus import JsonSerializer, _serialize_message

        serializer = JsonSerializer()

        @dataclass(frozen=True)
        class _Cmd(Command):
            value: int

        raw = _serialize_message(_Cmd(value=7), serializer)
        result = _fields_for_xadd(raw)
        # All keys and values must be strings
        for k, v in result.items():
            assert isinstance(k, str), f"Key {k!r} is not str"
            assert isinstance(v, str), f"Value {v!r} is not str"

    def test_dispatch_passes_str_fields_to_xadd(self) -> None:
        """dispatch() must pass str fields (via _fields_for_xadd) to Redis xadd."""
        from unittest.mock import MagicMock, patch

        from message_bus.redis_bus import JsonSerializer, RedisMessageBus

        @dataclass(frozen=True)
        class _Task(Task):
            value: int

        mock_client = MagicMock()
        with patch("message_bus.redis_bus.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = mock_client

            bus = RedisMessageBus(
                redis_url="redis://localhost:6379/0",
                app_name="test",
                serializer=JsonSerializer(),
            )
            bus.dispatch(_Task(value=99))

        assert mock_client.xadd.called
        call_args = mock_client.xadd.call_args[0]
        fields_arg = call_args[1]
        for k, v in fields_arg.items():
            assert isinstance(k, str), f"xadd key {k!r} is not str"
            assert isinstance(v, str), f"xadd value {v!r} is not str"
