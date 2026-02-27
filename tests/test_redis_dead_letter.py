"""Unit tests for RedisDeadLetterStore."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pytest

pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402

from message_bus.dead_letter import DeadLetterEntry, QueryableDeadLetterStore
from message_bus.ports import Command, Event, Task
from message_bus.redis_dead_letter import RedisDeadLetterStore

# ---------------------------------------------------------------------------
# Test message types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TestEvent(Event):
    value: int


@dataclass(frozen=True)
class TestCommand(Command):
    value: int


@dataclass(frozen=True)
class TestTask(Task):
    value: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def store(fake_redis: fakeredis.FakeRedis) -> RedisDeadLetterStore:
    return RedisDeadLetterStore(
        redis_url="redis://localhost:6379/0",
        _redis_client=fake_redis,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRedisDeadLetterStore:
    """Tests for RedisDeadLetterStore."""

    def test_is_queryable_dead_letter_store(self, store: RedisDeadLetterStore) -> None:
        """RedisDeadLetterStore must satisfy QueryableDeadLetterStore ABC."""
        assert isinstance(store, QueryableDeadLetterStore)

    # ------------------------------------------------------------------
    # append / count
    # ------------------------------------------------------------------

    def test_append_event_increments_count(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=42), ValueError("fail"), "TestEvent")
        assert store.count() == 1

    def test_append_command_increments_count(self, store: RedisDeadLetterStore) -> None:
        store.append(TestCommand(value=99), RuntimeError("fail"), "TestCommand")
        assert store.count() == 1

    def test_append_task_increments_count(self, store: RedisDeadLetterStore) -> None:
        store.append(TestTask(value=7), OSError("fail"), "TestTask")
        assert store.count() == 1

    def test_append_multiple(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("e1"), "h1")
        store.append(TestEvent(value=2), ValueError("e2"), "h2")
        assert store.count() == 2

    def test_count_empty_store(self, store: RedisDeadLetterStore) -> None:
        assert store.count() == 0

    # ------------------------------------------------------------------
    # list
    # ------------------------------------------------------------------

    def test_list_empty_returns_empty_list(self, store: RedisDeadLetterStore) -> None:
        assert store.list() == []

    def test_list_returns_all_entries(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("e1"), "h1")
        store.append(TestEvent(value=2), ValueError("e2"), "h2")
        store.append(TestEvent(value=3), ValueError("e3"), "h3")

        entries = store.list()
        assert len(entries) == 3

    def test_list_returns_newest_first(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("first"), "handler1")
        store.append(TestEvent(value=2), ValueError("second"), "handler2")
        store.append(TestEvent(value=3), ValueError("third"), "handler3")

        entries = store.list()
        assert entries[0].handler_name == "handler3"
        assert entries[1].handler_name == "handler2"
        assert entries[2].handler_name == "handler1"

    def test_list_respects_limit(self, store: RedisDeadLetterStore) -> None:
        for i in range(5):
            store.append(TestEvent(value=i), ValueError(f"err {i}"), f"h{i}")

        entries = store.list(limit=3)
        assert len(entries) == 3

    # ------------------------------------------------------------------
    # entry field correctness
    # ------------------------------------------------------------------

    def test_entry_message_type(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=42), ValueError("err"), "handler")
        entry = store.list()[0]
        assert entry.message_type == "TestEvent"

    def test_entry_error_type(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), RuntimeError("boom"), "handler")
        entry = store.list()[0]
        assert entry.error_type == "RuntimeError"

    def test_entry_error_message(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("specific error text"), "handler")
        entry = store.list()[0]
        assert entry.error_message == "specific error text"

    def test_entry_handler_name(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("err"), "my_handler")
        entry = store.list()[0]
        assert entry.handler_name == "my_handler"

    def test_entry_failed_at_is_tz_aware_datetime(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("err"), "handler")
        entry = store.list()[0]
        assert isinstance(entry.failed_at, datetime)
        assert entry.failed_at.tzinfo is not None

    def test_entry_message_data_contains_fields(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=42), ValueError("err"), "handler")
        entry = store.list()[0]
        assert "value" in entry.message_data
        assert entry.message_data["value"] == "42"

    def test_entry_is_dead_letter_entry_instance(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("err"), "handler")
        entry = store.list()[0]
        assert isinstance(entry, DeadLetterEntry)

    def test_entry_has_non_empty_id(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("err"), "handler")
        entry = store.list()[0]
        assert entry.id != ""

    # ------------------------------------------------------------------
    # get
    # ------------------------------------------------------------------

    def test_get_returns_entry_by_id(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("first"), "h1")
        store.append(TestEvent(value=2), ValueError("second"), "h2")

        entries = store.list()
        target = entries[0]  # most recent

        fetched = store.get(target.id)
        assert fetched is not None
        assert fetched.id == target.id
        assert fetched.handler_name == target.handler_name

    def test_get_returns_none_for_missing_id(self, store: RedisDeadLetterStore) -> None:
        result = store.get("0-0")
        assert result is None

    def test_get_returns_correct_entry_among_multiple(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("e1"), "h1")
        store.append(TestEvent(value=2), ValueError("e2"), "h2")

        entries = store.list()  # newest first
        oldest_id = entries[-1].id

        fetched = store.get(oldest_id)
        assert fetched is not None
        assert fetched.handler_name == "h1"

    # ------------------------------------------------------------------
    # delete
    # ------------------------------------------------------------------

    def test_delete_removes_entry(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("err"), "h1")
        entries = store.list()
        assert len(entries) == 1

        store.delete(entries[0].id)
        assert store.count() == 0

    def test_delete_only_removes_target_entry(self, store: RedisDeadLetterStore) -> None:
        store.append(TestEvent(value=1), ValueError("e1"), "h1")
        store.append(TestEvent(value=2), ValueError("e2"), "h2")

        entries = store.list()
        store.delete(entries[0].id)  # delete newest

        assert store.count() == 1
        remaining = store.list()
        assert remaining[0].handler_name == "h1"

    def test_delete_nonexistent_id_does_not_raise(self, store: RedisDeadLetterStore) -> None:
        store.delete("0-0")  # should not raise

    # ------------------------------------------------------------------
    # close behaviour
    # ------------------------------------------------------------------

    def test_append_after_close_raises_runtime_error(self, store: RedisDeadLetterStore) -> None:
        """append() after close() must raise RuntimeError."""
        store.append(TestEvent(value=1), ValueError("before close"), "h1")
        store.close()

        with pytest.raises(RuntimeError, match="closed"):
            store.append(TestEvent(value=2), ValueError("after close"), "h2")

    def test_close_is_idempotent(self, store: RedisDeadLetterStore) -> None:
        """close() called multiple times should not raise."""
        store.close()
        store.close()

    # ------------------------------------------------------------------
    # ImportError guard
    # ------------------------------------------------------------------

    def test_requires_redis_package(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """RedisDeadLetterStore.__init__ should raise ImportError when redis is missing."""
        import message_bus.redis_dead_letter as rdl

        monkeypatch.setattr(rdl, "redis", None)
        with pytest.raises(ImportError, match="redis"):
            RedisDeadLetterStore(redis_url="redis://localhost:6379/0")
