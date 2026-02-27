"""Persistent dead letter store backed by Redis Streams.

Provides :class:`RedisDeadLetterStore`, a :class:`~message_bus.QueryableDeadLetterStore`
implementation that survives process restarts. Failed messages are written to a
Redis Stream so that operators can query, inspect, and delete them.

Requirements
------------
* ``redis >= 5.0.0`` (sync client, ``redis.Redis``)

Usage
-----
::

    from message_bus.redis_dead_letter import RedisDeadLetterStore

    store = RedisDeadLetterStore(
        redis_url="redis://localhost:6379/0",
        stream_key="myapp:dlq",
    )
    bus = AsyncRedisMessageBus(
        ...,
        dead_letter_store=store,
    )
"""

# mypy: disable-error-code="import-not-found, no-untyped-call, attr-defined"

from __future__ import annotations

import dataclasses
import json
import logging
from datetime import UTC, datetime
from typing import Any

from message_bus.dead_letter import DeadLetterEntry, QueryableDeadLetterStore
from message_bus.ports import Command, Event, Task

logger = logging.getLogger(__name__)

try:
    import redis as _redis_module
except ImportError:
    _redis_module = None

redis = _redis_module  # module-level name for monkeypatching in tests


class RedisDeadLetterStore(QueryableDeadLetterStore):
    """Redis Stream–backed persistent dead letter store.

    All ``append`` calls write a JSON entry to a Redis Stream (``XADD``).
    The stream is capped at *max_len* entries (approximate trimming).
    Entries are identified by the Redis-assigned stream entry ID
    (``{milliseconds}-{sequence}``), which can be used with
    :meth:`get` and :meth:`delete`.

    Parameters
    ----------
    redis_url:
        Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
        Ignored when *_redis_client* is provided.
    stream_key:
        Redis key for the dead-letter Stream.
        Defaults to ``"message_bus:dlq"``.
    max_len:
        Maximum number of entries kept in the stream (approximate).
        Defaults to ``10_000``.
    _redis_client:
        **Testing only.** Inject a pre-built sync Redis client
        (e.g. ``fakeredis.FakeRedis(decode_responses=True)``).
        When provided, *redis_url* is ignored.
    """

    __slots__ = ("_stream_key", "_max_len", "_redis", "_closed")

    def __init__(
        self,
        redis_url: str,
        stream_key: str = "message_bus:dlq",
        max_len: int = 10_000,
        _redis_client: Any | None = None,
    ) -> None:
        if redis is None:
            raise ImportError(
                "redis package is required for RedisDeadLetterStore. "
                "Install with: pip install 'redis>=5.0.0'"
            )
        self._stream_key = stream_key
        self._max_len = max_len
        self._closed = False
        if _redis_client is not None:
            self._redis = _redis_client
        else:
            self._redis = redis.Redis.from_url(redis_url, decode_responses=True)

    # ------------------------------------------------------------------
    # DeadLetterStore ABC
    # ------------------------------------------------------------------

    def append(self, message: Event | Command | Task, error: Exception, handler_name: str) -> None:
        """Append a failed message to the Redis Stream.

        Non-blocking: any Redis error is logged and swallowed so that the
        middleware's error propagation is never interrupted.
        """
        if self._closed:
            logger.warning("RedisDeadLetterStore.append() called after close, record dropped")
            return
        try:
            entry_fields = {
                "message_type": type(message).__name__,
                "message_data": json.dumps(self._serialize_message(message)),
                "error_type": type(error).__name__,
                "error_message": str(error),
                "handler_name": handler_name,
                "failed_at": datetime.now(UTC).isoformat(),
            }
            self._redis.xadd(
                self._stream_key,
                entry_fields,
                maxlen=self._max_len,
                approximate=True,
            )
        except Exception:
            logger.exception(
                "RedisDeadLetterStore.append() failed to write to Redis stream '%s'",
                self._stream_key,
            )

    def close(self) -> None:
        """Mark the store as closed and release the Redis connection."""
        if self._closed:
            return
        self._closed = True
        try:
            self._redis.close()
        except Exception:
            logger.debug("RedisDeadLetterStore.close() encountered an error closing client")

    # ------------------------------------------------------------------
    # QueryableDeadLetterStore ABC
    # ------------------------------------------------------------------

    def list(self, limit: int = 100) -> list[DeadLetterEntry]:
        """Return up to *limit* entries, newest first."""
        raw: list[tuple[str, dict[str, str]]] = self._redis.xrevrange(self._stream_key, count=limit)
        return [self._parse_entry(eid, fields) for eid, fields in raw]

    def get(self, entry_id: str) -> DeadLetterEntry | None:
        """Return the entry with *entry_id*, or ``None`` if not found."""
        raw: list[tuple[str, dict[str, str]]] = self._redis.xrange(
            self._stream_key, min=entry_id, max=entry_id, count=1
        )
        if not raw:
            return None
        eid, fields = raw[0]
        return self._parse_entry(eid, fields)

    def delete(self, entry_id: str) -> None:
        """Delete the entry with *entry_id* from the stream."""
        self._redis.xdel(self._stream_key, entry_id)

    def count(self) -> int:
        """Return the total number of entries in the stream."""
        return int(self._redis.xlen(self._stream_key))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_message(message: Event | Command | Task) -> dict[str, str]:
        """Convert a dataclass message to a JSON-serializable ``{field: str}`` dict."""
        if dataclasses.is_dataclass(message) and not isinstance(message, type):
            return {f.name: str(getattr(message, f.name)) for f in dataclasses.fields(message)}
        return {"__repr__": repr(message)}

    @staticmethod
    def _parse_entry(entry_id: str, fields: dict[str, str]) -> DeadLetterEntry:
        """Build a :class:`~message_bus.DeadLetterEntry` from raw Redis Stream fields."""
        raw_data = fields.get("message_data", "{}")
        try:
            message_data: dict[str, Any] = json.loads(raw_data)
        except json.JSONDecodeError:
            message_data = {"__raw__": raw_data}

        raw_ts = fields.get("failed_at", "")
        try:
            failed_at = datetime.fromisoformat(raw_ts)
        except ValueError:
            failed_at = datetime.now(UTC)

        return DeadLetterEntry(
            id=entry_id,
            message_type=fields.get("message_type", ""),
            message_data=message_data,
            error_type=fields.get("error_type", ""),
            error_message=fields.get("error_message", ""),
            handler_name=fields.get("handler_name", ""),
            failed_at=failed_at,
        )
