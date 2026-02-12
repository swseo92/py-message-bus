"""Recording decorator/middleware for message bus observability.

Provides RecordingBus and AsyncRecordingBus for recording message dispatches
to JSONL files or in-memory stores without adding external dependencies.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import queue
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

from message_bus.ports import AsyncMessageBus, Command, Event, MessageBus, Query, Task

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _safe_asdict(message: Query[Any] | Command | Event | Task) -> dict[str, Any]:
    """Convert message to dict, falling back to repr on failure.

    This handles cases where dataclass fields contain non-serializable objects.
    """
    try:
        return asdict(message)
    except Exception:
        return {"__repr__": repr(message)}


def _safe_json(obj: Any) -> Any:
    """json.dumps default handler for non-serializable objects.

    Falls back to repr() for objects that can't be JSON-serialized.
    """
    return repr(obj)


def _format_timestamp(timestamp: float) -> str:
    """Format timestamp as ISO-8601 with microseconds.

    Args:
        timestamp: Unix timestamp from time.time()

    Returns:
        Formatted string like "2026-02-12T14:30:45.123456"
    """
    base_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(timestamp))
    microseconds = int((timestamp % 1) * 1e6)
    return f"{base_time}.{microseconds:06d}"


_REDACTED = "[REDACTED]"


def _redact(data: dict[str, Any], fields: frozenset[str]) -> dict[str, Any]:
    """Recursively redact sensitive fields from a dict (handles nested lists)."""
    result: dict[str, Any] = {}
    for k, v in data.items():
        if k in fields:
            result[k] = _REDACTED
        elif isinstance(v, dict):
            result[k] = _redact(v, fields)
        elif isinstance(v, list):
            result[k] = [_redact(item, fields) if isinstance(item, dict) else item for item in v]
        else:
            result[k] = v
    return result


@dataclass(frozen=True, slots=True)
class Record:
    """A single message dispatch record.

    WARNING: `payload` stores the raw message object. Serialization happens
    in JsonLineStore's background thread to avoid hot-path overhead.
    """

    timestamp: float  # time.time() for wall-clock timestamp
    message_type: str  # "query" | "command" | "event" | "task"
    message_class: str  # type(message).__name__
    payload: Query[Any] | Command | Event | Task  # Raw message object (NOT dict)
    duration_ns: int  # perf_counter_ns delta
    result: Any | None  # query return value (None for others)
    error: str | None  # repr(exception) if handler failed


class RecordStore(ABC):
    """Abstract base class for storing message dispatch records."""

    @abstractmethod
    def append(self, record: Record) -> None:
        """Append a record. Must be non-blocking (use queue for I/O stores)."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Flush and close the store. Must be idempotent."""
        ...


class MemoryStore(RecordStore):
    """In-memory store for testing. Stores records in a list."""

    __slots__ = ("records", "_closed")

    def __init__(self) -> None:
        self.records: list[Record] = []
        self._closed: bool = False

    def append(self, record: Record) -> None:
        if self._closed:
            logger.warning("MemoryStore.append() called after close, record dropped")
            return
        self.records.append(record)

    def close(self) -> None:
        self._closed = True


class JsonLineStore(RecordStore):
    """JSONL file store with background writer thread.

    Appends are non-blocking (queue-based). Writer thread batches records
    for efficient I/O. Automatically cleans up old run files.

    Limitations (v1):
    - No payload size limits (large messages may cause memory issues)
    - No gzip compression
    """

    __slots__ = (
        "_file_path",
        "_queue",
        "_writer_thread",
        "_writer_error",
        "_closed",
        "_close_timeout",
        "_redact_fields",
        "_shutdown",
        "_dropped_count",
        "_state_lock",
        "_restart_attempted",
    )

    def __init__(
        self,
        directory: Path,
        max_runs: int = 10,
        max_queue_size: int = 10_000,
        close_timeout: float = 5.0,
        redact_fields: frozenset[str] = frozenset(),
    ) -> None:
        """Create a new JsonLineStore.

        Args:
            directory: Directory to store JSONL files
            max_runs: Maximum number of run files to keep (oldest deleted)
            max_queue_size: Maximum queue size (records dropped when full)
            close_timeout: Timeout in seconds for writer thread join on close()
            redact_fields: Field names to redact from JSONL payloads (recursive)

        Raises:
            ValueError: If max_runs < 1 or max_queue_size < 1
        """
        if max_runs < 1:
            raise ValueError(f"max_runs must be >= 1, got {max_runs}")
        if max_queue_size < 1:
            raise ValueError(f"max_queue_size must be >= 1, got {max_queue_size}")
        if close_timeout <= 0:
            raise ValueError(f"close_timeout must be > 0, got {close_timeout}")

        directory.mkdir(parents=True, exist_ok=True)

        # File naming: run_{YYYYMMDD_HHMMSS}_{pid}.jsonl
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        pid = os.getpid()
        self._file_path = directory / f"run_{timestamp_str}_{pid}.jsonl"

        self._queue: queue.Queue[Record | None] = queue.Queue(maxsize=max_queue_size)
        self._writer_error: Exception | None = None
        self._closed = False
        self._close_timeout = close_timeout
        self._redact_fields = redact_fields
        self._shutdown = threading.Event()
        self._dropped_count = 0
        self._state_lock = threading.Lock()
        self._restart_attempted = False

        # Cleanup old runs before starting
        self._cleanup_old_runs(directory, max_runs)

        # Start background writer
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            daemon=True,
            name=f"JsonLineStore-writer-{pid}",
        )
        self._writer_thread.start()

        # Register atexit for data safety (explicit close still recommended)
        atexit.register(self.close)

    def _cleanup_old_runs(self, directory: Path, max_runs: int) -> None:
        """Delete oldest run files when count exceeds max_runs."""
        run_files = sorted(
            (p for p in directory.glob("run_*.jsonl") if p.is_file() and not p.is_symlink()),
            key=lambda p: p.stat().st_mtime,
        )
        files_to_keep = max_runs - 1  # Reserve slot for new file
        files_to_delete = run_files if files_to_keep <= 0 else run_files[:-files_to_keep]
        for old_file in files_to_delete:
            if old_file.parent.resolve() == directory.resolve():
                old_file.unlink(missing_ok=True)

    def _writer_loop(self, *, append_mode: bool = False) -> None:
        """Background thread: drain queue and write to file in batches."""
        file_mode = "a" if append_mode else "w"
        try:
            with open(self._file_path, file_mode) as f:
                while True:
                    batch: list[Record] = []

                    # Blocking get with timeout to check shutdown event
                    try:
                        item = self._queue.get(timeout=0.1)
                    except queue.Empty:
                        if self._shutdown.is_set():
                            break
                        continue
                    if item is None:  # Sentinel: shutdown signal
                        break
                    batch.append(item)

                    # Non-blocking drain up to 64 items
                    while len(batch) < 64:
                        try:
                            item = self._queue.get_nowait()
                            if item is None:
                                break
                            batch.append(item)
                        except queue.Empty:
                            break

                    # Write batch
                    for record in batch:
                        payload = _safe_asdict(record.payload)
                        if self._redact_fields:
                            payload = _redact(payload, self._redact_fields)
                        record_dict = {
                            "ts": _format_timestamp(record.timestamp),
                            "type": record.message_type,
                            "class": record.message_class,
                            "payload": payload,
                            "duration_ns": record.duration_ns,
                            "result": record.result,
                            "error": record.error,
                        }
                        json_line = json.dumps(record_dict, default=_safe_json)
                        f.write(json_line + "\n")

                    f.flush()

                    if item is None:  # Sentinel found in batch
                        break
        except OSError as exc:
            with self._state_lock:
                self._writer_error = exc
            logger.error("JsonLineStore writer I/O error: %s", exc)
        except Exception as exc:
            with self._state_lock:
                self._writer_error = exc
            logger.exception("JsonLineStore writer unexpected error")

    def _attempt_writer_restart(self) -> None:
        """Attempt to restart the writer thread once after failure."""
        if self._closed:
            logger.warning("Writer restart skipped: store is closed")
            return

        if self._restart_attempted:
            logger.warning("Writer restart already attempted, not retrying")
            return

        self._restart_attempted = True
        logger.info("Attempting writer thread restart after failure...")
        time.sleep(1.0)  # Brief wait before restart

        # Start new writer thread
        pid = os.getpid()
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            kwargs={"append_mode": True},
            daemon=True,
            name=f"JsonLineStore-writer-{pid}-restart",
        )
        try:
            self._writer_thread.start()
            # Only clear error AFTER successful start
            with self._state_lock:
                self._writer_error = None
            logger.info("Writer thread restarted successfully")
        except Exception as exc:
            with self._state_lock:
                self._writer_error = exc
            logger.error("Writer thread restart failed: %s", exc)

    def append(self, record: Record) -> None:
        """Enqueue a record for background writing (non-blocking)."""
        if self._closed:
            logger.warning("JsonLineStore.append() called after close, record dropped")
            self._increment_dropped()
            return

        # Check for writer error and attempt restart if needed
        with self._state_lock:
            writer_error = self._writer_error
            restart_attempted = self._restart_attempted

        if writer_error is not None:
            # Attempt restart for OSError (transient I/O errors)
            if isinstance(writer_error, OSError) and not restart_attempted:
                self._attempt_writer_restart()
                # After restart attempt, re-check error state
                with self._state_lock:
                    writer_error = self._writer_error

            # If still errored, drop the record
            if writer_error is not None:
                logger.warning(
                    "JsonLineStore writer thread failed (%s), dropping record: %s",
                    writer_error,
                    record.message_class,
                )
                self._increment_dropped()
                return

        try:
            self._queue.put_nowait(record)
        except queue.Full:
            logger.warning(
                "JsonLineStore queue full (%d), dropping record: %s",
                self._queue.maxsize,
                record.message_class,
            )
            self._increment_dropped()

    def _increment_dropped(self) -> None:
        """Thread-safe increment of dropped record counter."""
        with self._state_lock:
            self._dropped_count += 1

    @property
    def dropped_count(self) -> int:
        """Number of records dropped due to writer failure or queue full."""
        with self._state_lock:
            return self._dropped_count

    def close(self) -> None:
        """Flush remaining records and stop writer thread. Idempotent."""
        if self._closed:
            return  # Already closed

        self._closed = True

        # Signal writer thread (blocking put if queue is full)
        try:
            self._queue.put(None, timeout=self._close_timeout)
        except queue.Full:
            logger.warning("JsonLineStore queue full at close, forcing writer shutdown")
        self._shutdown.set()

        # Join with timeout
        self._writer_thread.join(timeout=self._close_timeout)

        # If thread still alive, records may be lost
        if self._writer_thread.is_alive():
            logger.warning("JsonLineStore writer thread did not stop in time, records may be lost")

        # Check for writer errors
        if self._writer_error is not None:
            logger.warning(
                "JsonLineStore had writer errors during operation: %s", self._writer_error
            )

        # Log dropped records
        dropped = self.dropped_count
        if dropped > 0:
            logger.warning("JsonLineStore closed with %d dropped records", dropped)


class _RecordingMixin:
    """Shared logic for sync and async recording buses."""

    __slots__ = ()  # CRITICAL: empty slots to not break concrete class slots

    # These are declared in concrete classes' __slots__
    _inner: Any
    _store: RecordStore
    _record_mode: Literal["all", "errors"]
    _closed: bool

    @staticmethod
    def _validate_record_mode(record_mode: str) -> None:
        if record_mode not in ("all", "errors"):
            raise ValueError(f"Invalid record_mode: {record_mode!r}. Must be 'all' or 'errors'.")

    def _record_dispatch(
        self,
        message: Query[Any] | Command | Event | Task,
        message_type: str,
        duration_ns: int,
        timestamp: float,
        result: Any = None,
        error_str: str | None = None,
    ) -> None:
        """Record a dispatch if mode allows."""
        if self._record_mode == "all" or error_str is not None:
            try:
                self._store.append(
                    Record(
                        timestamp=timestamp,
                        message_type=message_type,
                        message_class=type(message).__name__,
                        payload=message,
                        duration_ns=duration_ns,
                        result=result if error_str is None else None,
                        error=error_str,
                    )
                )
            except Exception:
                logger.exception(
                    "Failed to record %s dispatch for %s",
                    message_type,
                    type(message).__name__,
                )

    @property
    def store(self) -> RecordStore:
        """Access the underlying store (for testing)."""
        return self._store

    def close(self) -> None:
        """Close the store and flush records. Idempotent."""
        if self._closed:
            return
        self._closed = True  # type: ignore[misc]
        self._store.close()

    def registered_queries(self) -> list[str]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_queries"):
            result: list[str] = self._inner.registered_queries()
            return result
        raise AttributeError("Inner bus does not support introspection")

    def registered_commands(self) -> list[str]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_commands"):
            result: list[str] = self._inner.registered_commands()
            return result
        raise AttributeError("Inner bus does not support introspection")

    def registered_events(self) -> dict[str, int]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_events"):
            result: dict[str, int] = self._inner.registered_events()
            return result
        raise AttributeError("Inner bus does not support introspection")

    def registered_tasks(self) -> list[str]:
        """Delegate to inner bus if it supports introspection."""
        if hasattr(self._inner, "registered_tasks"):
            result: list[str] = self._inner.registered_tasks()
            return result
        raise AttributeError("Inner bus does not support introspection")


class RecordingBus(_RecordingMixin, MessageBus):
    """Decorator/middleware bus that records message dispatches.

    Wraps any MessageBus implementation and records all dispatches to a store.
    Supports two modes:
    - "all": Record all dispatches (success and errors)
    - "errors": Record only failed dispatches

    IMPORTANT: This class adds `close()` and context manager support not in
    the MessageBus ABC. Callers needing these must type against RecordingBus
    directly, not MessageBus.

    Event recording limitation: publish() records one Record with aggregate
    error (ExceptionGroup or single exception), not per-handler details.
    This is a deliberate simplification to maintain the decorator pattern.
    """

    __slots__ = ("_inner", "_store", "_record_mode", "_closed")

    def __init__(
        self,
        inner: MessageBus,
        store: RecordStore,
        record_mode: Literal["all", "errors"] = "all",
    ) -> None:
        """Create a RecordingBus.

        Args:
            inner: The inner MessageBus to wrap
            store: Store for recording dispatches
            record_mode: "all" or "errors" only

        Raises:
            ValueError: If record_mode is invalid
        """
        self._validate_record_mode(record_mode)

        self._inner = inner
        self._store = store
        self._record_mode = record_mode
        self._closed = False

    def send(self, query: Query[T]) -> T:
        """Dispatch a query and record the result."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        result: Any = None
        try:
            result = self._inner.send(query)
            return cast(T, result)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(query, "query", duration, wall_time, result, error_str)

    def execute(self, command: Command) -> None:
        """Dispatch a command and record execution."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            self._inner.execute(command)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(command, "command", duration, wall_time, None, error_str)

    def publish(self, event: Event) -> None:
        """Dispatch an event and record publication."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            self._inner.publish(event)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(event, "event", duration, wall_time, None, error_str)

    def dispatch(self, task: Task) -> None:
        """Dispatch a task and record execution."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            self._inner.dispatch(task)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(task, "task", duration, wall_time, None, error_str)

    # Registration methods: pure delegation (no recording)

    def register_query(self, query_type: type[Query[T]], handler: Callable[[Query[T]], T]) -> None:
        """Register a query handler (delegates to inner bus)."""
        self._inner.register_query(query_type, handler)

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], None]
    ) -> None:
        """Register a command handler (delegates to inner bus)."""
        self._inner.register_command(command_type, handler)

    def subscribe(self, event_type: type[Event], handler: Callable[[Event], None]) -> None:
        """Subscribe to an event (delegates to inner bus)."""
        self._inner.subscribe(event_type, handler)

    def register_task(self, task_type: type[Task], handler: Callable[[Task], None]) -> None:
        """Register a task handler (delegates to inner bus)."""
        self._inner.register_task(task_type, handler)

    # Additional methods (NOT in MessageBus ABC)

    def __enter__(self) -> RecordingBus:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit: close store."""
        self.close()


class AsyncRecordingBus(_RecordingMixin, AsyncMessageBus):
    """Async decorator/middleware bus that records message dispatches.

    Same recording behavior as RecordingBus but for async handlers.
    Uses the same sync RecordStore (append is non-blocking queue operation).

    IMPORTANT: Primary interface is `async with`. `close()` is synchronous
    (store.close() is sync) to avoid async contamination of store hierarchy.
    """

    __slots__ = ("_inner", "_store", "_record_mode", "_closed")

    def __init__(
        self,
        inner: AsyncMessageBus,
        store: RecordStore,
        record_mode: Literal["all", "errors"] = "all",
    ) -> None:
        """Create an AsyncRecordingBus.

        Args:
            inner: The inner AsyncMessageBus to wrap
            store: Store for recording dispatches (sync store is fine)
            record_mode: "all" or "errors" only

        Raises:
            ValueError: If record_mode is invalid
        """
        self._validate_record_mode(record_mode)

        self._inner = inner
        self._store = store
        self._record_mode = record_mode
        self._closed = False

    async def send(self, query: Query[T]) -> T:
        """Dispatch a query and record the result."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        result: Any = None
        try:
            result = await self._inner.send(query)
            return cast(T, result)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(query, "query", duration, wall_time, result, error_str)

    async def execute(self, command: Command) -> None:
        """Dispatch a command and record execution."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            await self._inner.execute(command)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(command, "command", duration, wall_time, None, error_str)

    async def publish(self, event: Event) -> None:
        """Dispatch an event and record publication."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            await self._inner.publish(event)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(event, "event", duration, wall_time, None, error_str)

    async def dispatch(self, task: Task) -> None:
        """Dispatch a task and record execution."""
        wall_time = time.time()
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            await self._inner.dispatch(task)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            self._record_dispatch(task, "task", duration, wall_time, None, error_str)

    # Registration methods: sync delegation (these are sync in AsyncMessageBus too)

    def register_query(
        self, query_type: type[Query[T]], handler: Callable[[Query[T]], Awaitable[T]]
    ) -> None:
        """Register a query handler (delegates to inner bus)."""
        self._inner.register_query(query_type, handler)

    def register_command(
        self, command_type: type[Command], handler: Callable[[Command], Awaitable[None]]
    ) -> None:
        """Register a command handler (delegates to inner bus)."""
        self._inner.register_command(command_type, handler)

    def subscribe(
        self, event_type: type[Event], handler: Callable[[Event], Awaitable[None]]
    ) -> None:
        """Subscribe to an event (delegates to inner bus)."""
        self._inner.subscribe(event_type, handler)

    def register_task(
        self, task_type: type[Task], handler: Callable[[Task], Awaitable[None]]
    ) -> None:
        """Register a task handler (delegates to inner bus)."""
        self._inner.register_task(task_type, handler)

    # Additional methods (NOT in AsyncMessageBus ABC)

    async def __aenter__(self) -> AsyncRecordingBus:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close store (sync call is fine)."""
        self.close()
