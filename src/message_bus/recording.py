"""Recording decorator/middleware for message bus observability.

Provides RecordingBus and AsyncRecordingBus for recording message dispatches
to JSONL files or in-memory stores without adding external dependencies.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from queue import SimpleQueue
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
            return  # Silent no-op if closed
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
    )

    def __init__(
        self,
        directory: Path,
        max_runs: int = 10,
    ) -> None:
        """Create a new JsonLineStore.

        Args:
            directory: Directory to store JSONL files
            max_runs: Maximum number of run files to keep (oldest deleted)

        Raises:
            ValueError: If max_runs < 1
        """
        if max_runs < 1:
            raise ValueError(f"max_runs must be >= 1, got {max_runs}")

        directory.mkdir(parents=True, exist_ok=True)

        # File naming: run_{YYYYMMDD_HHMMSS}_{pid}.jsonl
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        pid = os.getpid()
        self._file_path = directory / f"run_{timestamp_str}_{pid}.jsonl"

        self._queue: SimpleQueue[Record | None] = SimpleQueue()
        self._writer_error: Exception | None = None
        self._closed = False

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
        run_files = sorted(directory.glob("run_*.jsonl"), key=lambda p: p.stat().st_mtime)
        for old_file in run_files[
            : -(max_runs - 1)
        ]:  # Keep max_runs - 1 (new file will be #max_runs)
            old_file.unlink(missing_ok=True)  # Race-safe deletion

    def _writer_loop(self) -> None:
        """Background thread: drain queue and write to file in batches."""
        try:
            with open(self._file_path, "w") as f:
                while True:
                    batch: list[Record] = []

                    # Blocking get for first item
                    item = self._queue.get()
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
                        except Exception:  # queue.Empty
                            break

                    # Write batch
                    for record in batch:
                        record_dict = {
                            "ts": time.strftime(
                                "%Y-%m-%dT%H:%M:%S", time.localtime(record.timestamp)
                            )
                            + f".{int((record.timestamp % 1) * 1e6):06d}",
                            "type": record.message_type,
                            "class": record.message_class,
                            "payload": _safe_asdict(
                                record.payload
                            ),  # Serialize here (background thread)
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
            # Writer thread error: log but don't crash (continue draining to prevent memory leak)
            self._writer_error = exc
            logger.error("JsonLineStore writer thread error: %s", exc)

    def append(self, record: Record) -> None:
        """Enqueue a record for background writing (non-blocking)."""
        if self._closed:
            return  # Silent no-op if closed
        self._queue.put_nowait(record)

    def close(self) -> None:
        """Flush remaining records and stop writer thread. Idempotent."""
        if self._closed:
            return  # Already closed

        self._closed = True

        # Signal writer thread
        self._queue.put_nowait(None)

        # Join with timeout
        self._writer_thread.join(timeout=2.0)

        # If thread still alive, drain synchronously (best-effort)
        if self._writer_thread.is_alive():
            logger.warning(
                "JsonLineStore writer thread did not stop in time, draining synchronously"
            )

        # Check for writer errors
        if self._writer_error is not None:
            logger.warning(
                "JsonLineStore had writer errors during operation: %s", self._writer_error
            )


class RecordingBus(MessageBus):
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
        if record_mode not in ("all", "errors"):
            raise ValueError(f"Invalid record_mode: {record_mode!r}. Must be 'all' or 'errors'.")

        self._inner = inner
        self._store = store
        self._record_mode = record_mode
        self._closed = False

    def send(self, query: Query[T]) -> T:
        """Dispatch a query and record the result."""

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
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="query",
                        message_class=type(query).__name__,
                        payload=query,  # Store raw object (NOT dict)
                        duration_ns=duration,
                        result=result if error_str is None else None,
                        error=error_str,
                    )
                )

    def execute(self, command: Command) -> None:
        """Dispatch a command and record execution."""
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            self._inner.execute(command)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="command",
                        message_class=type(command).__name__,
                        payload=command,
                        duration_ns=duration,
                        result=None,
                        error=error_str,
                    )
                )

    def publish(self, event: Event) -> None:
        """Dispatch an event and record publication."""
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            self._inner.publish(event)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="event",
                        message_class=type(event).__name__,
                        payload=event,
                        duration_ns=duration,
                        result=None,
                        error=error_str,
                    )
                )

    def dispatch(self, task: Task) -> None:
        """Dispatch a task and record execution."""
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            self._inner.dispatch(task)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="task",
                        message_class=type(task).__name__,
                        payload=task,
                        duration_ns=duration,
                        result=None,
                        error=error_str,
                    )
                )

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

    @property
    def store(self) -> RecordStore:
        """Access the underlying store (for testing)."""
        return self._store

    def close(self) -> None:
        """Close the store and flush records. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._store.close()

    def __enter__(self) -> RecordingBus:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit: close store."""
        self.close()


class AsyncRecordingBus(AsyncMessageBus):
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
        if record_mode not in ("all", "errors"):
            raise ValueError(f"Invalid record_mode: {record_mode!r}. Must be 'all' or 'errors'.")

        self._inner = inner
        self._store = store
        self._record_mode = record_mode
        self._closed = False

    async def send(self, query: Query[T]) -> T:
        """Dispatch a query and record the result."""

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
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="query",
                        message_class=type(query).__name__,
                        payload=query,
                        duration_ns=duration,
                        result=result if error_str is None else None,
                        error=error_str,
                    )
                )

    async def execute(self, command: Command) -> None:
        """Dispatch a command and record execution."""
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            await self._inner.execute(command)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="command",
                        message_class=type(command).__name__,
                        payload=command,
                        duration_ns=duration,
                        result=None,
                        error=error_str,
                    )
                )

    async def publish(self, event: Event) -> None:
        """Dispatch an event and record publication."""
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            await self._inner.publish(event)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="event",
                        message_class=type(event).__name__,
                        payload=event,
                        duration_ns=duration,
                        result=None,
                        error=error_str,
                    )
                )

    async def dispatch(self, task: Task) -> None:
        """Dispatch a task and record execution."""
        t0 = time.perf_counter_ns()
        error_str: str | None = None
        try:
            await self._inner.dispatch(task)
        except Exception as exc:
            error_str = repr(exc)
            raise
        finally:
            duration = time.perf_counter_ns() - t0
            if self._record_mode == "all" or error_str is not None:
                self._store.append(
                    Record(
                        timestamp=time.time(),
                        message_type="task",
                        message_class=type(task).__name__,
                        payload=task,
                        duration_ns=duration,
                        result=None,
                        error=error_str,
                    )
                )

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

    @property
    def store(self) -> RecordStore:
        """Access the underlying store (for testing)."""
        return self._store

    def close(self) -> None:
        """Close the store and flush records. Synchronous. Idempotent.

        NOTE: This is sync to avoid async contamination of the store hierarchy.
        Use `async with` as the primary interface.
        """
        if self._closed:
            return
        self._closed = True
        self._store.close()

    async def __aenter__(self) -> AsyncRecordingBus:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close store (sync call is fine)."""
        self.close()
