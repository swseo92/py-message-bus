# py-message-bus

Lightweight message bus for modular monolith architecture with zero core dependencies.

## Features

- **Zero dependencies** - No external packages required for core functionality
- **Type-safe** - Full Generic support with mypy strict mode compatibility
- **Interface Segregation** - Inject only the interfaces you need (QueryDispatcher, MessageDispatcher, etc.)
- **Four message types** - Query, Command, Event, Task with clear semantics
- **Recording/Observability** - Built-in middleware for dispatch logging to JSONL or memory
- **Composable Middleware** - Chain middleware for cross-cutting concerns (latency, logging, auth)
- **Latency Instrumentation** - Built-in middleware for percentile statistics and separation signal detection
- **Async support** - First-class async/await support with concurrent event handling
- **Multi-process ready** - Optional ZMQ-based distributed implementation

## Installation

```bash
# Core (zero dependencies)
pip install py-message-bus

# With ZMQ support for multi-process
pip install py-message-bus[zmq]

# Development
pip install py-message-bus[dev]
```

## Quick Start

```python
from dataclasses import dataclass
from message_bus import Query, Command, Event, LocalMessageBus

@dataclass(frozen=True)
class GetUser(Query[dict]):
    user_id: str

@dataclass(frozen=True)
class CreateOrder(Command):
    user_id: str
    items: list[str]

@dataclass(frozen=True)
class OrderCreated(Event):
    order_id: str

# Create bus and register handlers
bus = LocalMessageBus()
bus.register_query(GetUser, lambda q: {"id": q.user_id, "name": "Alice"})
bus.register_command(CreateOrder, lambda c: print(f"Order created for {c.user_id}"))
bus.subscribe(OrderCreated, lambda e: print(f"Order {e.order_id} notification sent"))

# Dispatch messages
user = bus.send(GetUser(user_id="123"))              # Returns dict
bus.execute(CreateOrder(user_id="123", items=["item1"]))  # No return
bus.publish(OrderCreated(order_id="456"))            # Multicast to all subscribers
```

## Message Types

| Type | Pattern | Handlers | Response | Use Case |
|------|---------|----------|----------|----------|
| Query[T] | 1:1 | Exactly 1 | Returns T | Read operations (e.g., GetUser) |
| Command | 1:1 | Exactly 1 | None | State changes (e.g., CreateOrder) |
| Event | 1:N | 0 or more | None | Notifications (e.g., OrderCreated) |
| Task | 1:1 | Exactly 1 | None | Distributed work units |

### Error Behavior

- **Query/Command/Task**: Duplicate registration → `ValueError`, missing handler → `LookupError`
- **Event**: No subscribers → silent (no error), handler failures → fault-isolated with `ExceptionGroup`

```python
# Event handlers are fault-isolated - all handlers execute even if some fail
def handler1(e): print("success")
def handler2(e): raise ValueError("error")
def handler3(e): print("also runs")

bus.subscribe(OrderCreated, handler1)
bus.subscribe(OrderCreated, handler2)
bus.subscribe(OrderCreated, handler3)

# All three execute, then raises ValueError (or ExceptionGroup if multiple fail)
bus.publish(OrderCreated(order_id="1"))
```

## Interface Segregation

Instead of injecting the full `MessageBus`, inject only what you need:

```python
from message_bus import QueryDispatcher, MessageDispatcher

class OrderService:
    def __init__(self, queries: QueryDispatcher):
        # Only has access to send() - cannot register handlers
        self._queries = queries

    def create_order(self, user_id: str):
        user = self._queries.send(GetUser(user_id=user_id))
        # ...

# Wire it up
bus: MessageBus = LocalMessageBus()
service = OrderService(queries=bus)  # Pass bus as QueryDispatcher
```

### Available Interfaces (Sync)

- `QueryDispatcher` - `send(query) -> T`
- `QueryRegistry` - `register_query(type, handler)`
- `MessageDispatcher` - `execute()`, `publish()`, `dispatch()`
- `HandlerRegistry` - `register_command()`, `subscribe()`, `register_task()`
- `MessageBus` - All of the above combined

Async variants: `AsyncQueryDispatcher`, `AsyncMessageBus`, etc.

## Implementations

### LocalMessageBus (Single Process)

Synchronous, in-memory message bus. No queuing, just direct function calls.

```python
from message_bus import LocalMessageBus

bus = LocalMessageBus()
# ~80ns overhead per dispatch - negligible for most applications
```

**Use cases:**
- Development and testing
- Modular monolith (single process)
- High-frequency trading simulations

**Thread safety:**
- Registration: NOT thread-safe (call during bootstrap only)
- Dispatch: Thread-safe for reads, but handlers must be thread-safe

### AsyncLocalMessageBus (Async Single Process)

Async variant with concurrent event handler execution.

```python
import asyncio
from message_bus import AsyncLocalMessageBus

bus = AsyncLocalMessageBus()

async def get_user(q: GetUser):
    return {"id": q.user_id}

bus.register_query(GetUser, get_user)

# Dispatch
user = await bus.send(GetUser(user_id="123"))

# Events execute concurrently with asyncio.gather
bus.subscribe(OrderCreated, async_handler1)
bus.subscribe(OrderCreated, async_handler2)
await bus.publish(OrderCreated(order_id="1"))  # Handlers run in parallel
```

**Use cases:**
- FastAPI, aiohttp, asyncio applications
- I/O-bound operations
- Concurrent event processing

### ZmqMessageBus + ZmqWorker (Multi-Process)

Distributed message bus using ZeroMQ for inter-process communication.

```python
from message_bus import ZmqMessageBus, ZmqWorker

# Server process (default: ipc:///tmp/message_bus_*_<pid> on Unix, tcp://127.0.0.1:port on Windows)
bus = ZmqMessageBus(
    task_socket="ipc:///tmp/message_bus_task",
    query_socket="ipc:///tmp/message_bus_query",
    event_socket="ipc:///tmp/message_bus_event",
    command_socket="ipc:///tmp/message_bus_command",
)
# Dispatch from any process
result = bus.send(GetUser(user_id="123"))

# Worker process
worker = ZmqWorker(
    task_socket="ipc:///tmp/message_bus_task",
    event_socket="ipc:///tmp/message_bus_event",
    command_socket="ipc:///tmp/message_bus_command",
)
worker.register_query(GetUser, lambda q: {"id": q.user_id})
worker.run()  # Blocks and processes messages
```

**Requires:** `pip install py-message-bus[zmq]`

**Use cases:**
- Multi-process Python applications
- CPU-bound task distribution
- Legacy systems transitioning to distributed architecture

**Custom Serializer:**

```python
from message_bus import ZmqMessageBus, Serializer, PickleSerializer

# Default: PickleSerializer (backward compatible)
bus = ZmqMessageBus(
    task_socket="ipc:///tmp/message_bus_task",
    query_socket="ipc:///tmp/message_bus_query",
    event_socket="ipc:///tmp/message_bus_event",
    command_socket="ipc:///tmp/message_bus_command",
)

# Custom serializer (implement Serializer protocol)
class JsonSerializer:
    def dumps(self, obj: object) -> bytes: ...
    def loads(self, data: bytes) -> object: ...

bus = ZmqMessageBus(
    task_socket="ipc:///tmp/message_bus_task",
    query_socket="ipc:///tmp/message_bus_query",
    event_socket="ipc:///tmp/message_bus_event",
    command_socket="ipc:///tmp/message_bus_command",
    serializer=JsonSerializer(),
)
```

**Limitations:**
- Pluggable serializer (defaults to pickle - security considerations apply)
- Configurable query timeout (`query_timeout_ms`, default 5000ms)
- No message persistence or delivery guarantees

## Recording (Observability)

Decorator pattern for recording message dispatches to JSONL files or memory.

```python
from message_bus import LocalMessageBus, RecordingBus, JsonLineStore
from pathlib import Path

inner = LocalMessageBus()
store = JsonLineStore(Path("./recordings"), max_runs=10)

with RecordingBus(inner, store) as bus:
    bus.register_query(GetUser, lambda q: {"id": q.user_id})
    user = bus.send(GetUser(user_id="123"))
# JSONL file written on close with all dispatch records
```

### JSONL Output Format

```json
{"ts": "2026-02-12T14:30:45.123456", "type": "query", "class": "GetUser", "payload": {"user_id": "123"}, "duration_ns": 850000, "result": {"id": "123"}, "error": null}
{"ts": "2026-02-12T14:30:45.234567", "type": "command", "class": "CreateOrder", "payload": {"user_id": "123", "items": ["item1"]}, "duration_ns": 1200000, "result": null, "error": null}
```

### Recording Modes

```python
# Record all dispatches (default)
RecordingBus(bus, store, record_mode="all")

# Record only failures
RecordingBus(bus, store, record_mode="errors")
```

### Stores

- `JsonLineStore(directory, max_runs=10)` - Write to JSONL files with background thread, auto-cleanup old runs
- `MemoryStore()` - In-memory list for testing

### Payload Redaction

Sensitive fields can be automatically redacted from JSONL files:

```python
store = JsonLineStore(
    Path("./recordings"),
    max_runs=10,
    redact_fields=frozenset({"password", "token", "secret"}),
)
# Payloads will have matching fields replaced with "***REDACTED***"
# Works recursively on nested dicts and list[dict]
```

### Bounded Queue (Backpressure)

Prevent memory issues when dispatch rate exceeds writer throughput:

```python
store = JsonLineStore(
    Path("./recordings"),
    max_queue_size=50_000,  # Default: 10_000
)
# When queue is full, oldest records are dropped with a warning log
```

### Configurable Close Timeout

```python
store = JsonLineStore(
    Path("./recordings"),
    close_timeout=10.0,  # Default: 5.0 seconds
)
```

### Async Recording

```python
from message_bus import AsyncLocalMessageBus, AsyncRecordingBus

inner = AsyncLocalMessageBus()
store = JsonLineStore(Path("./recordings"))

async with AsyncRecordingBus(inner, store) as bus:
    bus.register_query(GetUser, async_handler)
    result = await bus.send(GetUser(user_id="123"))
# Store flushed on exit
```

## Middleware

Composable middleware for cross-cutting concerns (latency instrumentation, logging, authorization, etc.). Middleware chains are pre-built at initialization time with zero per-dispatch overhead.

```python
from message_bus import (
    LocalMessageBus, MiddlewareBus, PassthroughMiddleware,
    Query, Command, Event,
)

class LoggingMiddleware(PassthroughMiddleware):
    def on_send(self, query, next_fn):
        print(f"Query: {type(query).__name__}")
        return next_fn(query)

    def on_execute(self, command, next_fn):
        print(f"Command: {type(command).__name__}")
        next_fn(command)

inner = LocalMessageBus()
bus = MiddlewareBus(inner, [LoggingMiddleware()])
# Registration delegates to inner bus
# Dispatch goes through middleware chain
```

### Middleware Classes

- `Middleware` (abstract) - Base class with hooks: `on_send`, `on_execute`, `on_publish`, `on_dispatch`
- `PassthroughMiddleware` - Extend and override only what you need
- `MiddlewareBus(inner, middlewares)` - Wraps any MessageBus, pre-builds chains at init
- Async variants: `AsyncMiddleware`, `AsyncPassthroughMiddleware`, `AsyncMiddlewareBus`

### Design Notes

- Chains pre-built at `__init__` time (zero per-dispatch allocation)
- First middleware in the list is outermost (first to intercept, last to complete)
- Registration methods are pure delegation (no middleware applied)
- Composable with RecordingBus: `RecordingBus(MiddlewareBus(inner, [mw]), store)`

## Latency Instrumentation

Built-in middleware for measuring handler execution latency with bounded-memory percentile statistics and module separation signal detection.

```python
from message_bus import (
    LocalMessageBus, MiddlewareBus,
    LatencyMiddleware, LatencyStats,
)

stats = LatencyStats(max_samples=10_000)
middleware = LatencyMiddleware(stats, threshold_ms=50.0)
bus = MiddlewareBus(LocalMessageBus(), [middleware])

# After dispatches, query statistics
percs = stats.all_percentiles()
for p in percs:
    print(f"{p.message_class}: p50={p.p50_ms:.2f}ms p95={p.p95_ms:.2f}ms p99={p.p99_ms:.2f}ms")
```

### Separation Signal Detection

Detect module coupling via latency spikes (handlers whose latency increased significantly):

```python
# Set baseline after warm-up period
stats.set_baseline()

# ... later, check for latency increases
signals = stats.check_separation_signals(threshold_ratio=2.0)
for s in signals:
    print(f"{s.message_class}: p95 {s.baseline_p95_ms:.1f}ms -> {s.current_p95_ms:.1f}ms ({s.increase_ratio}x)")
```

### Classes

- `LatencyStats(max_samples=10_000)` - Thread-safe collector using bounded deque per message type
- `LatencyPercentiles` - Frozen dataclass with p50/p95/p99/min/max in ms
- `SeparationSignal` - Signal that a handler may be coupled to another module's load
- `LatencyMiddleware(stats, threshold_ms=100.0)` - ~50ns overhead, logs warning when threshold exceeded
- `AsyncLatencyMiddleware` - Async variant using same LatencyStats

### Performance

- Overhead: ~50ns per dispatch (one `time.perf_counter_ns()` call pair)
- Memory: `max_samples * num_unique_message_types * 8 bytes` (default ~1.6MB worst case)
- Thread-safe: Protected by single lock held briefly for append

## Retry Middleware

Automatic retry with exponential backoff for transient failures.

```python
from message_bus import (
    LocalMessageBus, MiddlewareBus,
    RetryMiddleware,
)

retry = RetryMiddleware(
    max_attempts=3,        # Initial + 2 retries
    backoff_base=2.0,      # Exponential base (seconds)
    max_backoff=60.0,      # Maximum backoff delay (seconds)
    retryable=(ConnectionError, TimeoutError),  # Which exceptions to retry
)
bus = MiddlewareBus(LocalMessageBus(), [retry])

# Handler failures trigger retry: 2^1=2s, 2^2=4s delays
bus.register_command(CreateOrder, flaky_handler)
bus.execute(CreateOrder(user_id="123"))
# On ConnectionError: retries at t=0, t=2s, t=6s (cumulative)
```

### Backoff Strategy

Exponential backoff with formula: `delay = backoff_base ** attempt`

| Attempt | Delay (backoff_base=2.0) | Cumulative Time |
|---------|-------------------------|-----------------|
| 1 (initial) | 0s | 0s |
| 2 | 2^1 = 2s | 2s |
| 3 | 2^2 = 4s | 6s |

### Configuration

- `max_attempts` - Maximum attempts (initial + retries), default 3, must be >= 1
- `backoff_base` - Exponential backoff base in seconds, default 2.0, must be > 0
- `max_backoff` - Maximum backoff delay in seconds, default 60.0, must be > 0. Caps exponential growth.
- `retryable` - Tuple of exception types that trigger retry, default `(ConnectionError, TimeoutError)`

Only exceptions in the `retryable` whitelist trigger retry. All others propagate immediately.

### Event Exclusion

Events (publish) use passthrough with no retry due to multicast semantics and ExceptionGroup complexity.

### Async Variant

```python
from message_bus import AsyncLocalMessageBus, AsyncMiddlewareBus, AsyncRetryMiddleware

retry = AsyncRetryMiddleware(max_attempts=3, backoff_base=2.0)
bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [retry])
# Uses asyncio.sleep() for backoff
```

## Dead Letter Middleware

Capture failed messages for replay or analysis without altering error propagation behavior.

```python
from message_bus import (
    LocalMessageBus, MiddlewareBus,
    DeadLetterMiddleware, MemoryDeadLetterStore,
)

store = MemoryDeadLetterStore()
dlq = DeadLetterMiddleware(store)
bus = MiddlewareBus(LocalMessageBus(), [dlq])

# Register handlers
bus.register_command(CreateOrder, handler_that_fails)

# Failed dispatch is captured
try:
    bus.execute(CreateOrder(user_id="123"))
except Exception:
    pass  # Error still propagates

# Inspect failures
for record in store.records:
    print(f"Failed: {record.message}, Error: {record.error}, Handler: {record.handler_name}")
```

### DeadLetterStore ABC

Abstract base class for storing failed messages. Implementations must provide:

- `append(message, error, handler_name)` - Non-blocking append for middleware use
- `close()` - Flush and close, must be idempotent

### MemoryDeadLetterStore

In-memory store for testing. Stores records in a list accessible via `.records`.

### DeadLetterRecord

Frozen dataclass with fields:
- `message: Event | Command | Task` - The failed message
- `error: Exception` - The exception raised
- `handler_name: str` - Handler identifier

### Middleware Composition

Combine with other middleware for comprehensive error handling:

```python
retry = RetryMiddleware(max_attempts=3)
dlq = DeadLetterMiddleware(MemoryDeadLetterStore())
timeout = TimeoutMiddleware(default_timeout=5.0)

# Retry → DLQ → Timeout → Inner bus
bus = MiddlewareBus(LocalMessageBus(), [retry, dlq, timeout])
# Order: outermost first (retry wraps DLQ wraps timeout)
```

### Async Variant

```python
from message_bus import AsyncLocalMessageBus, AsyncMiddlewareBus, AsyncDeadLetterMiddleware

store = MemoryDeadLetterStore()  # Same store works for async
dlq = AsyncDeadLetterMiddleware(store)
bus = AsyncMiddlewareBus(AsyncLocalMessageBus(), [dlq])
```

## Architecture

```
src/message_bus/
├── __init__.py     # Public API exports
├── ports.py        # ABCs: Query, Command, Event, Task + segregated interfaces
├── local.py        # LocalMessageBus, AsyncLocalMessageBus
├── recording.py    # RecordingBus, AsyncRecordingBus, JsonLineStore, MemoryStore
├── middleware.py   # MiddlewareBus, AsyncMiddlewareBus, Middleware ABCs
├── latency.py      # LatencyMiddleware, LatencyStats, SeparationSignal
├── retry.py        # RetryMiddleware, AsyncRetryMiddleware (exponential backoff)
├── timeout.py      # TimeoutMiddleware, AsyncTimeoutMiddleware (handler timeout enforcement)
├── dead_letter.py  # DeadLetterMiddleware, AsyncDeadLetterMiddleware, DeadLetterStore
├── testing.py      # FakeMessageBus, AsyncFakeMessageBus (test utilities)
└── zmq_bus.py      # ZmqMessageBus, ZmqWorker (optional, requires pyzmq)
```

### Design Principles

1. **Ports & Adapters** - Core domain (`ports.py`) has zero dependencies on implementations
2. **Interface Segregation** - Components depend on minimal interfaces, not full MessageBus
3. **Direct Function Calls** - LocalMessageBus is a registry/router, not a real queue (minimal overhead)
4. **Decorator Pattern** - RecordingBus wraps any MessageBus without modifying it
5. **Composable Middleware** - MiddlewareBus chains middleware at init time with zero per-dispatch overhead
6. **Bounded Resources** - All buffers (deque, queue) have configurable size limits

## Use Cases

### Modular Monolith

Decouple modules without microservices overhead.

```python
# modules/user/application/handlers.py
from message_bus import QueryDispatcher

class UserService:
    def __init__(self, queries: QueryDispatcher):
        self._queries = queries

# modules/order/application/handlers.py
from shared.messages import GetUserQuery

class OrderService:
    def __init__(self, queries: QueryDispatcher):
        self._queries = queries

    def create_order(self, user_id: str):
        # No direct import of user module - decoupled via message bus
        user = self._queries.send(GetUserQuery(user_id=user_id))
```

### Event-Driven Architecture

Publish domain events for eventual consistency.

```python
# Order module publishes event
bus.publish(OrderCreated(order_id="1", user_id="123"))

# Other modules subscribe (inventory, email, analytics, etc.)
bus.subscribe(OrderCreated, inventory_service.on_order_created)
bus.subscribe(OrderCreated, email_service.send_confirmation)
bus.subscribe(OrderCreated, analytics_service.track_order)
```

### CQRS (Command Query Responsibility Segregation)

Separate reads from writes.

```python
# Queries - read side
@dataclass(frozen=True)
class GetUserQuery(Query[UserDTO]):
    user_id: str

# Commands - write side
@dataclass(frozen=True)
class CreateUserCommand(Command):
    name: str
    email: str

bus.register_query(GetUserQuery, read_model.get_user)
bus.register_command(CreateUserCommand, write_model.create_user)
```

## Performance

### LocalMessageBus Overhead

- Dict lookup: O(1)
- Function call overhead: ~80ns per dispatch
- Negligible for most applications (>10M dispatches/sec on modern hardware)

### Benchmark Example

```python
import time
from message_bus import LocalMessageBus, Query
from dataclasses import dataclass

@dataclass(frozen=True)
class TestQuery(Query[int]):
    value: int

bus = LocalMessageBus()
bus.register_query(TestQuery, lambda q: q.value * 2)

# Measure 1M dispatches
start = time.perf_counter()
for i in range(1_000_000):
    result = bus.send(TestQuery(value=i))
elapsed = time.perf_counter() - start

print(f"Throughput: {1_000_000 / elapsed:.0f} dispatches/sec")
# Typical: ~12M dispatches/sec (80ns each)
```

## Development

```bash
# Clone and setup
git clone https://github.com/yourusername/py-message-bus
cd py-message-bus
pip install -e ".[dev]"

# Run tests
pytest

# Run specific test
pytest tests/test_local_bus.py::TestEventFaultIsolation::test_all_handlers_execute_when_one_fails

# Lint and format
ruff check src tests
ruff format src tests

# Type check
mypy src

# Test with coverage
pytest --cov=message_bus --cov-report=term-missing
```

## Requirements

- Python >= 3.11
- Zero core dependencies
- Optional: `pyzmq >= 25.0` for ZMQ support

## License

MIT

## Contributing

Contributions welcome! Please ensure:

1. Tests pass: `pytest`
2. Type checking passes: `mypy src`
3. Linting passes: `ruff check src tests`
4. Code formatted: `ruff format src tests`

## Testing Utilities

FakeMessageBus and AsyncFakeMessageBus for unit testing without real handlers.

```python
from message_bus.testing import FakeMessageBus

def test_order_service_publishes_event():
    bus = FakeMessageBus()
    service = OrderService(bus)

    service.create_order(user_id="123", items=["item1"])

    bus.assert_published(OrderCreatedEvent, count=1)
    bus.assert_executed(CreateOrderCommand, count=1)

def test_order_service_queries_user():
    bus = FakeMessageBus()
    bus.given_query_result(GetUserQuery, {"id": "123", "name": "Alice"})
    service = OrderService(bus)

    service.create_order(user_id="123", items=["item1"])

    # Verify query was sent
    assert len(bus.sent_queries) == 1
    assert bus.sent_queries[0].user_id == "123"
```

### Features

- **Recording** - All dispatched messages are recorded in lists
- **Query Stubbing** - `given_query_result(query_type, result)` stubs query responses
- **Assertions** - `assert_published()`, `assert_executed()`, `assert_nothing_published()`
- **Reset** - `reset()` clears all recorded messages and stubs between tests

### API Reference

```python
class FakeMessageBus:
    # Recorded messages
    sent_queries: list[Query]
    executed_commands: list[Command]
    published_events: list[Event]
    dispatched_tasks: list[Task]

    # Stubbing
    def given_query_result(self, query_type: type[Query[T]], result: T) -> None: ...

    # Assertions
    def assert_published(self, event_type: type[Event], *, count: int = 1) -> None: ...
    def assert_executed(self, command_type: type[Command], *, count: int = 1) -> None: ...
    def assert_nothing_published(self) -> None: ...

    # Cleanup
    def reset(self) -> None: ...
```

### Async Testing

```python
from message_bus.testing import AsyncFakeMessageBus

@pytest.mark.asyncio
async def test_async_order_service():
    bus = AsyncFakeMessageBus()
    bus.given_query_result(GetUserQuery, {"id": "123"})
    service = AsyncOrderService(bus)

    await service.create_order(user_id="123", items=["item1"])

    bus.assert_published(OrderCreatedEvent, count=1)
```

### Why Use FakeMessageBus?

- **Fast** - No real handler execution, just recording
- **Simple** - No need to register handlers for tests
- **Clear Assertions** - Explicit assertion methods with good error messages
- **Isolation** - Test your service without dependencies

### Import Path

Testing utilities have a separate import path to keep them out of production code:

```python
# Production code - normal imports
from message_bus import MessageBus, LocalMessageBus

# Test code - testing imports
from message_bus.testing import FakeMessageBus, AsyncFakeMessageBus
```

## Roadmap

- [x] Latency instrumentation and separation signal detection
- [x] Testing utilities (FakeMessageBus, AsyncFakeMessageBus)
- [ ] KafkaMessageBus for production-grade distributed messaging
- [ ] Message persistence and replay
- [ ] Dead letter queue support
