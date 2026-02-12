"""Lightweight message bus for modular monolith architecture."""

from message_bus.local import AsyncLocalMessageBus, LocalMessageBus
from message_bus.ports import (
    AsyncHandlerRegistry,
    AsyncMessageBus,
    AsyncMessageDispatcher,
    AsyncQueryDispatcher,
    AsyncQueryRegistry,
    Command,
    Event,
    HandlerRegistry,
    MessageBus,
    MessageDispatcher,
    Query,
    QueryDispatcher,
    QueryRegistry,
    Task,
)
from message_bus.recording import (
    AsyncRecordingBus,
    JsonLineStore,
    MemoryStore,
    Record,
    RecordingBus,
    RecordStore,
)

__all__ = [
    # Core message types
    "Query",
    "Command",
    "Event",
    "Task",
    # Sync interfaces (segregated)
    "QueryDispatcher",
    "QueryRegistry",
    "MessageDispatcher",
    "HandlerRegistry",
    "MessageBus",
    # Async interfaces (segregated)
    "AsyncQueryDispatcher",
    "AsyncQueryRegistry",
    "AsyncMessageDispatcher",
    "AsyncHandlerRegistry",
    "AsyncMessageBus",
    # Implementations
    "LocalMessageBus",
    "AsyncLocalMessageBus",
    # Recording
    "Record",
    "RecordStore",
    "MemoryStore",
    "JsonLineStore",
    "RecordingBus",
    "AsyncRecordingBus",
]

# Optional ZMQ support
try:
    from message_bus.zmq_bus import (  # noqa: F401
        PickleSerializer,
        Serializer,
        ZmqMessageBus,
        ZmqWorker,
    )

    __all__.extend(["ZmqMessageBus", "ZmqWorker", "Serializer", "PickleSerializer"])
except ImportError:
    pass

__version__ = "0.1.0"
