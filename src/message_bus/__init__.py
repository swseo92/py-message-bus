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
]

# Optional ZMQ support
try:
    from message_bus.zmq_bus import ZmqMessageBus, ZmqWorker  # noqa: F401

    __all__.extend(["ZmqMessageBus", "ZmqWorker"])
except ImportError:
    pass

__version__ = "0.1.0"
