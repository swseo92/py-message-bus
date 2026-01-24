"""Lightweight message bus for modular monolith architecture."""

from message_bus.local import LocalMessageBus
from message_bus.ports import Command, Event, MessageBus, Query

__all__ = [
    "MessageBus",
    "Query",
    "Command",
    "Event",
    "LocalMessageBus",
]

__version__ = "0.1.0"
