"""Pytest fixtures for contract tests."""

import pytest

from message_bus import (
    AsyncLocalMessageBus,
    AsyncRecordingBus,
    LocalMessageBus,
    MemoryStore,
    RecordingBus,
)


@pytest.fixture(params=["local", "recording"])
def bus(request):
    """
    Fixture for synchronous MessageBus implementations.

    Currently includes:
    - LocalMessageBus: Full contract support
    - RecordingBus: Decorator/middleware with recording support

    Note: ZmqMessageBus is excluded because it only supports Query directly.
    Command/Event/Task require ZmqWorker which has different semantics.
    """
    if request.param == "local":
        yield LocalMessageBus()
    elif request.param == "recording":
        yield RecordingBus(LocalMessageBus(), MemoryStore())


@pytest.fixture(params=["local", "recording"])
def bus_full_contract(request):
    """
    Fixture for MessageBus implementations that support full contract.

    Full contract means:
    - Query: register_query + send
    - Command: register_command + execute
    - Event: subscribe + publish
    - Task: register_task + dispatch

    Currently includes:
    - LocalMessageBus: Full contract support
    - RecordingBus: Decorator/middleware with recording support

    ZmqMessageBus is excluded because register_command, subscribe, register_task
    are no-ops. These are handled by ZmqWorker instead.
    """
    if request.param == "local":
        yield LocalMessageBus()
    elif request.param == "recording":
        yield RecordingBus(LocalMessageBus(), MemoryStore())


@pytest.fixture(params=["async_local", "async_recording"])
def async_bus(request):
    """
    Fixture for asynchronous AsyncMessageBus implementations.

    Currently includes:
    - AsyncLocalMessageBus: Full async contract support
    - AsyncRecordingBus: Async decorator/middleware with recording support
    """
    if request.param == "async_local":
        yield AsyncLocalMessageBus()
    elif request.param == "async_recording":
        yield AsyncRecordingBus(AsyncLocalMessageBus(), MemoryStore())
