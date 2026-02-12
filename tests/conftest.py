"""Pytest fixtures for contract tests."""

import pytest

from message_bus import (
    AsyncLatencyMiddleware,
    AsyncLocalMessageBus,
    AsyncMiddlewareBus,
    AsyncRecordingBus,
    LatencyMiddleware,
    LatencyStats,
    LocalMessageBus,
    MemoryStore,
    MiddlewareBus,
    RecordingBus,
)


@pytest.fixture(params=["local", "recording", "middleware"])
def bus(request):
    """
    Fixture for synchronous MessageBus implementations.

    Currently includes:
    - LocalMessageBus: Full contract support
    - RecordingBus: Decorator/middleware with recording support
    - MiddlewareBus: Middleware chain with LatencyMiddleware

    Note: ZmqMessageBus is excluded because it only supports Query directly.
    Command/Event/Task require ZmqWorker which has different semantics.
    """
    if request.param == "local":
        yield LocalMessageBus()
    elif request.param == "recording":
        yield RecordingBus(LocalMessageBus(), MemoryStore())
    elif request.param == "middleware":
        stats = LatencyStats()
        yield MiddlewareBus(LocalMessageBus(), [LatencyMiddleware(stats)])


@pytest.fixture(params=["local", "recording", "middleware"])
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
    - MiddlewareBus: Middleware chain with LatencyMiddleware

    ZmqMessageBus is excluded because register_command, subscribe, register_task
    are no-ops. These are handled by ZmqWorker instead.
    """
    if request.param == "local":
        yield LocalMessageBus()
    elif request.param == "recording":
        yield RecordingBus(LocalMessageBus(), MemoryStore())
    elif request.param == "middleware":
        stats = LatencyStats()
        yield MiddlewareBus(LocalMessageBus(), [LatencyMiddleware(stats)])


@pytest.fixture(params=["async_local", "async_recording", "async_middleware"])
def async_bus(request):
    """
    Fixture for asynchronous AsyncMessageBus implementations.

    Currently includes:
    - AsyncLocalMessageBus: Full async contract support
    - AsyncRecordingBus: Async decorator/middleware with recording support
    - AsyncMiddlewareBus: Async middleware chain with AsyncLatencyMiddleware
    """
    if request.param == "async_local":
        yield AsyncLocalMessageBus()
    elif request.param == "async_recording":
        yield AsyncRecordingBus(AsyncLocalMessageBus(), MemoryStore())
    elif request.param == "async_middleware":
        stats = LatencyStats()
        yield AsyncMiddlewareBus(AsyncLocalMessageBus(), [AsyncLatencyMiddleware(stats)])
