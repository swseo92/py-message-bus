"""Pytest fixtures for contract tests."""

import pytest

from message_bus import AsyncLocalMessageBus, LocalMessageBus


@pytest.fixture(params=["local"])
def bus(request):
    """
    Fixture for synchronous MessageBus implementations.

    Currently includes:
    - LocalMessageBus: Full contract support

    Note: ZmqMessageBus is excluded because it only supports Query directly.
    Command/Event/Task require ZmqWorker which has different semantics.
    """
    if request.param == "local":
        yield LocalMessageBus()


@pytest.fixture(params=["local"])
def bus_full_contract(request):
    """
    Fixture for MessageBus implementations that support full contract.

    Full contract means:
    - Query: register_query + send
    - Command: register_command + execute
    - Event: subscribe + publish
    - Task: register_task + dispatch

    ZmqMessageBus is excluded because register_command, subscribe, register_task
    are no-ops. These are handled by ZmqWorker instead.
    """
    if request.param == "local":
        yield LocalMessageBus()


@pytest.fixture(params=["async_local"])
def async_bus(request):
    """
    Fixture for asynchronous AsyncMessageBus implementations.

    Currently includes:
    - AsyncLocalMessageBus: Full async contract support
    """
    if request.param == "async_local":
        yield AsyncLocalMessageBus()
