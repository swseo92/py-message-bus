"""Tests for Interface Segregation Principle compliance."""

# mypy: disable-error-code="import-not-found"

import pytest

# Check if zmq is available
try:
    import zmq  # noqa: F401

    HAS_ZMQ = True
except ImportError:
    HAS_ZMQ = False


class TestSyncInterfaceHierarchy:
    """Verify sync interface inheritance."""

    def test_local_message_bus_is_full_message_bus(self):
        """LocalMessageBus implements full MessageBus."""
        from message_bus import LocalMessageBus, MessageBus

        assert issubclass(LocalMessageBus, MessageBus)

    def test_message_bus_inherits_all_interfaces(self):
        """MessageBus inherits from all 4 segregated interfaces."""
        from message_bus import (
            HandlerRegistry,
            MessageBus,
            MessageDispatcher,
            QueryDispatcher,
            QueryRegistry,
        )

        assert issubclass(MessageBus, QueryDispatcher)
        assert issubclass(MessageBus, QueryRegistry)
        assert issubclass(MessageBus, MessageDispatcher)
        assert issubclass(MessageBus, HandlerRegistry)


class TestAsyncInterfaceHierarchy:
    """Verify async interface inheritance."""

    def test_async_local_message_bus_is_full_async_message_bus(self):
        """AsyncLocalMessageBus implements full AsyncMessageBus."""
        from message_bus import AsyncLocalMessageBus, AsyncMessageBus

        assert issubclass(AsyncLocalMessageBus, AsyncMessageBus)

    def test_async_message_bus_inherits_all_interfaces(self):
        """AsyncMessageBus inherits from all 4 segregated interfaces."""
        from message_bus import (
            AsyncHandlerRegistry,
            AsyncMessageBus,
            AsyncMessageDispatcher,
            AsyncQueryDispatcher,
            AsyncQueryRegistry,
        )

        assert issubclass(AsyncMessageBus, AsyncQueryDispatcher)
        assert issubclass(AsyncMessageBus, AsyncQueryRegistry)
        assert issubclass(AsyncMessageBus, AsyncMessageDispatcher)
        assert issubclass(AsyncMessageBus, AsyncHandlerRegistry)


@pytest.mark.skipif(not HAS_ZMQ, reason="pyzmq not installed")
class TestZmqInterfaceHierarchy:
    """Verify ZMQ interface inheritance."""

    def test_zmq_message_bus_is_dispatcher_only(self):
        """ZmqMessageBus implements dispatcher interfaces only."""
        from message_bus import (
            HandlerRegistry,
            MessageDispatcher,
            QueryDispatcher,
            QueryRegistry,
        )
        from message_bus.zmq_bus import ZmqMessageBus

        assert issubclass(ZmqMessageBus, QueryDispatcher)
        assert issubclass(ZmqMessageBus, QueryRegistry)
        assert issubclass(ZmqMessageBus, MessageDispatcher)
        assert not issubclass(ZmqMessageBus, HandlerRegistry)

    def test_zmq_worker_is_handler_registry(self):
        """ZmqWorker implements HandlerRegistry."""
        from message_bus import HandlerRegistry
        from message_bus.zmq_bus import ZmqWorker

        assert issubclass(ZmqWorker, HandlerRegistry)
