# py-message-bus

Lightweight message bus for modular monolith architecture.

## Features

- **Zero dependencies** - Pure Python, no external packages required
- **Type-safe** - Full typing support with generics
- **Simple** - ~100 lines of core code
- **Fast** - Direct function calls, ~80ns overhead per dispatch
- **Swappable** - Same interface for Local, Kafka, Redis implementations

## Installation

```bash
pip install py-message-bus
```

## Quick Start

### Define Messages

```python
from dataclasses import dataclass
from message_bus import Query, Command, Event

@dataclass(frozen=True)
class GetUserQuery(Query[dict]):
    user_id: str

@dataclass(frozen=True)
class CreateOrderCommand(Command):
    user_id: str
    items: list[str]

@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    order_id: str
    user_id: str
```

### Register Handlers

```python
from message_bus import LocalMessageBus

bus = LocalMessageBus()

# Query: 1:1, returns response
bus.register_query(GetUserQuery, user_service.get_user)

# Command: 1:1, no response
bus.register_command(CreateOrderCommand, order_service.create)

# Event: 1:N, multiple subscribers
bus.subscribe(OrderCreatedEvent, inventory_service.on_order_created)
bus.subscribe(OrderCreatedEvent, notification_service.on_order_created)
```

### Use in Modules

```python
class OrderService:
    def __init__(self, bus: MessageBus):
        self._bus = bus

    def create_order(self, cmd: CreateOrderCommand):
        # Query another module (sync response)
        user = self._bus.send(GetUserQuery(cmd.user_id))

        order = Order.create(user, cmd.items)

        # Notify other modules (multicast)
        self._bus.publish(OrderCreatedEvent(
            order_id=order.id,
            user_id=user["id"]
        ))

        return order
```

## Message Types

| Type | Pattern | Subscribers | Response |
|------|---------|-------------|----------|
| Query | 1:1 | 1 | Sync response |
| Command | 1:1 | 1 | None |
| Event | 1:N | Multiple | None |

## Performance

LocalMessageBus is essentially a registry + router, not a real message queue.

```
Direct function call:  ~100ns
LocalMessageBus:       ~180ns
Overhead:              ~80ns per call
```

For most applications, this overhead is negligible. The bottleneck is typically I/O (database, network), not the message bus.

### When to optimize

| Scenario | Recommendation |
|----------|----------------|
| Normal app (< 10K calls/sec) | LocalMessageBus is fine |
| Backtest (1M+ calls/sec) | Consider sync-only or direct calls |
| Production distributed | Use KafkaMessageBus |

## Swapping Implementations

Modules depend only on the `MessageBus` interface:

```python
# Development
bus = LocalMessageBus()

# Production (future)
bus = KafkaMessageBus(bootstrap_servers="kafka:9092")

# Same module code works with both
order_service = OrderService(bus)
```

## License

MIT
