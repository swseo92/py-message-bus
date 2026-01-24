"""Common message types for contract tests."""

from dataclasses import dataclass

from message_bus import Command, Event, Query, Task


@dataclass(frozen=True)
class GetUserQuery(Query[dict]):
    """Query to get user by ID."""

    user_id: str


@dataclass(frozen=True)
class CreateOrderCommand(Command):
    """Command to create an order."""

    user_id: str
    item: str


@dataclass(frozen=True)
class OrderCreatedEvent(Event):
    """Event when order is created."""

    order_id: str
    user_id: str


@dataclass(frozen=True)
class SendEmailTask(Task):
    """Task to send email."""

    email: str
    subject: str


@dataclass(frozen=True)
class GetOptionalQuery(Query[dict | None]):
    """Query that may return None.

    Note: `dict | None` syntax requires Python 3.10+.
    This project requires Python 3.11+ (see pyproject.toml).
    """

    key: str
