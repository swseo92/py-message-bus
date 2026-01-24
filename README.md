# py-message-bus

모듈러 모놀리스 아키텍처를 위한 경량 메시지 버스.

## 특징

- **Zero dependencies** - 순수 Python, 외부 패키지 불필요
- **Type-safe** - Generic을 활용한 완전한 타입 지원
- **Simple** - 코어 코드 ~100줄
- **Fast** - 직접 함수 호출, 디스패치당 ~80ns 오버헤드
- **Swappable** - Local, Kafka, Redis 등 동일 인터페이스로 교체 가능

## 설치

```bash
pip install py-message-bus
```

## 빠른 시작

### 메시지 정의

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

### 핸들러 등록

```python
from message_bus import LocalMessageBus

bus = LocalMessageBus()

# Query: 1:1, 응답 반환
bus.register_query(GetUserQuery, user_service.get_user)

# Command: 1:1, 응답 없음
bus.register_command(CreateOrderCommand, order_service.create)

# Event: 1:N, 다중 구독자
bus.subscribe(OrderCreatedEvent, inventory_service.on_order_created)
bus.subscribe(OrderCreatedEvent, notification_service.on_order_created)
```

### 모듈에서 사용

```python
class OrderService:
    def __init__(self, bus: MessageBus):
        self._bus = bus

    def create_order(self, cmd: CreateOrderCommand):
        # 다른 모듈에 쿼리 (동기 응답)
        user = self._bus.send(GetUserQuery(cmd.user_id))

        order = Order.create(user, cmd.items)

        # 다른 모듈에 알림 (멀티캐스트)
        self._bus.publish(OrderCreatedEvent(
            order_id=order.id,
            user_id=user["id"]
        ))

        return order
```

## 메시지 타입

| 타입 | 패턴 | 구독자 | 응답 |
|------|------|--------|------|
| Query | 1:1 | 1개 | 동기 응답 |
| Command | 1:1 | 1개 | 없음 |
| Event | 1:N | 여러 개 | 없음 |

## 성능

LocalMessageBus는 실제 메시지 큐가 아닌 레지스트리 + 라우터입니다.

```
직접 함수 호출:      ~100ns
LocalMessageBus:    ~180ns
오버헤드:           ~80ns per call
```

대부분의 애플리케이션에서 이 오버헤드는 무시할 수준입니다. 병목은 보통 I/O(데이터베이스, 네트워크)이지 메시지 버스가 아닙니다.

### 최적화 시점

| 시나리오 | 권장사항 |
|----------|----------|
| 일반 앱 (< 10K calls/sec) | LocalMessageBus로 충분 |
| 백테스트 (1M+ calls/sec) | 동기 전용 또는 직접 호출 고려 |
| 프로덕션 분산 환경 | KafkaMessageBus 사용 |

## 구현체 교체

모듈은 `MessageBus` 인터페이스에만 의존합니다:

```python
# 개발 환경
bus = LocalMessageBus()

# 프로덕션 (예정)
bus = KafkaMessageBus(bootstrap_servers="kafka:9092")

# 동일한 모듈 코드가 둘 다에서 동작
order_service = OrderService(bus)
```

## 라이선스

MIT
