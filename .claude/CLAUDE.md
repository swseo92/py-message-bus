# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

모듈러 모놀리스 아키텍처를 위한 경량 메시지 버스 라이브러리.

- Zero dependencies (코어)
- Type-safe (Generic 지원)
- Interface Segregation (필요한 인터페이스만 주입 가능)
- 직접 함수 호출 방식 (실제 메시지 큐잉 없음)

## 명령어

```bash
# 테스트
pytest

# 단일 테스트
pytest tests/test_local_bus.py::TestQueryHandling::test_send_query_returns_handler_result

# 린트
ruff check src tests
ruff format src tests

# 타입 체크
mypy src

# 설치 (개발 모드)
pip install -e ".[dev]"

# ZMQ 지원 설치
pip install -e ".[zmq]"
```

## 아키텍처

### 핵심 구조

```
src/message_bus/
├── ports.py    # 인터페이스: 분리된 Dispatcher/Registry ABC들
├── local.py    # 구현체: LocalMessageBus, AsyncLocalMessageBus
└── zmq_bus.py  # 구현체: ZmqMessageBus, ZmqWorker (optional, pyzmq 필요)
```

### Interface Segregation (ports.py)

**Sync 인터페이스:**

| 인터페이스        | 역할                         |
| ----------------- | ---------------------------- |
| `QueryDispatcher` | `send(query) -> T`           |
| `QueryRegistry`   | `register_query(type, handler)` |
| `MessageDispatcher` | `execute()`, `publish()`, `dispatch()` |
| `HandlerRegistry` | `register_command()`, `subscribe()`, `register_task()` |
| `MessageBus`      | 위 4개 모두 상속 (full interface) |

**Async 인터페이스:** 동일 구조에 `Async` 접두사 (`AsyncMessageBus` 등)

**사용 예시 - 필요한 인터페이스만 주입:**
```python
class OrderService:
    def __init__(self, queries: QueryDispatcher):  # send()만 필요
        self._queries = queries
```

### 메시지 타입

| 타입     | 패턴 | 구독자   | 응답   | 용도 |
| -------- | ---- | -------- | ------ | ---- |
| Query[T] | 1:1  | 1개 필수 | T 반환 | 조회 |
| Command  | 1:1  | 1개 필수 | 없음   | 상태 변경 |
| Event    | 1:N  | 0개 이상 | 없음   | 알림 |
| Task     | 1:1  | 1개 필수 | 없음   | 분산 작업 (워커용) |

### 구현체

| 구현체 | 용도 | 특징 |
| ------ | ---- | ---- |
| `LocalMessageBus` | 단일 프로세스 | dict 조회 + 직접 함수 호출, ~80ns 오버헤드 |
| `AsyncLocalMessageBus` | 단일 프로세스 (async) | asyncio.gather로 이벤트 병렬 처리 |
| `ZmqMessageBus` + `ZmqWorker` | 멀티프로세스 | PUSH/PULL, REQ/REP, PUB/SUB 패턴, pickle 직렬화 |

### 에러 규칙

- Query/Command/Task 중복 등록 시 `ValueError`
- 미등록 Query/Command/Task 호출 시 `LookupError`
- Event는 구독자 없어도 예외 없음 (silent)

## 확장 계획

1. **KafkaMessageBus** - 프로덕션 분산 환경용
