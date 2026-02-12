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
├── ports.py         # 인터페이스: 분리된 Dispatcher/Registry ABC들
├── local.py         # 구현체: LocalMessageBus, AsyncLocalMessageBus
├── recording.py     # 미들웨어: RecordingBus, AsyncRecordingBus (dispatch 녹화)
├── middleware.py    # 미들웨어: MiddlewareBus, AsyncMiddlewareBus, Middleware ABCs
├── latency.py       # 미들웨어: LatencyMiddleware (지연 측정, separation signal)
├── retry.py         # 미들웨어: RetryMiddleware (exponential backoff retry)
├── timeout.py       # 미들웨어: TimeoutMiddleware (handler timeout enforcement)
├── dead_letter.py   # 미들웨어: DeadLetterMiddleware (실패 메시지 캡처)
├── testing.py       # 테스팅: FakeMessageBus, AsyncFakeMessageBus
└── zmq_bus.py       # 구현체: ZmqMessageBus, ZmqWorker (optional, pyzmq 필요)
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
| `RecordingBus` | 디스패치 녹화 (sync) | 데코레이터 패턴, MemoryStore/JsonLineStore |
| `AsyncRecordingBus` | 디스패치 녹화 (async) | 동일 RecordStore 사용, async context manager |
| `MiddlewareBus` | 미들웨어 체인 (sync) | 조합 가능한 미들웨어 (logging, auth, 등) |
| `AsyncMiddlewareBus` | 미들웨어 체인 (async) | async 미들웨어 지원 |
| `LatencyMiddleware` | 지연 측정 | 백분위수 통계, separation signal 감지 |
| `RetryMiddleware` | 재시도 | exponential backoff, 일시적 실패 복구 |
| `TimeoutMiddleware` | 타임아웃 | 핸들러 실행 시간 제한 |
| `DeadLetterMiddleware` | 실패 추적 | 실패한 메시지 캡처, DeadLetterStore에 저장 |
| `FakeMessageBus` | 테스팅 유틸리티 | 핸들러 없이 메시지 녹화, assertion 헬퍼 |
| `ZmqMessageBus` + `ZmqWorker` | 멀티프로세스 | PUSH/PULL, REQ/REP, PUB/SUB 패턴, pickle 직렬화 |

### 에러 규칙

- Query/Command/Task 중복 등록 시 `ValueError`
- 미등록 Query/Command/Task 호출 시 `LookupError`
- Event는 구독자 없어도 예외 없음 (silent)

### Recording (recording.py)

메시지 디스패치를 녹화하는 데코레이터/미들웨어. 에러 추적 및 디버깅 용도.

**컴포넌트:**

| 클래스 | 역할 |
| ------ | ---- |
| `Record` | 단일 디스패치 기록 (frozen dataclass) |
| `RecordStore` | 저장소 ABC (append + close) |
| `MemoryStore` | 인메모리 저장소 (테스트용) |
| `JsonLineStore` | JSONL 파일 저장소 (백그라운드 스레드, run별 파일 관리) |
| `RecordingBus` | sync MessageBus 데코레이터 |
| `AsyncRecordingBus` | async AsyncMessageBus 데코레이터 |

**사용 예시:**
```python
from message_bus import LocalMessageBus, RecordingBus, MemoryStore

inner = LocalMessageBus()
store = MemoryStore()
bus = RecordingBus(inner, store, record_mode="all")
# bus를 MessageBus처럼 사용 — 모든 디스패치가 store에 기록됨
```

**JsonLineStore (프로덕션):**
```python
from message_bus import LocalMessageBus, RecordingBus, JsonLineStore
from pathlib import Path

store = JsonLineStore(directory=Path("./logs/bus"), max_runs=10)
bus = RecordingBus(LocalMessageBus(), store)
# 앱 종료 시 bus.close() 또는 context manager 사용
```

## 확장 계획

1. **KafkaMessageBus** - 프로덕션 분산 환경용
