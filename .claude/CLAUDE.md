# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

모듈러 모놀리스 아키텍처를 위한 경량 메시지 버스 라이브러리.

- Zero dependencies (코어 전용; Redis async 구현은 `redis>=5.0.0` 필요)
- Type-safe (Generic 지원)
- Interface Segregation (필요한 인터페이스만 주입 가능)
- 직접 함수 호출 방식 (LocalMessageBus) 또는 Redis Streams 기반 분산 처리 (AsyncRedisMessageBus)

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

# 벤치마크 (AsyncRedisMessageBus 성능 프로파일링, ROUNDS=10 다중 라운드 통계)
# 출력: mean/stddev/p95/p99/CV/CI95 칼럼 포함, Markdown 보고서 자동 생성
python benchmarks/bench_async_redis.py
# 벤치마크 (AsyncRedisMessageBus 메타데이터 모드 오버헤드 검증)
# 출력: legacy/standard/none 모드별 mean/stddev/CI95 비교 (ROUNDS=10)
python benchmarks/bench_metadata_mode.py
# 벤치마크 (AsyncRedisMessageBus 멱등성 오버헤드 검증)
python benchmarks/benchmark_idempotency.py
# 벤치마크 하네스 단위 테스트 (percentile, compute_stats, BenchStats 통계 헬퍼)
pytest tests/test_bench_harness.py
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
├── metrics.py       # 메트릭: MetricsSnapshot (처리량/에러 카운터)
├── testing.py       # 테스팅: FakeMessageBus, AsyncFakeMessageBus
├── redis_bus.py     # 구현체: RedisMessageBus, RedisWorker (sync, optional)
├── redis_dead_letter.py  # 구현체: RedisDeadLetterStore (Redis 기반 DLQ)
├── zmq_bus.py       # 구현체: ZmqMessageBus, ZmqWorker (optional, pyzmq 필요)
├── async_redis_bus.py  # 구현체: AsyncRedisMessageBus (optional, redis>=5.0.0 필요)
└── lua/
    └── ack_and_dedup.lua  # Lua 스크립트: XACK + SET NX 원자 연산 (멱등성 dedup)
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
| `AsyncRedisMessageBus` | 분산 async (Redis) | 아래 별도 섹션 참조 |

### AsyncRedisMessageBus (async_redis_bus.py)

Redis Streams 기반 분산 메시지 버스. Command/Task 경쟁 소비, Event fan-out, Query reply는 **Pub/Sub 2-RTT** (XADD→XREADGROUP→PUBLISH→push; shared PSUBSCRIBE + asyncio.Future).

**핵심 파라미터:**

| 파라미터 | 기본값 | 설명 |
| -------- | ------ | ---- |
| `query_reply_timeout` | `30.0` | Query 응답 대기 시간 (초) |
| `metadata_mode` | `"standard"` | `"standard"`: source_id/message_type/timestamp enrichment / `"none"`: dedup 필드만 |
| `command_idempotency` | `True` | Command 멱등성 ON |
| `task_idempotency` | `True` | Task 멱등성 ON |
| `event_idempotency` | `False` | Event 멱등성 OFF (기본) |
| `query_idempotency` | `False` | Query 멱등성 OFF (기본) |
| `dead_letter_store` | `None` | DLQ 스토어 설정 시 Consumer-level DLQ 직접 연동 활성화 |
| `max_retry` | `3` | 최대 재시도 횟수 (초과 시 DLQ 라우팅) |

**헬스체크 API:**
- `health_check()` → `{is_healthy, redis_connected, consumer_loop_active, pubsub_active, reconnect_count, last_reconnect_at, redis_error}`
- `is_healthy()` → `bool`

**안정성 기능:**
- 자동 재연결 (`_reconnect()`) — Redis + Pub/Sub 재구독 포함, `reconnect_count`/`last_reconnect_at` 추적
- Pub/Sub Reply Listener 자동 재구독 — 연결 끊김 시 자동 복구 루프
- `_release_dedup_key` exponential backoff retry (3회, 1s→2s→4s)
- Consumer-level DLQ — `max_retry` 초과 시 Consumer 루프에서 즉시 DLQ 라우팅 (XAUTOCLAIM 대기 없이)
- `ack_and_dedup.lua` Lua 스크립트로 XACK + SET NX 원자 연산 (1 RTT 절감)
- Circuit Breaker 미적용 (Redis Streams 내구성 보장 + 핸들러 레이어 권장)

### 에러 규칙

- Query/Command/Task 중복 등록 시 `ValueError`
- 미등록 Query/Command/Task 호출 시 `LookupError` (LocalMessageBus 계열)
- `AsyncRedisMessageBus.send()`: 핸들러가 없으면 `TimeoutError` (분산 환경에서는 로컬 체크 불가)
- `AsyncRedisMessageBus.send()`: 핸들러 예외는 `RuntimeError`로 전파 (error_type: error_message 형식)
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

## 테스트 구조

```
tests/
├── test_local_bus.py          # LocalMessageBus 단위 테스트
├── test_async_local_bus.py    # AsyncLocalMessageBus 단위 테스트
├── test_async_redis_bus.py    # AsyncRedisMessageBus 단위 테스트 (fakeredis)
├── test_redis_bus.py          # RedisMessageBus 단위 테스트 (실제 Redis 필요)
├── test_bench_harness.py      # 벤치마크 통계 헬퍼 테스트
├── e2e/
│   └── test_async_redis_e2e.py  # E2E (실제 Redis 필요)
├── integration/
│   └── test_dead_letter_integration.py
└── contract/
    └── messages.py            # 공유 메시지 정의
```

## Gotchas

- **`test_redis_bus.py` 전체 스위트 hang**: `test_send_query_returns_response`가 실제 Redis가 필요하여 fakeredis 환경에서 timeout → 전체 pytest 세션이 멈출 수 있음. 해결: `pytest tests/test_async_redis_bus.py` 처럼 특정 파일만 실행
- **redis `py.typed` 호환성**: redis 7.2.1+ fresh install 시 `py.typed` 마커 포함 → 기존 `type: ignore` 주석이 `unused-ignore` 오류 발생. 이미 정리 완료 (PR #44)
- **CI mypy `.` vs `src`**: CI는 `mypy .` (전체)을 실행하여 tests/benchmarks의 arg-type 오류도 검출됨. 로컬에서는 `mypy src`로 소스만 검사하는 것이 실용적

## 확장 계획

1. **KafkaMessageBus** - 프로덕션 분산 환경용
