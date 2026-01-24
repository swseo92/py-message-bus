# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

모듈러 모놀리스 아키텍처를 위한 경량 메시지 버스 라이브러리.

- Zero dependencies
- Type-safe (Generic 지원)
- ~100줄 코어 코드
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
```

## 아키텍처

### 핵심 구조

```
src/message_bus/
├── ports.py    # 인터페이스: MessageBus(ABC), Query[T], Command, Event
└── local.py    # 구현체: LocalMessageBus
```

### MessageBus 인터페이스 (ports.py)

**등록 메서드 (부트스트랩 시점):**
- `register_query(query_type, handler)` - 1:1, 응답 반환
- `register_command(command_type, handler)` - 1:1, 응답 없음
- `subscribe(event_type, handler)` - 1:N, 다중 구독자

**디스패치 메서드 (런타임 시점):**
- `send(query) -> T` - Query 전송, 동기 응답
- `execute(command)` - Command 실행
- `publish(event)` - Event 발행 (멀티캐스트)

### LocalMessageBus는 "라우터"

- 실제 메시지 큐잉 없음
- dict 조회 + 직접 함수 호출
- 오버헤드: ~80ns/call

### 메시지 타입

| 타입 | 패턴 | 구독자 | 응답 |
|------|------|--------|------|
| Query[T] | 1:1 | 1개 필수 | T 반환 |
| Command | 1:1 | 1개 필수 | 없음 |
| Event | 1:N | 0개 이상 | 없음 |

### 에러 규칙

- Query/Command 중복 등록 시 `ValueError`
- 미등록 Query/Command 호출 시 `LookupError`
- Event는 구독자 없어도 예외 없음 (silent)

## 확장 계획

1. **AsyncLocalMessageBus** - async/await 지원
2. **KafkaMessageBus** - 프로덕션 분산 환경용
