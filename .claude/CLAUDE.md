# CLAUDE.md

py-message-bus 프로젝트 가이드.

## 프로젝트 개요

모듈러 모놀리스 아키텍처를 위한 경량 메시지 버스 라이브러리.

**핵심 특징:**
- Zero dependencies
- Type-safe (Generic 지원)
- ~100줄 코어 코드
- 직접 함수 호출 방식 (실제 메시지 큐잉 없음)

## 프로젝트 구조

```
py-message-bus/
├── src/message_bus/
│   ├── __init__.py     # 패키지 export
│   ├── ports.py        # MessageBus 인터페이스, Query/Command/Event 베이스
│   └── local.py        # LocalMessageBus 구현
└── tests/
    └── test_local_bus.py
```

## 명령어

```bash
# 테스트
pytest

# 린트
ruff check src tests
ruff format src tests

# 타입 체크
mypy src

# 설치 (개발 모드)
pip install -e ".[dev]"
```

## 아키텍처 결정

### 1. LocalMessageBus는 "버스"가 아니라 "라우터"
- 실제 메시지 큐잉 없음
- dict 조회 + 직접 함수 호출
- 오버헤드: ~80ns/call

### 2. 동기 전용
- async 버전은 별도 클래스로 분리 예정
- 백테스트 등 CPU 바운드 작업에서 async는 오버헤드만 추가

### 3. 메시지 타입
| 타입 | 패턴 | 구독자 | 응답 |
|------|------|--------|------|
| Query | 1:1 | 1개 | 동기 응답 |
| Command | 1:1 | 1개 | 없음 |
| Event | 1:N | 여러 개 | 없음 |

## 확장 계획

1. **AsyncLocalMessageBus** - async/await 지원
2. **KafkaMessageBus** - 프로덕션 분산 환경용
3. **벤치마크** - 성능 비교 테스트

## 테스트 원칙

- 모든 public API에 대한 테스트 필수
- 에러 케이스 테스트 포함
- pytest 마커: `unit`, `integration`
