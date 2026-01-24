#!/usr/bin/env python3
"""
파일 쓰기 권한을 제한하는 Claude Code 훅.

화이트리스트 + 블랙리스트 동시 적용.
설정: .claude/path-guard.json

검사 순서:
  1. 하드코딩 보안 경로 → 차단 (우회 불가)
  2. always_block 패턴 → 차단
  3. blocked_paths 경로 → 차단
  4. always_allow 패턴 → 허용
  5. allowed_paths 경로 → 허용
  6. allowed_paths 비어있으면 → 허용 (블랙리스트만 사용)
  7. 그 외 → 차단

보안:
  - 이 스크립트와 설정 파일은 하드코딩으로 항상 차단됨
  - 설정 파일 수정으로 우회 불가

사용법 (settings.json):
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Write",
        "hooks": [{ "type": "command", "command": "python .claude/scripts/path-guard.py" }]
      },
      {
        "matcher": "Edit",
        "hooks": [{ "type": "command", "command": "python .claude/scripts/path-guard.py" }]
      }
    ]
  }
}
"""

import json
import sys
import os
import fnmatch
from pathlib import Path

# ============================================
# 하드코딩된 보안 경로 (설정으로 우회 불가)
# ============================================
HARDCODED_BLOCK = [
    ".claude/scripts/path-guard.py",
    ".claude/path-guard.json",
    ".claude/settings.json",
    ".claude/settings.local.json",
    ".pre-commit-config.yaml",
]


def get_working_dir() -> Path:
    """Claude 작업 디렉토리."""
    return Path(os.environ.get("CLAUDE_WORKING_DIRECTORY", os.getcwd()))


def load_config() -> dict:
    """프로젝트 설정 로드."""
    config_file = get_working_dir() / ".claude" / "path-guard.json"

    if config_file.exists():
        try:
            return json.loads(config_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print(json.dumps({
                "decision": "block",
                "reason": "path-guard.json 파싱 오류"
            }))
            sys.exit(0)

    # 설정 파일 없으면 기본값 (제한 없음)
    return {"enabled": False}


def normalize_path(file_path: str) -> Path:
    """경로 정규화."""
    p = Path(file_path)
    if not p.is_absolute():
        p = get_working_dir() / p
    return p.resolve()


def get_relative_path(file_path: Path) -> str:
    """작업 디렉토리 기준 상대 경로."""
    try:
        return str(file_path.relative_to(get_working_dir())).replace("\\", "/")
    except ValueError:
        return str(file_path).replace("\\", "/")


def matches_pattern(path: Path, pattern: str) -> bool:
    """glob 패턴 매칭."""
    name = path.name
    full = str(path).replace("\\", "/")
    rel = get_relative_path(path)

    # 패턴 정규화
    pattern = pattern.replace("\\", "/")

    return (
        fnmatch.fnmatch(name, pattern) or
        fnmatch.fnmatch(full, f"*{pattern}") or
        fnmatch.fnmatch(rel, pattern) or
        fnmatch.fnmatch(rel, f"*/{pattern}")
    )


def is_under_path(file_path: Path, paths: list[str]) -> bool:
    """지정된 경로 하위인지 확인."""
    working_dir = get_working_dir()

    for target in paths:
        target_path = Path(target.replace("\\", "/"))
        if not target_path.is_absolute():
            target_path = working_dir / target_path
        target_path = target_path.resolve()

        try:
            file_path.relative_to(target_path)
            return True
        except ValueError:
            continue

    return False


def check_hardcoded_block(file_path: Path) -> dict | None:
    """하드코딩된 보안 경로 확인 (우회 불가)."""
    rel_path = get_relative_path(file_path)

    for blocked in HARDCODED_BLOCK:
        if rel_path == blocked or rel_path.endswith(f"/{blocked}"):
            return {
                "decision": "block",
                "reason": (
                    f"[BLOCKED - SECURITY] {rel_path}\n"
                    f"보안상 수정이 금지된 파일입니다. 이 파일은 우회할 수 없습니다.\n"
                    f"정말 필요한 경우 사용자에게 직접 수정을 요청하세요."
                )
            }

    return None


def format_block_message(rel_path: str, reason: str, allowed_paths: list[str]) -> dict:
    """차단 메시지 포맷."""
    paths_str = ", ".join(allowed_paths) if allowed_paths else "(없음)"
    return {
        "decision": "block",
        "reason": (
            f"[BLOCKED] {rel_path}\n"
            f"사유: {reason}\n"
            f"허용된 경로: {paths_str}\n"
            f"\n"
            f"[모듈러 모놀리식 원칙]\n"
            f"다른 모듈의 코드를 직접 수정하지 마세요.\n"
            f"모듈 간 통신은 public API 또는 이벤트를 통해야 합니다.\n"
            f"\n"
            f"현재 모듈 내에서 문제를 해결할 방법을 먼저 찾으세요.\n"
            f"정말 불가피한 경우에만 사용자에게 권한 추가를 요청하세요."
        )
    }


def check_permission(file_path: str, config: dict) -> dict | None:
    """
    파일 쓰기 권한 확인.

    검사 순서 (블랙리스트 + 화이트리스트 동시 적용):
    1. 하드코딩 보안 경로 → 차단 (우회 불가)
    2. always_block 패턴 → 차단
    3. blocked_paths 경로 → 차단
    4. always_allow 패턴 → 허용
    5. allowed_paths 경로 → 허용
    6. allowed_paths 비어있으면 → 허용
    7. 그 외 → 차단
    """
    path = normalize_path(file_path)
    rel_path = get_relative_path(path)

    # 0. 하드코딩된 보안 경로 (최우선, 설정 무시)
    result = check_hardcoded_block(path)
    if result:
        return result

    always_block = config.get("always_block", [])
    always_allow = config.get("always_allow", [])
    blocked_paths = config.get("blocked_paths", [])
    allowed_paths = config.get("allowed_paths", [])

    # 1. 항상 차단 패턴 확인 (블랙리스트)
    for pattern in always_block:
        if matches_pattern(path, pattern):
            return format_block_message(
                rel_path,
                f"차단된 패턴 '{pattern}'과 일치",
                allowed_paths
            )

    # 2. 블랙리스트 경로 확인
    if is_under_path(path, blocked_paths):
        return format_block_message(
            rel_path,
            "차단된 경로(blocked_paths)에 포함됨",
            allowed_paths
        )

    # 3. 항상 허용 패턴 확인
    for pattern in always_allow:
        if matches_pattern(path, pattern):
            return None  # 허용

    # 4. 화이트리스트 경로 확인
    if allowed_paths:
        if is_under_path(path, allowed_paths):
            return None  # 허용
        else:
            return format_block_message(
                rel_path,
                "허용된 경로(allowed_paths)에 포함되지 않음",
                allowed_paths
            )

    # 5. allowed_paths가 비어있으면 모두 허용 (블랙리스트만 사용)
    return None


def main():
    config = load_config()

    # 비활성화 시 종료
    if not config.get("enabled", False):
        return

    event = json.load(sys.stdin)
    tool_input = event.get("tool_input", {})

    # 파일 경로 추출 (Write/Edit: file_path, NotebookEdit: notebook_path)
    file_path = tool_input.get("file_path") or tool_input.get("notebook_path", "")

    if not file_path:
        return

    result = check_permission(file_path, config)
    if result:
        print(json.dumps(result))


if __name__ == "__main__":
    main()
