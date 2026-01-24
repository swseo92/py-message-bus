#!/usr/bin/env python3
"""
Claude Multi-Perspective Code Review

pre-push hook에서 실행되어 여러 관점으로 코드 리뷰를 병렬 실행합니다.
모든 관점이 PASS해야 push가 진행됩니다.

사용법:
    python .claude/scripts/claude-review.py

구조:
    .claude-review/
    ├── meta.json                 # diff_hash
    ├── results/
    │   ├── security.json         # 관점별 결과
    │   └── bugs.json
    └── reports/
        ├── security.md           # 상세 보고서
        └── bugs.md

설정 파일:
    .claude/review-prompts.yaml - 프로젝트별 관점 정의
"""

import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import yaml

# Windows cp949 인코딩 문제 해결
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# 설정
GLOBAL_PROMPTS_FILE = Path.home() / ".claude" / "review-prompts.yaml"
PROJECT_PROMPTS_FILE = Path(".claude/review-prompts.yaml")
REVIEW_DIR = ".claude-review"
META_FILE = "meta.json"
RESULTS_DIR = "results"
REPORTS_DIR = "reports"
MAX_WORKERS = 4  # 병렬 실행 수

# 공통 프롬프트 (스크립트에서 주입)
COMMON_HEADER = "다음 diff를 **{name}** 관점에서 리뷰하세요."

COMMON_FOOTER = """
## 리뷰 보고서 형식

1. **요약**: 전체적인 상태 (1-2문장)
2. **발견된 이슈**: 각 이슈에 대해
   - 파일:라인
   - 심각도 (CRITICAL/HIGH/MEDIUM/LOW)
   - 설명
   - 수정 제안
3. **권장사항**: 추가 개선 제안 (선택)

사소한 것은 무시하고, 실제 위험이 있는 것만 FAIL 처리하세요.

마지막 줄에 반드시 `VERDICT: PASS` 또는 `VERDICT: FAIL`을 작성하세요.
"""


def get_review_dir() -> Path:
    """리뷰 디렉토리 경로."""
    return Path(REVIEW_DIR)


def get_diff() -> str:
    """push될 변경사항의 diff를 가져옵니다."""
    try:
        # 1. origin/main...HEAD (push될 커밋들)
        result = subprocess.run(
            ["git", "diff", "origin/main...HEAD"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.stdout.strip():
            return result.stdout

        # 2. origin/master...HEAD (main이 없는 경우)
        result = subprocess.run(
            ["git", "diff", "origin/master...HEAD"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.stdout.strip():
            return result.stdout

        # 3. staged changes (아직 커밋 안 된 경우)
        result = subprocess.run(
            ["git", "diff", "--cached"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.stdout.strip():
            return result.stdout

        # 4. unstaged changes (fallback)
        result = subprocess.run(
            ["git", "diff"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return result.stdout

    except Exception as e:
        print(f"Error getting diff: {e}")
        return ""


def get_diff_hash(diff: str) -> str:
    """diff의 hash를 계산합니다."""
    return hashlib.sha256(diff.encode()).hexdigest()[:12]


def load_prompts_from_file(path: Path) -> list[dict]:
    """파일에서 프롬프트를 로드합니다."""
    if not path.exists():
        return []

    try:
        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config.get("perspectives", []) if config else []
    except Exception as e:
        print(f"Warning: Failed to load {path}: {e}")
        return []


def load_prompts() -> list[dict]:
    """전역 + 프로젝트 프롬프트를 병합하여 로드합니다.

    병합 전략:
    - 전역 prompts 먼저 로드
    - 프로젝트 prompts로 오버라이드/추가
    - 같은 name이면 프로젝트 것 우선
    """
    # 1. 전역 prompts 로드
    global_prompts = load_prompts_from_file(GLOBAL_PROMPTS_FILE)

    # 2. 프로젝트 prompts 로드
    project_prompts = load_prompts_from_file(PROJECT_PROMPTS_FILE)

    # 둘 다 없으면 기본값 사용
    if not global_prompts and not project_prompts:
        print("Warning: No prompts files found. Using default prompts.")
        return get_default_prompts()

    # 3. 병합 (프로젝트가 전역을 오버라이드)
    merged = {p["name"]: p for p in global_prompts}
    for p in project_prompts:
        merged[p["name"]] = p  # 오버라이드 또는 추가

    # 로드 소스 표시
    global_names = [p["name"] for p in global_prompts]
    project_names = [p["name"] for p in project_prompts]
    if global_prompts:
        print(f"Global prompts: {global_names}")
    if project_prompts:
        print(f"Project prompts: {project_names}")

    return list(merged.values())


def get_default_prompts() -> list[dict]:
    """기본 리뷰 관점을 반환합니다."""
    return [
        {
            "name": "security",
            "prompt": """## 확인 항목
- SQL Injection, XSS, Command Injection 취약점
- 하드코딩된 시크릿, API 키, 비밀번호
- 인증/인가 누락 또는 우회 가능성
- 민감 정보 로깅
- 안전하지 않은 역직렬화
- Path Traversal 취약점""",
        },
        {
            "name": "bugs",
            "prompt": """## 확인 항목
- 논리 오류 (조건문, 루프)
- Null/None 참조 위험
- 예외 처리 누락
- 리소스 누수 (파일, DB 커넥션, 메모리)
- 경계 조건 오류 (off-by-one, 빈 배열)
- 동시성 문제 (race condition)""",
        },
    ]


def build_full_prompt(name: str, perspective_prompt: str, diff: str) -> str:
    """공통 프롬프트를 주입하여 전체 프롬프트를 생성합니다."""
    header = COMMON_HEADER.format(name=name)
    return f"{header}\n\n{perspective_prompt}\n{COMMON_FOOTER}\n\n--- DIFF ---\n{diff}"


def load_meta() -> dict:
    """meta.json을 로드합니다."""
    meta_path = get_review_dir() / META_FILE
    if meta_path.exists():
        with open(meta_path, encoding="utf-8") as f:
            return json.load(f)
    return {"diff_hash": None}


def save_meta(diff_hash: str):
    """meta.json을 저장합니다."""
    review_dir = get_review_dir()
    review_dir.mkdir(parents=True, exist_ok=True)

    meta_path = review_dir / META_FILE
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"diff_hash": diff_hash}, f, indent=2)


def initialize_review_dir(diff_hash: str):
    """리뷰 디렉토리를 초기화합니다 (hash 변경 시)."""
    review_dir = get_review_dir()
    results_dir = review_dir / RESULTS_DIR
    reports_dir = review_dir / REPORTS_DIR

    # results/ 비우기
    if results_dir.exists():
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    # reports/ 비우기
    if reports_dir.exists():
        shutil.rmtree(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    # meta.json 업데이트
    save_meta(diff_hash)

    print(f"Initialized review directory for hash: {diff_hash}")


def get_result_path(perspective: str) -> Path:
    """관점별 결과 파일 경로."""
    return get_review_dir() / RESULTS_DIR / f"{perspective}.json"


def get_report_path(perspective: str) -> Path:
    """관점별 보고서 파일 경로."""
    return get_review_dir() / REPORTS_DIR / f"{perspective}.md"


def load_result(perspective: str) -> dict | None:
    """관점별 결과를 로드합니다."""
    result_path = get_result_path(perspective)
    if result_path.exists():
        with open(result_path, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_result(perspective: str, result: str):
    """관점별 결과를 저장합니다."""
    result_path = get_result_path(perspective)
    result_path.parent.mkdir(parents=True, exist_ok=True)

    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({
            "result": result,
            "reviewed_at": datetime.now().isoformat()
        }, f, indent=2, ensure_ascii=False)


def save_report(perspective: str, content: str):
    """관점별 보고서를 저장합니다."""
    report_path = get_report_path(perspective)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)


def parse_verdict(output: str) -> str:
    """출력에서 VERDICT를 파싱합니다."""
    lines = output.strip().split("\n")

    # 마지막 몇 줄에서 VERDICT 찾기
    for line in reversed(lines[-10:]):
        line_upper = line.upper().strip()
        if "VERDICT:" in line_upper:
            if "PASS" in line_upper:
                return "PASS"
            elif "FAIL" in line_upper:
                return "FAIL"

    # VERDICT 없으면 전체에서 찾기
    output_upper = output.upper()
    if "VERDICT: PASS" in output_upper or "VERDICT:PASS" in output_upper:
        return "PASS"
    elif "VERDICT: FAIL" in output_upper or "VERDICT:FAIL" in output_upper:
        return "FAIL"

    # 파싱 실패 시 FAIL로 처리
    return "FAIL"


def run_claude_review(diff: str, perspective: dict) -> tuple[str, str, str]:
    """Claude로 특정 관점의 리뷰를 실행합니다.

    Returns:
        (perspective_name, result, report_content)
    """
    name = perspective["name"]
    prompt = perspective["prompt"]

    full_prompt = build_full_prompt(name, prompt, diff)

    try:
        result = subprocess.run(
            ["claude", "-p", full_prompt, "--output-format", "text"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,  # 5분 타임아웃
        )

        output = result.stdout.strip()

        if not output:
            output = f"Error: No output from Claude\nstderr: {result.stderr}"
            return (name, "FAIL", output)

        verdict = parse_verdict(output)
        return (name, verdict, output)

    except subprocess.TimeoutExpired:
        return (name, "FAIL", "Error: Review timeout (5분 초과)")
    except FileNotFoundError:
        return (name, "FAIL", "Error: claude CLI not found. Install with: npm install -g @anthropic-ai/claude-code")
    except Exception as e:
        return (name, "FAIL", f"Error: {e}")


def review_perspective(diff: str, perspective: dict) -> tuple[str, str]:
    """단일 관점 리뷰를 실행합니다.

    Returns:
        (perspective_name, result)
    """
    name = perspective["name"]

    # 이미 결과가 있으면 스킵
    existing = load_result(name)
    if existing:
        print(f"  [{name}] 캐시 사용 → {existing['result']}")
        return (name, existing["result"])

    # 리뷰 실행
    print(f"  [{name}] 리뷰 중...")
    name, verdict, report = run_claude_review(diff, perspective)

    # 보고서 저장
    save_report(name, report)
    print(f"  [{name}] 보고서 저장 → {get_report_path(name)}")

    # 결과 저장
    save_result(name, verdict)
    print(f"  [{name}] 완료 → {verdict}")

    return (name, verdict)


def collect_all_results(perspectives: list[dict]) -> dict[str, str]:
    """모든 관점의 결과를 수집합니다."""
    results = {}
    for p in perspectives:
        name = p["name"]
        result_data = load_result(name)
        if result_data:
            results[name] = result_data["result"]
        else:
            results[name] = "NOT_REVIEWED"
    return results


def main():
    print("=" * 60)
    print("Claude Multi-Perspective Code Review")
    print("=" * 60)

    # diff 가져오기
    diff = get_diff()
    if not diff:
        print("변경사항 없음. 통과.")
        sys.exit(0)

    diff_hash = get_diff_hash(diff)
    print(f"Diff hash: {diff_hash}")
    print(f"Diff size: {len(diff)} bytes")

    # 프롬프트 로드
    perspectives = load_prompts()
    print(f"리뷰 관점: {[p['name'] for p in perspectives]}")

    # meta.json 확인 및 초기화
    meta = load_meta()
    if meta.get("diff_hash") != diff_hash:
        print(f"Hash 변경 감지: {meta.get('diff_hash')} → {diff_hash}")
        initialize_review_dir(diff_hash)
    else:
        print("Hash 동일 - 기존 결과 사용 가능")

    print("-" * 60)

    # 병렬 리뷰 실행
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(review_perspective, diff, p): p
            for p in perspectives
        }
        for future in as_completed(futures):
            name, verdict = future.result()
            results[name] = verdict

    # 최종 결과 수집 (파일에서 다시 읽어서 확인)
    print("-" * 60)
    print("리뷰 결과:")

    final_results = collect_all_results(perspectives)
    all_passed = True

    for name, result in final_results.items():
        status = "PASS" if result == "PASS" else "FAIL"
        marker = "[O]" if result == "PASS" else "[X]"
        print(f"  {marker} [{name}] {status}")

        if result != "PASS":
            all_passed = False
            # 실패 시 보고서 위치 안내
            report_path = get_report_path(name)
            if report_path.exists():
                print(f"      → 상세 보고서: {report_path}")

    print("=" * 60)

    if all_passed:
        print("모든 관점 통과. Push를 진행합니다.")
        sys.exit(0)
    else:
        print("일부 관점에서 실패. Push가 차단됩니다.")
        print()
        print("=" * 60)
        print("[Claude Code 에이전트 안내]")
        print("=" * 60)
        print("다음 보고서를 읽고 이슈를 수정하세요:")
        print()
        for name, result in final_results.items():
            if result != "PASS":
                report_path = get_report_path(name)
                print(f"  Read: {report_path}")
        print()
        print("수정 후 다시 push하면 자동으로 재검증됩니다.")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
