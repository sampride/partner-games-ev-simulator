from __future__ import annotations

import argparse
import math
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", ".venv", "__pycache__", "data", "build", "dist", "wheels"}
SKIP_FILES = {"uv.lock"}
SKIP_SUFFIXES = {".pyc", ".pyo", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".zip"}
CONFIG_SUFFIXES = {".env", ".json", ".toml", ".yaml", ".yml"}
ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
KEY_VALUE_RE = re.compile(
    r"""(?ix)
    ^\s*
    (?P<key>[A-Za-z0-9_-]*(?:secret|password|api[_-]?key|access[_-]?token|refresh[_-]?token)[A-Za-z0-9_-]*)
    \s*[:=]\s*
    (?P<value>.+?)
    \s*(?:\#.*)?$
    """
)
ENV_KEY_RE = re.compile(
    r"""(?ix)
    ^\s*
    (?P<key>[A-Za-z0-9_-]+_env)
    \s*[:=]\s*
    (?P<value>.+?)
    \s*(?:\#.*)?$
    """
)
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")
PRIVATE_KEY_RE = re.compile(r"BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY")


def _run_git(args: list[str]) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _staged_files() -> list[Path]:
    files = _run_git(["diff", "--cached", "--name-only", "--diff-filter=ACMRT"])
    return [ROOT / path for path in files]


def _tracked_files() -> list[Path]:
    files = _run_git(["ls-files"])
    return [ROOT / path for path in files]


def _untracked_files() -> list[Path]:
    files = _run_git(["ls-files", "--others", "--exclude-standard"])
    return [ROOT / path for path in files]


def _candidate_files(all_files: bool) -> list[Path]:
    paths = [*_tracked_files(), *_untracked_files()] if all_files else _staged_files()
    return [path for path in paths if _should_scan(path)]


def _should_scan(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        relative = path.relative_to(ROOT)
    except ValueError:
        return False
    if any(part in SKIP_DIRS for part in relative.parts):
        return False
    if path.name in SKIP_FILES:
        return False
    return path.suffix.lower() not in SKIP_SUFFIXES


def _clean_value(raw: str) -> str:
    value = raw.strip().strip(",")
    if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
        value = value[1:-1]
    return value.strip()


def _looks_placeholder(value: str) -> bool:
    lowered = value.lower()
    return (
        not value
        or "<" in value
        or "example" in lowered
        or "placeholder" in lowered
        or lowered in {"changeme", "change-me", "secret", "password", "token"}
    )


def _entropy(value: str) -> float:
    if not value:
        return 0.0
    counts = {char: value.count(char) for char in set(value)}
    length = len(value)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())


def _find_issues(path: Path) -> list[tuple[int, str]]:
    issues: list[tuple[int, str]] = []
    is_config_like = path.suffix.lower() in CONFIG_SUFFIXES or path.name.startswith(".env")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return issues

    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if PRIVATE_KEY_RE.search(line):
            issues.append((line_number, "private key material"))
            continue
        if JWT_RE.search(line):
            issues.append((line_number, "JWT-like token"))
            continue

        if is_config_like:
            env_match = ENV_KEY_RE.match(line)
            if env_match:
                value = _clean_value(env_match.group("value"))
                if not _looks_placeholder(value) and not ENV_NAME_RE.fullmatch(value):
                    issues.append((line_number, f"{env_match.group('key')} is not an env var name"))
                continue

            key_match = KEY_VALUE_RE.match(line)
            if key_match:
                value = _clean_value(key_match.group("value"))
                if _looks_placeholder(value) or value.startswith("${"):
                    continue
                issues.append((line_number, f"literal value assigned to {key_match.group('key')}"))
                continue

            lowered = line.lower()
            if any(word in lowered for word in ("secret", "token", "password", "api_key", "apikey")):
                tokens = re.findall(r"[A-Za-z0-9_+/=-]{32,}", line)
                for token in tokens:
                    if _looks_placeholder(token):
                        continue
                    if _entropy(token) >= 4.0 and not ENV_NAME_RE.fullmatch(token):
                        issues.append((line_number, "high-entropy token-like value"))
                        break

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail if committed files appear to contain secrets.")
    parser.add_argument("--all", action="store_true", help="scan all tracked files instead of staged files")
    parser.add_argument("--staged", action="store_true", help="scan staged files; this is the default")
    args = parser.parse_args()

    findings: list[str] = []
    for path in _candidate_files(all_files=args.all):
        for line_number, reason in _find_issues(path):
            findings.append(f"{path.relative_to(ROOT)}:{line_number}: {reason}")

    if findings:
        print("Potential secrets found. Move credentials to environment variables or ignored local config.")
        for finding in findings:
            print(f"  {finding}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
