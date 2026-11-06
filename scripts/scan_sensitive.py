#!/usr/bin/env python3
"""Pre-push scan: fail if any committed text file contains a deny-listed term.

Keep your deny-list OUTSIDE git (it is itself sensitive): one term per line in
.sensitive-terms.txt (gitignored) - internal workspace, lakehouse, capacity,
model, notebook, queue and department names, colleague names, etc.

Usage:
    python scripts/scan_sensitive.py               # scan repo
    python scripts/scan_sensitive.py --terms path  # custom deny-list
Hook it up:  echo 'python scripts/scan_sensitive.py' > .git/hooks/pre-push && chmod +x .git/hooks/pre-push
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

TEXT_EXT = {".py", ".sql", ".dax", ".md", ".html", ".css", ".js", ".json", ".yml", ".yaml", ".txt", ".csv", ".ipynb"}
SKIP_DIRS = {".git", ".venv", "venv", "__pycache__", "node_modules", ".pytest_cache"}

# Generic patterns worth a second look even without a deny-list.
GENERIC_PATTERNS = {
    "SSN-like": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "MRN-like label": re.compile(r"\bMRN\s*[:#=]\s*\w+", re.IGNORECASE),
    "Fabric workspace id": re.compile(r"\bonelake\.dfs\.fabric\.microsoft\.com/[0-9a-f-]{36}", re.IGNORECASE),
}


def load_terms(path: Path) -> list[str]:
    if not path.exists():
        print(f"warning: deny-list {path} not found - only generic patterns will be checked", file=sys.stderr)
        return []
    return [t.strip() for t in path.read_text(encoding="utf-8").splitlines() if t.strip() and not t.startswith("#")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=Path(__file__).resolve().parents[1], type=Path)
    ap.add_argument("--terms", default=None, type=Path)
    args = ap.parse_args()
    terms_path = args.terms or args.root / ".sensitive-terms.txt"
    terms = load_terms(terms_path)
    # whole-word match: a short code like "OPS" must not fire inside "Developer"
    term_res = [
        (t, re.compile(r"(?<![A-Za-z0-9])" + re.escape(t) + r"(?![A-Za-z0-9])", re.IGNORECASE))
        for t in terms
    ]

    hits = 0
    for f in sorted(args.root.rglob("*")):
        if not f.is_file() or f.suffix.lower() not in TEXT_EXT or f.resolve() == terms_path.resolve():
            continue
        if any(part in SKIP_DIRS for part in f.relative_to(args.root).parts):
            continue
        for n, line in enumerate(f.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            for label, rx in [*term_res, *GENERIC_PATTERNS.items()]:
                if rx.search(line):
                    hits += 1
                    print(f"{f.relative_to(args.root)}:{n}: [{label}] {line.strip()[:120]}")
    print(f"{hits} potential issue(s)")
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main())
