"""Profile PGN header fields and movetext features from a stream on stdin.

Usage:
    zstdcat data/raw/lichess_db_standard_rated_2013-02.pgn.zst | python3 -I scripts/profile_headers.py

Stdlib only on purpose: phase 0 runs before the uv project exists, and reading
from a zstdcat pipe works the same for a full file or a byte-range sample.
"""

from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict

HEADER_RE = re.compile(r'^\[(\w+) "((?:[^"\\]|\\.)*)"\]\s*$')
COMMENT_RE = re.compile(r"\{[^}]*\}")


def profile(lines) -> dict:
    present: Counter[str] = Counter()
    values: defaultdict[str, Counter[str]] = defaultdict(Counter)
    missing: Counter[str] = Counter()
    key_orders: Counter[tuple[str, ...]] = Counter()
    move = Counter()
    games = 0
    cur_keys: list[str] = []
    movetext: list[str] = []
    bad_header_lines = 0

    def finish() -> None:
        nonlocal games
        if not cur_keys:
            return
        games += 1
        key_orders[tuple(cur_keys)] += 1
        text = " ".join(movetext)
        if not text.strip():
            move["no_movetext_line"] += 1
        body = COMMENT_RE.sub("", text)
        tokens = [
            t
            for t in body.split()
            if not re.fullmatch(r"\d+\.+|1-0|0-1|1/2-1/2|\*|\$\d+", t)
        ]
        if not tokens:
            move["zero_moves"] += 1
        if "{" in text:
            move["has_comment"] += 1
        if "[%clk" in text:
            move["has_clk"] += 1
        if "[%eval" in text:
            move["has_eval"] += 1
        if "(" in body:
            move["has_variation"] += 1
        if re.search(r"\$\d+", body):
            move["has_nag"] += 1
        if re.search(r"[?!]", body):
            move["has_annotation_glyph"] += 1
        if len(movetext) > 1:
            move["movetext_multi_line"] += 1

    prev_blank = True
    for raw in lines:
        line = raw.rstrip("\n")
        if line.startswith("["):
            # A header after movetext, or a second [Event, starts a new game.
            if movetext or (cur_keys and line.startswith("[Event ")):
                if not prev_blank:
                    move["new_game_without_blank_before"] += 1
                finish()
                cur_keys, movetext = [], []
            m = HEADER_RE.match(line)
            if not m:
                bad_header_lines += 1
                continue
            k, v = m.group(1), m.group(2)
            cur_keys.append(k)
            present[k] += 1
            values[k][v] += 1
            if v in ("?", "", "????.??.??", "??:??:??"):
                missing[k] += 1
            if '\\"' in v:
                move["escaped_quote_in_header"] += 1
        elif line.strip():
            movetext.append(line)
        prev_blank = not line.strip()
    finish()
    return {
        "games": games,
        "present": present,
        "values": values,
        "missing": missing,
        "key_orders": key_orders,
        "move": move,
        "bad_header_lines": bad_header_lines,
    }


def report(p: dict) -> None:
    g = p["games"]
    print(f"games: {g}")
    print(f"unparseable header lines: {p['bad_header_lines']}\n")
    print("| key | present | present % | distinct | ?/empty | top 10 values |")
    print("|---|---|---|---|---|---|")
    for k, n in p["present"].most_common():
        top = ", ".join(f"`{v}` ({c})" for v, c in p["values"][k].most_common(10))
        print(
            f"| {k} | {n} | {100 * n / g:.2f} | {len(p['values'][k])} | {p['missing'][k]} | {top} |"
        )
    print("\nmovetext features (games):")
    for k, n in sorted(p["move"].items()):
        print(f"  {k}: {n} ({100 * n / g:.2f}%)")
    print("\nheader key orders:")
    for order, n in p["key_orders"].most_common(5):
        print(f"  {n}: {' '.join(order)}")


if __name__ == "__main__":
    report(profile(sys.stdin))
