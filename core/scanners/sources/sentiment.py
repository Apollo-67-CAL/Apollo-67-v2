from __future__ import annotations

import re
from typing import Tuple

_POS = {
    "beat", "beats", "beating", "bull", "bullish", "upgrade", "upgraded", "surge", "surges", "surged",
    "growth", "record", "strong", "strength", "outperform", "buy", "adds", "added", "rally", "rallies",
    "profit", "profits", "profitable", "win", "wins", "winning", "positive", "breakout",
}
_NEG = {
    "miss", "misses", "missing", "bear", "bearish", "downgrade", "downgraded", "plunge", "plunges", "plunged",
    "weak", "weaker", "warning", "lawsuit", "fraud", "loss", "losses", "negative", "sell", "cut", "cuts",
    "crash", "dump", "bankrupt", "bankruptcy", "investigation",
}

_WORD = re.compile(r"[a-zA-Z]{2,}")


def score_text(text: str) -> int:
    words = _WORD.findall((text or "").lower())
    pos = sum(1 for word in words if word in _POS)
    neg = sum(1 for word in words if word in _NEG)
    return pos - neg


def classify(text: str) -> Tuple[int, int]:
    score = score_text(text)
    if score > 0:
        return 1, 0
    if score < 0:
        return 0, 1
    return 0, 0
