from __future__ import annotations

import math
from typing import Tuple

from colorama import Fore


def pct_change(new: float, base: float) -> float:
    try:
        if not base or not math.isfinite(base):
            return 0.0
        return (new - base) / base * 100.0
    except Exception:
        return 0.0


def clamp(val: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(max_val, val))


def fmt_vol(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        return str(v)
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}K"
    return f"{v:.2f}"


def grade(score: int) -> Tuple[str, str]:
    if score >= 75:
        return "A", Fore.GREEN
    if score >= 60:
        return "B", Fore.LIGHTGREEN_EX
    if score >= 45:
        return "C", Fore.YELLOW
    return "D", Fore.RED
