#!/usr/bin/env python3
"""
Phemex Dual Scanner — runs LONG + SHORT in parallel, then prints a unified report.

Usage:
  python3 phemex_scan.py [options]

Passes all recognised flags to both scanners; direction-specific flags
(--long-only / --short-only) control which scanners run.

Options:
  --timeframe TF       Candle timeframe (default: 15m)
  --min-vol VOL        Min 24h USDT turnover (default: 1,000,000)
  --top N              Top N results per direction (default: 20)
  --min-score S        Minimum score to show (default: 130)
  --workers N          Worker threads per scanner (default: 100)
  --rate R             Requests/sec per scanner (default: 100.0)
  --no-ai              Disable DeepSeek AI commentary
  --no-entity          Disable Entity API persistence
  --write-json         Write last_scan_{long,short}.json
  --long-only          Run only the long scanner
  --short-only         Run only the short scanner
  --combined N         Show unified top-N across both directions (0 = off)
  --debug              Enable debug logging
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import List, Tuple

# ── colour setup ────────────────────────────────────────────────────
from colorama import init, Fore, Style
from dotenv import load_dotenv

load_dotenv()
init(autoreset=True)

# ── import both scanner modules (must live in same directory) ────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

try:
    from legacy import phemex_common as pc
    import long as scanner_long
    import short as scanner_short
except ImportError as e:
    print(Fore.RED + f"[ERROR] Could not import scanner modules: {e}")
    print(Fore.RED + "  Make sure long.py and short.py are in the same directory.")
    sys.exit(1)


# ────────────────────────────────────────────────────────────────────
# Helpers shared by the display layer
# ────────────────────────────────────────────────────────────────────

BANNER_WIDTH = 92

def fmt_vol(v: float) -> str:
    return pc.fmt_vol(v)

def grade(score: int) -> Tuple[str, str]:
    return pc.grade(score)

def hr(char: str = "─", width: int = BANNER_WIDTH) -> str:
    return char * width


# ────────────────────────────────────────────────────────────────────
# Scanning helpers
# ────────────────────────────────────────────────────────────────────

_print_lock = threading.Lock()


def run_scan(scanner_module, direction: str, cfg: dict, args, tickers: List[dict]) -> List[dict]:
    """
    Executes a scan for a specific direction (LONG/SHORT) using the provided scanner module.
    
    Args:
        scanner_module: The module (phemex_long or phemex_short) to use for analysis.
        direction: String indicating scan direction ('LONG' or 'SHORT').
        cfg: Configuration dictionary containing volume and rate limit settings.
        args: Parsed command-line arguments.
        tickers: Pre-filtered list of tickers to analyse.
        
    Returns:
        A list of result dictionaries for instruments passing the scan criteria.
    """
    rps = cfg.get("RATE_LIMIT_RPS", 8.0)
    enable_ai = not args.no_ai
    enable_entity = not args.no_entity

    # Use the pre-filtered tickers passed from main() instead of fetching them again.
    filtered = tickers
    total = len(filtered)
    if total == 0:
        return []

    results: List[dict] = []
    done = 0
    lock = threading.Lock()
    arrow = "▲" if direction == "LONG" else "▼"
    dir_color = Fore.GREEN if direction == "LONG" else Fore.RED
    # Estimate: ~4 requests per ticker (Klines TF, Orderbook, Klines 1H, plus some overhead - funding is prefetched)
    est_total_s = (total * 4) / rps if rps > 0 else 0
    start_t = time.time()

    if hasattr(scanner_module, "prefetch_all_funding_rates"):
        scanner_module.prefetch_all_funding_rates(rps=rps)

    def _task(ticker_item):
        nonlocal done
        r = scanner_module.analyse(
            ticker_item, cfg,
            enable_ai=enable_ai,
            enable_entity=enable_entity,
            scan_id=None,
        )
        with lock:
            if r:
                results.append(r)
            done += 1
            pct = done / total
            bar = "█" * int(pct * 28) + "░" * (28 - int(pct * 28))
            # Calculate remaining time based on theoretical RPS
            rem_s = max(0, (total - done) * 4 / rps) if rps > 0 else 0
            elapsed = time.time() - start_t
            
            with _print_lock:
                sys.stdout.write(
                    f"\r  {dir_color}{arrow} {direction:<5}{Style.RESET_ALL}  "
                    f"[{bar}] {done}/{total} ({pct*100:.0f}%)  "
                    f"Hits: {len(results)}  "
                    f"Elapsed: {elapsed:.1f}s  "
                    f"Est: {rem_s:.1f}s rem"
                )
                sys.stdout.flush()
        return r

    workers = min(cfg["MAX_WORKERS"], max(1, total))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
        futures = [exe.submit(_task, t) for t in filtered]
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                # Log the exception instead of silently passing
                logging.getLogger("phemex_scanner").warning(f"Worker exception: {e}")

    print()  # newline after progress bar
    return results


# ────────────────────────────────────────────────────────────────────
# Display — per-direction card
# ────────────────────────────────────────────────────────────────────

def print_direction_results(
    results: List[dict],
    direction: str,
    cfg: dict,
    limit: int,
):
    """
    Formats and prints the top scan results for a specific direction.
    
    Args:
        results: List of scan result dictionaries.
        direction: String indicating the direction ('LONG' or 'SHORT').
        cfg: Configuration dictionary containing score thresholds and timeframes.
        limit: Maximum number of results to display.
    """
    dir_color = Fore.GREEN if direction == "LONG" else Fore.RED
    arrow = "▲ LONG SETUPS" if direction == "LONG" else "▼ SHORT SETUPS"
    funding_label = "Neg=Bullish" if direction == "LONG" else "Pos=Bearish"

    sorted_r = sorted(results, key=lambda x: x["score"], reverse=True)
    top = [r for r in sorted_r[:limit] if r["score"] >= cfg["MIN_SCORE"]]

    print(dir_color + Style.BRIGHT + hr("═"))
    print(dir_color + Style.BRIGHT + f"  {arrow}  ({cfg['TIMEFRAME']} | {len(top)} shown of {len(results)} hits)")
    print(dir_color + Style.BRIGHT + hr("═"))

    if not top:
        print(Fore.YELLOW + "  No setups pass the minimum score threshold.\n")
        return

    for i, r in enumerate(top, 1):
        g, gc = grade(r["score"])
        conf = r.get("confidence", "N/A")
        cc = r.get("conf_color", Fore.WHITE)

        fp = r.get("funding_pct")
        if fp is None:
            fp_str = "N/A"
            fp_color = Fore.WHITE
        else:
            fp_str = f"{fp:+.4f}%"
            if direction == "LONG":
                fp_color = Fore.GREEN if fp < -0.01 else (Fore.RED if fp > 0.05 else Fore.WHITE)
            else:
                fp_color = Fore.RED if fp > 0.05 else (Fore.GREEN if fp < -0.01 else Fore.WHITE)

        rsi_val = r.get("rsi")
        rsi_str = f"{rsi_val:.1f}" if rsi_val is not None else "N/A"

        print(gc + Style.BRIGHT + f"  {'─'*88}")
        print(gc + Style.BRIGHT +
              f"  #{i:02d}  {r['inst_id']:<16} Grade: {g}  Score: {r['score']:>3}  "
              f"Price: {r['price']:.4g}  "
              f"Change: {r.get('change_24h', 0):+.2f}%")

        print(f"       RSI: {rsi_str:<8}  "
              f"Funding: {fp_color}{fp_str}{Style.RESET_ALL} ({funding_label})  "
              f"Vol 24h: {pc.fmt_vol(r.get('vol_24h', 0))}  "
              f"Confidence: {cc}{conf}{Style.RESET_ALL}")

        # BB / EMA line
        bb_pct = r.get("bb_pct")
        ema21 = r.get("ema21")
        bb_str = f"BB%: {bb_pct:.1f}" if bb_pct is not None else ""
        ema_str = f"EMA21: {ema21:.4g}" if ema21 is not None else ""
        vol_spike = r.get("vol_spike", 1.0)
        atr_stop = r.get("atr_stop_pct")
        extras = "  ".join(filter(None, [
            bb_str,
            f"BB Width: {r.get('bb_width', 0):.2f}%" if r.get("bb_width") else None,
            ema_str,
            f"Vol Spike: {vol_spike:.2f}x",
            f"ATR Stop: {atr_stop:.2f}%" if atr_stop else None,
        ]))
        if extras:
            print(f"       {extras}")

        # Flags
        if r.get("conf_notes"):
            print(Fore.YELLOW + f"       ⚑  {', '.join(r['conf_notes'])}")

        # Key signals (top 5)
        signals = r.get("signals", [])
        if signals:
            print(dir_color + "       Signals:")
            for sig in signals[:5]:
                print(dir_color + f"         • {sig}")
            if len(signals) > 5:
                print(dir_color + f"         … +{len(signals)-5} more")

        # News
        if r.get("news_count", 0) > 0 and r.get("news_titles"):
            print(Fore.YELLOW + f"       📰 {r['news_count']} news items: {r['news_titles'][0][:70]}")

        print()


# ────────────────────────────────────────────────────────────────────
# Display — combined top-N across both directions
# ────────────────────────────────────────────────────────────────────

def print_combined(long_results: List[dict], short_results: List[dict], n: int, cfg: dict):
    """
    Formats and prints a unified table showing the top-N results across both directions.
    
    Args:
        long_results: List of result dictionaries from the long scanner.
        short_results: List of result dictionaries from the short scanner.
        n: Number of top results to display.
        cfg: Configuration dictionary.
    """
    if n <= 0:
        return

    tagged_long  = [dict(r, _dir="LONG")  for r in long_results]
    tagged_short = [dict(r, _dir="SHORT") for r in short_results]
    combined = sorted(tagged_long + tagged_short, key=lambda x: x["score"], reverse=True)
    top = [r for r in combined[:n] if r["score"] >= cfg["MIN_SCORE"]]

    print(Fore.CYAN + Style.BRIGHT + hr("═"))
    print(Fore.CYAN + Style.BRIGHT + f"  ⚡ COMBINED TOP {n} — HIGHEST SCORE ACROSS BOTH DIRECTIONS")
    print(Fore.CYAN + Style.BRIGHT + hr("═"))

    if not top:
        print(Fore.YELLOW + "  Nothing passes minimum score.\n")
        return

    # Header row
    print(Fore.CYAN +
          f"  {'#':>3}  {'Symbol':<16} {'Dir':^6} {'Gr':^4} {'Score':>5}  "
          f"{'Price':>10}  {'Chg%':>6}  {'RSI':>5}  {'Funding%':>9}  "
          f"{'Vol 24h':>8}  {'Conf':<7}")
    print(Fore.CYAN + hr("─"))

    for i, r in enumerate(top, 1):
        direction = r.get("_dir", "?")
        dir_color = Fore.GREEN if direction == "LONG" else Fore.RED
        dir_sym   = "▲" if direction == "LONG" else "▼"

        g, gc = grade(r["score"])
        fp = r.get("funding_pct")
        fp_str = f"{fp:+.5f}" if fp is not None else "   N/A  "

        rsi_val = r.get("rsi")
        rsi_str = f"{rsi_val:.1f}" if rsi_val is not None else " N/A"

        conf = r.get("confidence", "N/A")
        cc = r.get("conf_color", Fore.WHITE)

        print(
            f"  {Fore.WHITE}{i:>3}{Style.RESET_ALL}  "
            f"{r['inst_id']:<16} "
            f"{dir_color}{dir_sym} {direction:<4}{Style.RESET_ALL} "
            f"{gc}{g:^4}{Style.RESET_ALL} "
            f"{gc}{r['score']:>5}{Style.RESET_ALL}  "
            f"{r['price']:>10.4g}  "
            f"{r.get('change_24h', 0):>+6.2f}  "
            f"{rsi_str:>5}  "
            f"{fp_str:>9}  "
            f"{pc.fmt_vol(r.get('vol_24h', 0)):>8}  "
            f"{cc}{conf:<7}{Style.RESET_ALL}"
        )

    print()


# ────────────────────────────────────────────────────────────────────
# Summary stats
# ────────────────────────────────────────────────────────────────────

def print_summary(long_results: List[dict], short_results: List[dict], elapsed: float, cfg: dict):
    """
    Prints a concise summary of the scan results, including hit counts and average scores.
    
    Args:
        long_results: List of results from the long scanner.
        short_results: List of results from the short scanner.
        elapsed: Time taken for the scan in seconds.
        cfg: Configuration dictionary.
    """
    def grade_counts(results):
        counts = {"A": 0, "B": 0, "C": 0, "D": 0}
        for r in results:
            g, _ = grade(r["score"])
            counts[g] = counts.get(g, 0) + 1
        return counts

    lg = grade_counts(long_results)
    sg = grade_counts(short_results)

    avg_long  = (sum(r["score"] for r in long_results)  / len(long_results)  if long_results  else 0)
    avg_short = (sum(r["score"] for r in short_results) / len(short_results) if short_results else 0)

    print(Fore.WHITE + Style.BRIGHT + hr("═"))
    print(Fore.WHITE + Style.BRIGHT + "  SCAN SUMMARY")
    print(Fore.WHITE + Style.BRIGHT + hr("─"))
    print(f"  Timeframe : {cfg['TIMEFRAME']}     Min Volume: {pc.fmt_vol(cfg['MIN_VOLUME'])} USDT     "
          f"Completed: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Elapsed   : {elapsed:.1f}s\n")

    for label, results, lc, gc_list in [
        ("LONG  ▲", long_results,  Fore.GREEN, lg),
        ("SHORT ▼", short_results, Fore.RED,   sg),
    ]:
        total_dir = len(results)
        avg = (sum(r["score"] for r in results) / total_dir) if total_dir else 0
        best = max(results, key=lambda x: x["score"]) if results else None

        gc_a = Fore.GREEN if gc_list["A"] else Fore.WHITE
        gc_b = Fore.LIGHTGREEN_EX if gc_list["B"] else Fore.WHITE
        gc_c = Fore.YELLOW if gc_list["C"] else Fore.WHITE
        gc_d = Fore.RED if gc_list["D"] else Fore.WHITE

        print(lc + Style.BRIGHT + f"  {label}:")
        print(f"    Hits  : {total_dir}   Avg Score: {avg:.1f}   "
              f"Grades → "
              f"{gc_a}A:{gc_list['A']}{Style.RESET_ALL}  "
              f"{gc_b}B:{gc_list['B']}{Style.RESET_ALL}  "
              f"{gc_c}C:{gc_list['C']}{Style.RESET_ALL}  "
              f"{gc_d}D:{gc_list['D']}{Style.RESET_ALL}")
        if best:
            g, gc = grade(best["score"])
            print(f"    Best  : {gc}{best['inst_id']}{Style.RESET_ALL}  "
                  f"Score {gc}{best['score']}{Style.RESET_ALL}  Grade {gc}{g}{Style.RESET_ALL}  "
                  f"RSI {best.get('rsi', 0) or 0:.1f}  "
                  f"Funding {best.get('funding_pct') or 0:+.4f}%")
        print()

    print(Fore.WHITE + Style.BRIGHT + hr("─"))
    print(Fore.YELLOW + "  ⚠  Scanner output is NOT financial advice.")
    print(Fore.YELLOW + "     Confirm all setups on the chart with proper risk management.")
    print(Fore.WHITE + Style.BRIGHT + hr("═"))
    print()


# ────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Phemex Dual Scanner — runs LONG + SHORT scanners in parallel",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--timeframe",  default="15m",       help="Candle timeframe")
    parser.add_argument("--min-vol",    type=int, default=1_000_000, help="Min 24h USDT turnover")
    parser.add_argument("--top",        type=int, default=20,  help="Top N per direction")
    parser.add_argument("--min-score",  type=int, default=130, help="Min score to display")
    parser.add_argument("--workers",    type=int, default=100,   help="Worker threads per scanner")
    parser.add_argument("--rate",       type=float, default=100.0, help="Requests/sec per scanner")
    parser.add_argument("--combined",   type=int, default=10,  help="Unified top-N table (0 = off)")
    parser.add_argument("--no-ai",      action="store_true",   help="Disable DeepSeek AI")
    parser.add_argument("--no-entity",  action="store_true",   help="Disable Entity API")
    parser.add_argument("--write-json", action="store_true",   help="Write JSON result files")
    parser.add_argument("--long-only",  action="store_true",   help="Run only long scanner")
    parser.add_argument("--short-only", action="store_true",   help="Run only short scanner")
    parser.add_argument("-time", "--time", action="store_true", help="Print estimated scan duration and exit")
    parser.add_argument("--debug",      action="store_true",   help="Enable debug logging")
    args = parser.parse_args()

    if args.long_only and args.short_only:
        print(Fore.RED + "[ERROR] --long-only and --short-only are mutually exclusive.")
        sys.exit(1)

    timeframe_map = pc.TIMEFRAME_MAP
    if args.timeframe not in timeframe_map:
        print(Fore.RED + f"[ERROR] Unknown timeframe '{args.timeframe}'. "
              f"Valid: {list(timeframe_map.keys())}")
        sys.exit(1)

    if args.debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)

    cfg = {
        "MIN_VOLUME":      args.min_vol,
        "TIMEFRAME":       args.timeframe,
        "TOP_N":           args.top,
        "MIN_SCORE":       args.min_score,
        "MAX_WORKERS":     max(1, args.workers),
        "RATE_LIMIT_RPS":  max(0.0, args.rate),
    }

    run_long  = not args.short_only
    run_short = not args.long_only

    print(Fore.CYAN + Style.BRIGHT + hr("═"))
    print(Fore.CYAN + Style.BRIGHT +
          f"  ⚡ PHEMEX DUAL SCANNER  |  {args.timeframe}  |  "
          f"MinVol: {pc.fmt_vol(args.min_vol)} USDT  |  "
          f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    dirs_running = " + ".join(filter(None, [
        "▲ LONG" if run_long else None,
        "▼ SHORT" if run_short else None,
    ]))
    print(Fore.CYAN + Style.BRIGHT + f"  Directions: {dirs_running}")
    print(Fore.CYAN + Style.BRIGHT + hr("═"))
    print()

    # ── Estimation ────────────────────────────────────────────────────
    if args.time:
        print(Fore.WHITE + "  Calculating estimate...")
        raw_tickers = pc.get_tickers(rps=cfg["RATE_LIMIT_RPS"])
        # Simplified comparison
        filtered = [
            t for t in raw_tickers
            if float(t.get("turnoverRv") or 0.0) >= cfg["MIN_VOLUME"]
        ]
        total = len(filtered)
        # ~4 requests per ticker per direction (funding is prefetched)
        # Both directions run in parallel with their own rate limits
        est_s = (total * 4) / cfg["RATE_LIMIT_RPS"] if cfg["RATE_LIMIT_RPS"] > 0 else 0
        
        print(Fore.CYAN + f"  Estimated scan duration for {total} instruments:")
        print(Fore.CYAN + f"  {est_s:.1f} seconds (~{est_s/60:.1f} minutes)")
        print(Fore.CYAN + Style.BRIGHT + hr("═"))
        sys.exit(0)

    long_results: List[dict] = []
    short_results: List[dict] = []
    start = time.time()

    # Pre-fetch and filter tickers once for all branches
    print(Fore.WHITE + "  Fetching USDT-M tickers from Phemex...")
    # Use long scanner's get_tickers as it's identical to short's
    raw_tickers = pc.get_tickers(rps=cfg["RATE_LIMIT_RPS"])
    filtered = [
        t for t in raw_tickers 
        if float(t.get("turnoverRv") or 0.0) >= cfg["MIN_VOLUME"]
    ]

    if run_long and run_short:
        print(f"  {len(filtered)} instruments pass volume filter. "
              f"Running LONG & SHORT scanners in parallel...\n")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as exe:
            fut_long  = exe.submit(run_scan, scanner_long,  "LONG",  cfg, args, filtered)
            fut_short = exe.submit(run_scan, scanner_short, "SHORT", cfg, args, filtered)
            long_results  = fut_long.result()
            short_results = fut_short.result()

    elif run_long:
        print(Fore.GREEN + "  Running LONG scanner only...\n")
        long_results = run_scan(scanner_long, "LONG", cfg, args, filtered)

    else:
        print(Fore.RED + "  Running SHORT scanner only...\n")
        short_results = run_scan(scanner_short, "SHORT", cfg, args, filtered)

    elapsed = time.time() - start
    print()

    # ── Per-direction results ─────────────────────────────────────────
    if run_long:
        print_direction_results(long_results, "LONG", cfg, limit=args.top)

    if run_short:
        print_direction_results(short_results, "SHORT", cfg, limit=args.top)

    # ── Combined table ───────────────────────────────────────────────
    if run_long and run_short and args.combined > 0:
        print_combined(long_results, short_results, args.combined, cfg)

    # ── Summary ───────────────────────────────────────────────────────
    print_summary(long_results, short_results, elapsed, cfg)

    # ── Optional JSON dumps ──────────────────────────────────────────
    if args.write_json:
        for label, res in [("long", long_results), ("short", short_results)]:
            if res:
                p = Path(__file__).parent / f"last_scan_{label}.json"
                try:
                    p.write_text(json.dumps(res, indent=2))
                    print(Fore.LIGHTBLACK_EX + f"  Wrote {p.name}")
                except Exception as e:
                    print(Fore.YELLOW + f"  Warning: could not write {p.name}: {e}")


if __name__ == "__main__":
    main()