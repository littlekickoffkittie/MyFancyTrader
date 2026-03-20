 /**
 * SimDashboard.jsx  (Telegram Mini App edition)
 * ─────────────────────────────────────────────────────────────────────────────
 * Real-time dashboard for simulation_bot.py — runs as a Telegram Mini App.
 * Also works in a regular browser (dev mode).
 *
 * PROPS
 *   basePath    {string}  URL prefix for fetch calls.   Default: ''
 *   pollMs      {number}  Polling interval ms.          Default: 2000
 *   initBalance {number}  Starting balance.
 *   simSession  {string}  Session label in header.
 *
 * FILES POLLED
 *   paper_account.json      balance + open positions
 *   sim_trade_results.json  closed trade history
 *   sim_cooldowns.json      re-entry & fast-track cooldowns
 *   sim_logs.json           system log lines
 * ─────────────────────────────────────────────────────────────────────────────
 */

import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { useTelegram } from './hooks/useTelegram'

// ─── Constants ────────────────────────────────────────────────────────────────

const DEFAULT_POLL_MS           = 2000
const DEFAULT_INIT_BAL          = parseFloat(import.meta?.env?.VITE_INITIAL_BALANCE ?? '100')
const DEFAULT_SESSION           = import.meta?.env?.VITE_SIM_SESSION ?? '1'
const FAST_TRACK_COOLDOWN_SECS  = 300
const MAX_REENTRY_COOLDOWN_SECS = 4 * 4 * 3600
const MAX_EQUITY_HISTORY        = 120
const MAX_LOG_LINES             = 20
const TRADE_HISTORY_LIMIT       = 12

// ─── CSS ─────────────────────────────────────────────────────────────────────

const CSS = /* css */`
  @import url('https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;0,500;0,600;0,700&display=swap');

  .sdb {
    --bg:         #04070f;
    --bg2:        #070c18;
    --bg3:        #0a1020;
    --bg-hover:   #0e1828;
    --border:     #14202e;
    --border-hi:  #1e3048;
    --cyan:       #00c8e8;
    --cyan-dim:   #09485a;
    --cyan-mid:   #007a90;
    --green:      #00e87a;
    --green-dim:  #004a28;
    --red:        #ff3c5a;
    --red-dim:    #5a0e1a;
    --yellow:     #f5c542;
    --yellow-dim: #4a3a08;
    --white:      #c8d8e8;
    --dim:        #304050;
    --dim2:       #1e2e3e;
    --font:       'Fira Code', 'Courier New', monospace;

    background: var(--bg);
    color: var(--white);
    font-family: var(--font);
    font-size: 12.5px;
    line-height: 1.55;
    /* Safe area padding injected inline by component */
    padding: 8px;
    /* Let the component control height via viewportHeight prop */
    min-height: 100%;
    width: 100%;
  }

  .sdb *, .sdb *::before, .sdb *::after {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }

  /* ── Scroll root — fills Telegram's viewport exactly ────── */
  .sdb-root {
    width: 100%;
    overflow-y: auto;
    overflow-x: hidden;
    -webkit-overflow-scrolling: touch;
    overscroll-behavior: contain;
  }

  /* ── Shell ──────────────────────────────────────────────── */
  .sdb-shell {
    max-width: 960px;
    margin: 0 auto;
    display: flex;
    flex-direction: column;
    gap: 5px;
  }

  /* ── Header ─────────────────────────────────────────────── */
  .sdb-header {
    border: 1px solid var(--border-hi);
    background: var(--bg3);
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 6px 14px;
  }
  .sdb-header-left {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .sdb-title {
    color: var(--cyan);
    font-weight: 700;
    font-size: 13.5px;
    letter-spacing: 0.1em;
    text-shadow: 0 0 12px #00c8e850;
  }
  .sdb-badge {
    color: var(--yellow);
    font-size: 10.5px;
    font-weight: 600;
    border: 1px solid #f5c54228;
    padding: 1px 7px;
    letter-spacing: 0.06em;
  }
  .sdb-header-right {
    display: flex;
    align-items: center;
    gap: 12px;
    font-size: 11px;
    flex-shrink: 0;
  }
  .sdb-session  { color: var(--cyan-mid); }
  .sdb-clock    { color: var(--white); font-weight: 600; font-variant-numeric: tabular-nums; }
  .sdb-tg-user  {
    color: var(--cyan);
    font-size: 10px;
    display: flex;
    align-items: center;
    gap: 4px;
    border: 1px solid var(--cyan-dim);
    padding: 1px 6px;
    white-space: nowrap;
    overflow: hidden;
    max-width: 110px;
    text-overflow: ellipsis;
  }

  /* ── Summary strip ──────────────────────────────────────── */
  .sdb-summary {
    border: 1px solid var(--border);
    background: var(--bg2);
    padding: 5px 14px;
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    align-items: center;
  }
  .sdb-sum-item {
    display: flex;
    align-items: baseline;
    gap: 5px;
  }
  .sdb-sum-key { color: var(--dim); font-size: 10px; letter-spacing: 0.08em; }
  .sdb-sum-val { font-weight: 700; font-size: 14px; font-variant-numeric: tabular-nums; }

  /* ── Panel ──────────────────────────────────────────────── */
  .sdb-panel {
    border: 1px solid var(--border);
    background: var(--bg2);
  }
  .sdb-panel-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 3px 10px;
    border-bottom: 1px solid var(--border);
    background: var(--bg3);
  }
  .sdb-panel-title {
    color: var(--cyan);
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.16em;
    text-shadow: 0 0 6px #00c8e830;
  }
  .sdb-panel-tag { color: var(--dim); font-size: 10px; }
  .sdb-panel-body { padding: 8px 10px; }

  /* ── Two-column grid ────────────────────────────────────── */
  .sdb-cols {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 5px;
  }
  /* On narrow phones collapse to single column */
  @media (max-width: 480px) {
    .sdb-cols { grid-template-columns: 1fr; }
  }

  /* ── Open Positions ─────────────────────────────────────── */
  .sdb-pos {
    padding: 8px 0 6px;
    border-bottom: 1px solid var(--border);
  }
  .sdb-pos:last-child { border-bottom: none; }
  .sdb-pos-row1 {
    display: flex;
    align-items: baseline;
    flex-wrap: wrap;
    gap: 7px;
    margin-bottom: 6px;
  }
  .sdb-dir-long  { color: var(--green); font-weight: 700; font-size: 11px; white-space: nowrap; }
  .sdb-dir-short { color: var(--red);   font-weight: 700; font-size: 11px; white-space: nowrap; }
  .sdb-sym       { color: var(--white); font-weight: 700; min-width: 96px; }
  .sdb-price-val { color: var(--white); font-variant-numeric: tabular-nums; font-size: 14px; }
  .sdb-arrow     { color: var(--dim);   font-size: 11px; }
  .sdb-live-na   { color: var(--dim);   font-size: 11px; font-style: italic; }
  .sdb-pos-meta  { color: var(--dim);   font-size: 10.5px; margin-left: auto; white-space: nowrap; }
  .sdb-score     { color: var(--yellow); font-size: 11px; white-space: nowrap; }
  .sdb-pos-age   {
    color: var(--cyan-mid);
    font-size: 10px;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
    border: 1px solid var(--cyan-dim);
    padding: 0 5px;
  }

  /* ── Priceline ──────────────────────────────────────────── */
  .sdb-pl-wrap { padding: 0 2px; }
  .sdb-pl-bar  { position: relative; height: 20px; margin-bottom: 3px; }
  .sdb-pl-track {
    position: absolute;
    top: 50%; left: 0; right: 0;
    height: 1px;
    background: var(--dim2);
    transform: translateY(-50%);
  }
  .sdb-pl-fill {
    position: absolute;
    top: 50%;
    height: 2px;
    transform: translateY(-50%);
    pointer-events: none;
  }
  .sdb-pl-marker {
    position: absolute;
    top: 50%;
    transform: translate(-50%, -50%);
    font-size: 11px;
    font-weight: 700;
    line-height: 1;
    white-space: nowrap;
    user-select: none;
  }
  .sdb-pl-labels {
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: var(--dim);
  }
  .sdb-pl-sl    { color: var(--red); }
  .sdb-pl-entry { color: var(--yellow); }
  .sdb-pl-tp    { color: var(--green); }
  .sdb-pl-stage {
    position: absolute;
    top: 50%;
    transform: translate(-50%, -50%);
    width: 5px; height: 5px;
    border-radius: 50%;
    background: var(--green-dim);
    border: 1px solid var(--green);
    pointer-events: none;
  }
  .sdb-pl-stage.hit { background: var(--green); box-shadow: 0 0 4px var(--green); }

  /* ── KV rows (wallet) ───────────────────────────────────── */
  .sdb-kv {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 2px 0;
    border-bottom: 1px solid var(--border);
  }
  .sdb-kv:last-child { border-bottom: none; }
  .sdb-kv-key  { color: var(--dim); font-size: 11px; }
  .sdb-kv-val  { font-weight: 600; font-variant-numeric: tabular-nums; font-size: 14px; }
  .sdb-usdt    { color: var(--cyan-mid); font-size: 10px; margin-left: 3px; font-weight: 400; }
  .sdb-eq-pos  { color: var(--green); text-shadow: 0 0 6px #00e87a30; }
  .sdb-eq-neg  { color: var(--red);   text-shadow: 0 0 6px #ff3c5a30; }

  /* ── Sparkline ──────────────────────────────────────────── */
  .sdb-spark { margin-top: 6px; }
  .sdb-spark svg { display: block; width: 100%; overflow: visible; }

  /* ── Stats ──────────────────────────────────────────────── */
  .sdb-stat-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 2px 0;
    border-bottom: 1px solid var(--border);
  }
  .sdb-stat-row:last-child { border-bottom: none; }
  .sdb-stat-key { color: var(--dim); font-size: 11px; }
  .sdb-stat-val { font-weight: 600; font-variant-numeric: tabular-nums; }
  .sdb-green  { color: var(--green); }
  .sdb-red    { color: var(--red); }
  .sdb-yellow { color: var(--yellow); }
  .sdb-muted  { color: var(--dim); font-weight: 400; }

  /* ── Trade history ──────────────────────────────────────── */
  .sdb-th-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1px 12px;
  }
  @media (max-width: 480px) { .sdb-th-grid { grid-template-columns: 1fr; } }
  .sdb-th-row {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 3px 0;
    border-bottom: 1px solid var(--border);
    font-size: 11.5px;
    overflow: hidden;
  }
  .sdb-th-row:last-child { border-bottom: none; }
  .sdb-th-ts     { color: var(--dim);   font-size: 10px; white-space: nowrap; min-width: 38px; }
  .sdb-th-sym    { color: var(--white); font-weight: 700; white-space: nowrap; min-width: 72px; }
  .sdb-th-long   { color: var(--green); font-size: 10px; white-space: nowrap; }
  .sdb-th-short  { color: var(--red);   font-size: 10px; white-space: nowrap; }
  .sdb-th-reason { color: var(--dim);   font-size: 10px; flex: 1; text-align: right; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .sdb-th-pnl-pos { color: var(--green); font-weight: 700; white-space: nowrap; margin-left: auto; font-size: 14px; }
  .sdb-th-pnl-neg { color: var(--red);   font-weight: 700; white-space: nowrap; margin-left: auto; font-size: 14px; }

  /* ── System log ─────────────────────────────────────────── */
  .sdb-log-scroll {
    max-height: 176px;
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: var(--dim2) transparent;
    /* Smooth momentum scrolling on iOS */
    -webkit-overflow-scrolling: touch;
  }
  .sdb-log-scroll::-webkit-scrollbar { width: 4px; }
  .sdb-log-scroll::-webkit-scrollbar-track { background: transparent; }
  .sdb-log-scroll::-webkit-scrollbar-thumb { background: var(--dim2); border-radius: 2px; }
  .sdb-log-line {
    display: flex;
    gap: 4px;
    padding: 2px 0;
    border-bottom: 1px solid #080e18;
    font-size: 11.5px;
    white-space: nowrap;
    overflow: hidden;
  }
  .sdb-log-line:last-child { border-bottom: none; }
  .sdb-log-ts     { color: var(--dim); flex-shrink: 0; }
  .sdb-log-msg    { overflow: hidden; text-overflow: ellipsis; }
  .sdb-log-fast   { color: var(--yellow); }
  .sdb-log-enter  { color: var(--green); }
  .sdb-log-close  { color: var(--red); }
  .sdb-log-scan   { color: var(--cyan-mid); }
  .sdb-log-error  { color: var(--red); font-weight: 700; }
  .sdb-log-normal { color: var(--white); }

  /* ── Cooldowns ──────────────────────────────────────────── */
  .sdb-cd-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 11.5px;
  }
  .sdb-cd-table th {
    color: var(--dim);
    font-weight: 400;
    font-size: 10px;
    letter-spacing: 0.1em;
    text-align: left;
    padding: 2px 6px 4px 0;
    border-bottom: 1px solid var(--border);
  }
  .sdb-cd-table td {
    padding: 3px 6px 3px 0;
    border-bottom: 1px solid #080e18;
    font-variant-numeric: tabular-nums;
    vertical-align: middle;
  }
  .sdb-cd-table tr:last-child td { border-bottom: none; }
  .sdb-cd-sym   { color: var(--white); font-weight: 700; }
  .sdb-cd-exit  { color: var(--yellow); font-size: 10px; }
  .sdb-cd-ft    { color: var(--cyan);   font-size: 10px; }
  .sdb-cd-score { color: var(--dim); }
  .sdb-cd-timer { color: var(--red); font-weight: 600; }
  .sdb-cd-bar-wrap {
    width: 100%;
    height: 3px;
    background: var(--dim2);
    border-radius: 2px;
    overflow: hidden;
    margin-top: 2px;
  }
  .sdb-cd-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 1s linear;
  }

  /* ── Empty ──────────────────────────────────────────────── */
  .sdb-empty { color: var(--dim); font-size: 11px; font-style: italic; padding: 4px 0; }

  /* ── Footer ─────────────────────────────────────────────── */
  .sdb-footer {
    border: 1px solid var(--border);
    background: var(--bg3);
    padding: 5px 14px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 10.5px;
    color: var(--dim);
  }
  .sdb-footer-brand { color: var(--cyan); font-weight: 700; letter-spacing: 0.06em; }
  .sdb-poll-ok  { color: var(--green); }
  .sdb-poll-err { color: var(--red); }
  .sdb-last-poll { color: var(--dim); font-size: 10px; }

  @keyframes sdb-pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.3; }
  }
  .sdb-poll-ok { animation: sdb-pulse 2s ease-in-out infinite; }
`

// ─── Utility helpers ──────────────────────────────────────────────────────────

function fmtTimestamp(ms) {
  return new Date(ms).toLocaleTimeString('en-GB', { hour12: false })
}

function fmtDuration(secs) {
  if (secs <= 0) return 'expired'
  const h = Math.floor(secs / 3600)
  const m = Math.floor((secs % 3600) / 60)
  const s = Math.floor(secs % 60)
  if (h > 0) return `${h}h ${String(m).padStart(2, '0')}m`
  if (m > 0) return `${m}m ${String(s).padStart(2, '0')}s`
  return `${s}s`
}

function fmtAge(entryTimeStr, nowMs) {
  if (!entryTimeStr) return null
  const ms = new Date(entryTimeStr).getTime()
  if (isNaN(ms)) return null
  const secs = Math.floor((nowMs - ms) / 1000)
  return secs >= 0 ? fmtDuration(secs) : null
}

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)) }

function toPct(v, lo, hi) {
  if (hi === lo) return 50
  return clamp(((v - lo) / (hi - lo)) * 100, 1, 99)
}

function computeStreak(trades) {
  if (!trades.length) return 0
  const sorted = [...trades].sort((a, b) =>
    (a.timestamp ? new Date(a.timestamp).getTime() : 0) -
    (b.timestamp ? new Date(b.timestamp).getTime() : 0)
  )
  const lastIsWin = sorted[sorted.length - 1].pnl > 0
  let streak = 0
  for (let i = sorted.length - 1; i >= 0; i--) {
    if ((sorted[i].pnl > 0) === lastIsWin) streak++
    else break
  }
  return lastIsWin ? streak : -streak
}

function computeMaxDrawdown(history) {
  if (history.length < 2) return 0
  let peak = history[0], maxDD = 0
  for (const v of history) {
    if (v > peak) peak = v
    const dd = peak > 0 ? (peak - v) / peak : 0
    if (dd > maxDD) maxDD = dd
  }
  return maxDD
}

function computeProfitFactor(trades) {
  const grossWin  = trades.filter(t => t.pnl > 0).reduce((s, t) => s + t.pnl, 0)
  const grossLoss = trades.filter(t => t.pnl <= 0).reduce((s, t) => s + Math.abs(t.pnl), 0)
  if (grossLoss === 0) return grossWin > 0 ? Infinity : null
  return grossWin / grossLoss
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function Sparkline({ data, height = 32, isPos }) {
  if (!data || data.length < 2) return null
  const W = 300
  const lo = Math.min(...data), hi = Math.max(...data)
  const range = hi - lo || 1
  const pts = data.map((v, i) => ({
    x: (i / (data.length - 1)) * W,
    y: height - ((v - lo) / range) * (height - 3) - 1.5,
  }))
  const polyPts = pts.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ')
  const fillPts = [`0,${height}`, ...pts.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`), `${W},${height}`].join(' ')
  const color = isPos ? '#00e87a' : '#ff3c5a'
  const last  = pts[pts.length - 1]
  const gid   = isPos ? 'spark-pos' : 'spark-neg'
  return (
    <svg viewBox={`0 0 ${W} ${height}`} height={height} preserveAspectRatio="none">
      <defs>
        <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%"   stopColor={color} stopOpacity="0.18" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon points={fillPts} fill={`url(#${gid})`} />
      <polyline points={polyPts} fill="none" stroke={color} strokeWidth="1.4"
        strokeLinejoin="round" strokeLinecap="round" opacity="0.9" />
      <circle cx={last.x} cy={last.y} r="2" fill={color} />
    </svg>
  )
}

function PriceLine({ pos }) {
  const isLong   = pos.side === 'Buy'
  const entry    = parseFloat(pos.entry        ?? 0)
  const stop     = parseFloat(pos.stop_price   ?? 0)
  const origStop = parseFloat(pos.original_stop ?? stop)
  const tp       = parseFloat(pos.take_profit  ?? 0)
  const mp       = pos.mark_price != null ? parseFloat(pos.mark_price) : null

  const stagePrices = (pos.tp_stages ?? []).map(s => parseFloat(s.price))
  const allPts = [origStop, stop, entry, tp, ...stagePrices, ...(mp != null ? [mp] : [])]
  const lo = Math.min(...allPts), hi = Math.max(...allPts)

  const fl = Math.min(toPct(entry, lo, hi), toPct(stop, lo, hi))
  const fr = Math.max(toPct(entry, lo, hi), toPct(stop, lo, hi))
  const fw = fr - fl

  const inProfit = mp != null ? (isLong ? mp > entry : mp < entry) : null
  const fillColor = inProfit === true ? '#00e87a28' : inProfit === false ? '#ff3c5a28' : '#1a3a4820'

  return (
    <div className="sdb-pl-wrap">
      <div className="sdb-pl-bar">
        <div className="sdb-pl-track" />
        {fw > 0.5 && <div className="sdb-pl-fill" style={{ left: `${fl}%`, width: `${fw}%`, background: fillColor }} />}
        {(pos.tp_stages ?? []).map((s, i) => (
          <div key={i} className={`sdb-pl-stage${s.hit ? ' hit' : ''}`}
            style={{ left: `${toPct(parseFloat(s.price), lo, hi)}%` }}
            title={`TP ${i+1}: ${s.price}${s.hit ? ' (hit)' : ''}`} />
        ))}
        <span className="sdb-pl-marker" style={{ left: `${toPct(origStop, lo, hi)}%`, color: '#7a1828' }}>╳</span>
        <span className="sdb-pl-marker" style={{ left: `${toPct(stop, lo, hi)}%`,     color: '#ff3c5a', fontSize: '12px' }}>S</span>
        <span className="sdb-pl-marker" style={{ left: `${toPct(entry, lo, hi)}%`,    color: '#f5c542' }}>E</span>
        <span className="sdb-pl-marker" style={{ left: `${toPct(tp, lo, hi)}%`,       color: '#00e87a', fontSize: '12px' }}>T</span>
        {mp != null && (
          <span className="sdb-pl-marker"
            style={{ left: `${toPct(mp, lo, hi)}%`, color: '#fff', fontSize: '14px', textShadow: '0 0 6px #fff8' }}>●</span>
        )}
      </div>
      <div className="sdb-pl-labels">
        <span className="sdb-pl-sl">SL {stop.toPrecision(5)}</span>
        <span className="sdb-pl-entry">E {entry.toPrecision(5)}</span>
        <span className="sdb-pl-tp">TP {tp.toPrecision(5)}</span>
      </div>
    </div>
  )
}

function LogLine({ line }) {
  const bc  = line.indexOf(']')
  const ts  = bc >= 0 ? line.slice(0, bc + 1) : ''
  const msg = bc >= 0 ? line.slice(bc + 1)    : line
  const up  = msg.toUpperCase()
  let cls = 'sdb-log-normal'
  if      (up.includes('⚡') || up.includes('FAST'))                                         cls = 'sdb-log-fast'
  else if (up.includes('ENTERED') || up.includes('TAKE PROFIT') || up.includes('PARTIAL TP')) cls = 'sdb-log-enter'
  else if (up.includes('STOP') || up.includes('CLOSED'))                                     cls = 'sdb-log-close'
  else if (up.includes('ERROR') || up.includes('WARN'))                                      cls = 'sdb-log-error'
  else if (up.includes('SCAN') || up.includes('COMPLETE'))                                   cls = 'sdb-log-scan'
  return (
    <div className="sdb-log-line">
      <span className="sdb-log-ts">{ts}</span>
      <span className={`sdb-log-msg ${cls}`}>{msg}</span>
    </div>
  )
}

function CooldownRow({ cd }) {
  const pct      = clamp(((cd.max - cd.rem) / cd.max) * 100, 0, 100)
  const barColor = cd.type === 'FAST-TRACK' ? 'var(--cyan)' : 'var(--red)'
  return (
    <>
      <tr>
        <td className="sdb-cd-sym">{cd.sym}</td>
        <td className={cd.type === 'FAST-TRACK' ? 'sdb-cd-ft' : 'sdb-cd-exit'}>{cd.type}</td>
        <td className="sdb-cd-score">{cd.score ?? '—'}</td>
        <td className="sdb-cd-timer">{fmtDuration(cd.rem)}</td>
      </tr>
      <tr>
        <td colSpan={4} style={{ paddingTop: 0, paddingBottom: '4px' }}>
          <div className="sdb-cd-bar-wrap">
            <div className="sdb-cd-bar-fill" style={{ width: `${pct}%`, background: barColor, opacity: 0.5 }} />
          </div>
        </td>
      </tr>
    </>
  )
}

// ─── Main export ──────────────────────────────────────────────────────────────

export default function SimDashboard({
  basePath    = '',
  pollMs      = DEFAULT_POLL_MS,
  initBalance = DEFAULT_INIT_BAL,
  simSession  = DEFAULT_SESSION,
}) {
  // ── Telegram integration ─────────────────────────────────────────────────
  const {
    user,
    viewportHeight,
    isTelegram,
    haptic,
    safeAreaTop,
    safeAreaBottom,
  } = useTelegram()

  // ── State ────────────────────────────────────────────────────────────────
  const [account,    setAccount]    = useState({ balance: 0, positions: [] })
  const [trades,     setTrades]     = useState([])
  const [cooldowns,  setCooldowns]  = useState({ last_exit: {}, fast_track: {} })
  const [logs,       setLogs]       = useState([])
  const [tickMs,     setTickMs]     = useState(Date.now())
  const [pollStatus, setPollStatus] = useState('ok')
  const [lastPollMs, setLastPollMs] = useState(null)
  const prevPollStatus = useRef('ok')
  const equityHistory  = useRef([])

  // ── Data polling ─────────────────────────────────────────────────────────
  const poll = useCallback(async () => {
    const bust = `?t=${Date.now()}`
    const [accR, tradesR, cdR, logsR] = await Promise.allSettled([
      fetch(`${basePath}/paper_account.json${bust}`).then(r => { if (!r.ok) throw r; return r.json() }),
      fetch(`${basePath}/sim_trade_results.json${bust}`).then(async r => {
        if (!r.ok) throw r
        const text = await r.text()
        if (!text.trim()) return []
        try {
          const json = JSON.parse(text)
          return Array.isArray(json) ? json : [json]
        } catch {
          // Assume JSONL
          return text.trim().split('\n').map(line => {
            try { return JSON.parse(line) } catch { return null }
          }).filter(Boolean)
        }
      }),
      fetch(`${basePath}/sim_cooldowns.json${bust}`).then(r => { if (!r.ok) throw r; return r.json() }),
      fetch(`${basePath}/sim_logs.json${bust}`).then(r => { if (!r.ok) throw r; return r.json() }),
    ])
    let anyOk = false
    if (accR.status    === 'fulfilled') { setAccount(accR.value);   anyOk = true }
    if (tradesR.status === 'fulfilled') { setTrades(tradesR.value); anyOk = true }
    if (cdR.status     === 'fulfilled') { setCooldowns(cdR.value);  anyOk = true }
    if (logsR.status   === 'fulfilled') { setLogs(logsR.value);     anyOk = true }

    const nextStatus = anyOk ? 'ok' : 'err'
    setPollStatus(nextStatus)
    if (anyOk) setLastPollMs(Date.now())

    // Haptic feedback when connection is lost or recovered
    if (isTelegram) {
      if (nextStatus === 'err' && prevPollStatus.current === 'ok') {
        haptic.notify('error')
      } else if (nextStatus === 'ok' && prevPollStatus.current === 'err') {
        haptic.notify('success')
      }
    }
    prevPollStatus.current = nextStatus
  }, [basePath, isTelegram, haptic])

  useEffect(() => {
    poll()
    const id = setInterval(poll, pollMs)
    return () => clearInterval(id)
  }, [poll, pollMs])

  // ── Clock ────────────────────────────────────────────────────────────────
  useEffect(() => {
    const id = setInterval(() => setTickMs(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  // ── Derived ──────────────────────────────────────────────────────────────
  const balance      = account.balance ?? 0
  const positions    = account.positions ?? []
  const lockedMargin = positions.reduce((s, p) => s + (p.margin ?? 0), 0)

  // Calculate uPnL based on mark_price if available
  const currentUpnl = positions.reduce((sum, pos) => {
    if (pos.mark_price == null) return sum
    const entry = parseFloat(pos.entry ?? 0)
    const mark  = parseFloat(pos.mark_price)
    const size  = parseFloat(pos.size ?? 0)
    const isLong = pos.side === 'Buy'
    const pnl = isLong ? (mark - entry) * size : (entry - mark) * size
    return sum + pnl
  }, 0)

  // Equity = balance + locked margin + uPnL
  const equity      = balance + lockedMargin + currentUpnl
  const equityDelta = equity - initBalance

  useEffect(() => {
    if (equity > 0) {
      equityHistory.current.push(equity)
      if (equityHistory.current.length > MAX_EQUITY_HISTORY) equityHistory.current.shift()
    }
  }, [equity])

  const maxDrawdown    = useMemo(() => computeMaxDrawdown(equityHistory.current), [equity])
  const wins           = trades.filter(t => t.pnl > 0)
  const losses         = trades.filter(t => t.pnl <= 0)
  const totalTrades    = trades.length
  const winRate        = totalTrades > 0 ? (wins.length / totalTrades) * 100 : 0
  const totalClosedPnl = trades.reduce((s, t) => s + (t.pnl ?? 0), 0)
  const avgPnl         = totalTrades > 0 ? totalClosedPnl / totalTrades : null
  const recentTrades   = [...trades].reverse().slice(0, TRADE_HISTORY_LIMIT)
  const profitFactor   = useMemo(() => computeProfitFactor(trades), [trades])
  const streak         = useMemo(() => computeStreak(trades), [trades])
  const bestTrade      = trades.length ? Math.max(...trades.map(t => t.pnl ?? 0)) : null
  const worstTrade     = trades.length ? Math.min(...trades.map(t => t.pnl ?? 0)) : null

  // ── Cooldowns ────────────────────────────────────────────────────────────
  const nowSec = tickMs / 1000
  const activeCooldowns = []
  for (const [sym, val] of Object.entries(cooldowns.last_exit ?? {})) {
    const [ts, score] = Array.isArray(val) ? val : [Number(val), 0]
    const rem = Math.max(0, MAX_REENTRY_COOLDOWN_SECS - (nowSec - ts))
    if (rem > 0) activeCooldowns.push({ sym, type: 'RE-ENTRY', score, rem, max: MAX_REENTRY_COOLDOWN_SECS })
  }
  for (const [sym, ts] of Object.entries(cooldowns.fast_track ?? {})) {
    const rem = Math.max(0, FAST_TRACK_COOLDOWN_SECS - (nowSec - ts))
    if (rem > 0) activeCooldowns.push({ sym, type: 'FAST-TRACK', score: null, rem, max: FAST_TRACK_COOLDOWN_SECS })
  }
  activeCooldowns.sort((a, b) => a.rem - b.rem)

  // ── Render ───────────────────────────────────────────────────────────────

  // The scroll root is pinned to Telegram's reported stable viewport height.
  // In a browser it just uses window.innerHeight (via the hook fallback).
  const rootStyle = {
    height:     `${viewportHeight}px`,
    paddingTop:    safeAreaTop    > 0 ? `${safeAreaTop}px`    : undefined,
    paddingBottom: safeAreaBottom > 0 ? `${safeAreaBottom}px` : undefined,
  }

  // Build display name for Telegram user chip
  const tgName = user
    ? [user.first_name, user.last_name].filter(Boolean).join(' ') || user.username || 'tg user'
    : null

  return (
    <div className="sdb-root" style={rootStyle}>
      <div className="sdb">
        <style>{CSS}</style>

        <div className="sdb-shell">

          {/* ─── Header ────────────────────────────────────────────────────── */}
          <div className="sdb-header">
            <div className="sdb-header-left">
              <span style={{ color: '#f5c542' }}>⚡</span>
              <span className="sdb-title">PHEMEX SIM BOT</span>
              <span className="sdb-badge">◈ PAPER</span>
            </div>
            <div className="sdb-header-right">
              {tgName && (
                <span className="sdb-tg-user" title={` @${user.username ?? tgName}`}>
                  👤 {tgName}
                </span>
              )}
              <span className="sdb-session">SESS: {simSession}</span>
              <span className="sdb-clock">{fmtTimestamp(tickMs)}</span>
            </div>
          </div>

          {/* ─── Summary strip ─────────────────────────────────────────────── */}
          <div className="sdb-summary">
            <div className="sdb-sum-item">
              <span className="sdb-sum-key">EQUITY</span>
              <span className={`sdb-sum-val ${equityDelta >= 0 ? 'sdb-green' : 'sdb-red'}`}>
                ${equity.toFixed(2)}
              </span>
            </div>
            <div className="sdb-sum-item">
              <span className="sdb-sum-key">DELTA</span>
              <span className={`sdb-sum-val ${equityDelta >= 0 ? 'sdb-green' : 'sdb-red'}`}>
                {equityDelta >= 0 ? '+' : ''}{equityDelta.toFixed(2)}
              </span>
            </div>
            <div className="sdb-sum-item">
              <span className="sdb-sum-key">OPEN</span>
              <span className="sdb-sum-val" style={{ color: positions.length > 0 ? 'var(--yellow)' : 'var(--dim)' }}>
                {positions.length}
              </span>
            </div>
            <div className="sdb-sum-item">
              <span className="sdb-sum-key">WIN%</span>
              <span className={`sdb-sum-val ${winRate >= 50 ? 'sdb-green' : winRate > 0 ? 'sdb-yellow' : 'sdb-muted'}`}>
                {totalTrades > 0 ? `${winRate.toFixed(0)}%` : '—'}
              </span>
            </div>
            <div className="sdb-sum-item">
              <span className="sdb-sum-key">STREAK</span>
              <span className={`sdb-sum-val ${streak > 0 ? 'sdb-green' : streak < 0 ? 'sdb-red' : 'sdb-muted'}`}>
                {streak === 0 ? '—' : streak > 0 ? `+${streak}W` : `${Math.abs(streak)}L`}
              </span>
            </div>
            <div className="sdb-sum-item">
              <span className="sdb-sum-key">MAX DD</span>
              <span className={`sdb-sum-val ${maxDrawdown > 0.05 ? 'sdb-red' : maxDrawdown > 0 ? 'sdb-yellow' : 'sdb-muted'}`}>
                {maxDrawdown > 0 ? `${(maxDrawdown * 100).toFixed(1)}%` : '—'}
              </span>
            </div>
          </div>

          {/* ─── Open Positions ────────────────────────────────────────────── */}
          <div className="sdb-panel">
            <div className="sdb-panel-head">
              <span className="sdb-panel-title">OPEN POSITIONS</span>
              <span className="sdb-panel-tag">
                {positions.length > 0 ? `─ ${positions.length} open` : '─ idle'}
              </span>
            </div>
            <div className="sdb-panel-body">
              {positions.length === 0 ? (
                <p className="sdb-empty">Waiting for qualifying setups ·</p>
              ) : positions.map((pos, i) => {
                const isLong    = pos.side === 'Buy'
                const entry     = parseFloat(pos.entry ?? 0)
                const score     = pos.entry_score ?? 0
                const margin    = pos.margin ?? 0
                const lev       = pos.leverage ?? 0
                const entryTime = pos.entry_time
                  ? new Date(pos.entry_time).toLocaleTimeString('en-GB', { hour12: false })
                  : '—'
                const age = fmtAge(pos.entry_time, tickMs)
                return (
                  <div key={i} className="sdb-pos">
                    <div className="sdb-pos-row1">
                      <span className={isLong ? 'sdb-dir-long' : 'sdb-dir-short'}>
                        {isLong ? '▲ LONG' : '▼ SHORT'}
                      </span>
                      <span className="sdb-sym">{pos.symbol}</span>
                      <span className="sdb-price-val">{entry.toPrecision(6)}</span>
                      <span className="sdb-arrow">──▶</span>
                      {pos.mark_price != null
                        ? <span className="sdb-price-val">{parseFloat(pos.mark_price).toPrecision(6)}</span>
                        : <span className="sdb-live-na">no live price</span>
                      }
                      {pos.mark_price != null && (() => {
                        const mark = parseFloat(pos.mark_price)
                        const size = parseFloat(pos.size ?? 0)
                        const pnl = isLong ? (mark - entry) * size : (entry - mark) * size
                        const pnlPct = margin > 0 ? (pnl / margin) * 100 : 0
                        return (
                          <span className={pnl >= 0 ? 'sdb-green' : 'sdb-red'} style={{ fontWeight: 700, fontSize: '14px', whiteSpace: 'nowrap' }}>
                            {pnl >= 0 ? '+' : ''}{pnl.toFixed(3)} ({pnlPct.toFixed(1)}%)
                          </span>
                        )
                      })()}
                      {age && <span className="sdb-pos-age">⏱ {age}</span>}
                      <span className="sdb-pos-meta">M: ${margin.toFixed(1)} · {lev}x · @{entryTime}</span>
                      <span className="sdb-score">[{score}]</span>
                    </div>
                    <PriceLine pos={pos} />
                  </div>
                )
              })}
            </div>
          </div>

          {/* ─── Wallet + Stats ────────────────────────────────────────────── */}
          <div className="sdb-cols">
            <div className="sdb-panel">
              <div className="sdb-panel-head"><span className="sdb-panel-title">WALLET</span></div>
              <div className="sdb-panel-body">
                <div className="sdb-kv">
                  <span className="sdb-kv-key">Available</span>
                  <span className="sdb-kv-val" style={{ color: 'var(--white)' }}>
                    ${balance.toFixed(2)}<span className="sdb-usdt">USDT</span>
                  </span>
                </div>
                <div className="sdb-kv">
                  <span className="sdb-kv-key">Locked margin</span>
                  <span className="sdb-kv-val" style={{ color: 'var(--yellow)' }}>
                    ${lockedMargin.toFixed(2)}<span className="sdb-usdt">USDT</span>
                  </span>
                </div>
                <div className="sdb-kv">
                  <span className="sdb-kv-key">uPnL</span>
                  <span className={`sdb-kv-val ${currentUpnl >= 0 ? 'sdb-green' : 'sdb-red'}`}>
                    {currentUpnl >= 0 ? '+' : ''}{currentUpnl.toFixed(2)}<span className="sdb-usdt">USDT</span>
                  </span>
                </div>
                <div className="sdb-kv">
                  <span className="sdb-kv-key">Equity</span>
                  <span className={`sdb-kv-val ${equityDelta >= 0 ? 'sdb-eq-pos' : 'sdb-eq-neg'}`}>
                    ${equity.toFixed(2)}<span className="sdb-usdt">USDT</span>
                  </span>
                </div>
                <div className="sdb-kv">
                  <span className="sdb-kv-key">Δ from start</span>
                  <span className={`sdb-kv-val ${equityDelta >= 0 ? 'sdb-eq-pos' : 'sdb-eq-neg'}`}>
                    {equityDelta >= 0 ? '+' : ''}{equityDelta.toFixed(4)}<span className="sdb-usdt">USDT</span>
                  </span>
                </div>
                <div className="sdb-spark">
                  <Sparkline data={equityHistory.current} height={32} isPos={equityDelta >= 0} />
                </div>
              </div>
            </div>

            <div className="sdb-panel">
              <div className="sdb-panel-head"><span className="sdb-panel-title">STATISTICS</span></div>
              <div className="sdb-panel-body">
                {[
                  ['Trades',        <span style={{ color: 'var(--white)' }}>{totalTrades}</span>],
                  ['W / L',         <span><span className="sdb-green">✅ {wins.length}W</span>{'  '}<span className="sdb-red">❌ {losses.length}L</span></span>],
                  ['Win rate',      <span className="sdb-yellow">{winRate.toFixed(1)}%</span>],
                  ['Realized PnL',  <span className={totalClosedPnl >= 0 ? 'sdb-green' : 'sdb-red'}>{totalClosedPnl >= 0 ? '+' : ''}{totalClosedPnl.toFixed(4)}<span className="sdb-usdt">USDT</span></span>],
                  ['Avg / trade',   avgPnl == null ? <span className="sdb-muted">—</span> : <span className={avgPnl >= 0 ? 'sdb-green' : 'sdb-red'}>{avgPnl >= 0 ? '+' : ''}{avgPnl.toFixed(4)}</span>],
                  ['Profit factor', profitFactor == null ? <span className="sdb-muted">—</span> : <span className={profitFactor >= 1 ? 'sdb-green' : 'sdb-red'}>{profitFactor === Infinity ? '∞' : profitFactor.toFixed(2)}</span>],
                  ['Best / Worst',  bestTrade != null ? <span><span className="sdb-green">+{bestTrade.toFixed(3)}</span>{'  '}<span className="sdb-red">{worstTrade.toFixed(3)}</span></span> : <span className="sdb-muted">—</span>],
                  ['Streak',        <span className={streak > 0 ? 'sdb-green' : streak < 0 ? 'sdb-red' : 'sdb-muted'}>{streak === 0 ? '—' : streak > 0 ? `${streak}W running` : `${Math.abs(streak)}L running`}</span>],
                ].map(([k, v]) => (
                  <div key={k} className="sdb-stat-row">
                    <span className="sdb-stat-key">{k}</span>
                    <span className="sdb-stat-val">{v}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* ─── Trade History ──────────────────────────────────────────────── */}
          <div className="sdb-panel">
            <div className="sdb-panel-head">
              <span className="sdb-panel-title">TRADE HISTORY</span>
              <span className="sdb-panel-tag">last {Math.min(recentTrades.length, TRADE_HISTORY_LIMIT)} of {totalTrades}</span>
            </div>
            <div className="sdb-panel-body">
              {recentTrades.length === 0 ? (
                <p className="sdb-empty">No closed trades yet</p>
              ) : (
                <div className="sdb-th-grid">
                  {recentTrades.map((t, i) => {
                    const d    = t.timestamp ? new Date(t.timestamp) : null
                    const ts   = d ? `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}` : '—'
                    const pos  = t.pnl > 0
                    const heldM = t.hold_time_s != null ? Math.round(t.hold_time_s / 60) : null
                    return (
                      <div key={i} className="sdb-th-row">
                        <span className="sdb-th-ts">{ts}</span>
                        <span className="sdb-th-sym">{(t.symbol ?? '').slice(0, 10)}</span>
                        <span className={t.direction === 'LONG' ? 'sdb-th-long' : 'sdb-th-short'}>
                          {(t.direction ?? '').slice(0, 5)}
                        </span>
                        <span className="sdb-th-reason">{t.reason}{heldM != null ? ` ${heldM}m` : ''}</span>
                        <span className={pos ? 'sdb-th-pnl-pos' : 'sdb-th-pnl-neg'}>
                          {pos ? '✅' : '❌'}&nbsp;{t.pnl >= 0 ? '+' : ''}{t.pnl.toFixed(4)}
                        </span>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          </div>

          {/* ─── System Log + Cooldowns ────────────────────────────────────── */}
          <div className="sdb-cols">
            <div className="sdb-panel">
              <div className="sdb-panel-head">
                <span className="sdb-panel-title">SYSTEM LOG</span>
                <span className="sdb-panel-tag">sim_logs.json · {logs.length} entries</span>
              </div>
              <div className="sdb-panel-body">
                {logs.length === 0 ? (
                  <p className="sdb-empty">No entries — see component header for setup</p>
                ) : (
                  <div className="sdb-log-scroll">
                    {[...logs].reverse().slice(0, MAX_LOG_LINES).map((line, i) => (
                      <LogLine key={i} line={String(line)} />
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div className="sdb-panel">
              <div className="sdb-panel-head">
                <span className="sdb-panel-title">COOLDOWNS</span>
                <span className="sdb-panel-tag">{activeCooldowns.length} active</span>
              </div>
              <div className="sdb-panel-body">
                {activeCooldowns.length === 0 ? (
                  <p className="sdb-empty">No active cooldowns</p>
                ) : (
                  <table className="sdb-cd-table">
                    <thead><tr><th>SYMBOL</th><th>TYPE</th><th>SCORE</th><th>REMAINING</th></tr></thead>
                    <tbody>
                      {activeCooldowns.map((cd, i) => <CooldownRow key={i} cd={cd} />)}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </div>

          {/* ─── Footer ─────────────────────────────────────────────────────── */}
          <div className="sdb-footer">
            <span>
              <span className="sdb-footer-brand">⚡ FANCYBOT</span>{' '}v2 — paper trading
            </span>
            <span style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              {lastPollMs && <span className="sdb-last-poll">last ok {fmtTimestamp(lastPollMs)}</span>}
              <span>
                poll {pollMs}ms{'  '}
                <span className={pollStatus === 'ok' ? 'sdb-poll-ok' : 'sdb-poll-err'}>
                  ● {pollStatus === 'ok' ? 'live' : 'fetch error'}
                </span>
              </span>
            </span>
          </div>

        </div>
      </div>
    </div>
  )
}