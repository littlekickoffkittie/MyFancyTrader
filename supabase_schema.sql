-- ═══════════════════════════════════════════════════════════════════
--  FangBlenny Bot — Supabase Schema
--  Run this in your Supabase SQL Editor (Project → SQL Editor → New query)
-- ═══════════════════════════════════════════════════════════════════

-- ── Enable UUID extension ─────────────────────────────────────────
create extension if not exists "uuid-ossp";

-- ── trades ───────────────────────────────────────────────────────
-- Mirrors every entry written to bot_trades.json
create table if not exists trades (
    id            uuid primary key default uuid_generate_v4(),
    timestamp     timestamptz not null default now(),
    symbol        text not null,
    direction     text,          -- LONG | SHORT
    status        text,          -- entered | stopped | trail_stop_failed | closed_by_exchange
    price         numeric,
    qty           numeric,
    margin_usdt   numeric,
    leverage      int,
    score         int,
    pnl           numeric,
    reason        text,
    raw           jsonb          -- full original payload
);

create index if not exists trades_symbol_idx   on trades(symbol);
create index if not exists trades_timestamp_idx on trades(timestamp desc);
create index if not exists trades_status_idx   on trades(status);

-- ── positions ────────────────────────────────────────────────────
-- Live snapshot of open positions (upserted every refresh cycle)
create table if not exists positions (
    symbol         text primary key,
    side           text,          -- Buy | Sell
    direction      text,          -- LONG | SHORT
    qty            numeric,
    entry_price    numeric,
    mark_price     numeric,
    unrealized_pnl numeric,
    leverage       int,
    stop_price     numeric,
    score          int,
    entry_time     timestamptz,
    updated_at     timestamptz default now()
);

-- ── signals ──────────────────────────────────────────────────────
-- Every scanner result (executed or not)
create table if not exists signals (
    id                  uuid primary key default uuid_generate_v4(),
    signal_id           text unique,
    timestamp           timestamptz not null default now(),
    symbol              text not null,
    direction           text,
    raw_score           int,
    effective_score     numeric,
    passed_quality_gate boolean default false,
    executed            boolean default false,
    skip_reason         text
);

create index if not exists signals_timestamp_idx on signals(timestamp desc);
create index if not exists signals_symbol_idx    on signals(symbol);

-- ── bot_state ────────────────────────────────────────────────────
-- Single-row live snapshot (upserted by the controller)
create table if not exists bot_state (
    id                   int primary key default 1,  -- singleton
    running              boolean default false,
    paused               boolean default false,
    balance_usdt         numeric,
    unrealized_pnl       numeric,
    equity               numeric,
    open_positions       int default 0,
    max_positions        int default 3,
    account_halted       boolean default false,
    scan_number          int default 0,
    uptime_seconds       int default 0,
    started_at           timestamptz,
    updated_at           timestamptz default now()
);

-- seed singleton row
insert into bot_state (id) values (1) on conflict (id) do nothing;

-- ── bot_config ───────────────────────────────────────────────────
-- Key/value config store (editable from Mini App)
create table if not exists bot_config (
    key          text primary key,
    value        text not null,
    description  text,
    updated_at   timestamptz default now()
);

-- seed defaults
insert into bot_config (key, value, description) values
    ('BOT_MARGIN_USDT',  '50.0',  'Margin per trade in USDT'),
    ('BOT_MIN_SCORE',    '125',   'Minimum scanner score to enter'),
    ('MAX_POSITIONS',    '3',     'Max concurrent open positions'),
    ('TIMEFRAME',        '15m',   'Scanner candle timeframe'),
    ('MIN_VOLUME',       '1000000', 'Minimum 24h volume filter'),
    ('TOP_N',            '50',    'Top N symbols to scan'),
    ('ACCOUNT_TRAIL_PCT','0.15',  'Account-level trailing stop %'),
    ('TRAIL_PCT',        '0.02',  'Per-position trailing stop %'),
    ('PHEMEX_BASE_URL',  'https://api.phemex.com', 'Phemex API base URL')
on conflict (key) do nothing;

-- ── Row Level Security ───────────────────────────────────────────
-- Disable RLS for service-role key (used by the bot)
-- Enable RLS + anon policy if you expose to frontend directly
alter table trades     enable row level security;
alter table positions  enable row level security;
alter table signals    enable row level security;
alter table bot_state  enable row level security;
alter table bot_config enable row level security;

-- Allow service role (used by the bot) full access
create policy "service_role_all" on trades     for all using (true);
create policy "service_role_all" on positions  for all using (true);
create policy "service_role_all" on signals    for all using (true);
create policy "service_role_all" on bot_state  for all using (true);
create policy "service_role_all" on bot_config for all using (true);
