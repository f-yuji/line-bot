-- Add valuation columns used by rebound_lab / box_lab display.
-- Safe to run multiple times in Supabase SQL Editor.

create table if not exists nikkei_financials (
  code text primary key,
  is_deficit boolean,
  dividend_per_share numeric,
  updated_at timestamptz default now()
);

alter table nikkei_financials add column if not exists statement_date date;
alter table nikkei_financials add column if not exists per numeric;
alter table nikkei_financials add column if not exists pbr numeric;
alter table nikkei_financials add column if not exists eps numeric;
alter table nikkei_financials add column if not exists bps numeric;
alter table nikkei_financials add column if not exists equity_ratio numeric;
alter table nikkei_financials add column if not exists roe numeric;
alter table nikkei_financials add column if not exists dividend_yield_pct numeric;
alter table nikkei_financials add column if not exists operating_profit numeric;
alter table nikkei_financials add column if not exists operating_cf numeric;
alter table nikkei_financials add column if not exists net_income numeric;

create index if not exists idx_nikkei_financials_statement_date
  on nikkei_financials (statement_date desc);
