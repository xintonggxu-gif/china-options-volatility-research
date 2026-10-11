# cn_option_data: Live and End-of-Day Quotes for China's Index-Linked Options

`cn_option_data` records bid/ask quotes for every listed option on China's equity-index products, together with the matching ETF or index price and the matching index futures, and saves them as Parquet files. It covers 12 option products on three exchanges (about 1,700 contracts) and can run continuously through the trading day or take one snapshot before and after the close.

It exists because historical bid/ask data for Chinese options is not freely available: a research project that needs quotes has to start recording them itself. A separate command, `backfill-daily`, rebuilds **daily** history (contract lists, terms and daily prices) back to 2015; that history has prices but no bid/ask ([section 11](#11-historical-daily-data-backfill-daily)). Nothing here computes implied volatility, Greeks, surfaces, or PnL; those belong to the analysis code that reads these files.

## Contents

1. [Quick start](#1-quick-start)
2. [Background: the markets and the terms used here](#2-background-the-markets-and-the-terms-used-here)
3. [Products covered](#3-products-covered)
4. [Commands](#4-commands)
5. [How one collection cycle works](#5-how-one-collection-cycle-works)
6. [Data sources and how fields were verified](#6-data-sources-and-how-fields-were-verified)
7. [Output files and column reference](#7-output-files-and-column-reference)
8. [Timestamps and quality flags](#8-timestamps-and-quality-flags)
9. [Matching options to the underlying and futures](#9-matching-options-to-the-underlying-and-futures)
10. [Reading the data](#10-reading-the-data)
11. [Historical daily data: `backfill-daily`](#11-historical-daily-data-backfill-daily)
12. [Reliability: timeouts, retries, fallbacks, shutdown](#12-reliability-timeouts-retries-fallbacks-shutdown)
13. [Configuration files](#13-configuration-files)
14. [Contract metadata rules](#14-contract-metadata-rules)
15. [Limitations](#15-limitations)
16. [Troubleshooting](#16-troubleshooting)
17. [Project layout and tests](#17-project-layout-and-tests)
18. [Changes in this version](#18-changes-in-this-version)

---

## 1. Quick start

Requirements: Python 3.11 or newer, internet access to `hq.sinajs.cn` and the exchange/AKShare endpoints. No accounts or API keys are needed.

```bash
git clone <this repository> && cd china-options-volatility-research
python -m venv .venv
source .venv/bin/activate                 # Windows: .venv\Scripts\activate
python -m pip install -e '.[test]'        # the package plus pytest
python -m pytest                          # offline tests; no network needed
```

Take one snapshot of all 12 products (works during or after trading hours on a trading day):

```bash
python -m cn_option_data.cli collect-snapshot --label first-try
```

It takes about one minute, most of it spent discovering which contracts exist. Then look at what was written:

```bash
python - <<'PY'
import pyarrow.dataset as ds
from cn_option_data.storage.parquet_writer import SCHEMAS
t = ds.dataset("data/raw/option_quotes", format="parquet", schema=SCHEMAS["option_quotes"]).to_table()
print(t.num_rows, "option quotes")
print(t.select(["product_id", "contract_code", "bid_price1", "ask_price1", "volume", "batch_id"]).slice(0, 5).to_pandas())
PY
```

For continuous collection during the day, use `collect-live` ([section 4](#4-commands)).

---

## 2. Background: the markets and the terms used here

**Three exchanges list options on Chinese equity indices.** The Shanghai Stock Exchange (SSE) and the Shenzhen Stock Exchange (SZSE) list options on exchange-traded funds (ETFs) that track an index; the China Financial Futures Exchange (CFFEX) lists options on the indices themselves. All of them are European (exercisable only at expiry).

| Term | Meaning in this project |
| --- | --- |
| **ETF option** | An option on an ETF, e.g. 510050 (an ETF tracking the SSE 50 index). Prices are in CNY per ETF unit. One contract normally covers 10,000 ETF units, so a price of 0.0160 is worth CNY 160 per contract. Exercise delivers the ETF itself. |
| **Index option** | A CFFEX option on an index: HO (SSE 50), IO (CSI 300), MO (CSI 1000). Prices are in **index points**; one contract is worth CNY 100 per point. Settled in cash. |
| **Contract unit / adjusted contract** | When an ETF pays a dividend, the exchange adjusts existing option contracts: the strike falls and the unit rises above 10,000 (for example 510500 contracts had a unit of 10,180 on 2026-09-30), and the contract code's adjustment letter changes from `M` to `A`. The collector stores each contract's actual unit in `volume_multiple`. |
| **Underlying** | What the option is written on: the ETF for ETF options, the index for index options. An index cannot be traded, so it has no bid/ask. |
| **Hedge future** | The CFFEX index future on the same index: IH (SSE 50), IF (CSI 300), IC (CSI 500), IM (CSI 1000). Used for delta hedging and, for index options, as the natural forward price (IO2610 and IF2610 expire on the same day). |
| **Basis** | Futures price minus index level. Chinese index futures often trade below the index, which is why the spot index is not the right forward for index options. |
| **Forward (put-call parity)** | For European options, call − put = D × (F − K) at every strike K, where D is the discount factor. Fitting this across strikes of one expiry gives F, the forward price the option market is actually using; it already includes dividends, borrowing costs and the basis. See [section 9](#9-matching-options-to-the-underlying-and-futures). |
| **BBO** | Best bid and offer: the highest bid (`bid_price1`, `bid_volume1`) and lowest ask (`ask_price1`, `ask_volume1`) in the order book. |
| **Level-1 data** | The public market-data feed (best prices, last trade, volume). It is published as periodic snapshots, not tick by tick. |
| **Closing call auction** | SSE and SZSE options and ETFs stop continuous trading at 14:57 and set closing prices in an auction ending at 15:00. After 15:00 the quotes stop changing. |
| **Contract master** | The exchange's list of currently listed contracts and their terms (strike, expiry, unit). The collector downloads it to know what to request. |
| **Sina** | Sina Finance's public quote endpoint `hq.sinajs.cn`, which returns Level-1 data for many symbols in one HTTP request. Most quotes here come from it. |
| **AKShare** | An open-source Python library wrapping Chinese financial data websites. Used here for contract masters, futures lists and fallbacks. |
| **Batch / `batch_id`** | One HTTP request to Sina. Every row parsed from the same response carries the same `batch_id`. |

---

## 3. Products covered

The product list lives in `config/product_registry.csv` ([section 13](#13-configuration-files)). Contract counts are from 2026-09-30.

| Product | Exchange | Underlying | Hedge future | Contracts | Price unit | Contract size |
| --- | --- | --- | --- | --- | --- | --- |
| 510050 | SSE | SSE 50 ETF (sh510050) | IH | 94 | CNY per ETF unit | 10,000 units (adjusted contracts differ) |
| 510300 | SSE | CSI 300 ETF (sh510300) | IF | 102 | CNY per ETF unit | 10,000 units |
| 510500 | SSE | CSI 500 ETF (sh510500) | IC | 122 | CNY per ETF unit | 10,000 units (10,180 for adjusted) |
| 588000 | SSE | STAR 50 ETF (sh588000) | none | 132 | CNY per ETF unit | 10,000 units |
| 588080 | SSE | STAR 50 ETF (sh588080) | none | 130 | CNY per ETF unit | 10,000 units |
| 159919 | SZSE | CSI 300 ETF (sz159919) | IF | 100 | CNY per ETF unit | 10,000 units |
| 159922 | SZSE | CSI 500 ETF (sz159922) | IC | 104 | CNY per ETF unit | 10,000 units |
| 159915 | SZSE | ChiNext ETF (sz159915) | none | 118 | CNY per ETF unit | 10,000 units |
| 159901 | SZSE | SZSE 100 ETF (sz159901) | none | 110 | CNY per ETF unit | 10,000 units |
| HO | CFFEX | SSE 50 index (sh000016) | IH | 158 | index points | CNY 100 per point |
| IO | CFFEX | CSI 300 index (sh000300) | IF | 236 | index points | CNY 100 per point |
| MO | CFFEX | CSI 1000 index (sh000852) | IM | 262 | index points | CNY 100 per point |

Total: 1,668 contracts on 2026-09-30 and 1,722 on 2026-10-09. Counts change as new strikes and months are listed; the collector re-reads the contract masters every 30 minutes and picks up new contracts automatically.

Expiry months differ between products. ETF options list four months (the next two months and the next two quarter months). CFFEX index options list six (the next three months and the next three quarter months), but the index futures list only four (the next two months and the next two quarter months). On 2026-09-30, for example, IO had 2610, 2611, 2612, 2703, 2706 and 2709 while IF had only 2610, 2611, 2612 and 2703. The two longest IO, HO and MO months therefore have no future expiring with them.

---

## 4. Commands

All commands run from the repository root with the virtual environment active. Logs use Asia/Shanghai time.

### `collect-live`: continuous polling

```bash
python -m cn_option_data.cli collect-live --products all --interval-seconds 60
python -m cn_option_data.cli collect-live --products 510050,IO --interval-seconds 30 --trading-hours-only
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--products` | `all` | `all`, one product ID, or a comma-separated list from the registry. |
| `--interval-seconds` | `60` (or `POLL_INTERVAL_SECONDS`) | Target time between the **starts** of cycles. A cycle that runs long is logged and the next starts after a 1-second pause; cycles never overlap. |
| `--trading-hours-only` | off | Only send requests between 09:30–11:30 and 13:00–15:00 on trading days in `config/trading_calendar.json`. Outside those windows the process stays alive (heartbeat, flushing) but sends nothing. |
| `--flush-interval-seconds` | `30` | Write buffered rows to disk at least this often. |
| `--max-buffer-rows` | `50000` | Also write when this many rows are buffered. |
| `--project-root` | the repository | Where `config/`, `data/` and `logs/` live. |

Stop with `Ctrl+C` or `SIGTERM`. The program stops sending requests, writes everything still in memory, records an `ingestion_runs` row, and exits. Wait for the final `采集结束 ... buffered_rows=0` log line. Log file: `logs/collect-live.log`.

`POLL_INTERVAL_SECONDS` can be set in the shell environment; `.env.example` documents it, but the program does not read `.env` files.

### `collect-snapshot`: one sweep, then exit

```bash
python -m cn_option_data.cli collect-snapshot --products all --label close
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--products` | `all` | As above. |
| `--label` | `snapshot` | Stored as `ingestion_runs.run_mode = snapshot:<label>`. Letters, digits, `_` and `-` only, at most 32 characters. |
| `--force` | off | Skip the trading-day check (see below). |
| `--project-root` | the repository | As above. |

What it does:

1. Checks `config/trading_calendar.json`. On a weekend or exchange holiday it logs that and exits without sending any request. If today is outside the calendar's coverage it stops with an error, because it cannot know whether the market is open; update the calendar or pass `--force`.
2. Downloads the contract masters. If one fails, it retries twice, 5 seconds apart.
3. Runs exactly one collection cycle ([section 5](#5-how-one-collection-cycle-works)), writes everything, records the run, and exits.

It logs to `logs/collect-snapshot.log`, so it can run at the same time as `collect-live`; both append their own Parquet files and never overwrite each other. On 2026-09-30 a full snapshot took about 55 seconds: roughly 46 seconds of contract discovery and 9 seconds for the quote cycle (the option requests themselves took about 1 second).

### Scheduling snapshots

The research plan needs one consistent end-of-day picture plus a pre-close picture of tradable spreads. Run `crontab -e` and add (the machine's clock must be on UTC+8, as Singapore and Shanghai both are, and it must be awake at those times):

```cron
# 14:53: before the SSE/SZSE closing call auction starts at 14:57. Spreads here are tradable.
53 14 * * 1-5  cd /path/to/repo && .venv/bin/python -m cn_option_data.cli collect-snapshot --label preclose >/dev/null 2>&1
# 15:05: after the close every quote is frozen, so the whole snapshot is internally consistent.
5 15 * * 1-5   cd /path/to/repo && .venv/bin/python -m cn_option_data.cli collect-snapshot --label close >/dev/null 2>&1
```

Weekday-only scheduling is not enough by itself (Chinese holidays fall on weekdays); the built-in calendar check handles those days. The calendar covers 2026 only, so it must be extended before 2027 ([section 13](#13-configuration-files)).

---

## 5. How one collection cycle works

```text
every 30 min (or 90 s after a failure)      every cycle
┌───────────────────────────────┐           ┌──────────────────────────────────────────────────────┐
│ contract discovery            │           │ 1. underlying_before  ETF/index prices      (~0.5 s)  │
│  SSE master   (AKShare)       │──────────▶│ 2. futures_before     IF/IH/IC/IM, all months (~4 s)  │
│  SZSE master  (AKShare)       │  active   │ 3. options            per product, one request holds: │
│  CFFEX lists  (AKShare/Sina)  │ contracts │       the product's ETF or index                      │
│  CFFEX expiry dates (OpenCTP) │           │     + every month of its hedge future                 │
└───────────────────────────────┘           │     + up to 200 of its options              (~1 s)   │
                                            │ 4. underlying_after                                   │
                                            │ 5. futures_after                                      │
                                            └──────────────────────────────────────────────────────┘
```

**Discovery.** The SSE and SZSE contract masters come from AKShare (`option_current_day_sse`, `option_current_day_szse`). CFFEX contract months come from `option_cffex_*_list_sina` and the contracts in each month from `option_cffex_*_spot_sina`. Expiry dates for CFFEX contracts come from OpenCTP ([section 14](#14-contract-metadata-rules)). Discovery writes a `contract_snapshots` row the first time the process sees a contract on a given day, and again if its terms change that day.

**Phases 1 and 2** fetch every product's underlying in one Sina request, and the futures lists through AKShare `futures_zh_realtime`. They also tell the options phase which futures months currently exist.

**Phase 3** is the core. For each product, the collector sends one Sina request (two for IO and MO, which have more than 200 contracts) containing the product's underlying, every listed month of its hedge future, and its options. Every row parsed from that response shares a `batch_id`. This is what lets each option be matched to an ETF/index price and futures prices observed in the same response ([section 9](#9-matching-options-to-the-underlying-and-futures)). Contracts that Sina does not return are re-requested in smaller batches if there are 60 or fewer; any still missing go to the fallback sources ([section 12](#12-reliability-timeouts-retries-fallbacks-shutdown)).

**Phases 4 and 5** repeat phases 1 and 2. The `before`/`after` rows bracket the cycle. They were the original way to approximate the underlying price during the option requests and are kept for continuity and for the fallback rows that have no `batch_id`.

Each phase's duration is logged as `采集阶段 cycle=… phase=… seconds=…`, followed by a per-product coverage line `期权覆盖 product=… discovered=… returned=… accepted=… missing=… rejected=…`. Measured on 2026-09-30 for all 12 products: option phase about 1 second (it was about 14 seconds before bundling), whole cycle about 9 seconds. These timings were measured after the close; check them during trading hours before relying on them.

---

## 6. Data sources and how fields were verified

### Sina symbols

All Sina requests go to `https://hq.sinajs.cn/list=<symbol>,<symbol>,…` with a `Referer: https://stock.finance.sina.com.cn/` header. Each line of the response is `var hq_str_<symbol>="<comma-separated fields>";`. An empty string means Sina has no data for that symbol; it is never turned into a quote.

| What | Symbol format | Example |
| --- | --- | --- |
| SSE/SZSE ETF option | `CON_OP_<contract ID>` | `CON_OP_10012313` |
| CFFEX index option | `P_OP_<product, lowercase><yymm><C/P, uppercase><strike>` | `P_OP_io2610C4600` |
| CFFEX index future | `CFF_RE_<symbol>` | `CFF_RE_IF2610` |
| ETF or index | `sh<code>` or `sz<code>` | `sh510050`, `sh000300`, `sz159919` |

The CFFEX option case rule matters: `P_OP_IO2610C4600` and `P_OP_io2610c4600` both return empty payloads. Mixed symbol types in one request work; requests with 500 option symbols (about 8 KB of URL) succeeded on 2026-09-30 and 600 failed, so the collector uses at most 200 options per request.

### Field positions used

Option payloads (`CON_OP_` and `P_OP_` share the layout, matching AKShare `option_sse_spot_price_sina`):

| Position | Field | Column |
| --- | --- | --- |
| 2 | last trade price | `last_price` |
| 5 | open interest | `open_interest` |
| 7 | strike | checked against the contract master |
| 10, 11 | upper and lower price limits | `upper_limit`, `lower_limit` |
| 20, 21 | best ask price and size | `ask_price1`, `ask_volume1` |
| 22, 23 | best bid price and size | `bid_price1`, `bid_volume1` |
| 32 | time field, `YYYY-MM-DD HH:MM:SS` ([section 8](#8-timestamps-and-quality-flags)) | `source_quote_time`, `quote_timestamp` |
| 36 | underlying code: `510050` for ETF options, `sh000300` for IO | checked against the registry |
| 41 | volume (contracts) | `volume` |
| 42 | turnover | `amount` |
| 45 | `C` or `P` | checked against the contract master |

A quote is rejected (and the product flagged `INVALID_QUOTE`) if the strike, underlying, or call/put in the payload disagrees with the contract master.

Futures payloads (`CFF_RE_`): 0 open, 1 high, 2 low, 3 last, 4 volume, 6 open interest, 13 previous close, 14 previous settlement, 16/17 bid price/size, 26/27 ask price/size, 36 trade date, 37 tick time.

ETF/index payloads: 3 last price, 8 volume, 9 turnover, 10/11 bid size/price, 20/21 ask size/price, 30/31 date and time. Index payloads have no bid/ask.

### Verification performed (closing data of 2026-09-30 and 2026-10-09)

- **CFFEX options:** all 656 IO/HO/MO contracts were requested through `P_OP_` and compared with the AKShare `spot_sina` tables (the previous source). Bid, ask, both sizes, last price, open interest, strike, underlying and call/put agreed for every contract. That table has no volume, so volume was checked against AKShare `option_cffex_hs300_daily_sina` daily bars instead: it matched for the three IO contracts compared, and a fourth could not be compared because its daily bar for that day had not yet been published.
- **CFFEX turnover units:** Sina reports CFFEX turnover as price × contracts in index points, not CNY. For all 656 contracts, turnover ÷ volume fell inside that contract's daily low–high price range. The collector therefore multiplies it by 100 so `amount` is in CNY for every product (stored as a float, so tiny rounding such as `56879.99999999999` can appear). ETF option turnover is already in CNY: 64,181 contracts × about 0.0162 × 10,000 units ≈ the reported CNY 10.37 million.
- **Futures:** for every IF contract, the 14 `CFF_RE_` positions listed above matched the named fields returned by AKShare `futures_zh_realtime`.
- **CFFEX contracts that did not trade in the session:** Sina keeps such a contract's record from its last trading day. MO2703P9200, last traded on 2026-09-11, still showed that day's time, volume (3) and close (2289.0) on 2026-09-30, matching its 2026-09-11 daily bar. Its price limits are also that day's: on 2026-10-09, IO2612P4900 (last traded 2026-10-08) had limit-up minus previous settlement of 435.6 points, while every IO December contract that traded on 2026-10-09 showed 431.0. Its bid/ask did look current: the midpoint, 653.5, sat between those of the neighbouring strikes on 2026-10-09. The collector handles these rows as described in [section 8](#8-timestamps-and-quality-flags). ETF options are not affected: Sina reports an untraded ETF option with today's date, a midnight time and zero volume.
- **ETF options:** the positions used are the existing AKShare mapping; bid/ask at positions 22/20 agreed with the duplicate values at positions 1/3 on the contracts checked.

### Other sources

| Source | Used for |
| --- | --- |
| AKShare `option_current_day_sse` / `option_current_day_szse` | SSE and SZSE contract masters |
| AKShare `option_cffex_*_list_sina` / `option_cffex_*_spot_sina` | CFFEX months and contracts; fallback CFFEX quotes |
| OpenCTP `dict.openctp.cn/instruments?types=option` | CFFEX last trading dates |
| AKShare `futures_symbol_mark` / `futures_zh_realtime` | Which futures months exist; `before`/`after` futures quotes |
| AKShare `option_current_em` (Eastmoney) | Fallback for SSE option quotes Sina did not return (last price only) |

---

## 7. Output files and column reference

```text
data/raw/contract_snapshots/   contract terms as discovered
data/raw/option_quotes/        one row per option per observation
data/raw/underlying_quotes/    ETF and index observations
data/raw/futures_quotes/       index futures observations
data/metadata/ingestion_runs/  one row per collector run
logs/collect-live.log          live collector log (10 MiB × 4 files, rotating)
logs/collect-snapshot.log      snapshot log
```

These are the live and snapshot outputs described in this section. The daily history from `backfill-daily` lives separately under `data/historical/` ([section 11](#11-historical-daily-data-backfill-daily)).

Files are written with PyArrow and ZSTD compression into folders `trade_date=…/exchange=…/hour=…/part-<uuid>.parquet`. Files are only ever added, never modified, so restarts and concurrent runs are safe. The partition values are also stored as string columns inside every file. `hour` is the **local hour when the row was received**, not a market time. `data/` and `logs/` are excluded from git.

### Columns shared by the three quote tables

| Column | Type | Meaning |
| --- | --- | --- |
| `trade_date` | string | Market date taken from the source's own date field; the literal `unknown` if the source gave none. For an option flagged `NO_TRADE_THIS_SESSION`, the date of the underlying fetched in the same response. Never filled from the local clock. |
| `exchange` | string | `SSE`, `SZSE` or `CFFEX` (futures rows are always `CFFEX`). |
| `hour` | string | Local receipt hour, `00`–`23`. Partition only. |
| `bid_price1`, `ask_price1` | float | Best bid/ask. Null if missing, crossed, or for an index. Never filled from the last price. |
| `bid_volume1`, `ask_volume1` | int | Sizes at the best bid/ask: contracts for options and futures; shares for ETFs. |
| `last_price` | float | Last trade price. May be hours old for illiquid options ([section 8](#8-timestamps-and-quality-flags)). |
| `volume` | int | Cumulative volume today: contracts for options and futures; raw Sina units for ETFs and indices. `0` for options flagged `NO_TRADE_THIS_SESSION`. |
| `quote_timestamp` | timestamp | Source date and time, only when the source supplied both and the time is not midnight. Its meaning is unverified ([section 8](#8-timestamps-and-quality-flags)). |
| `exchange_timestamp` | timestamp | Same value as `quote_timestamp` (kept for compatibility). |
| `request_timestamp` | timestamp | When the HTTP request (or outer AKShare call) started. |
| `received_timestamp` | timestamp | When the response arrived. |
| `source` | string | `Sina` for direct requests, `AKShare` for AKShare functions (some of which also call Sina). |
| `source_interface` | string | Exact interface, e.g. `hq.sinajs.cn/list` or `option_cffex_hs300_spot_sina`. |
| `provider_symbol` | string | The provider's identifier, e.g. `CON_OP_10012313`, `P_OP_io2610C4600`, `sh510050`, `IF2610`. |
| `batch_id` | string | The Sina response the row came from. Rows with the same `batch_id` came back in one response. Null for AKShare-sourced rows. |
| `run_id` | string | The `ingestion_runs` row of the process that wrote it. |

All timestamps are timezone-aware (Asia/Shanghai, microsecond precision).

### `option_quotes`

Shared columns plus:

| Column | Type | Meaning |
| --- | --- | --- |
| `product_id` | string | Registry product, e.g. `510050`, `IO`. |
| `contract_code` | string | SSE/SZSE: the numeric contract ID (`10012313`). CFFEX: `IO2610C4600` (product, yymm, side, strike). |
| `underlying_symbol` | string | Registry form, e.g. `SSE.510050`, `SSE.000300`. |
| `option_class` | string | `C` or `P`. |
| `strike_price` | float | CNY for ETF options; index points for CFFEX. Adjusted contracts carry the adjusted strike. |
| `expire_datetime` | timestamp | Expiry **date** (midnight is a date placeholder, not an expiry time). Null for CFFEX if OpenCTP could not confirm it. |
| `amount` | float | Turnover today in CNY for every product (CFFEX converted from index points). `0` when flagged `NO_TRADE_THIS_SESSION`. |
| `open_interest` | int | Open contracts. |
| `upper_limit`, `lower_limit` | float | Daily price limits (Sina rows only). Null when flagged `NO_TRADE_THIS_SESSION`, because Sina's values then belong to an earlier session. |
| `source_quote_time` | string | Sina position 32 exactly as received. |
| `supplement_request_timestamp` | timestamp | Reserved; currently always null. |
| `quote_status` | string | `OK` or quality flags joined by `\|` ([section 8](#8-timestamps-and-quality-flags)). |

### `underlying_quotes`

Shared columns plus `underlying_symbol` (registry form), `product_id` (the product this row was collected for), `amount` (turnover, CNY), and:

| Column | Values | Meaning |
| --- | --- | --- |
| `collection_phase` | `before`, `after`, `bundled` | `before`/`after`: the bracketing requests in phases 1 and 4. `bundled`: fetched inside that product's option request in phase 3, joinable by `batch_id`. |

A `before`/`after` row is written once per product that uses the underlying.

### `futures_quotes`

Shared columns plus `future_symbol` (e.g. `IF2610`), `future_product` (`IF`, `IH`, `IC`, `IM`), `mapped_option_product` (the option product it hedges), `open_interest`, and `collection_phase` as above. `before`/`after` futures rows come from AKShare `futures_zh_realtime` and are written once per mapped product (IF maps to 510300, 159919 and IO). `bundled` rows come from `CFF_RE_` in the product's own option request.

### `contract_snapshots`

`snapshot_timestamp`, `product_id`, `contract_code`, `contract_name`, `underlying_symbol`, `option_class`, `strike_price`, `expire_datetime`, `last_trading_date`, `volume_multiple` (ETF units per contract, or 100 for CFFEX), `price_tick` (0.0001 CNY for ETF options, 0.2 points for CFFEX), `is_trading`, `source`, `provider_symbol`, `source_interface` (includes the rule URL used), `request_timestamp`, plus `trade_date`/`exchange`/`hour`. A process writes a row the first time it sees a contract on a given day and again if the contract's terms change that day. Every new process writes its own rows, so expect duplicates across runs; deduplicate on `contract_code` and the term columns.

### `ingestion_runs`

One row per process: `run_id`, `started_at`, `finished_at`, `provider`, `run_mode` (`live` or `snapshot:<label>`), `contracts_discovered`, `products_discovered`, `quote_rows_written`, `error_type`, `error_message`, and `status`:

| Status | Meaning |
| --- | --- |
| `STOPPED` | Normal stop with no recorded problems. |
| `PARTIAL` | Everything was saved, but something was incomplete: an interface failed, a product was missing, or quality flags were raised. `error_message` lists them. Common and usually harmless; see [section 16](#16-troubleshooting). |
| `NO_ACTIVE_CONTRACTS` | No contracts were discovered. |
| `FAILED` | A fatal local error, such as a disk write failure. Do not assume buffered data was saved. |

### Files written before this version

Older files lack `batch_id`, `run_id`, `collection_phase` and `run_mode`. Read with the writer's schema so they load as null ([section 10](#10-reading-the-data)). Old rows marked `quote_status=OK` predate the current time checks and old partitions may use a receipt date; separate them by `received_timestamp` before combining.

---

## 8. Timestamps and quality flags

### What each time means

- `request_timestamp` / `received_timestamp`: when **this program** asked and got the answer. They say when a quote was observed, not when it last changed.
- `quote_timestamp`: the source's own date and time, kept only when both parts are present and the time is not midnight.
- `trade_date`: the source's date, even when the time is missing.

The same rules apply to all three quote tables; only `option_quotes` has a `quote_status` column to record the flag.

| Source supplied | `quote_timestamp` | `trade_date` | Flag |
| --- | --- | --- | --- |
| Full date and time | the source value | source date | `TIME_SEMANTICS_UNVERIFIED` |
| Date only, or midnight | null (raw text kept in `source_quote_time`) | source date | `SOURCE_TIME_DATE_ONLY` |
| No usable date | null | `unknown` | `SOURCE_TIME_UNKNOWN` |
| An option date earlier than the date of the underlying in the same response | null (raw text kept in `source_quote_time`) | the underlying's date | `NO_TRADE_THIS_SESSION` |

Quotes are not rejected for carrying an older source date. A whole quote from a previous session (for example, polled before the open) is stored under that session's date. ETF and index times from Sina can be later than 15:00 (for example `15:34:59`) because Sina keeps updating its record after the close.

**Options that have not traded this session.** For a CFFEX contract with no trade in the current session, Sina repeats the record from the contract's last trading day: its time field, volume, turnover and price limits all belong to that day, while its bid/ask is current ([section 6](#6-data-sources-and-how-fields-were-verified) gives the evidence). Without special handling, such a row would be filed under the old date with an old day's volume. The collector detects these rows by comparing the option's date with the date of the underlying (ETF or index) fetched in the same Sina response. When the option's date is earlier, the contract cannot have traded in the current session, so the row is stored with:

- `trade_date` = the underlying's date (the session the bid/ask belongs to);
- `quote_timestamp` null, with the original text kept in `source_quote_time`;
- `volume` = 0 and `amount` = 0, because there has been no trade this session;
- `upper_limit` and `lower_limit` null, because Sina's values belong to the earlier session;
- `bid_price1`/`ask_price1`, their sizes and `open_interest` as received (open interest changes only through trades, so it is unchanged since the last one);
- `last_price` as received: the price of the last trade, on the day shown in `source_quote_time`;
- `quote_status` = `NO_TRADE_THIS_SESSION`.

In a snapshot of the 2026-10-09 close (taken on 2026-10-11 with `--force`), 20 of 1,722 contracts were flagged, all CFFEX, and every row was dated 2026-10-09. Rows from the fallback sources carry no `batch_id` and are not checked this way.

### What Sina's option time field actually is

A test on 2026-09-30 at 17:52, after the close, read all 1,012 ETF option contracts:

| Time shown | Contracts | Volume that day |
| --- | --- | --- |
| `15:00:00` | 249 | all > 0 |
| intraday, 09:34–14:57 | 638 | all > 0 |
| midnight | 94 | 0 |
| midnight | 31 (all SSE) | > 0 |

Every untraded contract showed midnight and every intraday time belonged to a traded contract, so the field behaves like a **last-trade time**, not a quote or snapshot time. The 31 traded SSE contracts showing midnight are not explained by this. All 1,012 contracts still had a two-sided bid/ask.

Practical consequences: for end-of-day prices use the closing bid/ask midpoint, which is current, rather than `last_price`, which may be hours old; and treat `source_quote_time` as a measure of how stale `last_price` is.

### `quote_status` flags (option rows)

| Flag | Meaning |
| --- | --- |
| `OK` | No flag raised. |
| `BBO_NOT_AVAILABLE` | Bid or ask missing. The other fields are kept. |
| `CROSSED_BBO` | Bid above ask. Bid, ask and sizes are set to null. |
| `SOURCE_TIME_DATE_ONLY` | The source gave a date but no usable time. |
| `SOURCE_TIME_UNKNOWN` | The source gave no usable date. |
| `NO_TRADE_THIS_SESSION` | The contract has not traded in the current session; see above for how the row is stored. |
| `TIME_SEMANTICS_UNVERIFIED` | A full time was given, but it is not proven to be the time the bid/ask was set (see above). |

Several flags are joined with `|` in this order: bid/ask condition, then the time flag (`SOURCE_TIME_DATE_ONLY`, `SOURCE_TIME_UNKNOWN` or `NO_TRADE_THIS_SESSION`), then `TIME_SEMANTICS_UNVERIFIED`. A row with none of bid, ask, last price, volume or open interest is not written.

The collector saves what passed these checks exactly as observed. It does not remove repeated identical quotes, compare against earlier files, or drop late records.

---

## 9. Matching options to the underlying and futures

### The join

Every option row from Sina carries the `batch_id` of its response. The same response contains the product's underlying (`underlying_quotes`, `collection_phase = 'bundled'`) and every listed month of its hedge future (`futures_quotes`, `collection_phase = 'bundled'`). The match is therefore exact: no choosing between the `before` and `after` prices, and no averaging. With DuckDB:

```python
import duckdb
con = duckdb.connect()
for t in ("option_quotes", "underlying_quotes", "futures_quotes"):
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('data/raw/{t}/**/*.parquet', "
                "hive_partitioning=false, union_by_name=true)")

options_with_underlying = con.execute("""
    SELECT o.*, u.last_price AS underlying_last, u.bid_price1 AS underlying_bid, u.ask_price1 AS underlying_ask
    FROM option_quotes o
    JOIN underlying_quotes u
      ON u.batch_id = o.batch_id AND u.product_id = o.product_id AND u.collection_phase = 'bundled'
""").df()

io_with_same_month_future = con.execute("""
    SELECT o.contract_code, o.bid_price1, o.ask_price1, f.future_symbol, f.bid_price1 AS fut_bid, f.ask_price1 AS fut_ask
    FROM option_quotes o
    JOIN futures_quotes f
      ON f.batch_id = o.batch_id AND f.mapped_option_product = o.product_id AND f.collection_phase = 'bundled'
    WHERE o.product_id = 'IO' AND substr(f.future_symbol, 3, 4) = substr(o.contract_code, 3, 4)
""").df()
```

(`pip install duckdb` first; it is not a dependency of the collector.) On the 2026-09-30 test data, all 6,198 option rows joined to exactly one bundled underlying row. The second query pairs each IO option with the IF future of the same month (IO2610 with IF2610): they expire on the same day, so that future is the natural forward for the option. It returns nothing for the two longest IO months, because no IF contract expires with them ([section 3](#3-products-covered)): in the test data, 728 of 944 IO rows found a same-month future. For those months, use the parity forward below.

Rows from the fallback sources (Eastmoney for SSE, the AKShare table for CFFEX) have no `batch_id`. For those, use the `before` and `after` rows of the same `run_id` around the row's `request_timestamp`.

### What "same response" does and does not guarantee

Rows sharing a `batch_id` were served together by Sina. They are not guaranteed to be the same exchange instant: Level-1 data is itself a series of snapshots a few seconds apart, and Sina may refresh symbols at slightly different moments. After 15:00 nothing moves, which is why the `close` snapshot is fully consistent.

### Use the forward, not the ETF price, for implied volatility

The underlying price is a sanity check rather than the input to implied volatility. Fit put-call parity (C − P = D·(F − K)) across the strikes of one expiry to get the forward F that the options actually price; it already includes dividends, borrowing costs and the futures basis. On 2026-09-30, the 510050 October chain implied a forward of 2.9342 against an ETF close of 2.9380 (13 bp lower). Using the ETF price instead of the forward made call and put implied volatilities differ by 2–4 volatility points at the same strike, even though every price came from one post-close response. The SSE's own published IVs (AKShare `option_risk_indicator_sse`, 2026-09-29) show a similar 2–3 point call/put gap, consistent with the exchange using the spot price. For index options, use the same-month future or the parity forward, not the index.

---

## 10. Reading the data

**PyArrow** (pass the writer's schema so files from before and after this version combine, with missing columns as null; without the schema, new columns can be silently dropped):

```python
import pyarrow.dataset as ds
from cn_option_data.storage.parquet_writer import SCHEMAS

options = ds.dataset("data/raw/option_quotes", format="parquet", schema=SCHEMAS["option_quotes"]).to_table()
close = options.filter(ds.field("trade_date") == "2026-09-30").to_pandas()
```

**pandas**: `pd.read_parquet` on the folder infers `hour` from folder names as an integer, which clashes with the string column inside the files. Use the PyArrow call above and `.to_pandas()`, or read individual files with `pyarrow.parquet.ParquetFile`.

**DuckDB**: use `hive_partitioning=false, union_by_name=true` as in [section 9](#9-matching-options-to-the-underlying-and-futures).

**Selecting one snapshot**: join to `ingestion_runs` on `run_id` and filter `run_mode = 'snapshot:close'`.

Keep `trade_date = 'unknown'` rows separate; never replace their date with the local date. For date filters, `pandas.to_datetime(..., errors="coerce")` turns `unknown` into `NaT`.

A quick look at the latest files without loading everything:

```bash
python - <<'PY'
from pathlib import Path
import pyarrow.parquet as pq
for folder in ('data/metadata/ingestion_runs', 'data/raw/option_quotes'):
    files = sorted(Path(folder).rglob('*.parquet'), key=lambda p: p.stat().st_mtime)
    print(folder, 'files=', len(files), 'bytes=', sum(p.stat().st_size for p in files))
    if files:
        table = pq.ParquetFile(files[-1]).read()
        columns = [c for c in ('run_id', 'run_mode', 'status', 'quote_rows_written', 'error_message',
                   'product_id', 'contract_code', 'trade_date', 'quote_timestamp', 'source_quote_time',
                   'bid_price1', 'ask_price1', 'batch_id', 'quote_status') if c in table.column_names]
        print(table.select(columns).slice(0, 5).to_pandas().to_string(index=False))
PY
```

---

## 11. Historical daily data: `backfill-daily`

The live collector only sees the market from the moment it starts. `backfill-daily` is a separate command that rebuilds **daily** history from 2015 onward: which contracts existed on each trading day, their terms, daily open/high/low/close for options, underlyings and futures, and settlement prices where the exchange publishes them. It writes to its own tables under `data/historical/` and does not touch `collect-live`, `collect-snapshot` or their tables.

Daily OHLC and settlement prices are **not** bid/ask quotes. They must never be written into, or mixed with, the live `bid_price1`/`ask_price1` columns.

### What it does for each trading day

1. Reads the historical trading calendar and skips dates before each product's `listing_date` in `config/product_registry.csv`. The earliest chain, 510050, starts on 2015-02-09.
2. Discovers which contracts existed that day, from exchange records rather than from live snapshots:
   - **SSE:** the dated option risk report, with exact terms carried forward from SSE listing and adjustment records.
   - **SZSE:** the dated risk report joined with historical listing, delisting and adjustment files.
   - **CFFEX:** the official daily CSV, which lists every option and futures contract trading that day.
3. Downloads prices:
   - **ETFs:** raw, unadjusted daily OHLC from Tencent through AKShare `stock_zh_a_hist_tx(adjust="")`.
   - **Indices:** AKShare `stock_zh_index_daily`.
   - **SSE/SZSE options:** Sina per-contract daily OHLC (the endpoint behind AKShare `option_sse_daily_sina`, called directly so that a historical `null` can be told apart from a network failure).
   - **CFFEX options and IH/IF/IC/IM futures:** OHLC and settlement from the official daily CSV.
4. Writes Parquet tables recording the source, interface, provider symbol, source trading day, local request and receipt times, and a reason for any missing field, then writes a coverage report per product and day. Missing contracts and prices stay missing.

### Running it safely

Every run needs explicit dates and defaults to a preview that makes no HTTP requests:

```bash
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2015-02-09 --end-date 2015-02-09 --dry-run --max-requests 20
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2015-02-09 --end-date 2015-02-09 --execute --max-requests 20
```

| Option | Meaning |
| --- | --- |
| `--products` | As for the other commands. |
| `--start-date`, `--end-date` | Required, `YYYY-MM-DD`. |
| `--dry-run` / `--execute` | Preview (default; no requests, no writes) or actually download. |
| `--max-requests` | Cap on HTTP requests for the run, counting requests made inside AKShare. Default 20, so a full year needs many resumed runs or a deliberately larger cap. |
| `--retry-absent-prices` | Re-request contract/date prices that an earlier run found explicitly absent. |
| `--repair-sse-terms` | Fill missing SSE contract terms only (see below). SSE products only. |
| `--output-root` | Write to a separate directory instead of the project's `data/`, e.g. for a trial sample. |
| `--project-root` | As for the other commands. |

Each outer call has a 15-second deadline, calls are spaced at least 0.5 seconds apart, and a failed call is retried once within the request cap. `Ctrl+C` or `SIGTERM` stops new calls, saves completed rows and writes the report. Re-running the same command resumes: rows already saved are recognised by their business keys and skipped. The run prints a JSON report; before extending the date range, check `option_coverage_of_exchange_roster`, `option_missing_examples` and the per-category statuses in it.

**Caching.** A successful Sina per-contract response covers many dates and is cached in `data/metadata/backfill_price_cache/`, so later days and restarts reuse it without another request. Contract/date prices that are clearly absent at the source are recorded in `data/metadata/backfill_absences.json` and skipped on later runs unless `--retry-absent-prices` is given. Network errors and request-limit stops are never recorded as absent.

### Outputs

```text
data/historical/roster_daily/          contracts listed on each day
data/historical/option_daily/          option daily OHLC (and settlement for CFFEX)
data/historical/underlying_daily/      ETF and index daily OHLC
data/historical/futures_daily/         IH/IF/IC/IM daily OHLC and settlement
data/historical/contract_terms_daily/  contract terms in force on each day
data/historical/contract_adjustments/  dividend adjustments
data/metadata/backfill_runs/           one JSON report per run
```

Tables are partitioned by trade date and exchange. Join `option_daily` to `contract_terms_daily` on trade date, product and contract code to get expiry and contract unit; an `option_daily` row can have a null expiry on its own. A zero or missing source close is kept as received with a reason in `missing_reason` and cannot be used directly for implied volatility.

### Repairing SSE terms and auditing quality

Some earlier SSE runs saved a day's contracts and prices but not that day's terms. `--repair-sse-terms` requests only the dated SSE roster, listing and adjustment records needed to fill missing `contract_terms_daily` rows; it requests no prices. Start from the last day **from which terms are complete without gaps** and move forward in date order, because a listing or adjustment that happened inside an unprocessed gap cannot be reconstructed from a later day. Preview, then repeat bounded runs until the report no longer shows `REQUEST_LIMIT`:

```bash
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2024-03-01 --end-date 2025-01-02 --repair-sse-terms --dry-run --max-requests 50
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2024-03-01 --end-date 2025-01-02 --repair-sse-terms --execute --max-requests 50
```

The dates are only an illustration; choose the start from your own coverage reports. The repair only adds verified term rows. When the exchange records cannot verify a term, the report says `SOURCE_UNAVAILABLE` or `REQUEST_LIMIT` rather than inventing a value.

To audit saved data without any network request or change to the files:

```bash
python -m cn_option_data.collectors.historical_quality --products 510050 --start-date 2025-01-02 --end-date 2025-01-02
```

It reports missing or conflicting terms, null or non-positive closes, inconsistent OHLC, a missing same-day underlying, and contracts whose expiry is still unknown after joining the verified terms. It only examines dates that have saved files; use `backfill_runs/` to find dates with nothing saved.

### Known gaps in the history

Every dry-run report lists these source limits:

| Item | Status |
| --- | --- |
| Interest rates | Unsupported: no historical risk-free curve has been source-verified, so `rates_daily/` stays empty. |
| ETF dividends | Unsupported: historical cash distributions have not been source-verified, so `etf_dividends/` stays empty. |
| CFFEX terms | Unsupported: the official daily CSV has no exact option expiry or contract multiplier, so CFFEX option `contract_terms_daily` is unavailable. |
| Early SZSE prices | Some verified old SZSE contract codes return `null`, or omit early dates, in Sina's daily history. A historical contract list does not guarantee historical prices. |

Rates and dividends are needed for a reliable daily implied-volatility surface. SSE and SZSE rosters and terms were checked on a few early days, but complete multi-year coverage is not yet proven: read every run's coverage report. HO's `listing_date` in the registry was corrected to 2022-12-19 from the [CFFEX listing notice](https://www.cffex.com.cn/cn/jystz/20221214/30918.html).

---

## 12. Reliability: timeouts, retries, fallbacks, shutdown

**One request at a time, with a hard deadline.** Every network call runs in a single worker subprocess, one after another. Each call has a 15-second limit that includes AKShare's own internal requests; direct HTTP calls also have a 3-second connect and 10-second read timeout. If a call overruns, the worker is killed (up to about 2 extra seconds) and a fresh one is started. The main process keeps flushing data and emitting heartbeats while it waits. A response that never arrived cannot be recovered.

**Contract discovery retries.** Masters are refreshed every 30 minutes. If one fails, the next attempt is 90 seconds later instead of 30 minutes; without this, a single SZSE connection error on 2026-09-30 would have left four products uncollected for half an hour. A snapshot retries twice at 5-second intervals before collecting. If a master fails during live collection, contracts already known and not yet expired are kept.

**Missing quotes.** Contracts that Sina leaves out are re-requested in batches of 20 if there are 60 or fewer. Any still missing then go to a fallback:

| Product | Fallback | What the fallback lacks |
| --- | --- | --- |
| SSE ETF options | AKShare `option_current_em` (Eastmoney), called at most once per cycle; after 3 consecutive failures it pauses for 5 minutes | bid/ask, source time, price limits |
| CFFEX options | AKShare `option_cffex_*_spot_sina`, only for the months that have missing contracts | volume, turnover, source time, price limits |
| SZSE ETF options | none | — |

Fields from different requests are never combined into one row. A failed batch does not stop other batches or products, and missing contracts never produce made-up quotes.

**Futures scope.** Only the futures needed by the selected products are requested (selecting only 510050 requests IH only). AKShare identifies them by Sina's internal names: IF → `qz_qh`, IH → `szgz_qh`, IC → `zzgz_qh`, IM → `im_qh`. Futures months are read from the response, never hard-coded.

**Shutdown.** `Ctrl+C` or `SIGTERM` stops new requests; results already received are still saved. The program then flushes everything, writes `ingestion_runs`, and restores the original signal handlers. Repeated stop signals do not interrupt the final write. A write failure exits with a non-zero code and keeps the unsaved batch in memory until exit. Power loss or a forced kill can still lose buffered rows (at most about 30 seconds' worth at the default flush interval).

**Monitoring.** A heartbeat line every 60 seconds shows, per product and table, rows accepted into memory and rows persisted to disk, plus cumulative quality flags and missing fields. Each cycle logs discovered/returned/accepted/missing/rejected counts and contract codes per product.

---

## 13. Configuration files

### `config/product_registry.csv`

The only definition of which products are collected. Only rows with `status = ACTIVE` are used.

| Column | Meaning |
| --- | --- |
| `product_id` | ID used on the command line and in every table. |
| `exchange` | `SSE`, `SZSE` or `CFFEX`. |
| `option_category` | `ETF` or `INDEX`. |
| `underlying_code`, `underlying_type` | Descriptive. |
| `provider_underlying_symbol` | `SSE.510050`-style code; converted to Sina's `sh510050`. For ETF options it must equal `<exchange>.<product_id>`. |
| `hedge_future` | `IH`, `IF`, `IC`, `IM`, or empty. |
| `listing_date` | First trading day of the product's options. `backfill-daily` skips earlier dates. |
| `status`, `observation_end_date`, rule and version columns | Descriptive metadata about the product and the rules it follows. |

To add an ETF option product (for example a newly listed one), add a row with the same structure and `status = ACTIVE`; the next run picks it up. A new CFFEX index option product needs code changes as well, all in `providers/akshare_live.py`: its AKShare list and spot interfaces in `CFFEX_APIS`, its code in the `INDEX_CODE` and `MONTH_CODE` patterns, and, if it has a new hedge future, that future in `FUTURE_CODE` and `FUTURE_MARKS`.

### `config/trading_calendar.json`

Used by `--trading-hours-only` and by the snapshot trading-day check.

| Field | Meaning |
| --- | --- |
| `valid_from`, `valid_to` | Dates covered. Currently 2026-01-01 to 2026-12-31. A date outside this range is an error, never an assumption. |
| `closed_ranges` | Exchange holidays, inclusive. Weekends are always closed, including weekend make-up working days. |
| `sampling_windows` | 09:30–11:30 and 13:00–15:00. Includes the closing auction, excludes the opening auction. |
| `sources`, `verified_on`, `verification_note` | Where the holidays came from. SSE and SZSE notices were checked directly; the CFFEX site timed out, so its notice was checked through a syndicated copy. |

**Before 1 January 2027:** add the 2027 holidays from the exchanges' official notices (normally published in December), extend `valid_to`, update `sources` and `verified_on`, and restart any running collector (the file is read once at start).

---

## 14. Contract metadata rules

- **ETF option tick size:** CNY 0.0001 per ETF unit (SSE Article 59, SZSE Rule 4.3.18), including adjusted contracts.
- **ETF contract unit:** taken from each contract's master row, so adjusted units such as 10,180 are kept.
- **SSE last trading date:** when the master omits it, the expiry date is used only if the master's expiry and exercise dates agree (SSE Article 9). Conflicts stay null and are logged.
- **SZSE last trading date:** taken from the master as given.
- **CFFEX:** multiplier CNY 100 per point, tick 0.2 points ([CFFEX index option rules](https://www.cffex.com.cn/cn/ssxz/20221214/43100.html)). Last trading dates come from OpenCTP and are used only after the exchange, contract code, year/month, strike, multiplier, tick and call/put all match. If OpenCTP is unavailable or does not match, the date stays null with a warning. The program never infers a "third Friday" date.
- **Dates, not times:** `expire_datetime` and `last_trading_date` hold dates stored as midnight timestamps. Midnight is not an expiry time; do not use it as one for IV or Greeks.
- **No guessing:** no expiry is derived from the contract month.

Rules checked on 2026-09-27:
[SSE rules, Articles 9 and 59](https://www.sse.com.cn/lawandrules/sselawsrules2025/option/c/c_20250610_10781448.shtml) ·
[SZSE rules, Sections 2.5 and 4.3.18](https://docs.static.szse.cn/www/option/rules/optrules/W020191207434561721119.pdf) ·
[CFFEX SSE 50 index option specifications](https://www.cffex.com.cn/cn/sz50gzqq.html)

---

## 15. Limitations

- **No historical bid/ask.** Quotes exist only from the moment live collection starts. `backfill-daily` supplies daily prices from 2015, not quotes, and has its own gaps ([section 11](#11-historical-daily-data-backfill-daily)). `backfill-daily` reads the SSE's daily risk report to find which contracts existed, but does not store the official IV and Greeks in it. Index minute bars are not downloaded.
- **Polling, not streaming.** Each observation is a Level-1 snapshot at request time; changes between polls are not seen.
- **One book level.** Only the best bid and ask are stored, although Sina returns five levels for ETF options.
- **No derived quantities.** No interest rates, dividends, forwards, IV, Greeks, fees or margins are computed or assumed.
- **Timestamps are not proven.** See [section 8](#8-timestamps-and-quality-flags).
- **Third-party endpoints.** Sina, AKShare and OpenCTP are unofficial and can change, slow down or block heavy use without notice. Every field mapping above was verified on 2026-09-30 and should be re-checked if values start looking wrong. Polling about 1,700 contracts every minute all day has not been tested for rate limits.
- **Timings measured after the close.** The cycle timings in this README come from post-close runs on 2026-09-30; confirm them during a trading session.

---

## 16. Troubleshooting

| Symptom | Likely cause and what to do |
| --- | --- |
| Run status `PARTIAL` with `option_contract_info_ctp … HARD_TIMEOUT` | OpenCTP was slow. Quotes are still collected; CFFEX `expire_datetime` is null for that run. Usually fine on the next refresh. |
| `PARTIAL` listing `TIME_SEMANTICS_UNVERIFIED` counts | Expected on every run; it records that source times are unproven. |
| Options with `volume = 0` but a `last_price` | Normal: the contract has not traded today; `last_price` is the last trade on an earlier day (`source_quote_time` shows when). |
| CFFEX rows with `trade_date = 'unknown'` and no volume in older files | Files written before this version took CFFEX quotes from the AKShare table, which has no date, time or volume. |
| Log `合约发现失败 products=[…]，90 秒后重试` | A contract master failed. It is retried automatically; those products are skipped until it succeeds. |
| A product is missing from the `期权覆盖` lines | Its discovery failed (see above), or the registry selection excludes it. |
| `交易日历仅覆盖 … 无法确认 …` | The date is outside `trading_calendar.json`. Extend the calendar ([section 13](#13-configuration-files)) or use `--force` for a snapshot. |
| `… 不是交易日，快照未发起请求` | The snapshot ran on a holiday or weekend and correctly did nothing. No `ingestion_runs` row is written. |
| `Unable to merge: Field hour has incompatible types` | The reader inferred Hive partitions. Read as in [section 10](#10-reading-the-data). |
| New columns missing when reading | Old and new files were read without the writer's schema. |
| CFFEX quotes all missing, fallback warning `CFFEX P_OP 未取得报价` | Check that symbols are `P_OP_io2610C4600` style; Sina returns nothing for other spellings. |
| `轮询耗时 … 超过设置间隔` | A cycle took longer than `--interval-seconds`; raise the interval or select fewer products. |
| Snapshot did nothing from cron | The machine was asleep, its clock is not on UTC+8, or the cron line's path is wrong; check `logs/collect-snapshot.log`. |

---

## 17. Project layout and tests

```text
config/
  product_registry.csv         products, underlyings, hedge futures
  trading_calendar.json        2026 holidays and sampling windows
src/cn_option_data/
  cli.py                       commands: collect-live, collect-snapshot, backfill-daily
  config.py                    runtime settings (intervals, retries, paths)
  registry.py                  loads and validates the product registry
  trading_calendar.py          trading days and sampling windows
  models.py                    value parsing, timestamps, contract record
  collectors/
    live_collector.py          cycle loop, discovery refresh, row writing, run record
    daily_backfill.py          backfill-daily: per-day discovery, prices, resume, reports
    historical_quality.py      offline audit of saved daily history
  providers/
    akshare_live.py            discovery, per-product bundled requests, fallbacks, futures
    sina_quotes.py             Sina requests, batching, field mapping
    akshare_daily.py           daily-history calls through the bounded worker
    exchange_history.py        dated SSE/SZSE risk reports and CFFEX daily files
    bounded_calls.py           worker subprocess with hard deadlines and an optional request cap
    inspect_sse_raw.py         one-off script that dumps the SSE contract master to CSV
  storage/
    parquet_writer.py          live table schemas and append-only Parquet writer
    historical_writer.py       daily-history table schemas and writer
tests/test_bundled_quotes.py   offline tests for the live collector
```

The tests use Sina payloads captured after the 2026-09-30 close and make no network requests. They cover field mapping for each symbol type, the CFFEX symbol case rule and turnover conversion, rejection of payloads that disagree with the contract master, bundled batching, the `batch_id` join through real Parquet files, the handling of options that have not traded this session, both discovery-retry paths, and the trading-day check.

```bash
python -m pip install -e '.[test]'
python -m pytest
```

---

## 18. Changes in this version

Changes to the live collector on this branch, compared with `main` (which already includes `backfill-daily`):

- **Bundled requests.** Each product's options are requested together with its underlying and every month of its hedge future, linked by the new `batch_id`. Previously the underlying and futures were only observed before and after the whole option sweep (about 19 seconds apart on 2026-09-30); those `before`/`after` rows are still written.
- **CFFEX quotes from Sina `P_OP_`.** They now include volume, turnover (converted to CNY) and the source time field, which were missing before. The AKShare tables remain as a fallback.
- **Options that have not traded this session** are recognised and stored under the current session's date, with volume and turnover 0, price limits null, and the flag `NO_TRADE_THIS_SESSION`.
- **Faster recovery.** A failed contract master is retried after 90 seconds instead of 30 minutes.
- **`collect-snapshot` command** for scheduled pre-close and post-close snapshots.
- **New columns:** `batch_id` and `run_id` on the three quote tables, `collection_phase` on `underlying_quotes` and `futures_quotes`, and `run_mode` on `ingestion_runs`. Existing files are not modified.
- **Documentation** of the field checks, the meaning of Sina's time field, the join, and the forward-versus-spot effect on implied volatility.
- **Offline tests** (`tests/`).
