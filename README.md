# cn_option_data: Scheduled Live Quotes for 12 Option Product Chains

This project discovers current option contracts and polls HTTP quote interfaces. It uses AKShare for contract masters and some quotes. SSE/SZSE ETF option quotes and ETF/index underlying quotes are requested directly from Sina Finance in batches. The selected products and their related futures come from `config/product_registry.csv`.

## Installation

Python 3.11 or newer is recommended. From the project directory, run:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

The public interfaces used by this program do not require account credentials. The optional `POLL_INTERVAL_SECONDS` environment variable sets the default polling interval; `--interval-seconds` takes precedence. `.env.example` documents the variable, but the program does not load `.env` automatically.

## Starting and Stopping

Collect all 12 registered products:

```bash
python -m cn_option_data.cli collect-live --products all --interval-seconds 60
```

Collect only 510050:

```bash
python -m cn_option_data.cli collect-live --products 510050 --interval-seconds 60
```

`--interval-seconds` is the target interval between the starts of polling cycles. If a cycle exceeds that interval, the program logs its actual duration and starts the next cycle after a short pause; cycles do not overlap. Press `Ctrl+C` or send `SIGTERM` to stop. The program flushes buffered data and writes an `ingestion_runs` record before exiting. By default, it writes in batches every 30 seconds or after buffering 50,000 rows. Use `--flush-interval-seconds` and `--max-buffer-rows` to adjust those thresholds.

## Products and Data Interfaces

| Exchange | Option products | Contract discovery | Option quotes |
| --- | --- | --- | --- |
| SSE | 510050, 510300, 510500, 588000, 588080 | AKShare `option_current_day_sse` | Direct batched Sina `CON_OP_<contract ID>` requests, using the same upstream endpoint as AKShare `option_sse_spot_price_sina`. Missing quotes may trigger the `option_current_em` fallback, which may provide only a last price. |
| SZSE | 159919, 159922, 159915, 159901 | AKShare `option_current_day_szse` | Direct batched `https://hq.sinajs.cn/list=CON_OP_<contract ID>` requests for best bid/ask prices and sizes, last price, volume, turnover, open interest, and a source time field. |
| CFFEX | HO, IO, MO | The corresponding `option_cffex_*_list_sina` interfaces supply current months. The matching `option_cffex_*_spot_sina` tables supply contracts. OpenCTP contract data is used to cross-check last trading dates. | The same `spot_sina` tables provide BBO, sizes, last price, and open interest. They do not provide option volume or turnover. |

SSE and SZSE contracts are assigned to registry products using the exact six-digit underlying code and call/put marker in the current trading code, with a check against the underlying identifier. Quotes are joined using the exact contract ID. CFFEX contract codes are parsed as `HO/IO/MO + expiry year/month + C/P + strike`. The months are read from the current lists, not hard-coded.

The CFFEX option multiplier is **CNY 100 per index point** and its minimum price movement is **0.2 index points**, following the [CFFEX index option trading rules](https://www.cffex.com.cn/cn/ssxz/20221214/43100.html). Last trading dates come from the OpenCTP contract reference endpoint, which is also used by AKShare `option_contract_info_ctp`. A date is filled only after checking the exchange, contract ID, year/month, strike, and other terms. If the reference is unavailable or cannot be verified, the date stays null and a warning is logged; the program does not infer a “third Friday” date. This bounded request accompanies the 30-minute contract refresh and is not repeated every quote cycle. `expire_datetime` and `last_trading_date` store **dates**. Because the Parquet fields are timestamps, `00:00:00` represents that date; it **must not be treated as a verified expiry instant for IV or Greeks**. A later metadata supplement appends another same-day contract snapshot without overwriting the earlier one.

ETF underlyings and the indexes associated with HO/IO/MO are requested from Sina Finance in batches at `https://hq.sinajs.cn/list=<sh or sz code>`. An index value has no tradable best bid/ask, so those fields remain null. AKShare `futures_symbol_mark` identifies the CFFEX futures product names, and `futures_zh_realtime` supplies their quotes. The registry defines the hedge relationships: 510050 and HO → IH; 510300, 159919, and IO → IF; 510500 and 159922 → IC; MO → IM. Products 588000, 588080, 159915, and 159901 use ETF spot quotes without a configured hedge future.

## Parquet Storage and Timestamps

The live collector saves these five Parquet tables:

```text
data/raw/contract_snapshots/
data/raw/option_quotes/
data/raw/underlying_quotes/
data/raw/futures_quotes/
data/metadata/ingestion_runs/
```

Files are partitioned by `trade_date`, `exchange`, and `hour`. They are written with PyArrow, ZSTD compression, and UUID part filenames, so a restart appends files instead of replacing them. `source=AKShare` identifies rows obtained through AKShare functions; `source=Sina` identifies direct Sina requests. Some AKShare functions also use Sina upstream. `source_interface` records the exact interface and `provider_symbol` keeps the original provider identifier.

`request_timestamp` records when the primary quote or contract-discovery call began. OpenCTP supplement request times are logged separately. `received_timestamp` records when the local machine received the result. `quote_timestamp` and the existing `exchange_timestamp` are populated only from a complete source-supplied date and time. The raw Sina option time field is preserved in `source_quote_time`. The program never supplies a missing date from the local clock. If the source date is unknown, new rows use the literal string `unknown` as `trade_date`. The partition `hour` is the **local receipt hour**, not the source quote hour.

Missing prices and sizes remain null; last price is not substituted for bid or ask. An option with either BBO side missing receives `BBO_NOT_AVAILABLE` in `quote_status`. If bid exceeds ask, the BBO fields are cleared and the row receives `CROSSED_BBO`. Time problems can be combined with those flags. An index has no tradable BBO. ETF depth sizes and volume retain the raw Sina units (shares), without conversion to option contracts. CFFEX option quote tables do not supply volume, turnover, or a complete source timestamp, so those fields remain null. A source value of `00:00:00` or a date without a specific time does not establish an intraday quote time; the timestamp stays null while the raw option text remains available. Local request and receipt times establish when the program observed a quote, not when its BBO changed.

## Coverage, Time Rules, and Quote Quality

The program emits a heartbeat every 60 seconds with per-product and per-table accepted and persisted counts. Contract masters are refreshed every 30 minutes; new contracts are included automatically. If discovery fails, previously known contracts that have not been confirmed expired are retained. The coverage and quality logs report missing contracts and fields.

All interfaces use HTTP polling, not exchange tick streaming. Sina option requests contain at most 60 contracts per batch. Batching reduces the number of requests, but the source may lag, limit access, or stop updating after the close. Each cycle collects underlyings and futures, the complete selected option chains, then underlyings and futures again. Phase durations are logged. The two surrounding observations bound a later matching window; they do not make every option quote simultaneous. A complete source time can be compared with receipt time but is not automatically a verified BBO timestamp. An absent source time cannot reveal the actual market update time. A failed batch does not stop other independent batches and products, and missing contracts do not generate fabricated quotes. Old Parquet files remain unchanged.

All three quote tables retain a known older source date. Quotes are no longer rejected solely because the source date differs from the receipt date:

| Source information | `quote_timestamp` / `exchange_timestamp` | `trade_date` |
| --- | --- | --- |
| Complete, parseable source date and time | Preserve the source value. Its meaning as a BBO timestamp is still unverified. | Source date |
| Source date only, or midnight used as a date placeholder | null; retain the raw option `source_quote_time`. | Source date |
| Unknown source date, including a clock time without a date | null; never attach the local date. | `unknown` |

The `hour` partition remains a receipt-hour bucket. Do not combine it with `trade_date` to invent a complete source time. For AKShare functions that make several internal HTTP calls, the recorded request/receipt times bound the **outer function call**, not the event time of each returned quote. Contract snapshots are partitioned by snapshot date; `ingestion_runs` is partitioned by run start date.

Option `quote_status` can contain `BBO_NOT_AVAILABLE`, `CROSSED_BBO`, `SOURCE_TIME_DATE_ONLY`, `SOURCE_TIME_UNKNOWN`, and `TIME_SEMANTICS_UNVERIFIED`. Concurrent issues are joined with `|` in this order: BBO condition, time precision, then time semantics. A complete-looking timestamp is not proof of its meaning. There is insufficient evidence that Sina option field 32, underlying date/time fields, or futures `tradedate`/`ticktime` strictly represent the corresponding BBO update time or one synchronized snapshot instant. Missing BBO on an index is not treated as an ETF quote failure.

The live collector saves raw observations that pass the existing value checks. It does not scan earlier Parquet files, remove consecutive identical quotes, or discard late records based on the latest previously seen timestamp. Do not use quotes with uncertain timing directly for strict intraday matching. This project does not calculate IV, Greeks, volatility surfaces, or PnL.

## Historical Daily Market Data From 2015

`backfill-daily` now discovers dated contracts from exchange records. It never needs local live `contract_snapshots` to discover an old contract. Start at 2015-02-09 for the first listed chain, 510050. Other products are skipped before their actual listing dates in `config/product_registry.csv`. This is a separate path; `collect-live` and its five tables are unchanged.

The steps for each selected trading day are:

1. Read the historical trading calendar and skip dates before each product's listing.
2. Discover SSE contracts from its dated option risk report and carry exact terms forward from SSE listing/adjustment records. For SZSE, join its dated risk report with historical listing, delisting, and adjustment files. For CFFEX, read its dated official daily CSV, which itself identifies all option and futures month contracts present that day.
3. Get raw unadjusted ETF daily OHLC from Tencent via AKShare `stock_zh_a_hist_tx(adjust="")`, or index OHLC from AKShare `stock_zh_index_daily`. Request Sina per-contract option daily OHLC for SSE/SZSE; get CFFEX options and IH/IF/IC/IM futures OHLC and settlement from the official CSV.
4. Write separate Parquet tables with the source, source interface, provider symbol, source trading day, local request/receipt times, and missing-field reason. Generate a per-product/day coverage report. Missing exchange rows and missing option prices stay missing.

The same Sina endpoint used by AKShare `option_sse_daily_sina` is called directly for SSE and SZSE option codes. This makes its historical `null` response distinguishable from a network failure. A successful per-contract response contains multiple dates and is cached under `data/metadata/backfill_price_cache/`, so later days and restarted runs can reuse it without another HTTP call. Some old SZSE contracts returned `null` or began reporting only after their actual listing: a historical roster does **not** guarantee historical prices. Clearly absent contract/date prices are recorded in `data/metadata/backfill_absences.json` and skipped on restart; `--retry-absent-prices` explicitly checks them again. Temporary network errors and request limits are never cached as source absence.

Example, first preview with no HTTP calls, then execute a small bounded slice:

```bash
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2015-02-09 --end-date 2015-02-09 --dry-run --max-requests 20
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2015-02-09 --end-date 2015-02-09 --execute --max-requests 20
```

The default is preview; dates are mandatory. `--max-requests` caps underlying Requests HTTP calls, including calls made inside AKShare. The default cap is 20, so a complete year needs many explicit resumed runs or a deliberately larger cap. Each outer call has a 15-second deadline, calls are spaced by at least 0.5 seconds, and a failure is retried once within the cap. `Ctrl+C` and `SIGTERM` stop new calls, flush completed Parquet rows and save the report. Repeating the command resumes from persisted business keys; new parts use UUID filenames and atomic replacement. Check the report's `option_coverage_of_exchange_roster`, `option_missing_examples`, and per-category statuses before extending the range.

Outputs are under `data/historical/roster_daily/`, `option_daily/`, `underlying_daily/`, `futures_daily/`, `contract_terms_daily/`, and `contract_adjustments/`, partitioned by trade date and exchange. Reports are under `data/metadata/backfill_runs/`. `--output-root DIR` writes a sample to a separate directory. Daily OHLC/settlement are **not** historical bid/ask; they must never be written into live BBO columns. Zero or absent source close remains raw with a quality reason and cannot be used directly for IV. No IV, Greeks, or PnL is calculated here.

### Repair historical terms and inspect price quality

Some earlier SSE runs saved a day's roster and prices but missed that day's contract terms. The historical collector now checks terms even when the roster is already saved. `--repair-sse-terms` requests only the dated SSE roster/listing/adjustment information needed to fill missing `contract_terms_daily` rows; it does not request option or underlying prices and does not alter the live collector. Choose SSE products only. Start from the last **continuous** day with complete terms and proceed in date order: a listing or adjustment that occurred during an unprocessed gap cannot be reconstructed by querying only a later day. Preview first, then repeat bounded runs until the report no longer says `REQUEST_LIMIT`:

```bash
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2024-03-01 --end-date 2025-01-02 --repair-sse-terms --dry-run --max-requests 50
python -m cn_option_data.cli backfill-daily --products 510050 --start-date 2024-03-01 --end-date 2025-01-02 --repair-sse-terms --execute --max-requests 50
```

The dates above illustrate a gap; inspect your own coverage report to choose its actual start. The repair appends verified term rows only. An old `option_daily` row with a null expiry remains unchanged; later research must join `option_daily` to `contract_terms_daily` by trade date, product, and contract code. If the exchange records or exact code/version cannot verify a term, the run reports `SOURCE_UNAVAILABLE` or `REQUEST_LIMIT` instead of inventing a value.

Audit already saved data without making network requests or changing raw Parquet:

```bash
python -m cn_option_data.collectors.historical_quality --products 510050 --start-date 2025-01-02 --end-date 2025-01-02
```

The audit reports missing or conflicting terms, a null or nonpositive close, OHLC inconsistencies, missing same-day underlying, and dates whose expiry is still unresolved after joining verified terms. It examines dates with saved files; use `backfill_runs/` to identify dates with no saved data. New daily price rows carry OHLC quality reasons in `missing_reason`; old rows remain raw and are assessed by the audit. Neither source close nor settlement is historical bid/ask.

Current gaps are explicit: CFFEX's verified daily CSV does not supply exact option expiry/last trading dates or contract multipliers, so its option `contract_terms_daily` remains unavailable. Historical interest rates and ETF cash-dividend sources have not been verified, so `rates_daily/` and `etf_dividends/` remain empty. These are prerequisites for a reliable full daily IV surface. SSE/SZSE dated exchange rosters and terms were checked on a few launch/older days, but complete multi-year coverage is not yet proven; inspect every run's coverage report. HO's registry start was corrected to 2022-12-19 using the [CFFEX listing notice](https://www.cffex.com.cn/cn/jystz/20221214/30918.html).

## Network Protection, Futures Scope, and Shutdown

All collection network calls run serially through one terminable subprocess. The default target upper bound is 15 seconds per call, including the first dependency import, internal AKShare requests, and result serialization. Direct HTTP calls also use a 3-second connection timeout and a 10-second read timeout. AKShare functions without a `timeout` parameter rely on the subprocess deadline; the program does not invent unsupported parameters or modify global Requests settings. After a timeout, it terminates the old process before issuing another call; cleanup can add up to about two seconds. The main thread checks buffer flush and heartbeat while waiting. Disk operations and severe operating-system blocking are outside this network deadline.

This design does not add concurrent market-data requests. Normal calls reuse the worker's network session and AKShare's built-in cache; a terminated worker rebuilds its cache. A response that never arrived cannot be recovered. Valid results already returned are normalized and buffered even if a normal stop was requested.

`Ctrl+C` or `SIGTERM` stops new requests. The program flushes buffers, records the run, and only then restores its original signal handlers. Repeated normal stop signals do not intentionally interrupt the final flush. A write failure causes a nonzero exit without clearing the failed batch from memory. Power loss, forced termination, and disk failure can still prevent a successful save.

The application's log is `logs/collect-live.log`, using Asia/Shanghai timestamps. Each file is limited to 10 MiB with up to three rotated backups. This is not a sixth quote table. Use `run_id` to find a run's start and end summaries.

The Eastmoney `option_current_em` interface is only an optional supplement for SSE quotes missing or rejected by the primary path. It is called at most once per cycle. After three consecutive failures it pauses for 300 seconds; a successful retry resets the count. Its current wrapper does not supply the BBO or source timestamp needed here. The program does not combine fields from separate requests into a fabricated simultaneous quote or alter proxy settings.

Before making futures requests, the program selects the needed IH/IF/IC/IM products from the registry, then matches exact product identifiers in the checked Sina name table. Provider identifiers are IF → `qz_qh`, IH → `szgz_qh`, IC → `zzgz_qh`, and IM → `im_qh`. Selecting only 510050 requests IH; selecting all products requests IH/IF/IC/IM. Returned actual-month futures contracts are stored individually, without hard-coded months or unrelated Treasury futures requests. This provider-identifier conversion does not replace the registry's option-to-future mappings. AKShare already caches `futures_symbol_mark` inside its process.

## Optional Trading-Hours Control

The original launch commands still work. Add the optional switch to restrict requests to the configured trading windows:

```bash
python -m cn_option_data.cli collect-live --products all --interval-seconds 60 --trading-hours-only
```

The program reads `config/trading_calendar.json`; it does not download the calendar every cycle. The file covers 2026 weekends and exchange holidays, and records its sources and verification limitations. SSE/SZSE official notices were checked. A direct request to the CFFEX website timed out, so the annual notice was checked using a publicly syndicated copy of CFFEX's publication; that limitation remains documented.

The common Asia/Shanghai sampling windows are 09:30–11:30 and 13:00–15:00. They include the ETF closing auction but not the opening auction, and do not claim to cover every auction phase. Outside these windows, the program makes no quote requests but continues heartbeat, flush, and shutdown handling. A missing/invalid calendar or a date beyond its coverage is an explicit error, never an assumption that the market is open. Update the calendar from official notices and restart after a new year or an exceptional closure; the loaded version is kept in memory.

Without the switch, interfaces may still be checked manually when the market is closed. Trading-hours control changes when requests are made, not the rule for retaining quotes with an older source date.

## Contract Metadata Sources

The minimum ETF option quote increment is CNY 0.0001 per ETF unit under SSE Article 59 and SZSE Rule 4.3.18. The program preserves each adjusted contract's actual unit. When an SSE last trading date is absent, the current master supplies it only if the expiry and exercise dates agree, following Article 9. Conflicts remain null and are logged. The SZSE master-supplied last trading date is retained. No expiry day is guessed from the contract month.

- [SSE rules, Articles 9 and 59](https://www.sse.com.cn/lawandrules/sselawsrules2025/option/c/c_20250610_10781448.shtml)
- [SZSE rules, Sections 2.5 and 4.3.18](https://docs.static.szse.cn/www/option/rules/optrules/W020191207434561721119.pdf)
- [CFFEX SSE 50 index option contract specifications: expiry and last trading date](https://www.cffex.com.cn/cn/sz50gzqq.html)

These applicable rules were checked on 2026-09-27. ETF contract snapshots include the rule source in `source_interface`; logs record the application date and missing terms. OpenCTP CFFEX dates are used alongside the exchange rule relating expiry and last trading dates, but that does not mean every term has been independently certified by the exchange. Midnight in a contract-date field expresses date precision, not a verified expiry instant.

## Field Availability and Remaining Gaps

| Products | Current option quote interface | Availability and limits |
| --- | --- | --- |
| 510050, 510300, 510500, 588000, 588080 | Batched Sina `CON_OP`; Eastmoney supplement if needed | Sina fields are mapped to BBO, sizes, last price, volume, turnover, open interest, price limits, and raw time. Individual rows may still lack values. The Eastmoney wrapper does not provide BBO, source time, or price limits. |
| 159919, 159922, 159915, 159901 | Batched Sina `CON_OP` | Same Sina field mapping; a missing BBO side does not stop other products. |
| HO | `option_cffex_sz50_spot_sina` | BBO, sizes, last price, and open interest; the wrapper does not provide volume, turnover, source time, or price limits. |
| IO | `option_cffex_hs300_spot_sina` | Same limits as HO. |
| MO | `option_cffex_zz1000_spot_sina` | Same limits as HO. |

This table distinguishes fields absent from the interface wrappers. Coverage and quality logs identify missing values in individual rows, rejected rows, and contracts without returned quotes. No alternative source has been verified to provide every missing field. Interest rates/discounting, dividends or forward estimates, complete futures contract terms, fees, margin, and funding costs remain inputs for later research; this project does not fill them with assumptions.

Every cycle logs `discovered`, `returned`, `accepted`, `missing`, and `rejected` counts and contract codes. Heartbeats distinguish `accepted` (added to a buffer) from `persisted` (written to disk), by table and product, and report buffered rows and cumulative quality problems. `PARTIAL` can indicate an interface, coverage, timestamp, or field issue; it does not necessarily indicate a failed final flush. `FAILED` indicates a fatal local error. A final write failure must not be reported as successful storage.

These changes affect only new files. Existing Parquet data is neither migrated nor overwritten. An old partition based on a receipt date is not automatically corrected to the source date, and an old `quote_status=OK` does not establish trustworthy timing. Use original timestamps and run periods to distinguish old and new status semantics before combining them.

## Manual Short Run and Read-Only Inspection

During trading hours, run a few cycles and press `Ctrl+C`. Wait for the completion message and `buffered_rows=0`:

```bash
source .venv/bin/activate
python -m cn_option_data.cli collect-live --products 510050 --interval-seconds 60
```

The following reads recent files without modifying them:

```bash
python - <<'PY'
from pathlib import Path
import pyarrow.parquet as pq
for folder in ('data/metadata/ingestion_runs', 'data/raw/option_quotes'):
    files = sorted(Path(folder).rglob('*.parquet'), key=lambda p: p.stat().st_mtime)
    print(folder, 'files=', len(files), 'bytes=', sum(p.stat().st_size for p in files))
    if files:
        table = pq.ParquetFile(files[-1]).read()
        columns = [c for c in ('run_id', 'status', 'quote_rows_written', 'error_message',
                   'product_id', 'contract_code', 'trade_date', 'quote_timestamp',
                   'source_quote_time', 'received_timestamp', 'bid_price1', 'ask_price1',
                   'quote_status') if c in table.column_names]
        print(table.select(columns).slice(0, 5).to_pandas().to_string(index=False))
PY
```

For full reads, explicitly declare Hive partition fields as strings or read each `ParquetFile` separately. This avoids type conflicts if `hour` is inferred as an integer. Keep `unknown` as an unknown source date. Date filters can use `pandas.to_datetime(..., errors="coerce")` to obtain `NaT` and report the unknown group separately; never replace those dates with the local date. Check per-product coverage, timestamp precision, missing fields, and actual cycle duration before changing the 60-second target interval.
