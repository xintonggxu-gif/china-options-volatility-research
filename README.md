# cn_option_data · 12 条期权产品链定时行情采集

本项目只做当前合约发现和 HTTP 行情轮询。合约主表和部分行情使用 AKShare；沪深 ETF 期权及 ETF、指数标的行情直接批量读取新浪财经公开报价接口。产品范围和期货映射来自 `config/product_registry.csv`。

## 安装

建议 Python 3.11 或更高版本。在本目录执行：

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

这些公开接口不要求本程序配置账号。可用环境变量 `POLL_INTERVAL_SECONDS` 设置默认轮询秒数；命令行的 `--interval-seconds` 优先。`.env.example` 只展示该可选变量，程序不会自动读取 `.env` 文件。

## 启动和停止

采集注册表中的全部 12 个产品：

```bash
python -m cn_option_data.cli collect-live --products all --interval-seconds 60
```

只采集 510050：

```bash
python -m cn_option_data.cli collect-live --products 510050 --interval-seconds 60
```

`interval-seconds` 是两次轮询开始之间的目标间隔。如果单轮请求耗时超过间隔，程序会告警，并在短暂休息后开始下一轮。按 `Ctrl+C` 停止，或向进程发送 `SIGTERM`。退出前会保存内存缓冲并写入一条 `ingestion_runs` 记录。默认每 30 秒或累积 50,000 行批量写入；可用 `--flush-interval-seconds` 与 `--max-buffer-rows` 调整。

## 产品与接口

| 交易所 | 产品 | 合约发现 | 期权行情 |
| --- | --- | --- | --- |
| SSE | 510050、510300、510500、588000、588080 | `option_current_day_sse` | 直接批量读取新浪 `CON_OP_<合约编码>`（与 AKShare `option_sse_spot_price_sina` 使用同一上游）；缺报价时尝试 `option_current_em`，该备用接口可能只有最新价 |
| SZSE | 159919、159922、159915、159901 | `option_current_day_szse` | 新浪财经 `https://hq.sinajs.cn/list=CON_OP_<合约编码>`；批量读取买一、卖一、数量、最新价、成交量、成交额、持仓量、源时间 |
| CFFEX | HO、IO、MO | 对应的 `option_cffex_*_list_sina` 动态月份列表，再从对应 `option_cffex_*_spot_sina` 盘口表取合约；用 OpenCTP 合约资料核对最后交易日 | 同一 `spot_sina` 表；成交量与成交额留空，因为该表不提供这些字段 |

SSE 与 SZSE 合约由当日主表的合约代码前六位、看涨/看跌标识和标的代码精确归属到注册表；行情按 `合约编码` 精确关联。CFFEX 合约由 `HO/IO/MO + 到期年月 + C/P + 行权价` 解析，月份从当日列表读取，不固定到代码中。

中金所合约乘数 **100 元/点**、最小变动价位 **0.2 点**，依据[中金所股指期权合约交易细则](https://www.cffex.com.cn/cn/ssxz/20221214/43100.html)填写。最后交易日从 OpenCTP 合约资料接口（AKShare `option_contract_info_ctp` 使用的同一上游）读取，只在交易所、合约代码、年月、行权价等核对成功后填写；接口失败或无法核对时保留 null，并记录警告，不按“第三个星期五”推算。此补充请求随合约主表每 30 分钟刷新，设有超时，不影响期权行情采集。`expire_datetime` 与 `last_trading_date` 存储的是**日期**；Parquet 字段为时间类型，所以使用该日 `00:00:00` 表示日期，**不能当作精确到期时刻用于 IV/Greeks**。若稍后补齐了资料，同一天会追加一条更新后的合约快照，旧快照不会覆盖。

ETF 标的和 HO/IO/MO 对应的指数从新浪财经 `https://hq.sinajs.cn/list=<sh或sz代码>` 批量读取。指数没有买卖一盘口，保持 null。IH/IF/IC/IM 期货仍通过 AKShare `futures_symbol_mark` 查找中金所品种名称，再调用 `futures_zh_realtime`。对冲关系逐行读取注册表：510050、HO→IH；510300、159919、IO→IF；510500、159922→IC；MO→IM。588000、588080、159915、159901 采集 ETF 现货，不配置对冲期货。

## 数据位置与时间

只保存五张 Parquet 表：

```text
data/raw/contract_snapshots/
data/raw/option_quotes/
data/raw/underlying_quotes/
data/raw/futures_quotes/
data/metadata/ingestion_runs/
```

按 `trade_date`、`exchange`、`hour` 分区，使用 PyArrow、ZSTD 和 UUID part 文件名；重启后只追加新文件。`source` 写接入路径：通过 AKShare 函数获取的行写 `AKShare`，直接从新浪财经获取的行写 `Sina`；部分 AKShare 函数本身也以新浪为上游，具体函数见 `source_interface`。`source_interface` 记录使用的接口；`request_timestamp` 是该行主要行情或合约发现请求的发起时间，OpenCTP 补充请求的时间另记在日志中。`received_timestamp` 记录请求完成后本机收到数据的时间。`quote_timestamp` 与原有 `exchange_timestamp` 只在接口提供完整源日期和时间时填写；没有完整源时间时留空。新浪期权接口若只给 `行情时间`，原样保存在 `source_quote_time`，不会补造日期。源日期未知时，新数据的 trade_date 使用字符串 unknown；hour 仍是收到小时。详见下方新数据时间规则。

缺失价格和数量保持 null。期权买卖一任一侧缺失时 `quote_status` 包含 `BBO_NOT_AVAILABLE`；交叉盘口清空买卖一并标记 `CROSSED_BBO`。状态还可同时包含时间问题，详见下方组合规则。指数行情没有买卖一；ETF 盘口数量和成交量按新浪接口原始单位保存（股），不换算成张。中金所盘口表本身不提供成交量、成交额或完整源时间戳，这些字段为 null。源接口返回 `00:00:00` 或只有日期时，也不能把它当成有效的盘中报价时间：`quote_timestamp` 留空，期权原始文本保留在 `source_quote_time`。本机请求/接收时间只能说明何时观察到该报价，不能证明盘口何时变化。

## 如何检查缺失数据

每 60 秒输出 heartbeat，按产品及表分别报告接收与落盘数量。覆盖日志和质量含义详见下方说明。合约主表仍每 30 分钟刷新，新挂牌合约自动纳入；发现失败时保留已有未到期合约。

所有接口均为 HTTP 轮询，所得行不是逐笔 Tick。沪深两市期权按最多 60 个合约一批读取新浪接口；这能减少逐合约请求，但源站可能延迟、限流或收盘后停更。每轮按“标的与期货 → 期权全链 → 标的与期货”采集，并记录各阶段耗时；前后两次只是为之后配对提供时间窗口，不能让中间全部合约变成同一时刻的行情。源时间完整时可与接收时间比较；源时间缺失时无法判断行情实际更新的时刻。`--interval-seconds 60` 是目标轮询间隔，若一轮超过 60 秒，日志会提示实际耗时。源站结构、访问频率和字段可变；某批请求失败时其余批次和产品继续采集，未返回的合约不会生成伪造报价。旧 Parquet 文件不会被修改，新增数据会生成新的 part 文件。

## 新数据时间规则与状态（本轮修改后）

三张行情表都保留明确的旧日期，不再因为源日期与收到日期不同而拒收。

| 数据源信息 | quote_timestamp / exchange_timestamp | trade_date |
| --- | --- | --- |
| 完整源日期和时刻 | 保留解析结果；不等于已经核实为 BBO 时刻 | 源日期 |
| 只有日期，或零点日期占位值 | null，保留期权原始 source_quote_time | 源日期 |
| 源日期未知（包括只有时刻） | null，不拼接本机日期 | 字符串 unknown |

hour 保持接收小时，只是存储分桶，不能与目录里的 trade_date 拼接为报价时刻。request_timestamp / received_timestamp 是本机请求/收到时间；多个 HTTP 请求组成的 AKShare 函数返回的是外层调用范围，不是其中每条报价的发生时间。合约快照按快照日期分区，运行记录按启动日期分区。

quote_status 保留 BBO_NOT_AVAILABLE、CROSSED_BBO，并增加 SOURCE_TIME_DATE_ONLY、SOURCE_TIME_UNKNOWN、TIME_SEMANTICS_UNVERIFIED。问题并存时按盘口、时间精度、时间语义的固定顺序用 | 连接。格式完整不等于已验证：新浪期权的第 32 项、标的的日期/时间、期货的 tradedate/ticktime，尚未得到其严格对应 BBO 更新时刻或统一快照时点的足够依据。指数本身没有可交易 BBO，不计作 ETF 盘口故障。

原始观测全部按既有有效值校验保存；不在线查重，不扫描历史文件，不按最大已见时间过滤迟到数据。不能确认时间的记录不直接用于严格的盘中时间配对。本轮没有实现 IV、Greeks、曲面、PnL 或历史回填。

## 请求保护、期货范围及退出

所有采集网络调用串行进入一个可终止子进程，默认单次调用总等待目标上限 15 秒，包含首次加载依赖、AKShare 内部多次请求和结果序列化。直接 HTTP 请求另有 3 秒连接、10 秒读取超时；没有 timeout 参数的 AKShare 函数使用进程总期限，不改函数签名或全局 Requests。超时先终止旧进程再允许下一次调用，进程清理最多再等待约 2 秒。主线程等待时仍检查 flush 和 heartbeat；磁盘操作和操作系统严重阻塞不属于网络时限。

这不增加采集并发。正常调用复用工作进程的会话和 AKShare 自带缓存；进程超时终止后缓存重新建立。没有返回的网络结果无法保证取得。已经返回的有效记录会继续整理入缓冲，不因正常停止而无声丢弃。

Ctrl+C / SIGTERM 停止新请求，保存缓冲和 ingestion_runs 后才恢复信号处理。保存期间重复正常停止信号不会主动打断保存。写盘失败非零退出，失败批次不主动清空；不能保证断电、强制杀进程或磁盘故障时保存成功。

日志保存到 logs/collect-live.log，每文件 10 MiB，最多 3 个轮转备份，时间为 Asia/Shanghai。它不是第六张行情表。用 run_id 查找开始/结束摘要。

东方财富 option_current_em 只在 SSE 主报价缺失或校验失败时按现有条件调用，一轮最多一次；连续失败 3 次暂停 300 秒，到期尝试成功清零，失败再次暂停。它不提供本程序需要的 BBO/源时间，不能修补这些缺项。不会把不同请求取得的字段拼成一个伪造的同时刻快照，不会自动修改代理。

期货在发出请求前，从本次 registry 产品得到需要的品种，再精确匹配已核对的新浪名称表。Provider 标识映射为 IF→qz_qh、IH→szgz_qh、IC→zzgz_qh、IM→im_qh。只选 510050 时仅请求 IH，全产品请求 IH/IF/IC/IM；返回的实际月份合约分别保存，不写死月份，不请求无关国债期货。此标识转换不替代 registry 的产品选择和期权到期货映射。futures_symbol_mark 已有 AKShare 进程内缓存。

## 可选交易时段控制

原来的启动命令保持不变。启用可选限制：

```bash
python -m cn_option_data.cli collect-live --products all --interval-seconds 60 --trading-hours-only
```

读取 config/trading_calendar.json，不每轮下载日历。提供完整 2026 年周末及法定休市安排，来源和核对说明见 JSON。沪深官方公告已核对；中金所原站本次请求超时，年度正文通过 CFFEX 发布公告的公开转载核对，此来源限制如实保留。

共同采样窗口为 09:30–11:30、13:00–15:00（含 ETF 收盘集合竞价，不含开盘集合竞价），不声称覆盖所有竞价阶段。非窗口期间不发起请求，但保留 heartbeat、flush 和退出响应。日历缺失、无效或超出有效年份时明确失败，不把新一年沿用旧日历推算。跨年或临时休市公告发布后，依据正式公告更新配置并重启；程序载入后在内存使用。

不开启该开关，仍可手动在休市时检查接口。时段控制只决定何时请求，不重新加入“源日期必须等于今天”的过滤。

## 合约资料补充依据

ETF 最小报价单位 0.0001 元/份依据上交所第 59 条、深交所 4.3.18；不覆盖调整合约的实际合约单位。上交所最后交易日缺失时，仅在当日主表到期日与行权日一致时，依据第 9 条补为该日期；日期冲突留空并记录。深交所保留主表实际最后交易日。没有按合约月份自行推算日期。

- [上交所规则，第9、59条](https://www.sse.com.cn/lawandrules/sselawsrules2025/option/c/c_20250610_10781448.shtml)
- [深交所规则，2.5、4.3.18](https://docs.static.szse.cn/www/option/rules/optrules/W020191207434561721119.pdf)
- [中金所上证50股指期权合约表，到期日同最后交易日](https://www.cffex.com.cn/cn/sz50gzqq.html)

规则按 2026-09-27 查阅的适用文本记录；ETF 合约快照 source_interface 包含规则来源，日志记录适用日期和缺失字段。OpenCTP 的中金所日期结合交易所到期日同最后交易日规则使用，不等于全部资料已获交易所独立认证。日期字段的午夜只代表日期精度，不是精确到期时刻。

## 字段能力与未解决缺项

| 产品 | 当前期权接口 | 能力边界 |
| --- | --- | --- |
| 510050、510300、510500、588000、588080 | 新浪 CON_OP 批量，必要时东方财富补充 | 新浪映射 BBO、买卖量、最新价、成交量/额、持仓、涨跌停、原始时间；单条仍可能缺值。东方财富当前包装输出没有 BBO、源时间、涨跌停 |
| 159919、159922、159915、159901 | 新浪 CON_OP 批量 | 同上新浪字段；缺一侧 BBO 不中止其他产品 |
| HO | option_cffex_sz50_spot_sina | BBO、买卖量、最新价、持仓；包装输出没有成交量、成交额、源时间、涨跌停 |
| IO | option_cffex_hs300_spot_sina | 同 HO |
| MO | option_cffex_zz1000_spot_sina | 同 HO |

这张表区分接口包装不输出的字段；单条缺值、拒收和未返回由覆盖/质量日志体现。本轮没有确认某个新来源可补齐所有字段。利率/贴现、分红或远期处理方案、完整期货条款、费用/保证金/资金成本仍是后续研究输入，本轮不填假设值。

每轮日志分别显示 discovered、returned、accepted、missing、rejected 和代码清单。heartbeat 按产品和表分开显示 accepted（进入缓冲）与 persisted（成功写盘），并显示缓冲行数和质量问题累计数。PARTIAL 可以来自接口、覆盖或时间/字段问题，不等于退出未保存；FAILED 表示致命本地错误，最终写盘失败时不能认定保存完成。

修改只影响新文件，旧 Parquet 不迁移、不覆盖。旧分区中的收到日期不自动变成源日期，旧 quote_status=OK 不代表时间可信。应根据原始时间和修改后的运行起止时间区分，不直接混用新旧状态语义。

## 人工短跑和只读检查

在真实交易时段运行数轮后按 Ctrl+C，等待“采集结束”及 buffered_rows=0：

```bash
source .venv/bin/activate
python -m cn_option_data.cli collect-live --products 510050 --interval-seconds 60
```

下面只读最近文件，不修改数据：

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

全量读取时显式声明 Hive 分区为字符串，或逐个 ParquetFile 读取，避免 hour 自动推断为整数导致类型冲突。unknown 必须作为未知保留；日期筛选可用 pandas.to_datetime(..., errors="coerce") 得到 NaT 并另列未知组，不能替换为本机日期。检查每产品覆盖、时间精度、字段缺失和一轮真实耗时后，再决定是否调整 60 秒间隔。
