# cn_option_data · 12 条期权产品链定时行情采集

本项目只做当前合约发现和 HTTP 行情轮询。合约主表和部分行情使用 AKShare；深市期权及 ETF、指数标的行情直接读取新浪财经公开报价接口。产品范围和期货映射来自 `config/product_registry.csv`。

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
| SSE | 510050、510300、510500、588000、588080 | `option_current_day_sse` | `option_sse_spot_price_sina`，逐合约获取盘口；单合约失败时尝试 `option_current_em` |
| SZSE | 159919、159922、159915、159901 | `option_current_day_szse` | 新浪财经 `https://hq.sinajs.cn/list=CON_OP_<合约编码>`；批量读取买一、卖一、数量、最新价、成交量、成交额、持仓量、源时间 |
| CFFEX | HO、IO、MO | 对应的 `option_cffex_*_list_sina` 动态月份列表，再从对应 `option_cffex_*_spot_sina` 盘口表取合约 | 同一 `spot_sina` 表；成交量与成交额留空，因为该表不提供这些字段 |

SSE 与 SZSE 合约由当日主表的合约代码前六位、看涨/看跌标识和标的代码精确归属到注册表；行情按 `合约编码` 精确关联。CFFEX 合约由 `HO/IO/MO + 到期年月 + C/P + 行权价` 解析，月份从当日列表读取，不固定到代码中。

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

按 `trade_date`、`exchange`、`hour` 分区，使用 PyArrow、ZSTD 和 UUID part 文件名；重启后只追加新文件。`source` 写接入路径：通过 AKShare 函数获取的行写 `AKShare`，直接从新浪财经获取的行写 `Sina`；部分 AKShare 函数本身也以新浪为上游，具体函数见 `source_interface`。`source_interface` 和 `request_timestamp` 记录实际调用的函数或接口及发起时间。`received_timestamp` 记录请求完成后本机收到数据的时间。`quote_timestamp` 与原有 `exchange_timestamp` 只在接口提供完整源日期和时间时填写；没有完整源时间时留空。SSE 逐合约接口若只给 `行情时间`，原样保存在 `source_quote_time`，不会补造日期。缺少源日期时，分区 `trade_date` 使用接收日期，表示采集日。

缺失价格和数量保持 null。期权买卖一任一侧缺失时 `quote_status=BBO_NOT_AVAILABLE`；交叉盘口清空买卖一并标记 `CROSSED_BBO`。指数行情没有买卖一；ETF 盘口数量和成交量按新浪接口原始单位保存（股），不换算成张。中金所盘口表本身不提供成交量、成交额或完整时间戳，这些字段为 null。CFFEX 月份列表只给到期年月，不能据此填写精确到期日，`expire_datetime` 保持 null。

## 如何检查缺失数据

每 60 秒的 heartbeat 显示每个产品的活跃合约数、累计写入行数和状态。`NO_ACTIVE_CONTRACTS` 表示当前接口未发现该产品合约；`INTERFACE_ERROR` 表示本轮接口异常。各接口独立请求，一个失败时其余产品继续采集。检查具体函数名和发起时间可看日志；停止后再查看 `data/metadata/ingestion_runs/` 的状态、错误和累计行数。合约主表每 30 分钟刷新一次，新挂牌合约下一轮自动纳入，已不在主表中的合约停止采集。

所有接口均为 HTTP 轮询，所得行不是逐笔 Tick。新浪的深市期权行情按最多 60 个合约一批请求，可能有延迟或收盘后停更；应比较 `quote_timestamp` 与 `received_timestamp` 判断新鲜度。SSE 逐合约调用可能使全市场一轮超过 60 秒；程序会记录超时告警。源站结构、访问频率和字段可变；某批请求失败时其余批次和产品继续采集，未返回的合约不会生成伪造报价。旧 Parquet 文件不会被修改，新增数据会生成新的 part 文件。
