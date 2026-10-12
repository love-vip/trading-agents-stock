# TradingAgents A 股盘中选股工作台

一个基于 Python 标准库和公开行情接口的本地 A 股研究看板，提供盘中选股、MACD 选股、告警池、板块/概念分析、个股分时、日 K 线和历史数据缓存。

> 本项目用于行情研究和策略验证，不构成投资建议。公开行情接口可能出现延迟、限流、字段变化或暂时不可用。

## 功能

- 主板、创业板、科创板、北交所及全市场行情筛选
- MACD 动态选股与盘中告警
- 告警历史、告警涨跌幅和信号复盘
- 板块强度、概念排行、概念成分股和领涨股
- 个股分时图、近 30 日 K 线、近 10 日 MACD 和均线分布
- SQLite 本地缓存、历史日线、概念资料和告警记录
- 可选 EMT/Level-2 竞价数据接入

## 运行要求

| 项目 | 要求 |
| --- | --- |
| Python | 3.9+，推荐 3.11 或 3.12 |
| 浏览器 | Chrome、Edge、Safari 或 Firefox 最新版 |
| 网络 | 可访问行情数据源和相关公开接口 |
| 数据库 | SQLite，服务自动创建，不需要 MySQL/Redis |

后端只使用 Python 标准库，通常不需要安装第三方 Python 包。

## 文件结构

```text
trading-agents-stock/
├── market_server.py          # HTTP 服务、行情接口和选股逻辑
├── minute_prediction.py      # 分钟预测模块
├── trading-agents-stock.html # 单文件前端页面
├── data/
│   ├── backtest.sqlite       # 主数据库：行情缓存、选股、告警和回测
│   ├── threshold_times.json  # 阈值首次触发时间
│   └── auction/              # 可选竞价快照，自动生成
├── scripts/                  # 辅助脚本
├── test_*.py/js              # 测试文件
└── README.md
```

`data/backtest.sqlite-shm` 和 `data/backtest.sqlite-wal` 是 SQLite 运行时文件。服务运行时不要手动删除或复制它们。

## 快速启动

### macOS / Linux

```bash
cd /path/to/trading-agents-stock
python3 market_server.py --host 127.0.0.1 --port 8502
```

浏览器打开 <http://127.0.0.1:8502/>。端口被占用时，可以使用 `8505` 等其他端口。

检查服务：

```bash
curl -fsS http://127.0.0.1:8502/api/health
```

### Windows PowerShell

```powershell
cd C:\path\to\trading-agents-stock
py market_server.py --host 127.0.0.1 --port 8502
```

## 后台运行

### macOS / Linux 临时后台运行

```bash
nohup python3 market_server.py --host 127.0.0.1 --port 8502 > trading-agents.log 2>&1 &
```

查看端口和停止服务：

```bash
lsof -nP -iTCP:8502 -sTCP:LISTEN
kill <PID>
```

长期运行建议使用 macOS `launchd` 或 Linux `systemd`，核心配置是：

```text
WorkingDirectory=/path/to/trading-agents-stock
ExecStart=/usr/bin/python3 /path/to/trading-agents-stock/market_server.py --host 127.0.0.1 --port 8502
Restart=always
```

Windows 可使用“任务计划程序”设置登录时启动，程序填写 `py.exe`，起始目录填写项目根目录。

## 配置行情凭据

本地凭据放在 `.env.local`，不要提交到 Git，也不要直接分享给其他部署者：

```env
TUSHARE_TOKEN=对方自己的Token
```

没有配置时，仍可使用项目支持的其他公开行情源，但部分历史数据或补档功能可能不可用。

## 可选竞价数据

竞价数据需要外部 EMT/Level-2 导出程序。通过环境变量配置，`{date}` 会被替换成交易日：

```bash
export EMT_AUCTION_COMMAND='python3 /path/to/export_emt_auction.py --date {date}'
python3 market_server.py --port 8502
```

导出程序应输出 EMT `get_open_call_auction` 的 JSON 数组，或包含 `data` 数组的对象。没有配置时，其他行情、分时、日 K 和 MACD 功能仍可使用，竞价字段会显示为不可用。

## 数据库与迁移

核心数据只有：

- `data/backtest.sqlite`：行情历史、个股资料、概念缓存、MACD 选股池、告警记录和回测数据。
- `data/threshold_times.json`：评分、预测和涨幅阈值的首次触发时间。

迁移到另一台机器：

1. 停止源机器服务。
2. 复制 `data/backtest.sqlite` 和 `data/threshold_times.json`。
3. 不复制运行中的 `backtest.sqlite-shm`、`backtest.sqlite-wal`。
4. 在目标机器配置自己的 `.env.local`。
5. 启动服务，等待后台行情补档完成。

如果不复制数据库，服务也能启动，但会重新建立行情和概念缓存，首次加载可能较慢。

## 局域网或公网访问

默认监听 `127.0.0.1`，只允许本机访问。局域网访问可以改为：

```bash
python3 market_server.py --host 0.0.0.0 --port 8502
```

不建议直接暴露到公网。对外访问应使用 Nginx/Caddy 反向代理、HTTPS、访问认证和防火墙规则：

```text
浏览器 → HTTPS 反向代理 → 127.0.0.1:8502 → Python 服务 → 行情接口
```

同时应限制刷新频率，避免公开行情接口被限流。

## 常用接口

```text
GET /api/health               服务健康状态
GET /api/data-policy          当前行情和交易日数据策略
GET /api/macd-screener        MACD 选股池
GET /api/market-alert-history 当日告警历史
GET /api/concept-alerts       概念异动告警
GET /api/chart-history        个股历史日 K 和指标数据
GET /api/intraday             指定交易日分时数据
```

具体参数以 `market_server.py` 的路由实现为准。

## 验证与排查

检查 Python 语法：

```bash
python3 -c "compile(open('market_server.py', encoding='utf-8').read(), 'market_server.py', 'exec')"
```

检查前端脚本：

```bash
node -e 'const fs=require("fs"); const h=fs.readFileSync("trading-agents-stock.html","utf8"); new Function(h.split("<script>")[1].split("</script>")[0]); console.log("HTML script OK")'
```

页面打不开时依次检查：

1. 服务进程是否仍在运行；
2. 浏览器端口是否与启动参数一致；
3. `/api/health` 是否返回成功；
4. 网络或上游行情接口是否超时/限流；
5. `data/backtest.sqlite` 所在目录是否可写。

首次启动或全市场补档可能耗时较长。接口 502 通常表示上游行情源超时、限流或暂时不可用，不一定代表本地服务已停止。

## 安全说明

- 不要提交 API Token、Cookie、账号密码或 Level-2 导出文件。
- 不要把 `.env.local` 分享给其他部署者。
- 不要在服务运行时删除 SQLite 的 `-shm`、`-wal` 文件。
- 公开行情数据可能延迟或缺失，策略结果需要人工复核。
- 本项目的选股、评分、概率和告警仅用于辅助研究，不保证收益。
