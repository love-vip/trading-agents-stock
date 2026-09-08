# TradingAgents A 股盘中选股工作台

一个基于 Python 标准库和公开行情接口的本地 A 股研究看板，提供盘中选股、板块强度、热门板块、指数分析、涨停复盘数据、个股分时/K 线、MACD、评分轨迹和概率模型展示。

> 本项目用于行情研究和策略验证，不构成投资建议。公开接口可能限流、延迟或临时不可用。

## 1. 功能概览

- 盘中实时行情与自动刷新
- 主板、创业板、科创板、北交所和全部 A 股切换
- 综合评分、预测评分、分级提前预警
- 分钟级评分轨迹和个股当日分时缩略图
- 5/10/20/30 日均线、MACD、成交量和日 K 线
- 板块强度、热门概念和概念成分股
- 同花顺人气排名与概念补充
- SQLite 本地缓存、评分轨迹和历史数据

## 2. 运行要求

| 项目 | 建议版本 |
|---|---|
| Python | 3.9 及以上，推荐 3.11 或 3.12 |
| 浏览器 | Chrome、Edge、Safari 或 Firefox 最新版 |
| 内存 | 最低 4 GB，推荐 8 GB |
| 网络 | 能访问东方财富、腾讯行情和同花顺公开页面 |
| 数据库 | 不需要单独安装，项目自动使用 SQLite |

项目后端只使用 Python 标准库，通常不需要 `pip install` 第三方包。

## 3. 目录结构

```text
trading-agents-stock/
├── market_server.py          # 本地 HTTP 服务和行情接口
├── trading-agents-stock.html # 前端单页看板
├── data/
│   ├── backtest.sqlite       # 评分轨迹、回测和缓存数据库
│   ├── threshold_times.json  # 首次达到评分/概率阈值的时间
│   └── auction/              # 可选的竞价快照目录
├── .gitignore
└── README.md
```

## 4. macOS 部署

### 4.1 安装 Python

推荐从 Python 官网安装，或使用 Homebrew：

```bash
brew install python@3.12
```

确认版本：

```bash
python3 --version
```

### 4.2 启动

```bash
cd /path/to/trading-agents-stock
python3 market_server.py --host 127.0.0.1 --port 8504
```

浏览器打开：

```text
http://127.0.0.1:8504/
```

如果端口被占用，换一个端口：

```bash
python3 market_server.py --port 8505
```

### 4.3 macOS 后台启动

简单后台运行：

```bash
nohup python3 market_server.py --host 127.0.0.1 --port 8504 > trading-agents.log 2>&1 &
```

查看进程：

```bash
lsof -nP -iTCP:8504 -sTCP:LISTEN
```

停止服务：

```bash
kill <PID>
```

长期使用可配置 `launchd`，将下面内容保存为 `~/Library/LaunchAgents/com.tradingagents.stock.plist`，把路径替换成实际目录：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.tradingagents.stock</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/python3</string>
    <string>/path/to/trading-agents-stock/market_server.py</string>
    <string>--host</string><string>127.0.0.1</string>
    <string>--port</string><string>8504</string>
  </array>
  <key>WorkingDirectory</key><string>/path/to/trading-agents-stock</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/tmp/tradingagents-stock.log</string>
  <key>StandardErrorPath</key><string>/tmp/tradingagents-stock-error.log</string>
</dict>
</plist>
```

加载和停止：

```bash
launchctl load ~/Library/LaunchAgents/com.tradingagents.stock.plist
launchctl unload ~/Library/LaunchAgents/com.tradingagents.stock.plist
```

## 5. Windows 11 部署

### 5.1 安装 Python

安装 Python 3.11 或 3.12，并勾选 **Add Python to PATH**。

PowerShell 检查：

```powershell
py --version
```

### 5.2 启动

```powershell
cd C:\path\to\trading-agents-stock
py market_server.py --host 127.0.0.1 --port 8504
```

浏览器打开 `http://127.0.0.1:8504/`。

也可以双击创建 `start.bat`：

```bat
@echo off
cd /d C:\path\to\trading-agents-stock
py market_server.py --host 127.0.0.1 --port 8504
pause
```

### 5.3 Windows 后台运行

推荐使用“任务计划程序”：

1. 打开“任务计划程序”并创建基本任务。
2. 触发器选择“登录时”或“系统启动时”。
3. 程序填写 `py.exe`。
4. 参数填写 `market_server.py --host 127.0.0.1 --port 8504`。
5. “起始于”填写项目目录，例如 `C:\path\to\trading-agents-stock`。

如果只需临时后台运行，可以使用：

```powershell
Start-Process py -ArgumentList "market_server.py --host 127.0.0.1 --port 8504" -WorkingDirectory "C:\path\to\trading-agents-stock"
```

## 6. Linux 部署

### 6.1 安装 Python

Ubuntu/Debian：

```bash
sudo apt update
sudo apt install -y python3 python3-venv
```

启动：

```bash
cd /opt/trading-agents-stock
python3 market_server.py --host 127.0.0.1 --port 8504
```

### 6.2 systemd 服务

创建 `/etc/systemd/system/tradingagents-stock.service`：

```ini
[Unit]
Description=TradingAgents Stock Dashboard
After=network-online.target

[Service]
Type=simple
User=stock
WorkingDirectory=/opt/trading-agents-stock
ExecStart=/usr/bin/python3 /opt/trading-agents-stock/market_server.py --host 127.0.0.1 --port 8504
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

启用：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tradingagents-stock
sudo systemctl status tradingagents-stock
```

查看日志：

```bash
journalctl -u tradingagents-stock -f
```

## 7. 可选：竞价数据接入

竞价委买数据需要你自己的 EMT/Level-2 导出程序。通过环境变量提供命令，命令输出必须是全市场竞价 JSON：

```bash
export EMT_AUCTION_COMMAND='python3 /path/to/export_emt_auction.py --date {date}'
python3 market_server.py --port 8504
```

Windows PowerShell：

```powershell
$env:EMT_AUCTION_COMMAND = "py C:\path\to\export_emt_auction.py --date {date}"
py market_server.py --port 8504
```

没有配置时，其他行情、分时、日 K 和评分功能仍可使用，但竞价委买因子会显示为不可用。

## 8. 数据库与缓存

不需要安装 MySQL、PostgreSQL 或 Redis。项目自动创建：

- `data/backtest.sqlite`：SQLite 数据库，保存评分轨迹、回测结果、历史缓存和概念缓存。
- `data/threshold_times.json`：保存首次达到 60/70 分、预测概率阈值和涨幅阈值的时间。
- `data/auction/`：保存可选竞价快照。

首次启动或全市场扫描时可能需要较长时间。建议不要把运行中的 `data/backtest.sqlite-shm` 和 `data/backtest.sqlite-wal` 文件复制到其他机器；迁移时先停止服务，再复制整个 `data` 目录。

## 9. 生产部署建议

推荐架构：

```text
浏览器 → Nginx/Caddy（可选 HTTPS） → 127.0.0.1:8504 → Python 服务 → 公开行情接口
```

如果只在本机使用，保持 `--host 127.0.0.1`。如果需要局域网访问，可改为 `--host 0.0.0.0`，但必须配置防火墙和访问控制，不建议直接暴露到公网。

生产环境建议：

- 使用 systemd、launchd 或 Windows 任务计划程序自动拉起。
- 使用反向代理提供 HTTPS。
- 限制并发和刷新频率，避免公开接口限流。
- 定期备份 `data/backtest.sqlite` 和 `data/threshold_times.json`。
- 服务异常时优先检查网络、接口返回 502、端口占用和数据库文件权限。

## 10. 常见问题

### 页面打不开

确认服务是否启动，以及浏览器端口是否一致：

```bash
lsof -nP -iTCP:8504 -sTCP:LISTEN
curl http://127.0.0.1:8504/
```

Windows 可执行：

```powershell
Test-NetConnection 127.0.0.1 -Port 8504
```

### 接口返回 502

502 通常表示上游公开行情接口暂时不可用、超时或被限流。稍后刷新，检查网络，并降低并发或刷新频率。502 不一定是本地 Python 服务崩溃。

### 数据为空或加载较慢

首次加载会并发获取行情、历史 K 线、概念、人气和概率数据。等待后台扫描完成；同时检查 Python 服务终端日志。

### 端口被占用

换用其他端口启动：

```bash
python3 market_server.py --port 8505
```

然后访问 `http://127.0.0.1:8505/`。

### macOS 提示无法访问网络

在系统设置中允许终端或 Python 访问网络；如果使用公司网络、代理或 VPN，还需要确认公开行情域名可访问。

## 11. 安全与合规

- 不要把 EMT、Level-2、同花顺或其他账号密码写入代码和 Git。
- 不要提交个人 API Key、Cookie、导出的交易账户数据。
- 公开接口的字段和可用性可能变化，展示结果应结合人工复核。
- 本项目的评分、概率和推荐标签仅用于辅助研究，不保证收益。
