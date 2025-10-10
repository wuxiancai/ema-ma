# 币安合约 EMA/MA 交叉策略自动交易系统

本项目基于 Binance 合约行情，使用 EMA(5) 与 MA(15) 的交叉信号进行模拟自动交易。系统具备模块化设计、详细注释、SQLite 数据存储以及简单的 Web 实时监控页面，支持后续扩展。

## 功能特性

- 自动获取历史 K 线并存库；实时订阅最新未收盘 K 线
- 计算 EMA(5)、MA(15) 指标并检测金叉/死叉信号
- 模拟交易：初始资金、仓位比例、杠杆、手续费可配置
- 交易记录、资金变动、K 线数据写入 SQLite 数据库
- Web 页面展示当前价格、指标、仓位、余额与最新记录
- 配置文件统一管理参数，便于扩展与切换模式

## 快速开始

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 启动服务

```bash
python web_main.py
```

3. 打开浏览器访问 `http://localhost:5001/` 查看实时状态

## 配置

调整 `config.json` 中的参数：

```json
{
  "trading": {
    "api_key": "",
    "secret_key": "",
    "test_mode": true,
    "initial_balance": 1000.0,
    "percent": 0.5,
    "leverage": 10,
    "fee_rate": 0.0005,
    "symbol": "BTCUSDT",
    "interval": "1m",
    "data_period_days": 1,
    "base_url": "https://fapi.binance.com"
  },
  "indicators": {
    "ema_period": 5,
    "ma_period": 15
  },
  "web": {
    "port": 5001,
    "timezone_offset_hours": 8
  }
}
```

## 项目结构

```
binance_ema_ma/
├── web_main.py
├── binance_client.py
├── binance_websocket.py
├── indicators.py
├── trading.py
├── config.json
├── requirements.txt
├── README.md
├── deploy.sh
└── db/
    └── (运行时自动创建 trading.db)
```

## 说明

- 当前版本默认为模拟交易模式；真实交易需接入下单接口并进行签名鉴权。
- WebSocket 使用币安合约公共流，仅用于行情与未收盘 K 线。
- 数据库会在首次运行时自动创建，位于 `db/trading.db`。

## 许可证

MIT License