"""Web 主入口

职责：
- 加载配置，初始化交易引擎
- 拉取历史 K 线并订阅实时 K 线
- 提供 Web 状态页与 JSON API
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
import re
from typing import Any

from flask import Flask, jsonify, Response
import psutil
import queue

from binance_client import BinanceClient
from binance_websocket import BinanceWebSocket
from trading import TradingEngine


def load_config() -> dict:
    """加载配置，默认读取 config.jsonc，并兼容注释。

    支持的注释形式：
    - 单行注释：以 // 或 # 开头（行内或整行）
    - 块注释：/* ... */
    注意：不支持 JSONC 的“尾逗号”，请勿在最后一项后面加逗号。
    """
    # 优先使用 config.jsonc；若不存在则回退到 config.json
    cfg_path = Path("config.jsonc") if Path("config.jsonc").exists() else Path("config.json")
    txt = cfg_path.read_text(encoding="utf-8")
    # 去除块注释
    txt = re.sub(r"/\*[\s\S]*?\*/", "", txt)
    # 去除以 // 或 # 开头的行内注释（避免误删 URL 中的 //）
    txt = re.sub(r"(^|\s)//.*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"(^|\s)#.*$", "", txt, flags=re.MULTILINE)
    return json.loads(txt)


def get_sysinfo() -> dict:
    """采集系统信息（CPU/MEM/DISK），含剩余容量（字节）。"""
    try:
        cpu = psutil.cpu_percent(interval=0.1)
        vm = psutil.virtual_memory()
        du = psutil.disk_usage('/')
        return {
            "cpu_percent": cpu,
            "mem_percent": vm.percent,
            "mem_available_bytes": int(getattr(vm, "available", 0)),
            "mem_total_bytes": int(getattr(vm, "total", 0)),
            "disk_percent": du.percent,
            "disk_free_bytes": int(getattr(du, "free", 0)),
            "disk_total_bytes": int(getattr(du, "total", 0)),
        }
    except Exception:
        return {}

def get_config_summary(engine: TradingEngine, tz_offset_hours: int, enable_poller: bool) -> dict:
    """汇总需要在页面展示的配置参数（不含 API 密钥）。"""
    try:
        return {
            "trading": {
                "test_mode": engine.test_mode,
                "initial_balance": engine.initial_balance,
                "percent": engine.percent,
                "leverage": engine.leverage,
                "fee_rate": engine.fee_rate,
                "symbol": engine.symbol,
                "interval": engine.interval,
            },
            "indicators": {
                "ema_period": engine.ema_period,
                "ma_period": engine.ma_period,
                "use_closed_only": engine.use_closed_only,
                "use_slope": engine.use_slope,
            },
            "web": {
                "timezone_offset_hours": tz_offset_hours,
                "enable_price_poller": enable_poller,
            },
        }
    except Exception:
        return {}

def start_ws(
    engine: TradingEngine,
    symbol: str,
    interval: str,
    events_q: queue.Queue | None = None,
    *,
    client: BinanceClient,
    enable_fallback_poller: bool = True,
):
    """启动 Binance WS，并在 WS 异常/关闭时自动启用价格轮询作为回退。

    - enable_fallback_poller: True 时，WS 不稳定会自动启用轮询；WS 恢复后关闭轮询
    """

    poller_stop = {"fn": None}

    def start_poller_once():
        if poller_stop["fn"] is None and enable_fallback_poller:
            print("[Fallback] start price poller due to WS issue")
            poller_stop["fn"] = start_price_poller(engine=engine, client=client, events_q=events_q)

    def stop_poller_if_running():
        if poller_stop["fn"] is not None:
            try:
                poller_stop["fn"]()
            except Exception:
                pass
            poller_stop["fn"] = None

    def on_kline(k: dict):
        engine.on_realtime_kline(k)
        # 推送最新状态到前端（与 Binance WS 同步节奏）
        if events_q is not None:
            try:
                s = engine.status()
                s["recent_trades"] = engine.recent_trades(5)
                s["recent_klines"] = engine.recent_klines(5)
                s["server_time"] = int(time.time() * 1000)
                # 附带系统信息（CPU/MEM/DISK）
                s["sysinfo"] = get_sysinfo()
                # 汇总总盈亏/总手续费/总利润率
                s["totals"] = engine.totals()
                events_q.put_nowait(s)
            except Exception:
                pass

    def on_open():
        # WS 恢复，关闭回退轮询
        stop_poller_if_running()

    def on_error(_err):
        # WS 异常，启动回退轮询
        start_poller_once()

    def on_close():
        # WS 关闭，启动回退轮询
        start_poller_once()

    ws = BinanceWebSocket(
        symbol,
        interval,
        on_kline=on_kline,
        on_open_cb=on_open,
        on_error_cb=on_error,
        on_close_cb=on_close,
    )
    ws.start()
    return ws


    def start_price_poller(engine: TradingEngine, client: BinanceClient, events_q: queue.Queue | None = None):
      """轮询最新价格作为 WebSocket 的回退方案，保证页面与策略实时性。

      每 2 秒获取一次价格，并更新引擎的当前价与未收盘K线价格。
      """
    stop_flag = threading.Event()

    def run():
        while not stop_flag.is_set():
            try:
                price = client.get_price(engine.symbol)
                # 组装一个非最终的kline，close_time沿用最近一条，避免推进序列
                close_time = engine.timestamps[-1] if engine.timestamps else int(time.time() * 1000)
                k = {
                    "event_time": int(time.time() * 1000),
                    "open_time": close_time,
                    "close_time": close_time,
                    "interval": engine.interval,
                    "is_final": False,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0.0,
                }
                engine.on_realtime_kline(k)
                # 推送状态，保证 WS 不稳定时仍能更新前端
                if events_q is not None:
                    try:
                        s = engine.status()
                        s["recent_trades"] = engine.recent_trades(5)
                        s["recent_klines"] = engine.recent_klines(5)
                        s["server_time"] = int(time.time() * 1000)
                        s["sysinfo"] = get_sysinfo()
                        # 修复：轮询事件也附带 totals，避免页面在“-”与数值之间来回切换
                        s["totals"] = engine.totals()
                        events_q.put_nowait(s)
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(2)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return stop_flag


def create_app(engine: TradingEngine, port: int, tz_offset: int, events_q: queue.Queue, *, enable_poller: bool):
    app = Flask(__name__)

    @app.route("/status")
    def api_status():
        s = engine.status()
        s["recent_trades"] = engine.recent_trades(5)
        s["recent_klines"] = engine.recent_klines(5)
        s["server_time"] = int(time.time() * 1000) + tz_offset * 3600 * 1000
        s["sysinfo"] = get_sysinfo()
        s["config"] = get_config_summary(engine, tz_offset, enable_poller)
        # 修复：首次加载也返回 totals，避免首屏显示“-”随后切换为数值造成闪烁
        s["totals"] = engine.totals()
        return jsonify(s)

    @app.route("/")
    def index():
        # 前端：使用 SSE 订阅 /events/status，随 WS 推送实时更新
        html = """
        <!doctype html>
        <html lang=zh>
        <head>
          <meta charset=utf-8>
          <meta name=viewport content="width=device-width, initial-scale=1">
          <title>EMA/MA 自动交易系统</title>
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }
            h1 { font-size: 20px; }
            .grid { display: grid; grid-template-columns: repeat(2, minmax(300px, 1fr)); gap: 16px; }
            .grid1 { display: grid; grid-template-columns: 1fr; gap: 16px; }
            .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { border-bottom: 1px solid #eee; padding: 6px 8px; text-align: left; }
            .green { color: #16a34a; }
            .red { color: #dc2626; }
            code { background: #f5f5f5; padding: 2px 6px; border-radius: 4px; }
          </style>
        </head>
        <body>
          <h1>EMA/MA 自动交易系统 · __SYM__ · __INTERVAL__</h1>
          <div id="meta"></div>
          <div class="grid1" style="margin:8px 0 16px 0">
            <div class="card">
              <h2>系统参数配置</h2>
              <div id="cfg"></div>
            </div>
          </div>
          <div class="grid">
            <div class="card">
              <h2>实时合约价格及 EMA/MA</h2>
              <div id="status"></div>
            </div>
            <div class="card">
              <h2>当前持仓及总盈亏</h2>
              <div id="position"></div>
            </div>
            <div class="card">
              <h2>最近交易</h2>
              <table id="trades"><thead><tr><th>时间</th><th>方向</th><th>价格</th><th>数量</th><th>手续费</th><th>盈亏</th><th>利润率</th></tr></thead><tbody></tbody></table>
            </div>
            <div class="card">
              <h2>实时 K 线</h2>
              <table id="klines"><thead><tr><th>收盘时间</th><th>开</th><th>高</th><th>低</th><th>收</th><th>量</th></tr></thead><tbody></tbody></table>
            </div>
          </div>
          <script>
          function fmtPct(x){ return (x===undefined||x===null||isNaN(Number(x))) ? '-' : (Number(x).toFixed(1) + '%'); }
          function fmtBytes(b){
            const n = Number(b);
            if (!isFinite(n) || n <= 0) return '-';
            const KB = 1024, MB = KB*1024, GB = MB*1024, TB = GB*1024;
            if (n >= TB) return (n/TB).toFixed(1) + 'T';
            if (n >= GB) return (n/GB).toFixed(1) + 'G';
            if (n >= MB) return (n/MB).toFixed(0) + 'M';
            if (n >= KB) return (n/KB).toFixed(0) + 'K';
            return n.toFixed(0) + 'B';
          }
          function render(s) {
            const price = s.current_price ? s.current_price.toFixed(1) : '-';
            const ema = s.ema ? s.ema.toFixed(1) : '-';
            const ma = s.ma ? s.ma.toFixed(1) : '-';
            const bal = s.balance?.toFixed(2);
            const sys = s.sysinfo || {};
            const memLeft = fmtBytes(sys.mem_available_bytes);
            const diskLeft = fmtBytes(sys.disk_free_bytes);
            document.getElementById('meta').innerHTML = `
              <p>服务器时间: <code>${new Date(s.server_time).toLocaleString()}</code> · CPU <code>${fmtPct(sys.cpu_percent)}</code> · MEM <code>${fmtPct(sys.mem_percent)}</code> 余:<code>${memLeft}</code> · DISK <code>${fmtPct(sys.disk_percent)}</code> 余:<code>${diskLeft}</code></p>
            `;
            // 配置汇总（不展示 API 密钥），以单行在“系统参数配置”卡片中显示。
            if (s.config) {
              const cfg = s.config || {};
              const t = cfg.trading || {};
              const i = cfg.indicators || {};
              const w = cfg.web || {};
              const fmtBool = (b) => (b ? '开' : '关');
              document.getElementById('cfg').innerHTML = `
                <p>
                  交易类型: <code>${t.test_mode?'模拟':'真实'}</code> · 保证金余额:<code>${t.initial_balance}</code> · 开仓比例:<code>${(Number(t.percent)*100).toFixed(0)}%</code> · 杠杆:<code>${t.leverage}x</code> · 手续费率:<code>${(Number(t.fee_rate)*100).toFixed(3)}%</code> · 交易币对:<code>${t.symbol}</code> · ｜ K线周期:<code>${t.interval}</code>
                  指标: EMA<code>${i.ema_period}</code> · MA<code>${i.ma_period}</code> · K线收盘后交易:<code>${fmtBool(i.use_closed_only)}</code> · EMA/MA斜率约束:<code>${fmtBool(i.use_slope)}</code> · 价格轮询:<code>${fmtBool(w.enable_price_poller)}</code>
                  当前显示时区:UTC +<code>${w.timezone_offset_hours||0}</code>
                </p>
              `;
            }
            document.getElementById('status').innerHTML = `
              <p>价格: <b>${price}</b> · EMA(${s.ema_period||'-'}): <b>${ema}</b> · MA(${s.ma_period||'-'}): <b>${ma}</b></p>
              <p>实时余额: <b>${bal}</b> / 初始保证金: ${s.initial_balance} · 杠杆: ${s.leverage}x · 手续费率: ${(s.fee_rate*100).toFixed(3)}%</p>
            `;
            const pos = s.position || {};
            const side = pos.side || '-';
            const entry = pos.entry_price ? pos.entry_price.toFixed(1) : '-';
            const qty = pos.qty ? pos.qty.toFixed(4) : '-';
            const val = pos.value ? pos.value.toFixed(2) : '-';
            const totals = s.totals || {};
            const tp = (totals.total_pnl !== undefined && totals.total_pnl !== null) ? Number(totals.total_pnl).toFixed(2) : '-';
            const tf = (totals.total_fee !== undefined && totals.total_fee !== null) ? Number(totals.total_fee).toFixed(2) : '-';
            const tc = (totals.trade_count !== undefined && totals.trade_count !== null) ? Number(totals.trade_count) : '-';
            const roiPct = (totals.roi !== undefined && totals.roi !== null) ? (Number(totals.roi) * 100).toFixed(2) + '%' : '-';
            document.getElementById('position').innerHTML = `
              <p>总盈亏: <b>${tp}</b> · 总利润率: <b>${roiPct}</b> · 总手续费: <b>${tf}</b> · 交易次数: <b>${tc}</b></p>
              <p>方向: <b>${side}</b> · 开仓价: ${entry} · 数量: ${qty} · 当前价值: ${val}</p>
            `;
            const tb = document.querySelector('#trades tbody');
            tb.innerHTML = '';
            (s.recent_trades||[]).forEach(t => {
              // 仅显示时间（时:分:秒），不显示日期
              const d = new Date(t.time).toLocaleTimeString();
              const price = Number(t.price);
              const qty = Number(t.qty);
              const fee = Number(t.fee);
              const pnlNum = Number(t.pnl);
              const notional = (isFinite(price) && isFinite(qty)) ? price * qty : NaN;
              const rate = (isFinite(pnlNum) && isFinite(notional) && notional > 0)
                ? ((pnlNum / notional) * 100).toFixed(2)
                : '-';
              const pnl = (t.pnl === null || Number.isNaN(pnlNum)) ? '-' : pnlNum.toFixed(2);
              tb.innerHTML += `<tr>
                <td>${d}</td>
                <td>${t.side}</td>
                <td>${isFinite(price) ? price.toFixed(1) : '-'}</td>
                <td>${isFinite(qty) ? qty.toFixed(4) : '-'}</td>
                <td>${isFinite(fee) ? fee.toFixed(2) : '-'}</td>
                <td>${pnl}</td>
                <td>${rate === '-' ? '-' : rate + '%'}</td>
              </tr>`;
            });
            const kb = document.querySelector('#klines tbody');
            kb.innerHTML = '';
            // 仅显示最新未收盘K线的首行，后续历史行不展示
            if (s.latest_kline) {
              const k = s.latest_kline;
              // 仅显示时间（时:分:秒），不显示日期
              const d = new Date(k.close_time).toLocaleTimeString();
              kb.innerHTML += `<tr style="font-weight:600"><td>${d}</td><td>${Number(k.open).toFixed(1)}</td><td>${Number(k.high).toFixed(1)}</td><td>${Number(k.low).toFixed(1)}</td><td>${Number(k.close).toFixed(1)}</td><td>${Number(k.volume||0).toFixed(2)}</td></tr>`;
            }
            // 不再渲染 (s.recent_klines) 的其它历史行
          }
          // 首屏初始化一次
          (async () => { const r = await fetch('/status'); const s = await r.json(); render(s); })();
          // 订阅服务端事件，实现与 Binance WS 同步节奏的实时更新
          const es = new EventSource('/events/status');
          es.onmessage = (e) => { try { const s = JSON.parse(e.data); render(s); } catch (_) {} };
          </script>
        </body>
        </html>
        """
        html = html.replace("__SYM__", engine.symbol).replace("__INTERVAL__", engine.interval)
        return Response(html, mimetype="text/html")

    @app.route('/events/status')
    def events_status():
        def stream():
            while True:
                try:
                    s = events_q.get()
                    yield f"data: {json.dumps(s)}\n\n"
                except Exception:
                    time.sleep(0.1)
        return Response(stream(), mimetype='text/event-stream')

    return app


def main():
    cfg = load_config()
    tcfg = cfg.get("trading", {})
    wcfg = cfg.get("web", {})

    engine = TradingEngine(cfg)

    # 拉取历史 K 线初始化指标
    client = BinanceClient(base_url=tcfg.get("base_url", "https://fapi.binance.com"))
    hist = client.get_klines(
        symbol=tcfg.get("symbol", "BTCUSDT"),
        interval=tcfg.get("interval", "1m"),
        limit=200,
    )
    engine.ingest_historical(hist)

    # 事件队列供前端 SSE 使用
    events_q: queue.Queue = queue.Queue(maxsize=1000)
    enable_poller = bool(wcfg.get("enable_price_poller", False))
    # 启动 WS；当未开启价格轮询时，WS 出问题会自动启用轮询作回退
    ws = start_ws(
        engine,
        engine.symbol,
        engine.interval,
        events_q=events_q,
        client=client,
        enable_fallback_poller=not enable_poller,
    )
    if enable_poller:
        start_price_poller(engine, client, events_q=events_q)

    app = create_app(engine, port=wcfg.get("port", 5001), tz_offset=wcfg.get("timezone_offset_hours", 8), events_q=events_q, enable_poller=enable_poller)
    port = int(wcfg.get("port", 5001))
    print(f"Preview URL: http://localhost:{port}/")
    # 生产建议使用 WSGI；此处使用 Flask 内建服务器即可
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()