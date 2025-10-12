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
import os
import re
from typing import Any

from flask import Flask, jsonify, Response, request
import psutil
import queue

from binance_client import BinanceClient
from indicators import ema as calc_ema, sma as calc_sma
from binance_websocket import BinanceWebSocket
from trading import TradingEngine


def load_config() -> dict:
    """加载配置（支持 JSONC 注释），兼容 systemd 工作目录与环境变量指定。

    查找优先级：
    1) 环境变量 EMA_CONFIG_PATH 指定的绝对文件路径（如果存在则使用）
    2) 脚本所在目录下的 config.jsonc / config.json
    3) 当前工作目录下的 config.jsonc / config.json
    """
    candidates: list[Path] = []
    env_path = os.environ.get("EMA_CONFIG_PATH")
    if env_path:
        p = Path(env_path).expanduser()
        candidates.append(p)
    script_dir = Path(__file__).resolve().parent
    for name in ("config.jsonc", "config.json"):
        candidates.append(script_dir / name)
    cwd = Path.cwd()
    for name in ("config.jsonc", "config.json"):
        candidates.append(cwd / name)

    cfg_path: Path | None = None
    for p in candidates:
        try:
            if p.exists() and p.is_file():
                cfg_path = p
                break
        except Exception:
            pass
    if cfg_path is None:
        searched = ", ".join(str(x) for x in candidates)
        raise FileNotFoundError(f"Config file not found. Searched: {searched}. Set EMA_CONFIG_PATH to override.")

    txt = cfg_path.read_text(encoding="utf-8")
    # 去除块注释
    txt = re.sub(r"/\*[\s\S]*?\*/", "", txt)
    # 去除以 // 或 # 开头的行内注释（避免误删 URL 中的 //）
    txt = re.sub(r"(^|\s)//.*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"(^|\s)#.*$", "", txt, flags=re.MULTILINE)
    cfg = json.loads(txt)
    # 记录使用的配置路径，便于 systemd 日志排查
    try:
        print(f"[CONFIG] Using: {cfg_path}")
    except Exception:
        pass
    return cfg


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
        # 从完整配置中提取前端展示相关项（如图表高度），若不存在则提供默认值
        full_cfg = getattr(engine, "_config", {}) or {}
        wcfg = (full_cfg.get("web") if isinstance(full_cfg, dict) else {}) or {}
        chart_h = int(wcfg.get("chart_height_px", 420))
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
                "chart_height_px": chart_h,
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
                s["recent_trades"] = engine.recent_trades(50)
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
                        s["recent_trades"] = engine.recent_trades(50)
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

    def _interval_to_per_day(interval: str) -> int:
        try:
            s = interval.strip().lower()
            if s.endswith('m'):
                mins = int(s[:-1])
                return max(1, (24*60)//mins)
            if s.endswith('h'):
                hrs = int(s[:-1])
                return max(1, 24//hrs)
            if s.endswith('d'):
                days = int(s[:-1])
                return max(1, 1//max(1, days))
        except Exception:
            pass
        return 1440  # 默认按 1m 处理

    @app.route('/chart')
    def api_chart():
        # 严格默认仅返回最新 100 根；如需更多由 ?limit= 指定
        default_limit = 100
        try:
            limit = int(request.args.get('limit', default_limit))
        except Exception:
            limit = default_limit
        rows = engine.recent_klines(limit)
        # recent_klines 返回按 id DESC（时间倒序），此处转为时间升序供前端使用
        rows_asc = list(reversed(rows))
        ts = [r['close_time'] for r in rows_asc]
        opens = [float(r['open']) for r in rows_asc]
        highs = [float(r['high']) for r in rows_asc]
        lows = [float(r['low']) for r in rows_asc]
        closes = [float(r['close']) for r in rows_asc]
        vols = [float(r.get('volume', 0.0)) for r in rows_asc]
        # 计算 EMA/MA：使用引擎完整历史（data_period_days）计算的序列，随后截取最后 N 根
        N = len(rows_asc)
        ema_full = getattr(engine, 'ema_list', []) or []
        ma_full = getattr(engine, 'ma_list', []) or []
        ema_list = (ema_full[-N:] if len(ema_full) >= N else ema_full)
        ma_list = (ma_full[-N:] if len(ma_full) >= N else ma_full)
        return jsonify({
            'symbol': engine.symbol,
            'interval': engine.interval,
            'ema_period': engine.ema_period,
            'ma_period': engine.ma_period,
            'time': ts,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': vols,
            'ema': ema_list,
            'ma': ma_list,
        })

    @app.route("/status")
    def api_status():
        s = engine.status()
        s["recent_trades"] = engine.recent_trades(50)
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
          <title>__INTERVAL__ __SYM__ · 兑复量化系统</title>
          <style>
            :root {
              /* 玻璃主题（参考图2）：温润米色底 + 蓝色标题 */
              --bg-1: #eee6cf;
              --bg-2: #e5d5b0;
              --text-1: #1f2937;
              --text-2: #4b5563;
              --line: rgba(0,0,0,.10);
              --blue: #106697; /* 参考图的深蓝 */
              --card-bg: #f5f4f3; /* 卡片内部底色，统一为页面米色 */
              /* 参考图2卡片内部的斜向暖米色渐变 */
              --card-light: #f2e3c5;
              --card-dark: #dec79f;
              /* 最近交易行高用于计算显示 3 行高度 */
              --row-h: 35px;
              /* 标题字号：标题比原来大 4px，副标题再小 8px */
              --title-size: 24px;
              --subtitle-size: calc(var(--title-size) - 16px);
            }

            /* 背景：低饱和渐变 + 轻微噪点，素雅不抢眼 */
            body {
              font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
              margin: 24px;
              color: var(--text-1);
              background: #f5f4f3;
            }
            /* 顶左高光与底右暗角，模拟参考图2的环境光 */
            body::before, body::after {
              content: '';
              position: fixed; inset: 0; pointer-events: none;
            }
            body::before {
              /* 取消叠加高光 */
              background: none;
            }
            body::after {
              /* 取消叠加暗角 */
              background: none;
            }

            /* 标题：纯色蓝，不用渐变 */
            h1, h2 { margin: 0 0 8px 0; color: var(--blue); }
            h1 { font-size: var(--title-size); line-height: 1.25; }
            h2 { font-size: 18px; }
            .card h2 { text-align: center; }
            /* 副标题：更小一号、略淡、与主标题间距紧凑 */
            .subtitle { font-size: var(--subtitle-size); color: rgba(16,102,151,.78); margin: 2px 0 12px 0; }
            /* 单行标题容器：居中对齐、同一行显示 */
            .hero { display: flex; justify-content: center; align-items: baseline; gap: 16px; white-space: nowrap; margin: 0 0 8px 0; }
            .subtitle-inline { font-size: var(--subtitle-size); color: rgba(16,102,151,.78); }

            /* 网格布局保持不变，仅调整间距为紧凑视觉 */
            .grid { display: grid; grid-template-columns: repeat(2, minmax(300px, 1fr)); gap: 16px; }
            .grid1 { display: grid; grid-template-columns: 1fr; gap: 16px; }

            /* 卡片：玻璃质感（双层边框 + 高光 + 轻内阴影） */
            .card {
              position: relative;
              overflow: hidden;
              padding: 12px;
              
              border-radius: 28px;
              /* 内部改为磨砂玻璃：半透明叠层 + 细噪点 */
              background:
                linear-gradient(180deg, rgba(245,244,243,.60), rgba(245,244,243,.45)),
                radial-gradient(rgba(255,255,255,.10) 1px, transparent 1px),
                radial-gradient(rgba(0,0,0,.03) 1px, transparent 1px);
              background-size: auto, 2px 2px, 3px 3px;
              background-position: 0 0, 0 0, 1px 1px;
              background-blend-mode: normal, soft-light, soft-light;
              
              backdrop-filter: blur(16px) saturate(120%);
              -webkit-backdrop-filter: blur(16px) saturate(120%);
              box-shadow:
                0 6px 14px rgba(0,0,0,.18), /* 次级外部投影：提升立体层次 */
                0 16px 32px rgba(0,0,0,.24), /* 主外部投影 */
                inset 0 0 0 1.5px rgba(255,255,255,.45), /* 内沿细亮线（更亮更清晰） */
                inset -12px -14px 26px rgba(0,0,0,.22); /* 底右内暗角（更厚更立体） */
              /* 不使用边框渐变，改用伪元素绘制圆角环形高光 */
              border: 0;
            }

            /* 除“最近交易”外，其余卡片左侧略向右收一点 */
            .card:not(:has(#trades)) { padding-left: 18px; }

            /* 顶左高光斑与内圈暗带，增强厚度与高光走向 */
            .card::before {
              content: '';
              /* 使用遮罩绘制“环形高光”，严格沿圆角边缘，不影响内部 */
              position: absolute; inset: 0;
              border-radius: inherit;
              pointer-events: none;
              padding: 12px; /* 高光环加宽，强化厚边质感 */
              background: linear-gradient(135deg,
                rgba(255,255,255,.90) 0%,  /* 顶左更亮的切边高光 */
                rgba(255,255,255,.45) 40%,
                rgba(0,0,0,.22) 100%      /* 底右更明显的暗角走向 */
              );
              -webkit-mask:
                linear-gradient(#fff 0 0) content-box,
                linear-gradient(#fff 0 0);
              -webkit-mask-composite: xor;
                      mask-composite: exclude;
            }

            /* 额外外环（次级弧边，提升圆润感） */
            .card::after {
              content: '';
              position: absolute; inset: -3px; /* 外扩更明显，形成次级外缘 */
              border-radius: inherit;
              pointer-events: none;
              /* 外缘微环：顶左外沿轻亮、底右外沿轻暗，增强圆润厚度 */
              padding: 3px;
              background: linear-gradient(135deg,
                rgba(255,255,255,.35) 0%,
                rgba(255,255,255,.15) 35%,
                rgba(0,0,0,.10) 100%
              );
              -webkit-mask:
                linear-gradient(#fff 0 0) content-box,
                linear-gradient(#fff 0 0);
              -webkit-mask-composite: xor;
                      mask-composite: exclude;
              box-shadow: none;
            }

            /* 文本与分隔线：更柔和、素雅 */
            p { color: var(--text-2); margin: 0 0 8px 0; }
            /* K线图容器尺寸 */
            #plot_kline { width: 100%; height: 420px; }
            /* K线图标题左对齐，避免与悬停条冲突 */
            #title_kline { text-align: left; padding-left: 12px; }
            /* 顶部悬停信息条 */
            .hoverbar {
              position: absolute; top: 16px; right: 24px; left: auto;
              background: rgba(255,255,255,.35);
              backdrop-filter: blur(6px) saturate(120%);
              -webkit-backdrop-filter: blur(6px) saturate(120%);
              border: 1px solid rgba(255,255,255,.45);
              border-radius: 10px;
              padding: 6px 10px;
              font-size: 12px; color: #0f172a; text-align: right;
              box-shadow: 0 6px 12px rgba(0,0,0,.12);
              display: none; z-index: 10;
              pointer-events: none;
              max-width: 60%;
              white-space: nowrap;
            }
            /* 实时K线卡片中的系统信息（位于表格下方的一行） */
            .kmeta {
              color: var(--text-2);
              font-size: 16px; /* 与K线表格数值一致 */
              margin: 8px 0 0 0;
              display: flex;
              align-items: center;
              gap: 8px;
            }
            .kmeta .left { margin-left: 12px; }
            .kmeta .right { margin-left: auto; text-align: right; margin-right: 12px; }

            /* 表格：极细边与行悬停微亮 */
            table { width: 100%; border-collapse: collapse; border: 0; }
            thead th { color: var(--text-2); font-weight: 600; }
            th, td { border-bottom: 0; padding: 6px 8px; text-align: left; }
            tbody tr:hover { background: rgba(255,255,255,.04); }
            /* 最近交易：默认仅显示 3 行，支持滚动查看更多 */
            #trades { table-layout: fixed; }
            #trades thead, #trades tbody tr { display: table; width: 100%; table-layout: fixed; }
            #trades tbody { display: block; max-height: calc(3 * var(--row-h, 35px)); overflow-y: auto; }
            /* 缩短行间距，避免第三行被遮挡 */
            #trades th, #trades td { padding: 4px 8px; }
            /* 滚动条默认隐藏，悬停时显示 */
            #trades tbody { scrollbar-width: none; scrollbar-color: transparent transparent; }
            #trades tbody:hover { scrollbar-width: thin; scrollbar-color: rgba(0,0,0,.25) transparent; }
            #trades tbody::-webkit-scrollbar { width: 0; height: 0; }
            #trades tbody:hover::-webkit-scrollbar { width: 6px; height: 6px; }
            #trades tbody::-webkit-scrollbar-thumb { background: rgba(0,0,0,.25); border-radius: 6px; }
            #trades tbody::-webkit-scrollbar-track { background: transparent; }

            /* 盈亏颜色：稍微降低饱和度，保持辨识度 */
            .green { color: #0f766e; }
            .red { color: #b91c1c; }

            /* 数值 code：胶囊玻璃质感，紧凑呈现 */
            code {
              color: #1f2f4f;
              background: transparent; /* 完全移除白色背景 */
              border: 0;
              box-shadow: none;
              padding: 0; /* 取消内边距，去除“胶囊”形态 */
              border-radius: 0; /* 取消圆角 */
            }
            /* 系统参数配置：值颜色较 key 略深但不过分（仅作用于该卡片） */
            #cfg code { color: #106697; }
          </style>
          </head>
          <body>
          <div class="hero">
            <h1>兑复量化系统 · __SYM__ · __INTERVAL__</h1>
            <span class="subtitle-inline">兑复相生 · 财富自来 · Power by 无为</span>
          </div>
          <div id="meta"></div>
          <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
          <div class="grid1" style="margin:8px 0 16px 0">
            <div class="card">
              <h2 id="title_kline">K 线图</h2>
              <div id="plot_kline"></div>
              <div id="hoverbar" class="hoverbar"></div>
            </div>
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
              <div id="kmeta" class="kmeta"></div>
            </div>
          </div>
          <script>
          // 交互状态：当前加载的根数、当前可视范围、当前最早时间戳
          let K_LIMIT = 100;
          let K_EARLIEST_TS = null;
          let LAST_RANGE = null;
          let RELOADING = false;
          // 悬停与最新 K 线的实时刷新所需的全局状态
          let K_TIMES = [];
          let K_CLOSE = [];
          let CURRENT_HOVER_INDEX = null;
          // 图表高度（可通过配置调整），默认 260px
          let CHART_HEIGHT = __CHART_HEIGHT__;

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
          async function renderChart(limit = K_LIMIT, preserveRange = null) {
            try {
              K_LIMIT = limit;
              const r = await fetch(`/chart?limit=${K_LIMIT}`);
              const d = await r.json();
              const t = d.time.map(x => new Date(Number(x)));
              const vol = d.volume;
              const incColor = '#16a34a';
              const decColor = '#dc2626';
              const candle = {
                type: 'candlestick',
                x: t,
                open: d.open, high: d.high, low: d.low, close: d.close,
                name: '',
                increasing: { line: { color: incColor }, fillcolor: incColor },
                decreasing: { line: { color: decColor }, fillcolor: decColor },
                opacity: 0.95,
                hoverinfo: 'skip',
                customdata: vol,
              };
              const ema = {
                type: 'scatter', mode: 'lines', x: t, y: d.ema,
                name: `EMA(${d.ema_period})`, line: { color: '#106697', width: 1.6 },
                hoverinfo: 'skip'
              };
              const ma = {
                type: 'scatter', mode: 'lines', x: t, y: d.ma,
                name: `MA(${d.ma_period})`, line: { color: '#f59e0b', width: 1.6 },
                hoverinfo: 'skip'
              };
              // 自定义统一悬停触发：不可见散点，仅用于触发 hover 事件
              const hoverbox = {
                type: 'scatter', mode: 'markers', x: t, y: d.close,
                marker: { opacity: 0 },
                hovertemplate: '<extra></extra>',
                customdata: d.open.map((o,i)=>[o, d.high[i], d.low[i], d.close[i], d.ema[i], d.ma[i]])
              };
              const N = t.length;
              const stepMs = (N>1) ? (Number(d.time[N-1]) - Number(d.time[N-2])) : 0;
              // 增加更大的右侧时间留白，避免高周期（如4h）最后一根被遮挡
              const endPadMs = (stepMs > 0) ? Math.floor(stepMs * 1.5) : 7200000; // 至少留 2h
              const layout = {
                margin: { l: 20, r: 80, t: 10, b: 30 },
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                height: CHART_HEIGHT,
                xaxis: {
                  type: 'date',
                  rangeslider: { visible: false },
                  range: (preserveRange && preserveRange.length === 2) ? preserveRange : [t[0], new Date(Number(d.time[N-1]) + endPadMs)],
                  showspikes: true,
                  spikethickness: 1,
                  spikecolor: '#888'
                },
                yaxis: { fixedrange: false, side: 'right', tickformat: '.1f', separatethousands: false, automargin: true, ticks: 'outside', showexponent: 'none', exponentformat: 'none' },
                showlegend: false,
                legend: { orientation: 'h' },
                hovermode: 'x',
                dragmode: 'pan',
                uirevision: 'kchart',
              };
              const config = { scrollZoom: true, displayModeBar: false };
              const el = document.getElementById('plot_kline');
              if (el) el.style.height = CHART_HEIGHT + 'px';
              Plotly.newPlot('plot_kline', [candle, ema, ma, hoverbox], layout, config);
              // 更新状态：最早时间与当前范围
              K_TIMES = t;
              K_CLOSE = d.close.slice();
              K_EARLIEST_TS = Number(d.time[0]);
              LAST_RANGE = layout.xaxis.range;
              RELOADING = false;
              // 绑定缩放/拖拽事件：当范围左端早于已加载最早点时自动扩容加载
              const plot = document.getElementById('plot_kline');
              const hoverbar = document.getElementById('hoverbar');
              plot.on('plotly_relayout', (ev) => {
                try {
                  const r0 = ev['xaxis.range[0]'] ? new Date(ev['xaxis.range[0]']).getTime() : (Array.isArray(LAST_RANGE) ? new Date(LAST_RANGE[0]).getTime() : null);
                  const r1 = ev['xaxis.range[1]'] ? new Date(ev['xaxis.range[1]']).getTime() : (Array.isArray(LAST_RANGE) ? new Date(LAST_RANGE[1]).getTime() : null);
                  if (r0 && r1) {
                    LAST_RANGE = [new Date(r0), new Date(r1)];
                    if (r0 < K_EARLIEST_TS && !RELOADING) {
                      RELOADING = true;
                      const next = Math.min(K_LIMIT * 2, 2000);
                      renderChart(next, LAST_RANGE);
                    }
                  }
                } catch (e) { console.warn(e); }
              });
              // 顶部悬停信息条：仿截图样式
              function toTimeStr(dt){
                const y = dt.getFullYear();
                const m = String(dt.getMonth()+1).padStart(2,'0');
                const d2 = String(dt.getDate()).padStart(2,'0');
                const hh = String(dt.getHours()).padStart(2,'0');
                const mm = String(dt.getMinutes()).padStart(2,'0');
                return `${y}/${m}/${d2} ${hh}:${mm}`;
              }
              plot.on('plotly_hover', (ev) => {
                try {
                  const p = ev.points && ev.points[0];
                  if (!p) return;
                  const i = p.pointIndex;
                  CURRENT_HOVER_INDEX = i;
                  const ts = t[i];
                  const o = d.open[i], h = d.high[i], l = d.low[i], c = d.close[i];
                  const prev = (i>0) ? d.close[i-1] : NaN;
                  const chg = (isFinite(prev) && prev !== 0) ? ((c - prev) / prev * 100) : NaN;
                  const amp = (isFinite(o) && o !== 0) ? ((h - l) / o * 100) : NaN;
                  const chgCls = isFinite(chg) ? (chg>0?'green':(chg<0?'red':'')) : '';
                  const closeCls = isFinite(c) && isFinite(o) ? (c>o?'green':(c<o?'red':'')) : '';
                  const emaV = d.ema && Number.isFinite(d.ema[i]) ? d.ema[i] : NaN;
                  const maV = d.ma && Number.isFinite(d.ma[i]) ? d.ma[i] : NaN;
                  hoverbar.innerHTML = `
                    <span>${toTimeStr(ts)}</span>
                    · 开: <b>${isFinite(o)?o.toFixed(1):'-'}</b>
                    · 高: <b>${isFinite(h)?h.toFixed(1):'-'}</b>
                    · 低: <b>${isFinite(l)?l.toFixed(1):'-'}</b>
                    · 收: <b class="${closeCls}">${isFinite(c)?c.toFixed(1):'-'}</b>
                    · 涨跌幅: <b class="${chgCls}">${isFinite(chg)?chg.toFixed(2)+'%':'-'}</b>
                    · 振幅: <b>${isFinite(amp)?amp.toFixed(2)+'%':'-'}</b>
                    · EMA: <b style="color:#106697">${isFinite(emaV)?emaV.toFixed(1):'-'}</b>
                    · MA: <b style="color:#f59e0b">${isFinite(maV)?maV.toFixed(1):'-'}</b>
                  `;
                  hoverbar.style.display = 'inline-block';
                } catch(e) { console.warn(e); }
              });
              plot.on('plotly_unhover', () => { CURRENT_HOVER_INDEX = null; if (hoverbar) hoverbar.style.display = 'none'; });
              // 将“BTCUSDT 5m · EMA/MA”移动到标题后面显示
              const title = document.getElementById('title_kline');
              if (title) {
                title.innerHTML = `<span style="font-size:14px;margin-left:8px;color:#0f172a;">
                  ${d.symbol} ${d.interval} · 
                  <span style="display:inline-block;width:14px;border-top:2px solid #106697;margin-right:4px;vertical-align:middle;"></span>EMA(${d.ema_period}) · 
                  <span style="display:inline-block;width:14px;border-top:2px solid #f59e0b;margin-right:4px;vertical-align:middle;"></span>MA(${d.ma_period})
                </span>`;
              }
            } catch (e) { console.error(e); }
          }

          function render(s) {
            const price = s.current_price ? s.current_price.toFixed(1) : '-';
            const ema = s.ema ? s.ema.toFixed(1) : '-';
            const ma = s.ma ? s.ma.toFixed(1) : '-';
            const bal = s.balance?.toFixed(2);
            const sys = s.sysinfo || {};
            const memLeft = fmtBytes(sys.mem_available_bytes);
            const diskLeft = fmtBytes(sys.disk_free_bytes);
            const memAvail = Number(sys.mem_available_bytes || 0);
            const memLeftHtml = memAvail > 0 && memAvail < 100*1024*1024
              ? `<code class="red" style="font-weight:700">${memLeft}</code>`
              : `<code>${memLeft}</code>`;
            document.getElementById('kmeta').innerHTML = `
              <div class="left">⏰ 服务器时间: <code>${new Date(s.server_time).toLocaleString()}</code></div>
              <div class="right">⚙️ CPU <code>${fmtPct(sys.cpu_percent)}</code> · 内存余:${memLeftHtml} · 磁盘余:<code>${diskLeft}</code></div>
            `;
            // 配置汇总（不展示 API 密钥），以单行在“系统参数配置”卡片中显示。
            if (s.config) {
              const cfg = s.config || {};
              const t = cfg.trading || {};
              const i = cfg.indicators || {};
              const w = cfg.web || {};
              const fmtBool = (b) => (b ? '开' : '关');
              // 读取图表高度配置（最小260，最大1200），并应用到下次渲染
              if (w && Number(w.chart_height_px) > 0) {
                CHART_HEIGHT = Math.min(1200, Math.max(260, Number(w.chart_height_px)));
                const el2 = document.getElementById('plot_kline');
                if (el2) el2.style.height = CHART_HEIGHT + 'px';
              }
              document.getElementById('cfg').innerHTML = `
                <p>
                  交易类型: <code>${t.test_mode?'模拟':'实盘'}</code> · 保证金余额:<code>${t.initial_balance}</code> · 开仓比例:<code>${(Number(t.percent)*100).toFixed(0)}%</code> · 杠杆:<code>${t.leverage}x</code> · 手续费率:<code>${(Number(t.fee_rate)*100).toFixed(3)}%</code> · 交易币对:<code>${t.symbol}</code> · ｜ K线周期:<code>${t.interval}</code>
                  指标: EMA<code>${i.ema_period}</code> · MA<code>${i.ma_period}</code> · K线收盘后交易:<code>${fmtBool(i.use_closed_only)}</code> · EMA/MA斜率约束:<code>${fmtBool(i.use_slope)}</code> · 价格轮询:<code>${fmtBool(w.enable_price_poller)}</code>
                  当前显示时区:<code>UTC+${w.timezone_offset_hours||0}</code>
                </p>
              `;
            }
            const priceHtml = `<b class="green">${price}</b>`;
            const balDiff = (s.balance !== undefined && s.initial_balance !== undefined)
              ? (Number(s.balance) - Number(s.initial_balance))
              : 0;
            const balCls = balDiff > 0 ? 'green' : (balDiff < 0 ? 'red' : '');
            const balHtml = `<b class="${balCls}">${bal ?? '-'}</b>`;
            document.getElementById('status').innerHTML = `
              <p>价格: ${priceHtml} · EMA(${s.ema_period||'-'}): <b>${ema}</b> · MA(${s.ma_period||'-'}): <b>${ma}</b></p>
              <p>实时余额: ${balHtml} / 初始保证金: ${s.initial_balance} · 杠杆: ${s.leverage}x · 手续费率: ${(s.fee_rate*100).toFixed(3)}%</p>
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
            const tpNum = Number(totals.total_pnl);
            const roiNum = Number(totals.roi);
            // 若当前鼠标悬停在“最后一根未收盘K线”，则用实时数据刷新顶部悬停条
            try {
              const hoverbar = document.getElementById('hoverbar');
              const isHoveringLast = (CURRENT_HOVER_INDEX !== null) && (K_TIMES && K_TIMES.length > 0) && (CURRENT_HOVER_INDEX === K_TIMES.length - 1);
              if (hoverbar && isHoveringLast) {
                const k = s.latest_kline || {};
                const ts = new Date(Number(K_TIMES[CURRENT_HOVER_INDEX]));
                const o = Number(k.open);
                const h = Number(k.high);
                const l = Number(k.low);
                const c = Number(s.current_price ?? k.close);
                const prev = (K_CLOSE && K_CLOSE.length >= 2) ? Number(K_CLOSE[K_CLOSE.length - 2]) : NaN;
                const chg = (isFinite(prev) && prev !== 0) ? ((c - prev) / prev * 100) : NaN;
                const amp = (isFinite(o) && o !== 0) ? ((h - l) / o * 100) : NaN;
                const chgCls = isFinite(chg) ? (chg>0?'green':(chg<0?'red':'')) : '';
                const closeCls = (isFinite(c) && isFinite(o)) ? (c>o?'green':(c<o?'red':'')) : '';
                const emaV = s.ema;
                const maV = s.ma;
                const toTimeStr = (dt)=>{
                  const y = dt.getFullYear();
                  const m = String(dt.getMonth()+1).padStart(2,'0');
                  const d2 = String(dt.getDate()).padStart(2,'0');
                  const hh = String(dt.getHours()).padStart(2,'0');
                  const mm = String(dt.getMinutes()).padStart(2,'0');
                  return `${y}/${m}/${d2} ${hh}:${mm}`;
                };
                hoverbar.innerHTML = `
                  <span>${toTimeStr(ts)}</span>
                  · 开: <b>${isFinite(o)?o.toFixed(1):'-'}</b>
                  · 高: <b>${isFinite(h)?h.toFixed(1):'-'}</b>
                  · 低: <b>${isFinite(l)?l.toFixed(1):'-'}</b>
                  · 收: <b class="${closeCls}">${isFinite(c)?c.toFixed(1):'-'}</b>
                  · 涨跌幅: <b class="${chgCls}">${isFinite(chg)?chg.toFixed(2)+'%':'-'}</b>
                  · 振幅: <b>${isFinite(amp)?amp.toFixed(2)+'%':'-'}</b>
                  · EMA: <b style="color:#106697">${(emaV!==undefined && emaV!==null && isFinite(Number(emaV)))?Number(emaV).toFixed(1):'-'}</b>
                  · MA: <b style="color:#f59e0b">${(maV!==undefined && maV!==null && isFinite(Number(maV)))?Number(maV).toFixed(1):'-'}</b>
                `;
                hoverbar.style.display = 'inline-block';
              }
            } catch (_) {}
            const tpCls = isFinite(tpNum) ? (tpNum>0?'green':(tpNum<0?'red':'')) : '';
            const roiCls = isFinite(roiNum) ? (roiNum>0?'green':(roiNum<0?'red':'')) : '';
            let valCls = '';
            if (pos.side && pos.entry_price && pos.qty && s.current_price) {
              const ep = Number(pos.entry_price), cp = Number(s.current_price), q = Number(pos.qty);
              const openPnl = pos.side === 'LONG' ? (cp - ep) * q : (ep - cp) * q;
              valCls = openPnl>0 ? 'green' : (openPnl<0 ? 'red' : '');
            }
            document.getElementById('position').innerHTML = `
              <p>总盈亏: <b class="${tpCls}">${tp}</b> · 总利润率: <b class="${roiCls}">${roiPct}</b> · 总手续费: <b>${tf}</b> · 交易次数: <b>${tc}</b></p>
              <p>方向: <b>${side}</b> · 开仓价: ${entry} · 数量: ${qty} · 当前价值: <span class="${valCls}">${val}</span></p>
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
              const pnlCls = (isFinite(pnlNum) && pnlNum !== 0) ? (pnlNum>0?'green':'red') : '';
              const rateNum = (isFinite(pnlNum) && isFinite(notional) && notional > 0) ? (pnlNum / notional) : NaN;
              const rateCls = (isFinite(rateNum) && rateNum !== 0) ? (rateNum>0?'green':'red') : '';
              tb.innerHTML += `<tr>
                <td>${d}</td>
                <td>${t.side}</td>
                <td>${isFinite(price) ? price.toFixed(1) : '-'}</td>
                <td>${isFinite(qty) ? qty.toFixed(4) : '-'}</td>
                <td>${isFinite(fee) ? fee.toFixed(2) : '-'}</td>
                <td class="${pnlCls}">${pnl}</td>
                <td class="${rateCls}">${rate === '-' ? '-' : rate + '%'}</td>
              </tr>`;
            });
            const kb = document.querySelector('#klines tbody');
            kb.innerHTML = '';
            // 仅显示最新未收盘K线的首行，后续历史行不展示
            if (s.latest_kline) {
              const k = s.latest_kline;
              // 仅显示时间（时:分:秒），不显示日期
              const d = new Date(k.close_time).toLocaleTimeString();
              const closeCls = (Number(k.close) > Number(k.open)) ? 'green' : ((Number(k.close) < Number(k.open)) ? 'red' : '');
              kb.innerHTML += `<tr style="font-weight:600"><td>${d}</td><td>${Number(k.open).toFixed(1)}</td><td>${Number(k.high).toFixed(1)}</td><td>${Number(k.low).toFixed(1)}</td><td class="${closeCls}">${Number(k.close).toFixed(1)}</td><td>${Number(k.volume||0).toFixed(2)}</td></tr>`;
            }
            // 不再渲染 (s.recent_klines) 的其它历史行
          }
          // 首屏初始化一次
          (async () => { const r = await fetch('/status'); const s = await r.json(); render(s); renderChart(); })();
          // 订阅服务端事件，实现与 Binance WS 同步节奏的实时更新
          const es = new EventSource('/events/status');
          es.onmessage = (e) => { try { const s = JSON.parse(e.data); render(s); } catch (_) {} };
          </script>
        </body>
        </html>
        """
        try:
            full_cfg = getattr(engine, "_config", {}) or {}
            wcfg = (full_cfg.get("web") if isinstance(full_cfg, dict) else {}) or {}
            ch = int(wcfg.get("chart_height_px", 260))
        except Exception:
            ch = 260
        html = html.replace("__SYM__", engine.symbol).replace("__INTERVAL__", engine.interval).replace("__CHART_HEIGHT__", str(ch))
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

    # 拉取历史 K 线初始化指标（按照 data_period_days 计算需要的根数）
    client = BinanceClient(base_url=tcfg.get("base_url", "https://fapi.binance.com"))
    def _per_day(interval: str) -> int:
        s = str(interval).strip().lower()
        try:
            if s.endswith('m'):
                mins = int(s[:-1])
                return max(1, (24*60)//mins)
            if s.endswith('h'):
                hrs = int(s[:-1])
                return max(1, 24//hrs)
            if s.endswith('d'):
                days = int(s[:-1])
                return max(1, 1//max(1, days))
        except Exception:
            pass
        return 1440
    need_days = int(tcfg.get("data_period_days", 7))
    need = max(200, need_days * _per_day(tcfg.get("interval", "1m")))
    all_klines: list[dict] = []
    end_time_ms = None
    while len(all_klines) < need:
        chunk = client.get_klines(
            symbol=tcfg.get("symbol", "BTCUSDT"),
            interval=tcfg.get("interval", "1m"),
            limit=min(1000, need - len(all_klines)),
            end_time_ms=end_time_ms,
        )
        if not chunk:
            break
        all_klines.extend(chunk)
        # 下一次向更早时间回溯
        try:
            earliest_open = int(chunk[0]["open_time"]) if isinstance(chunk[0], dict) else int(chunk[0][0])
            end_time_ms = earliest_open - 1
        except Exception:
            end_time_ms = None
            break
    # 修复历史数据方向：all_klines 先加入“最新”再向过去回溯
    # 需要取“最近 need 根”并保证时间正序灌入引擎
    hist_sorted_latest = sorted(all_klines, key=lambda k: int(k["close_time"]))
    if len(hist_sorted_latest) > need:
        hist_sorted_latest = hist_sorted_latest[-need:]
    engine.ingest_historical(hist_sorted_latest)

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

    # 将完整配置附加到引擎供 /chart 使用（读取 data_period_days）
    try:
        engine._config = cfg  # type: ignore[attr-defined]
    except Exception:
        pass
    app = create_app(engine, port=wcfg.get("port", 5001), tz_offset=wcfg.get("timezone_offset_hours", 8), events_q=events_q, enable_poller=enable_poller)
    port = int(wcfg.get("port", 5001))
    print(f"Preview URL: http://localhost:{port}/")
    # 生产建议使用 WSGI；此处使用 Flask 内建服务器即可
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()