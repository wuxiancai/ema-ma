"""交易引擎（模拟模式）

负责：
- 维护资金、仓位、手续费与盈亏
- 接收历史与实时 K 线数据，计算指标并触发开/平仓
- 将 K 线与交易记录写入 SQLite 数据库

说明：
- 为简洁起见，真实下单未实现；后续可在此模块扩展 API 签名与下单。
"""
from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional
import math
import logging
from logging.handlers import RotatingFileHandler

from indicators import ema, sma, crossover, is_rising
from binance_client import BinanceClient


@dataclass
class Position:
    side: Optional[str]  # "LONG" | "SHORT" | None
    entry_price: float | None
    qty: float | None
    open_fee: float | None


class TradingEngine:
    def __init__(self, config: dict) -> None:
        tcfg = config.get("trading", {})
        icfg = config.get("indicators", {})

        self.symbol = tcfg.get("symbol", "BTCUSDT").upper()
        self.interval = tcfg.get("interval", "1m")
        # 初始保证金：默认使用配置；若为实盘模式且提供密钥，则从合约账户余额获取
        self.initial_balance = float(tcfg.get("initial_balance", 1000.0))
        self.balance = self.initial_balance
        self.percent = float(tcfg.get("percent", 0.5))
        self.leverage = int(tcfg.get("leverage", 10))
        self.fee_rate = float(tcfg.get("fee_rate", 0.0005))
        self.test_mode = bool(tcfg.get("test_mode", True))
        # 日志控制：关闭高频 [TICK] 与信号调试日志，避免刷屏
        self.enable_tick_log: bool = bool(tcfg.get("enable_tick_log", False))
        self.enable_signal_debug_log: bool = bool(tcfg.get("enable_signal_debug_log", False))

        self.ema_period = int(icfg.get("ema_period", 5))
        self.ma_period = int(icfg.get("ma_period", 15))

        # 状态
        self.position = Position(side=None, entry_price=None, qty=None, open_fee=None)
        self.current_price: float | None = None
        self.timestamps: list[int] = []  # close_time
        self.closes: list[float] = []
        self.ema_list: list[float] = []
        self.ma_list: list[float] = []
        self.latest_kline: dict | None = None  # 未收盘的实时K线（完整O/H/L/C/Vol）
        # 计算选项
        icfg = config.get("indicators", {})
        # 是否仅使用已收盘K线参与均线计算（更贴近多数交易所图表）
        self.use_closed_only: bool = bool(icfg.get("use_closed_only", True))
        # 是否将 EMA/MA 斜率（趋势）纳入开仓条件
        self.use_slope: bool = bool(icfg.get("use_slope", True))

        # 日志文件（项目目录下 trading.log）
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_path = os.path.join(base_dir, "trading.log")
            self._logger = logging.getLogger("trading_file_logger")
            self._logger.setLevel(logging.INFO)
            need_handler = True
            for h in list(self._logger.handlers):
                try:
                    if hasattr(h, "baseFilename") and getattr(h, "baseFilename", "") == log_path:
                        need_handler = False
                        break
                except Exception:
                    pass
            if need_handler:
                fh = RotatingFileHandler(log_path, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
                fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
                fh.setFormatter(fmt)
                self._logger.addHandler(fh)
                self._logger.propagate = False
        except Exception:
            try:
                self._logger = logging.getLogger("trading_file_logger")
                if not self._logger.handlers:
                    self._logger.addHandler(logging.NullHandler())
            except Exception:
                pass

        # 若为实盘：
        # - 初始保证金 = 合约总保证金余额（wallet + 未实现盈亏）
        # - 实时余额 = 合约总钱包余额（wallet）
        # 实盘账户信息与下单客户端
        self._client_auth: BinanceClient | None = None
        self._step_size: float = 0.001  # 默认步进（BTCUSDT 通常为 0.001）
        self._dual_side: bool = False   # 是否开启双向持仓（Hedge Mode）
        self._min_qty: float | None = None  # 交易对最小数量（LOT_SIZE.minQty 或 MARKET_LOT_SIZE.minQty）
        self._min_notional: float | None = None  # 交易对最小名义金额（MIN_NOTIONAL.minNotional）
        # 是否在启动时自动设置双向持仓（默认开启）
        self.auto_set_dual_side: bool = bool(tcfg.get("auto_set_dual_side", True))
        try:
            if not self.test_mode:
                api_key = str(tcfg.get("api_key") or "")
                secret_key = str(tcfg.get("secret_key") or "")
                base_url = str(tcfg.get("base_url") or "https://fapi.binance.com")
                if api_key and secret_key:
                    self._client_auth = BinanceClient(base_url=base_url, api_key=api_key, secret_key=secret_key)
                    # 启动时同步本地持仓（若存在真实持仓），避免与实盘状态不一致
                    try:
                        rp_any = self._client_auth.get_futures_position(self.symbol)
                        if rp_any and rp_any.get("positionAmt") is not None and abs(float(rp_any.get("positionAmt"))) > 0:
                            amt = float(rp_any.get("positionAmt"))
                            side_sync = ("LONG" if amt > 0 else ("SHORT" if amt < 0 else None))
                            if side_sync:
                                ep = rp_any.get("entryPrice")
                                entry_sync = (float(ep) if ep is not None else None)
                                qty_sync = abs(amt)
                                self.position = Position(side=side_sync, entry_price=entry_sync, qty=qty_sync, open_fee=0.0)
                                try:
                                    msg = f"[ACCOUNT] 启动同步持仓: side={side_sync} entry={entry_sync} qty={qty_sync}"
                                    print(msg)
                                    self._log(msg)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    totals = self._client_auth.get_futures_account_totals()
                    if totals:
                        twb = totals.get("totalWalletBalance")
                        tmb = totals.get("totalMarginBalance")
                        if tmb is not None:
                            self.initial_balance = float(tmb)
                        if twb is not None:
                            self.balance = float(twb)
                    else:
                        # 回退：若 totals 不可用，则以资产余额作为钱包余额
                        bal = self._client_auth.get_futures_balance(asset="USDT")
                        if bal is not None and bal >= 0:
                            self.balance = float(bal)
                    # 初始化交易参数：杠杆与数量步进
                    try:
                        self._client_auth.set_leverage(self.symbol, self.leverage)
                    except Exception:
                        pass
                    try:
                        # 优先一次性查询完整过滤器，包含步进、最小数量与最小名义
                        filters = self._client_auth.get_symbol_filters(self.symbol) if self._client_auth else None
                        if isinstance(filters, dict):
                            step = filters.get("stepSize")
                            if isinstance(step, (int, float)) and step > 0:
                                self._step_size = float(step)
                            mq = filters.get("marketMinQty") or filters.get("minQty")
                            if isinstance(mq, (int, float)) and mq > 0:
                                self._min_qty = float(mq)
                            mn = filters.get("minNotional")
                            if isinstance(mn, (int, float)) and mn > 0:
                                self._min_notional = float(mn)
                        else:
                            # 回退：只查询步进
                            step = self._client_auth.get_symbol_step_size(self.symbol)
                            if isinstance(step, float) and step > 0:
                                self._step_size = step
                    except Exception:
                        pass
                    try:
                        ds = self._client_auth.get_position_side_dual()
                        if isinstance(ds, bool):
                            self._dual_side = ds
                            # 按需自动设置为双向持仓
                            if self.auto_set_dual_side and (not ds):
                                ok = False
                                try:
                                    ok = self._client_auth.set_position_side_dual(True)
                                except Exception:
                                    ok = False
                                if ok:
                                    self._dual_side = True
                                    try:
                                        msg = "[ACCOUNT] 已自动开启双向持仓 (hedge mode)"
                                        print(msg)
                                        self._log(msg)
                                    except Exception:
                                        pass
                                else:
                                    try:
                                        msg = "[ACCOUNT] 自动开启双向持仓失败，请检查 API 权限或合约账户状态"
                                        print(msg)
                                        self._log(msg)
                                    except Exception:
                                        pass
                    except Exception:
                        pass
        except Exception:
            # 保持稳健：异常时保留配置中的初始值
            pass

        # DB
        self.db_path = os.path.join("db", "trading.db")
        os.makedirs("db", exist_ok=True)
        self._db = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._init_db()
        # 初始化余额：从 wallet 表恢复最近余额；若数据库为空，则写入初始余额
        # 这样在程序重启后，页面上的“实时余额”不会回到 initial_balance，
        # 而是延续上次运行的结果（例如 970），与累计的总盈亏保持一致。
        self._restore_balance_from_wallet()
        # 启动时恢复未平仓的持仓信息（保证重启后仍显示当前持仓）
        self._restore_open_position()
        # 实盘模式：用交易所真实持仓覆盖本地持仓，避免重启后本地状态与实盘不一致
        try:
            if (not self.test_mode) and self._client_auth:
                rp = self._client_auth.get_futures_position(self.symbol)
                if isinstance(rp, dict) and rp.get("positionAmt") is not None:
                    amt = float(rp.get("positionAmt"))
                    if abs(amt) > 0:
                        side = "LONG" if amt > 0 else "SHORT"
                        entry = float(rp.get("entryPrice") or 0.0) or None
                        qty = abs(amt)
                        # 以 0 记录开仓手续费占位；真实手续费在后续 CLOSE 记录计算净盈亏时体现
                        self.position = Position(side=side, entry_price=entry, qty=qty, open_fee=0.0)
                        # 覆盖 position 表，保证后续重启也能恢复到与实盘一致的持仓
                        self._clear_position()
                        self._save_position()
                        try:
                            print(f"[SYNC-POS] from API: side={side} entry={entry} qty={qty}")
                        except Exception:
                            pass
        except Exception:
            # 若同步失败，保留本地推断的持仓；状态接口仍会显示实盘的真实持仓
            pass

    # --------------------- DB ---------------------
    def _init_db(self):
        cur = self._db.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS klines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                interval TEXT,
                open_time INTEGER,
                close_time INTEGER,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time INTEGER,
                symbol TEXT,
                side TEXT,
                price REAL,
                qty REAL,
                fee REAL,
                pnl REAL,
                balance_after REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time INTEGER,
                balance REAL
            )
            """
        )
        # 记录当前未平仓的持仓，便于程序重启后恢复
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS position (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time INTEGER,
                side TEXT,
                entry_price REAL,
                qty REAL,
                open_fee REAL
            )
            """
        )
        # 系统元信息：记录数据库初始化时间，供“交易时长”展示使用
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time INTEGER
            )
            """
        )
        # 若不存在初始化时间，则写入当前时间；存在则沿用
        try:
            cur.execute("SELECT start_time FROM meta ORDER BY id ASC LIMIT 1")
            r = cur.fetchone()
            if not r or (r[0] is None):
                st = int(time.time() * 1000)
                cur.execute("INSERT INTO meta(start_time) VALUES (?)", (st,))
                self.db_start_ms = st
            else:
                self.db_start_ms = int(r[0])
        except Exception:
            # 兜底：若查询失败则以当前时间作为起点（不会影响已存在数据）
            try:
                st = int(time.time() * 1000)
                cur.execute("INSERT INTO meta(start_time) VALUES (?)", (st,))
                self.db_start_ms = st
            except Exception:
                self.db_start_ms = int(time.time() * 1000)
        self._db.commit()

    def _restore_balance_from_wallet(self):
        """在程序启动时恢复余额。

        模拟模式：
        - 若 wallet 表存在记录，则将引擎余额设为最近一条记录的余额；
        - 若不存在记录，则将当前余额写入 wallet，作为基准起点。

        实盘模式：
        - 跳过从 wallet 表恢复，保留已从 API 获取的实时余额；
        - 若 wallet 表为空，则将当前余额写入 wallet 作为首个快照。
        """
        try:
            cur = self._db.cursor()
            if self.test_mode:
                cur.execute("SELECT balance FROM wallet ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                if row and row[0] is not None:
                    self.balance = float(row[0])
                else:
                    self._insert_wallet()
            else:
                # 实盘：仅在 wallet 为空时写入当前（来自 API）余额
                cur.execute("SELECT COUNT(1) FROM wallet")
                cnt = int(cur.fetchone()[0] or 0)
                if cnt == 0:
                    self._insert_wallet()
        except Exception:
            pass

    def _restore_open_position(self):
        """在程序启动时恢复未平仓持仓。

        优先从 position 表恢复；若为空，则从 trades 表推断：
        - 找到最近一次开仓记录（LONG/SHORT）；
        - 若其后不存在 CLOSE 记录，则视为当前仍持仓。
        """
        try:
            cur = self._db.cursor()
            # 1) 优先读取 position 表最新记录
            cur.execute("SELECT side, entry_price, qty, open_fee FROM position ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row and row[0] is not None:
                self.position = Position(side=row[0], entry_price=float(row[1]), qty=float(row[2]), open_fee=float(row[3] or 0.0))
                return

            # 2) 回退：从 trades 推断是否仍有未平仓
            cur.execute("SELECT id, side, price, qty, fee FROM trades WHERE side IN ('LONG','SHORT') ORDER BY id DESC LIMIT 1")
            t = cur.fetchone()
            if t:
                last_open_id = int(t[0])
                # 检查是否存在晚于该开仓记录的平仓记录
                cur.execute("SELECT COUNT(1) FROM trades WHERE id > ? AND side = 'CLOSE'", (last_open_id,))
                closed_count = int(cur.fetchone()[0] or 0)
                if closed_count == 0:
                    self.position = Position(side=str(t[1]), entry_price=float(t[2]), qty=float(t[3]), open_fee=float(t[4] or 0.0))
        except Exception:
            pass

    def _insert_kline(self, k: dict):
        cur = self._db.cursor()
        cur.execute(
            """
            INSERT INTO klines(symbol, interval, open_time, close_time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.symbol,
                self.interval,
                int(k["open_time"]),
                int(k["close_time"]),
                float(k["open"]),
                float(k["high"]),
                float(k["low"]),
                float(k["close"]),
                float(k.get("volume", 0.0)),
            ),
        )
        self._db.commit()

    def _insert_trade(self, side: str, price: float, qty: float, fee: float, pnl: float):
        cur = self._db.cursor()
        cur.execute(
            """
            INSERT INTO trades(time, symbol, side, price, qty, fee, pnl, balance_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(time.time() * 1000), self.symbol, side, price, qty, fee, pnl, self.balance),
        )
        self._db.commit()

    def _save_position(self):
        """将当前未平仓持仓写入 position 表（只追加一条最新记录）。"""
        try:
            if self.position.side is None:
                return
            cur = self._db.cursor()
            cur.execute(
                "INSERT INTO position(time, side, entry_price, qty, open_fee) VALUES (?, ?, ?, ?, ?)",
                (int(time.time() * 1000), self.position.side, float(self.position.entry_price or 0.0), float(self.position.qty or 0.0), float(self.position.open_fee or 0.0)),
            )
            self._db.commit()
        except Exception:
            pass

    def _clear_position(self):
        """清空 position 表（表示当前无未平仓持仓）。"""
        try:
            cur = self._db.cursor()
            cur.execute("DELETE FROM position")
            self._db.commit()
        except Exception:
            pass

    def _insert_wallet(self):
        cur = self._db.cursor()
        cur.execute(
            "INSERT INTO wallet(time, balance) VALUES (?, ?)",
            (int(time.time() * 1000), self.balance),
        )
        self._db.commit()

    # --------------------- Aggregates ---------------------
    def totals(self) -> dict:
        """统计总盈亏、总手续费与总利润率。

        定义：
        - 总盈亏：自程序初始化数据库以来，所有平仓记录（side='CLOSE'）的净盈亏之和（trades.pnl）。
        - 总手续费：自程序初始化数据库以来，所有开/平仓记录的手续费之和（trades.fee）。
        - 交易次数：自程序初始化数据库以来，所有平仓记录的次数（每次平仓计 1 次）。
        - 总利润率：总盈亏除以基准资金，其中基准资金取 wallet 表的第一条记录；若不存在，则取配置的 initial_balance。
        """
        cur = self._db.cursor()
        # 平仓净盈亏总和（不含开仓的负手续费记录）
        cur.execute("SELECT COALESCE(SUM(pnl), 0.0) FROM trades WHERE side = 'CLOSE'")
        total_pnl = float(cur.fetchone()[0] or 0.0)

        # 开/平仓手续费总和
        cur.execute("SELECT COALESCE(SUM(fee), 0.0) FROM trades")
        total_fee = float(cur.fetchone()[0] or 0.0)

        # 交易次数：平仓记录计数
        cur.execute("SELECT COUNT(1) FROM trades WHERE side = 'CLOSE'")
        trade_count = int(cur.fetchone()[0] or 0)

        # 基准资金：wallet 首条记录，否则使用 initial_balance
        cur.execute("SELECT balance FROM wallet ORDER BY id ASC LIMIT 1")
        row = cur.fetchone()
        base_balance = float(row[0]) if row and row[0] is not None else float(self.initial_balance)

        roi = (total_pnl / base_balance) if base_balance > 0 else 0.0
        return {
            "total_pnl": round(total_pnl, 6),
            "total_fee": round(total_fee, 6),
            "trade_count": trade_count,
            "roi": roi,
            "base_balance": base_balance,
        }

    # --------------------- Data & Indicators ---------------------
    def _recalc_indicators(self):
        self.ema_list = ema(self.closes, self.ema_period)
        self.ma_list = sma(self.closes, self.ma_period)

    def ingest_historical(self, klines: list[dict]):
        for k in klines:
            self._insert_kline(k)
            self.timestamps.append(k["close_time"])
            self.closes.append(float(k["close"]))
        self._recalc_indicators()

    def on_realtime_kline(self, k: dict):
        # 未收盘也参与计算（更贴近实时策略）；收盘时落库
        price = float(k["close"])
        self.current_price = price
        close_time = int(k["close_time"])

        # 指标计算的数据推进策略
        if self.use_closed_only:
            # 仅在收盘事件时推进与更新，未收盘不影响均线计算
            if bool(k.get("is_final", False)):
                # 新K线或当前K线收盘
                if not self.timestamps or close_time != self.timestamps[-1]:
                    self.timestamps.append(close_time)
                    self.closes.append(price)
                else:
                    self.closes[-1] = price
                self._recalc_indicators()
        else:
            # 未收盘也进入计算：更灵敏，但与交易所图略有差异
            if not self.timestamps or close_time != self.timestamps[-1]:
                self.timestamps.append(close_time)
                self.closes.append(price)
            else:
                self.closes[-1] = price
            self._recalc_indicators()


        # 保存未收盘完整K线用于前端展示
        try:
            self.latest_kline = {
                "open_time": int(k.get("open_time", close_time)),
                "close_time": close_time,
                "open": float(k.get("open", price)),
                "high": float(k.get("high", price)),
                "low": float(k.get("low", price)),
                "close": float(k.get("close", price)),
                "volume": float(k.get("volume", 0.0)),
                "is_final": bool(k.get("is_final", False)),
            }
        except Exception:
            pass

        # 仅在最近两个点有效时评估信号
        cross = crossover(self.ema_list, self.ma_list)

        # 额外条件：价格相对均线、均线趋势
        ema_rising = is_rising(self.ema_list, lookback=3)
        ema_curr = self.ema_list[-1]
        ma_curr = self.ma_list[-1]
        if ema_curr is None or ma_curr is None:
            return

        # 触发交叉时写文件日志（含是否最终收盘事件）
        try:
            if cross.golden_cross or cross.death_cross:
                self._log(f"[CROSS] ts={close_time} final={bool(k.get('is_final', False))} golden={cross.golden_cross} death={cross.death_cross} price={price:.2f} ema={ema_curr:.2f} ma={ma_curr:.2f}")
        except Exception:
            pass

        # 轻量日志，便于观察实时更新
        if self.enable_tick_log:
            try:
                print(f"[TICK] price={price:.2f} ema={ema_curr:.2f} ma={ma_curr:.2f} cross(g={cross.golden_cross}, d={cross.death_cross})")
            except Exception:
                pass

        # 若配置为仅收盘交易，则在未收盘事件直接退出（但仍记录交叉日志）
        if self.use_closed_only and (not bool(k.get("is_final", False))):
            return

        if self.position.side is None:
            # 开仓逻辑（记录每个条件，便于对比 Binance 图表）
            slope_ok_long = (ema_rising if self.use_slope else True)
            slope_ok_short = ((not ema_rising) if self.use_slope else True)
            cond_long = cross.golden_cross and price > ema_curr and ema_curr > ma_curr and slope_ok_long
            cond_short = cross.death_cross and price < ema_curr and ema_curr < ma_curr and slope_ok_short
            if cond_long:
                if self.enable_signal_debug_log:
                    print(f"[OPEN-CHECK] LONG ok: price>{ema_curr:.2f} ema>{ma_curr:.2f} rising={ema_rising} slope_on={self.use_slope}")
                # 开多前检查实盘是否已有多仓（仅收盘事件时严格检查）
                if bool(k.get("is_final", False)) and (not self.test_mode) and self._client_auth:
                    try:
                        rp_long = self._client_auth.get_futures_position(self.symbol, prefer_side="LONG")
                        has_long = bool(rp_long and rp_long.get("positionAmt") is not None and abs(float(rp_long.get("positionAmt"))) > 0)
                        if has_long:
                            msg = "[OPEN-SKIP] 检测到实盘持有多仓，跳过开多"
                            print(msg)
                            self._log(msg)
                        else:
                            self._open_position("LONG", price)
                    except Exception:
                        self._open_position("LONG", price)
                else:
                    self._open_position("LONG", price)
            elif cross.golden_cross and self.enable_signal_debug_log:
                print(f"[OPEN-CHECK] LONG miss: price>{ema_curr:.2f}={price>ema_curr} ema>{ma_curr:.2f}={ema_curr>ma_curr} rising={ema_rising} slope_on={self.use_slope}")
            if cond_short:
                if self.enable_signal_debug_log:
                    print(f"[OPEN-CHECK] SHORT ok: price<{ema_curr:.2f} ema<{ma_curr:.2f} rising={ema_rising} slope_on={self.use_slope}")
                # 开空前检查实盘是否已有空仓（仅收盘事件时严格检查）
                if bool(k.get("is_final", False)) and (not self.test_mode) and self._client_auth:
                    try:
                        rp_short = self._client_auth.get_futures_position(self.symbol, prefer_side="SHORT")
                        has_short = bool(rp_short and rp_short.get("positionAmt") is not None and abs(float(rp_short.get("positionAmt"))) > 0)
                        if has_short:
                            msg = "[OPEN-SKIP] 检测到实盘持有空仓，跳过开空"
                            print(msg)
                            self._log(msg)
                        else:
                            self._open_position("SHORT", price)
                    except Exception:
                        self._open_position("SHORT", price)
                else:
                    self._open_position("SHORT", price)
            elif cross.death_cross and self.enable_signal_debug_log:
                print(f"[OPEN-CHECK] SHORT miss: price<{ema_curr:.2f}={price<ema_curr} ema<{ma_curr:.2f}={ema_curr<ma_curr} rising={ema_rising} slope_on={self.use_slope}")
        else:
            # 平仓逻辑
            # 变更说明：
            # 1) 将平仓条件简化为仅依赖交叉信号：
            #    - LONG 仓位：仅在出现“死叉”时平仓；不再因为价格 < EMA 提前平仓。
            #    - SHORT 仓位：仅在出现“金叉”时平仓；不再因为价格 > EMA 提前平仓。
            # 2) 支持“同一根 K 线事件中平仓后立即反向开仓”：
            #    - 若死叉导致平多仓，则在同次事件立即开空仓；
            #    - 若金叉导致平空仓，则在同次事件立即开多仓；
            #    - 该反向开仓不再额外检查价格相对均线或斜率条件，严格按交叉信号执行。
            #    - 说明：use_closed_only=true 时，交叉仅在收盘触发；false 时，未收盘也可能触发，频次更高。
            if self.position.side == "LONG":
                if cross.death_cross:
                    # 死叉：收盘时严格检查实盘是否持有多仓
                    if bool(k.get("is_final", False)) and (not self.test_mode) and self._client_auth:
                        try:
                            rp_long = self._client_auth.get_futures_position(self.symbol, prefer_side="LONG")
                            has_long = bool(rp_long and rp_long.get("positionAmt") is not None and abs(float(rp_long.get("positionAmt"))) > 0)
                            if not has_long:
                                msg = "[CLOSE-SKIP] 未持有多仓，跳过平多交易"
                                print(msg)
                                self._log(msg)
                            else:
                                # 用实盘仓位同步本地后再平仓
                                try:
                                    self.position = Position(side="LONG", entry_price=(float(rp_long.get("entryPrice")) if rp_long.get("entryPrice") is not None else self.position.entry_price), qty=abs(float(rp_long.get("positionAmt"))), open_fee=float(self.position.open_fee or 0.0))
                                except Exception:
                                    pass
                                closed = self._close_position(price)
                                if closed:
                                    # 平多后，若已存在空仓则跳过开空
                                    rp_short2 = self._client_auth.get_futures_position(self.symbol, prefer_side="SHORT")
                                    has_short2 = bool(rp_short2 and rp_short2.get("positionAmt") is not None and abs(float(rp_short2.get("positionAmt"))) > 0)
                                    if has_short2:
                                        msg2 = "[OPEN-SKIP] 检测到实盘持有空仓，跳过开空"
                                        print(msg2)
                                        self._log(msg2)
                                    else:
                                        self._open_position("SHORT", price)
                        except Exception:
                            # 出现异常则按本地逻辑执行
                            closed = self._close_position(price)
                            if closed:
                                self._open_position("SHORT", price)
                    else:
                        closed = self._close_position(price)
                        if closed:
                            self._open_position("SHORT", price)
            elif self.position.side == "SHORT":
                if cross.golden_cross:
                    # 金叉：收盘时严格检查实盘是否持有空仓
                    if bool(k.get("is_final", False)) and (not self.test_mode) and self._client_auth:
                        try:
                            rp_short = self._client_auth.get_futures_position(self.symbol, prefer_side="SHORT")
                            has_short = bool(rp_short and rp_short.get("positionAmt") is not None and abs(float(rp_short.get("positionAmt"))) > 0)
                            if not has_short:
                                msg = "[CLOSE-SKIP] 未持有空仓，跳过平空交易"
                                print(msg)
                                self._log(msg)
                            else:
                                # 用实盘仓位同步本地后再平仓
                                try:
                                    self.position = Position(side="SHORT", entry_price=(float(rp_short.get("entryPrice")) if rp_short.get("entryPrice") is not None else self.position.entry_price), qty=abs(float(rp_short.get("positionAmt"))), open_fee=float(self.position.open_fee or 0.0))
                                except Exception:
                                    pass
                                closed = self._close_position(price)
                                if closed:
                                    # 平空后，若已存在多仓则跳过开多
                                    rp_long2 = self._client_auth.get_futures_position(self.symbol, prefer_side="LONG")
                                    has_long2 = bool(rp_long2 and rp_long2.get("positionAmt") is not None and abs(float(rp_long2.get("positionAmt"))) > 0)
                                    if has_long2:
                                        msg2 = "[OPEN-SKIP] 检测到实盘持有多仓，跳过开多"
                                        print(msg2)
                                        self._log(msg2)
                                    else:
                                        self._open_position("LONG", price)
                        except Exception:
                            # 出现异常则按本地逻辑执行
                            closed = self._close_position(price)
                            if closed:
                                self._open_position("LONG", price)
                    else:
                        closed = self._close_position(price)
                        if closed:
                            self._open_position("LONG", price)

        # 收盘时落库
        if bool(k.get("is_final", False)):
            self._insert_kline(k)

    # --------------------- Trading Logic ---------------------
    def _notional_and_qty(self, price: float) -> tuple[float, float]:
        # 每次开仓金额 = 保证金余额 * 开仓比例 * 杠杆
        # 说明：用户要求以“保证金余额”（totalMarginBalance）为基准，而非钱包余额。
        base_amount = self.initial_balance * self.percent
        notional = base_amount * self.leverage
        qty = notional / price
        # 步进对齐工具
        def floor_to_step(x: float, step: float) -> float:
            return (math.floor(x / step)) * step
        def ceil_to_step(x: float, step: float) -> float:
            return (math.ceil(x / step)) * step
        # 1) 先按步进向下取整，避免 LOT_SIZE 步进拒单
        try:
            step = float(self._step_size or 0.001)
            if step > 0:
                qty = floor_to_step(qty, step)
        except Exception:
            pass
        # 2) 满足最小数量要求（LOT_SIZE/MARKET_LOT_SIZE）
        try:
            if isinstance(self._min_qty, (int, float)) and (self._min_qty or 0) > 0:
                step = float(self._step_size or 0.001)
                if qty < float(self._min_qty):
                    qty = ceil_to_step(float(self._min_qty), step)
        except Exception:
            pass
        # 3) 满足最小名义（MIN_NOTIONAL）：名义=价格×数量
        try:
            if isinstance(self._min_notional, (int, float)) and (self._min_notional or 0) > 0:
                step = float(self._step_size or 0.001)
                if (price * qty) < float(self._min_notional):
                    need_qty = float(self._min_notional) / price
                    qty = ceil_to_step(need_qty, step)
        except Exception:
            pass
        return notional, qty

    def _open_position(self, side: str, price: float):
        notional, qty = self._notional_and_qty(price)
        exec_price = price
        exec_qty = qty
        # 实盘：发送市价单
        if (not self.test_mode) and self._client_auth:
            try:
                order_side = "BUY" if side == "LONG" else "SELL"
                # 双向持仓传 LONG/SHORT；单向持仓不传 positionSide
                pos_side = ("LONG" if (self._dual_side and side == "LONG") else ("SHORT" if (self._dual_side and side == "SHORT") else None))
                msg_try = f"[ORDER-OPEN] try {order_side} {self.symbol} qty={qty:.6f} pos_side={pos_side or '-'} dual={self._dual_side}"
                print(msg_try)
                try:
                    self._log(msg_try)
                except Exception:
                    pass
                res = self._client_auth.create_futures_market_order(
                    self.symbol,
                    order_side,
                    quantity=round(qty, 6),
                    reduce_only=False,
                    position_side=pos_side,
                    new_order_resp_type="RESULT",
                )
                order_success = False
                if isinstance(res, dict):
                    if res.get("error"):
                        msg_err = f"[ORDER-OPEN] error: {res}"
                        print(msg_err)
                        try:
                            self._log(msg_err)
                        except Exception:
                            pass
                    else:
                        avg_price = res.get("avgPrice")
                        cum_qty = res.get("cumQty") or res.get("executedQty")
                        status = res.get("status")
                        if avg_price is not None:
                            exec_price = float(avg_price)
                        if cum_qty is not None:
                            exec_qty = float(cum_qty)
                        # 成功条件：有成交数量或状态为 FILLED
                        if (cum_qty is not None and float(cum_qty) > 0) or str(status).upper() == "FILLED":
                            order_success = True
                        msg_resp = f"[ORDER-OPEN] resp status={status} avgPrice={avg_price} executedQty={cum_qty}"
                        print(msg_resp)
                        try:
                            self._log(msg_resp)
                        except Exception:
                            pass
                else:
                    msg_no = "[ORDER-OPEN] failed: no response"
                    print(msg_no)
                    try:
                        self._log(msg_no)
                    except Exception:
                        pass
                # 若实盘下单失败，则不更新本地持仓与余额
                if not order_success:
                    msg_skip = "[OPEN] skipped local position update due to order failure"
                    print(msg_skip)
                    try:
                        self._log(msg_skip)
                    except Exception:
                        pass
                    return
            except Exception as e:
                try:
                    msg_exc = f"[ORDER-OPEN] exception during placing order: {e}"
                    print(msg_exc)
                    try:
                        self._log(msg_exc)
                    except Exception:
                        pass
                except Exception:
                    msg_exc2 = "[ORDER-OPEN] exception during placing order"
                    print(msg_exc2)
                    try:
                        self._log(msg_exc2)
                    except Exception:
                        pass
                # 异常同样跳过本地更新
                return
        fee = (exec_price * exec_qty * self.leverage) * self.fee_rate / self.leverage  # 近似开仓手续费
        self.balance -= fee
        self._insert_trade(side, exec_price, exec_qty, fee, pnl=-fee)
        self._insert_wallet()
        self.position = Position(side=side, entry_price=exec_price, qty=exec_qty, open_fee=fee)
        # 记录未平仓持仓，保证重启后可恢复
        self._clear_position()
        self._save_position()
        print(f"[OPEN] {side} price={exec_price:.2f} qty={exec_qty:.6f} fee={fee:.4f} bal={self.balance:.2f}")
        try:
            self._log(f"[OPEN] {side} price={exec_price:.2f} qty={exec_qty:.6f} fee={fee:.4f} bal={self.balance:.2f}")
        except Exception:
            pass

    def _close_position(self, price: float) -> bool:
        if self.position.side is None or self.position.entry_price is None or self.position.qty is None:
            return False
        side = self.position.side
        entry = float(self.position.entry_price)
        qty = float(self.position.qty)
        open_fee = float(self.position.open_fee or 0.0)

        exec_price = price
        exec_qty = qty
        # 实盘：发送减仓市价单
        if (not self.test_mode) and self._client_auth:
            try:
                order_side = "SELL" if side == "LONG" else "BUY"
                pos_side = ("LONG" if (self._dual_side and side == "LONG") else ("SHORT" if (self._dual_side and side == "SHORT") else None))
                msg_try = f"[ORDER-CLOSE] try {order_side} {self.symbol} qty={qty:.6f} pos_side={pos_side or '-'} dual={self._dual_side}"
                print(msg_try)
                try:
                    self._log(msg_try)
                except Exception:
                    pass
                res = self._client_auth.create_futures_market_order(
                    self.symbol,
                    order_side,
                    quantity=round(qty, 6),
                    reduce_only=True,
                    position_side=pos_side,
                    new_order_resp_type="RESULT",
                )
                order_success = False
                if isinstance(res, dict):
                    if res.get("error"):
                        msg_err = f"[ORDER-CLOSE] error: {res}"
                        print(msg_err)
                        try:
                            self._log(msg_err)
                        except Exception:
                            pass
                    else:
                        avg_price = res.get("avgPrice")
                        cum_qty = res.get("cumQty") or res.get("executedQty")
                        status = res.get("status")
                        if avg_price is not None:
                            exec_price = float(avg_price)
                        if cum_qty is not None:
                            exec_qty = float(cum_qty)
                        if (cum_qty is not None and float(cum_qty) > 0) or str(status).upper() == "FILLED":
                            order_success = True
                        msg_resp = f"[ORDER-CLOSE] resp status={status} avgPrice={avg_price} executedQty={cum_qty}"
                        print(msg_resp)
                        try:
                            self._log(msg_resp)
                        except Exception:
                            pass
                else:
                    msg_no = "[ORDER-CLOSE] failed: no response"
                    print(msg_no)
                    try:
                        self._log(msg_no)
                    except Exception:
                        pass
                if not order_success:
                    msg_skip = "[CLOSE] skipped local position update due to order failure"
                    print(msg_skip)
                    try:
                        self._log(msg_skip)
                    except Exception:
                        pass
                    return False
            except Exception as e:
                try:
                    msg_exc = f"[ORDER-CLOSE] exception during placing order: {e}"
                    print(msg_exc)
                    try:
                        self._log(msg_exc)
                    except Exception:
                        pass
                except Exception:
                    msg_exc2 = "[ORDER-CLOSE] exception during placing order"
                    print(msg_exc2)
                    try:
                        self._log(msg_exc2)
                    except Exception:
                        pass
                return False

        pnl = 0.0
        if side == "LONG":
            pnl = (exec_price - entry) * exec_qty
        elif side == "SHORT":
            pnl = (entry - exec_price) * exec_qty

        notional = exec_price * exec_qty
        fee = notional * self.fee_rate
        # 账户余额只变动价格差与当次手续费；开仓手续费已在开仓时扣除
        self.balance += pnl
        self.balance -= fee
        # 记录净盈亏：价格差 - 平仓手续费 - 开仓手续费
        net_pnl = pnl - fee - open_fee
        self._insert_trade("CLOSE", exec_price, exec_qty, fee, net_pnl)
        self._insert_wallet()
        print(f"[CLOSE] {side} @ {exec_price:.2f} gross_pnl={pnl:.4f} fee_close={fee:.4f} fee_open={open_fee:.4f} net_pnl={net_pnl:.4f} bal={self.balance:.2f}")
        try:
            self._log(f"[CLOSE] {side} @ {exec_price:.2f} gross_pnl={pnl:.4f} fee_close={fee:.4f} fee_open={open_fee:.4f} net_pnl={net_pnl:.4f} bal={self.balance:.2f}")
        except Exception:
            pass
        self.position = Position(side=None, entry_price=None, qty=None, open_fee=None)
        # 清除未平仓持仓记录
        self._clear_position()
        return True

    def _log(self, msg: str):
        try:
            self._logger.info(msg)
        except Exception:
            pass

    # --------------------- Status ---------------------
    def status(self) -> dict:
        # 默认使用本地持仓；若有 API 密钥，优先使用交易所真实持仓
        real_pos: dict | None = None
        try:
            if (not self.test_mode) and self._client_auth:
                real_pos = self._client_auth.get_futures_position(self.symbol)
        except Exception:
            real_pos = None

        # 组装持仓信息
        side = self.position.side
        entry_price = self.position.entry_price
        qty_coin = self.position.qty
        pos_margin = None
        unrealized_net: float | None = None
        if isinstance(real_pos, dict) and real_pos.get("positionAmt") is not None:
            amt = float(real_pos.get("positionAmt"))
            qty_coin = abs(amt)
            entry_price = float(real_pos.get("entryPrice") or (entry_price or 0.0)) or None
            # 推断方向（单向模式下 BOTH 用数量正负判断）
            if amt > 0:
                side = "LONG"
            elif amt < 0:
                side = "SHORT"
            else:
                side = None
            m = real_pos.get("margin")
            if isinstance(m, (int, float)):
                pos_margin = float(m)
            # 计算净未实现盈亏（原始未实现盈亏 - 预估平仓手续费）
            try:
                unp = real_pos.get("unrealizedProfit")
                if (unp is not None) and self.current_price and qty_coin:
                    unp_val = float(unp)
                    notional = float(self.current_price) * float(qty_coin)
                    close_fee_est = notional * float(self.fee_rate)
                    unrealized_net = float(unp_val) - float(close_fee_est)
            except Exception:
                pass
        else:
            # 模拟或无 API 情况：用本地持仓与当前价格估算净未实现盈亏
            try:
                if side and entry_price and qty_coin and self.current_price:
                    ep = float(entry_price)
                    cp = float(self.current_price)
                    q = float(qty_coin)
                    open_pnl = (cp - ep) * q if side == "LONG" else (ep - cp) * q
                    notional = cp * q
                    close_fee_est = notional * float(self.fee_rate)
                    unrealized_net = float(open_pnl) - float(close_fee_est)
            except Exception:
                pass
        # 当前持仓名义（用于“数量: USDT”显示）
        # 名义价值（数量(币)×价格），以及“实时价值=名义价值+净未实现盈亏”
        pos_val_nominal = 0.0
        if qty_coin and self.current_price:
            pos_val_nominal = float(qty_coin) * float(self.current_price)
        pos_val_display = pos_val_nominal + (float(unrealized_net) if isinstance(unrealized_net, (int, float)) else 0.0)

        latest_kline = self.latest_kline
        out = {
            "symbol": self.symbol,
            "interval": self.interval,
            "balance": round(self.balance, 4),
            "initial_balance": self.initial_balance,
            "leverage": self.leverage,
            "fee_rate": self.fee_rate,
            "percent": self.percent,
            "current_price": self.current_price,
            "ema": self.ema_list[-1] if self.ema_list else None,
            "ma": self.ma_list[-1] if self.ma_list else None,
            # 动态提供均线周期，供前端展示（避免硬编码 5/15）
            "ema_period": self.ema_period,
            "ma_period": self.ma_period,
            "position": {
                "side": side,
                "entry_price": entry_price,
                "qty": qty_coin,
                # 显示值：数量(USDT)+净未实现盈亏
                "value": pos_val_display,
                # 供前端显示 Binance UI 的“数量(USDT)”与“保证金(USDT)”
                "api_qty_usdt": pos_val_nominal if (qty_coin and self.current_price) else None,
                "margin_usdt": pos_margin,
                # 供前端直接展示“未实现盈亏”
                "unrealized_pnl": unrealized_net,
            },
            "latest_kline": latest_kline,
        }
        return out

    def recent_trades(self, limit: int = 5) -> list[dict]:
        cur = self._db.cursor()
        cur.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    def recent_klines(self, limit: int = 5) -> list[dict]:
        cur = self._db.cursor()
        cur.execute("SELECT * FROM klines ORDER BY id DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]