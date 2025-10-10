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

from indicators import ema, sma, crossover, is_rising


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
        self.initial_balance = float(tcfg.get("initial_balance", 1000.0))
        self.balance = self.initial_balance
        self.percent = float(tcfg.get("percent", 0.5))
        self.leverage = int(tcfg.get("leverage", 10))
        self.fee_rate = float(tcfg.get("fee_rate", 0.0005))
        self.test_mode = bool(tcfg.get("test_mode", True))

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
        self._db.commit()

    def _restore_balance_from_wallet(self):
        """在程序启动时恢复余额：
        - 若 wallet 表存在记录，则将引擎余额设为最近一条记录的余额；
        - 若不存在记录，则将当前余额（initial_balance）写入 wallet，作为基准起点。
        """
        try:
            cur = self._db.cursor()
            cur.execute("SELECT balance FROM wallet ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row and row[0] is not None:
                self.balance = float(row[0])
            else:
                # 数据库首次初始化：记录初始余额，便于后续累计统计
                self._insert_wallet()
        except Exception:
            # 出现异常时不影响程序继续运行；保留当前内存余额
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

        # 轻量日志，便于观察实时更新
        try:
            print(f"[TICK] price={price:.2f} ema={ema_curr:.2f} ma={ma_curr:.2f} cross(g={cross.golden_cross}, d={cross.death_cross})")
        except Exception:
            pass

        if self.position.side is None:
            # 开仓逻辑（记录每个条件，便于对比 Binance 图表）
            slope_ok_long = (ema_rising if self.use_slope else True)
            slope_ok_short = ((not ema_rising) if self.use_slope else True)
            cond_long = cross.golden_cross and price > ema_curr and ema_curr > ma_curr and slope_ok_long
            cond_short = cross.death_cross and price < ema_curr and ema_curr < ma_curr and slope_ok_short
            if cond_long:
                print(f"[OPEN-CHECK] LONG ok: price>{ema_curr:.2f} ema>{ma_curr:.2f} rising={ema_rising} slope_on={self.use_slope}")
                self._open_position("LONG", price)
            elif cross.golden_cross:
                print(f"[OPEN-CHECK] LONG miss: price>{ema_curr:.2f}={price>ema_curr} ema>{ma_curr:.2f}={ema_curr>ma_curr} rising={ema_rising} slope_on={self.use_slope}")
            if cond_short:
                print(f"[OPEN-CHECK] SHORT ok: price<{ema_curr:.2f} ema<{ma_curr:.2f} rising={ema_rising} slope_on={self.use_slope}")
                self._open_position("SHORT", price)
            elif cross.death_cross:
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
                    # 死叉：平多，并在同事件反向开空
                    self._close_position(price)
                    self._open_position("SHORT", price)
            elif self.position.side == "SHORT":
                if cross.golden_cross:
                    # 金叉：平空，并在同事件反向开多
                    self._close_position(price)
                    self._open_position("LONG", price)

        # 收盘时落库
        if bool(k.get("is_final", False)):
            self._insert_kline(k)

    # --------------------- Trading Logic ---------------------
    def _notional_and_qty(self, price: float) -> tuple[float, float]:
        # 每次开仓金额 = 当前余额 * percent
        base_amount = self.balance * self.percent
        notional = base_amount * self.leverage
        qty = notional / price
        return notional, qty

    def _open_position(self, side: str, price: float):
        notional, qty = self._notional_and_qty(price)
        fee = notional * self.fee_rate
        self.balance -= fee
        # 开仓盈亏应体现手续费（负数）
        self._insert_trade(side, price, qty, fee, pnl=-fee)
        self._insert_wallet()
        self.position = Position(side=side, entry_price=price, qty=qty, open_fee=fee)
        print(f"[OPEN] {side} price={price:.2f} qty={qty:.6f} fee={fee:.4f} bal={self.balance:.2f}")

    def _close_position(self, price: float):
        if self.position.side is None or self.position.entry_price is None or self.position.qty is None:
            return
        side = self.position.side
        entry = float(self.position.entry_price)
        qty = float(self.position.qty)
        open_fee = float(self.position.open_fee or 0.0)

        pnl = 0.0
        if side == "LONG":
            pnl = (price - entry) * qty
        elif side == "SHORT":
            pnl = (entry - price) * qty

        notional = price * qty
        fee = notional * self.fee_rate
        # 账户余额只变动价格差与当次手续费；开仓手续费已在开仓时扣除
        self.balance += pnl
        self.balance -= fee
        # 记录净盈亏：价格差 - 平仓手续费 - 开仓手续费
        net_pnl = pnl - fee - open_fee
        self._insert_trade("CLOSE", price, qty, fee, net_pnl)
        self._insert_wallet()
        print(f"[CLOSE] {side} @ {price:.2f} gross_pnl={pnl:.4f} fee_close={fee:.4f} fee_open={open_fee:.4f} net_pnl={net_pnl:.4f} bal={self.balance:.2f}")
        self.position = Position(side=None, entry_price=None, qty=None, open_fee=None)

    # --------------------- Status ---------------------
    def status(self) -> dict:
        pos_val = 0.0
        if self.position.side and self.current_price and self.position.qty:
            if self.position.side == "LONG":
                pos_val = self.position.qty * self.current_price
            else:
                pos_val = self.position.qty * self.current_price
        latest_kline = self.latest_kline
        return {
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
                "side": self.position.side,
                "entry_price": self.position.entry_price,
                "qty": self.position.qty,
                "value": pos_val,
            },
            "latest_kline": latest_kline,
        }

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