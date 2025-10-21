"""技术指标计算模块

提供简单且可扩展的 EMA、SMA 计算与交叉判断工具函数。
"""
from __future__ import annotations

from dataclasses import dataclass
import typing as t


def sma(values: list[float], period: int) -> list[float]:
    """简单移动平均 (SMA)。返回与输入等长的列表，前期不足的用 None 填充。"""
    out: list[float] = []
    window: list[float] = []
    for v in values:
        window.append(v)
        if len(window) > period:
            window.pop(0)
        if len(window) < period:
            out.append(None)
        else:
            out.append(sum(window) / period)
    return out


def ema(values: list[float], period: int) -> list[float]:
    """指数移动平均 (EMA)。返回与输入等长的列表，前期不足的用 None 填充。

    与主流交易所（含 Binance）一致：
    - 使用前 `period` 根的 SMA 作为 EMA 初始值；
    - 后续 EMA_t = price_t * k + EMA_{t-1} * (1-k)，其中 k = 2/(period+1)。
    """
    if period <= 0:
        raise ValueError("period must be > 0")
    out: list[float] = []
    k = 2 / (period + 1)
    ema_prev: float | None = None
    for i, v in enumerate(values):
        if i < period - 1:
            # 前期不足，填 None
            out.append(None)
            continue
        if i == period - 1:
            # 以首个完整窗口的 SMA 作为初始 EMA
            window = values[:period]
            ema_prev = sum(window) / period
            out.append(ema_prev)
            continue
        # 正常迭代
        ema_curr = v * k + (ema_prev if ema_prev is not None else v) * (1 - k)
        ema_prev = ema_curr
        out.append(ema_curr)
    return out


@dataclass
class CrossSignal:
    golden_cross: bool
    death_cross: bool


def crossover(ema_list: list[float], ma_list: list[float], eps: float = 1e-8) -> CrossSignal:
    """判断金叉/死叉。仅在最近两个点均有效时判断。

    说明：
    - 引入 `eps` 浮点容差，避免由于浮点舍入导致的“刚好相等不触发”的情况；
    - 将当前根的严格不等改为“包含等于”（>=/<=），以支持边界处的交叉识别。

    - 金叉：前一根 EMA <= MA（含容差）且当前 EMA >= MA（含容差）
    - 死叉：前一根 EMA >= MA（含容差）且当前 EMA <= MA（含容差）
    """
    if not ema_list or not ma_list:
        return CrossSignal(False, False)
    if len(ema_list) < 2 or len(ma_list) < 2:
        return CrossSignal(False, False)

    prev_ema, curr_ema = ema_list[-2], ema_list[-1]
    prev_ma, curr_ma = ma_list[-2], ma_list[-1]
    if prev_ema is None or curr_ema is None or prev_ma is None or curr_ma is None:
        return CrossSignal(False, False)

    golden = (prev_ema <= (prev_ma + eps)) and (curr_ema >= (curr_ma - eps))
    death = (prev_ema >= (prev_ma - eps)) and (curr_ema <= (curr_ma + eps))
    return CrossSignal(golden_cross=golden, death_cross=death)


def is_rising(series: t.Sequence[float | None], lookback: int = 3) -> bool:
    """判断指标是否呈上升趋势：最近 lookback 根单调非降。

    兼容 list/deque 等序列类型，避免切片带来的拷贝与不兼容。
    """
    try:
        n = len(series)
    except Exception:
        return False
    if n < lookback:
        return False
    vals: list[float] = []
    for i in range(n - lookback, n):
        v = series[i]
        if v is None:
            return False
        vals.append(float(v))
    return all(vals[i] <= vals[i + 1] for i in range(len(vals) - 1))