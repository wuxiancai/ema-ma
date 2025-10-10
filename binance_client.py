"""
Binance REST 客户端

仅使用公共行情接口获取合约 K 线数据，避免对密钥的依赖。
"""
from __future__ import annotations

import time
import typing as t
import requests


class BinanceClient:
    """简单的 Binance 合约 REST 客户端，用于拉取历史 K 线。

    - base_url: 合约 REST 基础地址，如 https://fapi.binance.com
    """

    def __init__(self, base_url: str = "https://fapi.binance.com") -> None:
        self.base_url = base_url.rstrip("/")

    def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 100,
        end_time_ms: int | None = None,
    ) -> list[dict]:
        """获取指定交易对与周期的 K 线数据。

        返回列表，元素结构与 Binance 原始返回一致（做了字段名映射方便阅读）。
        """
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }
        if end_time_ms:
            params["endTime"] = end_time_ms

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()

        klines: list[dict] = []
        for k in raw:
            # Binance kline fields
            # [
            #   0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume,
            #   6 closeTime, 7 quoteAssetVolume, 8 numberOfTrades,
            #   9 takerBuyBaseAssetVolume, 10 takerBuyQuoteAssetVolume, 11 ignore
            # ]
            klines.append(
                {
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "close_time": int(k[6]),
                }
            )

        return klines

    def get_price(self, symbol: str) -> float:
        """获取最新价格 (mark price/ticker price)。此处使用 ticker price。"""
        url = f"{self.base_url}/fapi/v1/ticker/price"
        params = {"symbol": symbol.upper()}
        resp = requests.get(url, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        return float(data["price"]) 


if __name__ == "__main__":
    # 简单自测
    client = BinanceClient()
    data = client.get_klines("BTCUSDT", "1m", limit=5)
    for d in data:
        print(d)