"""
Binance REST 客户端

提供：
- 公共行情接口（K线、最新价格）
- 受限/签名接口（仅在提供 api_key/secret_key 时可用），例如合约账户余额
"""
from __future__ import annotations

import time
import typing as t
import requests
import hmac
import hashlib
from urllib.parse import urlencode


class BinanceClient:
    """简单的 Binance 合约 REST 客户端：行情与账户。

    - base_url: 合约 REST 基础地址，如 https://fapi.binance.com
    - api_key/secret_key: 提供后可访问签名接口（如账户余额）
    """

    def __init__(self, base_url: str = "https://fapi.binance.com", api_key: str | None = None, secret_key: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key

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

    # ----------------- Auth helpers & signed endpoints -----------------
    def _signed_headers(self) -> dict:
        if not self.api_key:
            raise RuntimeError("API key not set for signed request")
        return {"X-MBX-APIKEY": self.api_key}

    def _sign_params(self, params: dict) -> dict:
        if not self.secret_key:
            raise RuntimeError("Secret key not set for signed request")
        query = urlencode(params)
        sig = hmac.new(self.secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    def get_futures_balance(self, asset: str = "USDT", recv_window_ms: int = 5000) -> float | None:
        """查询合约账户余额（USDⓈ-M）。

        返回指定资产的 wallet balance；若接口或资产不存在，返回 None。
        参考：GET /fapi/v2/balance （签名）
        """
        if not (self.api_key and self.secret_key):
            return None
        url = f"{self.base_url}/fapi/v2/balance"
        params = {
            "timestamp": int(time.time() * 1000),
            "recvWindow": recv_window_ms,
        }
        params = self._sign_params(params)
        try:
            resp = requests.get(url, params=params, headers=self._signed_headers(), timeout=8)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                for item in data:
                    try:
                        if str(item.get("asset")).upper() == asset.upper():
                            # futures balance 字段为 balance，可选 availableBalance
                            bal = item.get("balance")
                            return float(bal) if bal is not None else None
                    except Exception:
                        continue
            return None
        except Exception:
            # 保持稳健：失败时返回 None，调用方自行回退到配置值
            return None

    def get_futures_account_totals(self, recv_window_ms: int = 5000) -> dict | None:
        """查询合约账户总览，提取总钱包余额与总保证金余额。

        返回形如 {"totalWalletBalance": float, "totalMarginBalance": float}
        参考：GET /fapi/v2/account （签名）
        """
        if not (self.api_key and self.secret_key):
            return None
        url = f"{self.base_url}/fapi/v2/account"
        params = {
            "timestamp": int(time.time() * 1000),
            "recvWindow": recv_window_ms,
        }
        params = self._sign_params(params)
        try:
            resp = requests.get(url, params=params, headers=self._signed_headers(), timeout=8)
            resp.raise_for_status()
            data = resp.json()
            twb = data.get("totalWalletBalance")
            tmb = data.get("totalMarginBalance")
            out = {}
            if twb is not None:
                out["totalWalletBalance"] = float(twb)
            if tmb is not None:
                out["totalMarginBalance"] = float(tmb)
            return out if out else None
        except Exception:
            return None

    # ----------------- Futures trading helpers -----------------
    def get_symbol_step_size(self, symbol: str) -> float | None:
        """查询交易对的数量步进（LOT_SIZE.stepSize），用于数量取整。

        参考：GET /fapi/v1/exchangeInfo
        """
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            params = {"symbol": symbol.upper()}
            resp = requests.get(url, params=params, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            symbols = data.get("symbols") or []
            for s in symbols:
                if str(s.get("symbol")).upper() == symbol.upper():
                    filters = s.get("filters") or []
                    for f in filters:
                        if f.get("filterType") == "LOT_SIZE":
                            step = f.get("stepSize")
                            return float(step) if step is not None else None
            return None
        except Exception:
            return None

    def get_futures_position(self, symbol: str, recv_window_ms: int = 5000) -> dict | None:
        """查询指定交易对的合约持仓信息（USDⓈ-M）。

        来源：GET /fapi/v2/account（签名）。该接口返回 positions 列表。
        返回字段（若存在）：
        - positionAmt: 仓位数量（多为正，空为负）
        - entryPrice: 开仓均价
        - leverage: 杠杆
        - isolated: 是否逐仓
        - isolatedWallet: 逐仓保证金（仅逐仓）
        - initialMargin: 初始保证金（全仓/逐仓均可能提供）
        - positionSide: BOTH/LONG/SHORT
        - margin: 统一保证金数值（优先 initialMargin；否则使用 isolatedWallet）
        """
        if not (self.api_key and self.secret_key):
            return None
        try:
            url = f"{self.base_url}/fapi/v2/account"
            params = {
                "timestamp": int(time.time() * 1000),
                "recvWindow": recv_window_ms,
            }
            params = self._sign_params(params)
            resp = requests.get(url, params=params, headers=self._signed_headers(), timeout=8)
            resp.raise_for_status()
            data = resp.json()
            positions = [p for p in (data.get("positions") or []) if str(p.get("symbol")).upper() == symbol.upper()]
            if not positions:
                return None
            # 在双向持仓模式下，Binance 通常返回三条记录：BOTH、LONG、SHORT。
            # 其中 BOTH 的 positionAmt 常为 0。优先选择 positionAmt 非 0 的记录；
            # 若均为 0，则回退选择第一条（通常为 BOTH）。
            def amt_of(p):
                try:
                    v = p.get("positionAmt")
                    return float(v) if v is not None else 0.0
                except Exception:
                    return 0.0
            non_zero = [p for p in positions if abs(amt_of(p)) > 0]
            chosen = (non_zero[0] if non_zero else positions[0])
            out: dict = {
                "positionAmt": float(chosen.get("positionAmt")) if chosen.get("positionAmt") is not None else None,
                "entryPrice": float(chosen.get("entryPrice")) if chosen.get("entryPrice") is not None else None,
                "leverage": int(chosen.get("leverage")) if chosen.get("leverage") is not None else None,
                "isolated": bool(chosen.get("isolated")) if chosen.get("isolated") is not None else None,
                "isolatedWallet": float(chosen.get("isolatedWallet")) if chosen.get("isolatedWallet") is not None else None,
                "initialMargin": float(chosen.get("initialMargin")) if chosen.get("initialMargin") is not None else None,
                "positionSide": chosen.get("positionSide"),
            }
            # 统一保证金字段
            margin = out.get("initialMargin")
            if margin is None:
                margin = out.get("isolatedWallet")
            if margin is not None:
                out["margin"] = float(margin)
            return out
        except Exception:
            return None

    def set_leverage(self, symbol: str, leverage: int, recv_window_ms: int = 5000) -> bool:
        """设置杠杆。POST /fapi/v1/leverage（签名）"""
        if not (self.api_key and self.secret_key):
            return False
        try:
            url = f"{self.base_url}/fapi/v1/leverage"
            params = {
                "symbol": symbol.upper(),
                "leverage": int(leverage),
                "timestamp": int(time.time() * 1000),
                "recvWindow": recv_window_ms,
            }
            params = self._sign_params(params)
            # 使用表单提交以符合 Binance 要求
            resp = requests.post(url, data=params, headers=self._signed_headers(), timeout=8)
            resp.raise_for_status()
            return True
        except Exception:
            return False

    def create_futures_market_order(
        self,
        symbol: str,
        side: str,  # "BUY" | "SELL"
        quantity: float,
        *,
        reduce_only: bool = False,
        position_side: str | None = None,  # "LONG" | "SHORT"（仅对双向持仓有效）
        new_order_resp_type: str = "RESULT",
        recv_window_ms: int = 5000,
    ) -> dict | None:
        """创建合约市价单。POST /fapi/v1/order（签名）

        返回 Binance 原始响应字典（包含 avgPrice/cumQty 等，当 newOrderRespType=RESULT）。
        """
        if not (self.api_key and self.secret_key):
            return None
        try:
            url = f"{self.base_url}/fapi/v1/order"
            params = {
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "MARKET",
                "quantity": quantity,
                "newOrderRespType": new_order_resp_type,
                "timestamp": int(time.time() * 1000),
                "recvWindow": recv_window_ms,
            }
            # 仅当需要减仓时才发送 reduceOnly，避免 -1106 错误
            if reduce_only:
                params["reduceOnly"] = "true"
            # 双向持仓（Hedge Mode）需要显式传递 positionSide
            if position_side:
                params["positionSide"] = position_side.upper()
            params = self._sign_params(params)
            resp = requests.post(url, data=params, headers=self._signed_headers(), timeout=10)
            if resp.status_code >= 400:
                try:
                    return {"error": True, "status_code": resp.status_code, "details": resp.json()}
                except Exception:
                    return {"error": True, "status_code": resp.status_code, "details": {"message": resp.text}}
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": True, "exception": str(e)}

    def get_symbol_filters(self, symbol: str) -> dict | None:
        """查询交易对过滤器：LOT_SIZE、MARKET_LOT_SIZE、MIN_NOTIONAL。

        返回：{"stepSize": float, "minQty": float, "marketMinQty": float, "minNotional": float}
        参考：GET /fapi/v1/exchangeInfo
        """
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            params = {"symbol": symbol.upper()}
            resp = requests.get(url, params=params, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            symbols = data.get("symbols") or []
            for s in symbols:
                if str(s.get("symbol")).upper() == symbol.upper():
                    out: dict = {}
                    filters = s.get("filters") or []
                    for f in filters:
                        ftype = f.get("filterType")
                        if ftype == "LOT_SIZE":
                            v = f.get("stepSize")
                            if v is not None:
                                out["stepSize"] = float(v)
                            v = f.get("minQty")
                            if v is not None:
                                out["minQty"] = float(v)
                        elif ftype == "MARKET_LOT_SIZE":
                            v = f.get("minQty")
                            if v is not None:
                                out["marketMinQty"] = float(v)
                        elif ftype == "MIN_NOTIONAL":
                            v = f.get("minNotional")
                            if v is not None:
                                out["minNotional"] = float(v)
                    return out or None
            return None
        except Exception:
            return None

    def get_position_side_dual(self, recv_window_ms: int = 5000) -> bool | None:
        """查询是否开启双向持仓（hedge mode）。GET /fapi/v1/positionSide/dual（签名）

        返回 True/False，失败返回 None。
        """
        if not (self.api_key and self.secret_key):
            return None
        try:
            url = f"{self.base_url}/fapi/v1/positionSide/dual"
            params = {
                "timestamp": int(time.time() * 1000),
                "recvWindow": recv_window_ms,
            }
            params = self._sign_params(params)
            resp = requests.get(url, params=params, headers=self._signed_headers(), timeout=8)
            resp.raise_for_status()
            data = resp.json()
            # {"dualSidePosition": true/false}
            val = data.get("dualSidePosition")
            return bool(val) if val is not None else None
        except Exception:
            return None


if __name__ == "__main__":
    # 简单自测
    client = BinanceClient()
    data = client.get_klines("BTCUSDT", "1m", limit=5)
    for d in data:
        print(d)