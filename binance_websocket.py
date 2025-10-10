"""
Binance WebSocket 客户端

订阅合约 K 线实时流（未收盘与收盘），并在断线后自动重连。
"""
from __future__ import annotations

import json
import threading
import time
import typing as t

import websocket


class BinanceWebSocket:
    """简单的 WebSocket 封装：订阅 `{symbol}@kline_{interval}` 流。

    - on_kline: 回调函数，接收字典参数，包含 kline 关键字段
    - on_open_cb/on_error_cb/on_close_cb: 连接状态回调，便于上层做降级或恢复
    - auto_reconnect: 断线自动重连（指数退避）
    """

    def __init__(
        self,
        symbol: str,
        interval: str,
        on_kline: t.Callable[[dict], None],
        on_open_cb: t.Optional[t.Callable[[], None]] = None,
        on_error_cb: t.Optional[t.Callable[[t.Any], None]] = None,
        on_close_cb: t.Optional[t.Callable[[], None]] = None,
        auto_reconnect: bool = True,
    ) -> None:
        self.symbol = symbol.upper()
        self.interval = interval
        self.on_kline = on_kline
        self.on_open_cb = on_open_cb
        self.on_error_cb = on_error_cb
        self.on_close_cb = on_close_cb
        self.auto_reconnect = auto_reconnect

        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._reconnect_delay = 2  # 秒，指数退避

    @property
    def url(self) -> str:
        # 使用 combined stream 更通用：/stream?streams=
        # 兼容多流场景，且服务端行为更稳定
        stream = f"{self.symbol.lower()}@kline_{self.interval}"
        return f"wss://fstream.binance.com/stream?streams={stream}"

    def _on_message(self, _ws, message: str):
        try:
            data = json.loads(message)
            # combined stream: { stream, data: { k: {...} } }
            kline_container = data.get("data") if "data" in data else data
            k = (kline_container or {}).get("k", {})
            if not k:
                return
            payload = {
                "event_time": data.get("E"),
                "open_time": k.get("t"),
                "close_time": k.get("T"),
                "interval": k.get("i"),
                "is_final": bool(k.get("x")),
                "open": float(k.get("o")),
                "high": float(k.get("h")),
                "low": float(k.get("l")),
                "close": float(k.get("c")),
                "volume": float(k.get("v")),
            }
            self.on_kline(payload)
        except Exception:
            # 保持健壮性，避免回调异常导致断开
            print("[WS] parse message error")

    def _on_error(self, _ws, error):
        # 简单打印错误，可进一步接入日志系统
        print("[WS] error:", error)
        try:
            if self.on_error_cb:
                self.on_error_cb(error)
        except Exception:
            pass

    def _on_close(self, _ws, _a, _b):
        print("[WS] closed")
        try:
            if self.on_close_cb:
                self.on_close_cb()
        except Exception:
            pass
        if self.auto_reconnect and not self._stop.is_set():
            time.sleep(self._reconnect_delay)
            # 指数退避，封顶 60 秒
            self._reconnect_delay = min(self._reconnect_delay * 2, 60)
            self._start()

    def _on_open(self, _ws):
        print("[WS] opened", self.url)
        # 连接成功后重置退避
        self._reconnect_delay = 2
        try:
            if self.on_open_cb:
                self.on_open_cb()
        except Exception:
            pass

    def _start(self):
        self._ws = websocket.WebSocketApp(
            self.url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        # 调整心跳参数，降低超时概率
        self._ws.run_forever(ping_interval=10, ping_timeout=8)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._start, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)


if __name__ == "__main__":
    def demo_print(k):
        print("kline:", k)

    ws = BinanceWebSocket("BTCUSDT", "1m", on_kline=demo_print)
    ws.start()
    time.sleep(10)
    ws.stop()