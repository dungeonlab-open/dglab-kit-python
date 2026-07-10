from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from dglab_kit_python.errors import create_named_error
from dglab_kit_python.events import EventEmitter
from dglab_kit_python.types import DglabSocketCloseEvent, DglabSocketState

DEFAULT_CONNECT_TIMEOUT = 8_000

SocketSender = Callable[[str | bytes | bytearray | memoryview], Any]


class DglabSocketBase(EventEmitter):
    def __init__(
        self,
        *,
        url: str | None = None,
        protocols: str | list[str] | None = None,
        connectTimeout: int | None = None,
        connect_timeout: int | None = None,
        responseTimeout: int | None = None,
        response_timeout: int | None = None,
        **_: Any,
    ) -> None:
        super().__init__()
        self.url = url
        self.protocols = protocols
        self.connect_timeout = connect_timeout if connect_timeout is not None else connectTimeout
        self.response_timeout = response_timeout if response_timeout is not None else responseTimeout
        self.socket: Any = None
        self._manual_sender: SocketSender | None = None
        self._state = DglabSocketState.IDLE
        self._connect_future: asyncio.Future[dict[str, Any]] | None = None
        self._reader_task: asyncio.Task[None] | None = None

    @property
    def state(self) -> DglabSocketState:
        return self._state

    async def connect(self) -> dict[str, Any]:
        active = self._get_connected_result()
        if active is not None:
            return active

        if self._connect_future is not None:
            return await self._connect_future

        loop = asyncio.get_running_loop()
        self._connect_future = loop.create_future()
        self._set_state(DglabSocketState.CONNECTING)

        try:
            await self._open_socket()
            return await asyncio.wait_for(
                self._connect_future,
                timeout=self._connect_timeout_seconds(),
            )
        except TimeoutError as error:
            self._reject_active_connect(
                create_named_error("socket-connect-timeout", "连接超时")
            )
            raise create_named_error("socket-connect-timeout", "连接超时") from error
        except Exception as error:
            if self._connect_future is not None and not self._connect_future.done():
                self._reject_active_connect(
                    error
                    if isinstance(error, Exception)
                    else create_named_error("socket-connect", str(error))
                )
            raise

    def set_sender(self, sender: SocketSender) -> DglabSocketBase:
        self._manual_sender = sender
        return self

    setSender = set_sender

    def send_raw(self, data: str | bytes | bytearray | memoryview) -> None:
        if not isinstance(data, str | bytes | bytearray | memoryview):
            raise create_named_error("socket-send-error", "原始消息必须是字符串或二进制")

        if self._manual_sender is not None:
            self._manual_sender(data)
            return

        if self.socket is None:
            raise create_named_error("socket-send", "WebSocket 尚未连接")

        result = self.socket.send(data)
        if isinstance(result, Awaitable):
            asyncio.create_task(result)

    sendRaw = send_raw

    def send_frame(self, frame: Any) -> None:
        self.send_raw(json.dumps(frame, ensure_ascii=False, separators=(",", ":")))

    sendFrame = send_frame

    async def disconnect(self, code: int | None = None, reason: str | None = None) -> None:
        self._reject_active_connect(
            create_named_error("socket-connect-cancelled", "连接已取消")
        )
        self._set_state(DglabSocketState.DISCONNECTED)

        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None

        if self.socket is not None:
            close = getattr(self.socket, "close", None)
            if close is not None:
                result = close(code=code, reason=reason) if code else close()
                if isinstance(result, Awaitable):
                    await result
            self.socket = None

    async def destroy(self, code: int | None = None, reason: str | None = None) -> None:
        await self.disconnect(code, reason)
        self.remove_all_listeners()

    def handle_open(self, event: Any = None) -> None:
        self.emit("open", event)

    handleOpen = handle_open
    handleSocketOpen = handle_open

    def handle_message(self, data: Any) -> None:
        text = self._decode_message(data)
        self.emit("message", text, data)
        self._handle_protocol_message(text, data)

    handleMessage = handle_message
    handleSocketMessage = handle_message

    def handle_close(self, event_or_code: Any = None, reason: Any = None) -> None:
        close_event = self._normalize_close_event(event_or_code, reason)
        if self._connect_future is not None and not self._connect_future.done():
            self._reject_active_connect(
                create_named_error("socket-connect-closed", "连接完成前已关闭")
            )

        self._set_state(DglabSocketState.DISCONNECTED)
        self._on_socket_closed(close_event)
        self.emit("close", close_event)

    handleClose = handle_close
    handleSocketClose = handle_close

    def handle_error(self, error: Any) -> None:
        self.emit("error", error)

    handleError = handle_error
    handleSocketError = handle_error

    async def _open_socket(self) -> Any:
        if self.socket is not None:
            return self.socket

        if not self.url:
            self.socket = _ManualSocket(self)
            return self.socket

        try:
            from websockets.asyncio.client import connect
        except ImportError:
            try:
                from websockets import connect  # type: ignore[no-redef]
            except ImportError as error:
                raise create_named_error(
                    "socket-websocket",
                    "使用 url 连接需要安装 websockets：pip install websockets",
                ) from error

        self.socket = await connect(self.url, subprotocols=self._protocol_list())
        self.handle_open()
        self._reader_task = asyncio.create_task(self._read_socket())
        return self.socket

    async def _read_socket(self) -> None:
        try:
            async for message in self.socket:
                self.handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.handle_error(error)
        finally:
            self.handle_close()

    def _protocol_list(self) -> list[str] | None:
        if self.protocols is None:
            return None
        if isinstance(self.protocols, str):
            return [self.protocols]
        return self.protocols

    def _set_state(self, state: DglabSocketState) -> None:
        if state == self._state:
            return
        previous = self._state
        self._state = state
        self.emit("state", state, previous)

    def _resolve_active_connect(self, result: dict[str, Any]) -> None:
        future = self._connect_future
        self._connect_future = None
        if future is not None and not future.done():
            future.set_result(result)

    def _reject_active_connect(self, error: Exception) -> None:
        future = self._connect_future
        self._connect_future = None
        if future is not None and not future.done():
            future.set_exception(error)

    def _connect_timeout_seconds(self) -> float:
        return (self.connect_timeout or DEFAULT_CONNECT_TIMEOUT) / 1000

    @staticmethod
    def _decode_message(data: Any) -> str:
        if isinstance(data, str):
            return data
        if isinstance(data, bytes | bytearray | memoryview):
            return bytes(data).decode()
        if data is None:
            return "None"
        return str(data)

    @staticmethod
    def _normalize_close_event(event_or_code: Any, reason: Any) -> DglabSocketCloseEvent:
        if isinstance(event_or_code, dict):
            return {
                "code": event_or_code.get("code", 0)
                if isinstance(event_or_code.get("code"), int)
                else 0,
                "reason": event_or_code.get("reason", "")
                if isinstance(event_or_code.get("reason"), str)
                else "",
                "wasClean": event_or_code.get("wasClean", False)
                if isinstance(event_or_code.get("wasClean"), bool)
                else False,
                "event": event_or_code,
            }

        return {
            "code": event_or_code if isinstance(event_or_code, int) else 0,
            "reason": reason if isinstance(reason, str) else (str(reason) if reason else ""),
            "wasClean": False,
            "event": event_or_code,
        }

    def _get_connected_result(self) -> dict[str, Any] | None:
        raise NotImplementedError

    def _handle_protocol_message(self, text: str, raw: Any) -> None:
        raise NotImplementedError

    def _on_socket_closed(self, _event: DglabSocketCloseEvent) -> None:
        pass


class _ManualSocket:
    ready_state = 1

    def __init__(self, owner: DglabSocketBase) -> None:
        self._owner = owner

    def send(self, data: str | bytes | bytearray | memoryview) -> None:
        if self._owner._manual_sender is None:
            raise create_named_error("socket-send", "手动模式未绑定发送函数")
        self._owner._manual_sender(data)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        self._owner.handle_close(code, reason)
