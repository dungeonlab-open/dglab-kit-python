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
        self._connect_task: asyncio.Task[Any] | None = None
        self._connect_attempt: object | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._socket_generation = 0
        self._socket_closed = True

    @property
    def state(self) -> DglabSocketState:
        return self._state

    async def connect(self) -> dict[str, Any]:
        active = self._get_connected_result()
        if active is not None:
            return active

        if self._connect_future is not None:
            return await asyncio.shield(self._connect_future)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._connect_future = future
        attempt = object()
        self._connect_attempt = attempt
        self._set_state(DglabSocketState.CONNECTING)
        self._connect_task = asyncio.current_task()

        async def open_and_wait() -> dict[str, Any]:
            await self._open_socket(attempt)
            return await asyncio.shield(future)

        try:
            # 同一个超时同时覆盖 WebSocket 握手与协议 hello 等待
            return await asyncio.wait_for(
                open_and_wait(),
                timeout=self._connect_timeout_seconds(),
            )
        except TimeoutError as error:
            timeout_error = create_named_error("socket-connect-timeout", "连接超时")
            await self._shutdown(
                timeout_error,
                code=1000,
                reason="connect_timeout",
                attempt=attempt,
            )
            raise timeout_error from error
        except asyncio.CancelledError:
            cancelled_error = create_named_error(
                "socket-connect-cancelled", "连接已取消"
            )
            if self._connect_attempt is not attempt:
                raise cancelled_error
            await self._shutdown(
                cancelled_error,
                code=1000,
                reason="connect_cancelled",
                attempt=attempt,
            )
            raise
        except Exception as error:
            connect_error = (
                error
                if isinstance(error, Exception)
                else create_named_error("socket-connect", str(error))
            )
            await self._shutdown(
                connect_error,
                code=1000,
                reason="connect_error",
                attempt=attempt,
            )
            raise
        finally:
            if self._connect_task is asyncio.current_task():
                self._connect_task = None
            if self._connect_attempt is attempt:
                self._connect_attempt = None

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
        await self._shutdown(
            create_named_error("socket-connect-cancelled", "连接已取消"),
            code=code,
            reason=reason,
        )

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
        self._finalize_close(
            event_or_code,
            reason,
            create_named_error("socket-connect-closed", "连接完成前已关闭"),
        )

    handleClose = handle_close
    handleSocketClose = handle_close

    def handle_error(self, error: Any) -> None:
        self.emit("error", error)

    handleError = handle_error
    handleSocketError = handle_error

    async def _open_socket(self, attempt: object) -> Any:
        if self._connect_attempt is not attempt:
            raise create_named_error("socket-connect-cancelled", "连接已取消")

        if self.socket is not None and not self._socket_closed:
            return self.socket

        self.socket = None

        if not self.url:
            self._install_socket(_ManualSocket(self))
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

        socket = await connect(self.url, subprotocols=self._protocol_list())

        # disconnect()/超时可能在握手期间使本次连接失效
        if self._connect_attempt is not attempt:
            await self._close_socket(socket, 1000, "connect_cancelled")
            raise create_named_error("socket-connect-cancelled", "连接已取消")

        generation = self._install_socket(socket)
        self.handle_open()
        if self._is_current_socket(socket, generation):
            self._reader_task = asyncio.create_task(
                self._read_socket(socket, generation)
            )
        return socket

    async def _read_socket(self, socket: Any, generation: int) -> None:
        try:
            async for message in socket:
                if not self._is_current_socket(socket, generation):
                    return
                self.handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._is_current_socket(socket, generation):
                self.handle_error(error)
        finally:
            if self._is_current_socket(socket, generation):
                if self._reader_task is asyncio.current_task():
                    self._reader_task = None
                self.handle_close()

    async def _shutdown(
        self,
        error: Exception,
        *,
        code: int | None,
        reason: str | None,
        attempt: object | None = None,
    ) -> None:
        # 旧连接的异常不能关闭或覆盖新一轮连接。
        if attempt is not None and self._connect_attempt is not attempt:
            return

        socket = self.socket
        reader_task = self._reader_task
        connect_task = self._connect_task
        current_task = asyncio.current_task()

        try:
            self._finalize_close(code, reason, error)
        finally:
            if connect_task is not None and connect_task is not current_task:
                connect_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await connect_task

            if reader_task is not None and reader_task is not current_task:
                reader_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await reader_task

            if self._reader_task is reader_task:
                self._reader_task = None

            await self._close_socket(socket, code, reason)

    def _finalize_close(
        self,
        event_or_code: Any,
        reason: Any,
        connect_error: Exception,
    ) -> None:
        if self._socket_closed and self.socket is None and self._connect_future is None:
            return

        close_event = self._normalize_close_event(event_or_code, reason)
        if self._connect_future is not None:
            self._reject_active_connect(connect_error)

        self._set_state(DglabSocketState.DISCONNECTED)
        self._socket_closed = True
        self.socket = None
        self._socket_generation += 1
        self._connect_attempt = None

        reader_task = self._reader_task
        self._reader_task = None
        if reader_task is not None and reader_task is not asyncio.current_task():
            reader_task.cancel()

        self._on_socket_closed(close_event)
        self.emit("close", close_event)

    def _install_socket(self, socket: Any) -> int:
        self.socket = socket
        self._socket_closed = False
        self._socket_generation += 1
        return self._socket_generation

    def _is_current_socket(self, socket: Any, generation: int) -> bool:
        return (
            self.socket is socket
            and not self._socket_closed
            and self._socket_generation == generation
        )

    async def _close_socket(
        self,
        socket: Any,
        code: int | None,
        reason: str | None,
    ) -> None:
        if socket is None:
            return

        close = getattr(socket, "close", None)
        if close is None:
            return

        result = close(code=code, reason=reason) if code is not None else close()
        if isinstance(result, Awaitable):
            await result

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
            # 该 Future 主要由 connect() 内部持有；在超时/取消后可能没有
            # 主动读取一次异常以避免 asyncio 的未处理告警
            future.exception()

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
