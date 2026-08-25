from __future__ import annotations

from typing import Any

from dglab_kit_python.events import EventEmitter
from dglab_kit_python.types import DglabSocketVersion

from .v3 import DglabSocketV3
from .v4 import DglabSocketV4

SOCKET_EVENTS = (
    "message",
    "state",
    "open",
    "close",
    "error",
    "frame",
    "data",
    "action",
    "device",
    "devices",
    "client-attached",
    "client-disconnected",
)


class DglabSocket(EventEmitter):
    def __init__(self, **options: Any) -> None:
        super().__init__()
        version = options.get("version") or DglabSocketVersion.V4
        if version == DglabSocketVersion.V3 or version == DglabSocketVersion.V3.value:
            self.raw = DglabSocketV3(**options)
        else:
            self.raw = DglabSocketV4(**options)
        self._forward_adapter_events()

    @property
    def state(self) -> Any:
        return self.raw.state

    async def connect(self) -> dict[str, Any]:
        return await self.raw.connect()

    def send(self, *args: Any, **kwargs: Any) -> Any:
        return self.raw.send(*args, **kwargs)

    async def disconnect(self, code: int | None = None, reason: str | None = None) -> None:
        await self.raw.disconnect(code, reason)

    async def destroy(self, code: int | None = None, reason: str | None = None) -> None:
        try:
            await self.raw.destroy(code, reason)
        finally:
            self.remove_all_listeners()

    def set_sender(self, sender: Any) -> DglabSocket:
        self.raw.set_sender(sender)
        return self

    setSender = set_sender

    def send_raw(self, data: str | bytes | bytearray | memoryview) -> None:
        self.raw.send_raw(data)

    sendRaw = send_raw

    def handle_message(self, data: Any) -> None:
        self.raw.handle_message(data)

    handleMessage = handle_message

    def handle_open(self, event: Any = None) -> None:
        self.raw.handle_open(event)

    handleOpen = handle_open

    def handle_close(self, event_or_code: Any = None, reason: Any = None) -> None:
        self.raw.handle_close(event_or_code, reason)

    handleClose = handle_close

    def handle_error(self, error: Any) -> None:
        self.raw.handle_error(error)

    handleError = handle_error

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_") or name == "raw":
            if name == "raw" and "raw" in self.__dict__:
                raise AttributeError("Socket 的 raw 属性为只读")
            object.__setattr__(self, name, value)
            return

        raw = self.__dict__.get("raw")
        if raw is not None and hasattr(raw, name):
            raise AttributeError(f"无法设置只读 SOCKET 属性: {name}")

        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        if name in self.__dict__:
            object.__delattr__(self, name)
            return

        raw = self.__dict__.get("raw")
        if raw is not None and hasattr(raw, name):
            raise AttributeError(f"无法删除只读 SOCKET 属性: {name}")

        object.__delattr__(self, name)

    def _forward_adapter_events(self) -> None:
        for event in SOCKET_EVENTS:
            self.raw.on(event, self._make_forwarder(event))

    def _make_forwarder(self, event: str) -> Any:
        def forward(*args: Any) -> bool:
            return self.emit(event, *args)

        return forward
