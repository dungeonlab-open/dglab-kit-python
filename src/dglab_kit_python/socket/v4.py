from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from random import choices
from string import ascii_lowercase, digits
from typing import Any

from dglab_kit_python.errors import create_named_error
from dglab_kit_python.socket.base import DglabSocketBase
from dglab_kit_python.types import (
    V4ActionType,
    V4AppendPulseDataOptions,
    V4Channel,
    V4ClearOperateOptions,
    V4DeviceInfo,
    V4OperateOptions,
    DglabSocketCloseEvent,
    DglabSocketState,
)

DEFAULT_RESPONSE_TIMEOUT = 8_000
SERVER_PING_INTERVAL = 2.0
MAX_MISSED_SERVER_PONGS = 3


def _is_record(value: Any) -> bool:
    return isinstance(value, dict)


_DEVICE_PATCH_UNCHANGED = object()


def _merge_patch(current: Any, patch: Any) -> Any:
    if not isinstance(current, dict) or not isinstance(patch, dict):
        return patch

    next_value = dict(current)
    for key, value in patch.items():
        next_value[key] = _merge_patch(current.get(key), value)
    return next_value


def _create_patch_value(previous: Any, current: Any) -> Any:
    if previous == current:
        return _DEVICE_PATCH_UNCHANGED

    if not isinstance(previous, dict) or not isinstance(current, dict):
        return current

    patch: dict[str, Any] = {}
    for key in set(previous.keys()) | set(current.keys()):
        value = current.get(key) if key in current else None
        child_patch = _create_patch_value(previous.get(key), value)
        if child_patch is not _DEVICE_PATCH_UNCHANGED:
            patch[key] = child_patch

    return patch if patch else _DEVICE_PATCH_UNCHANGED


class V4Client:
    def __init__(self, clientId: str, devices: list[V4DeviceInfo] | None = None) -> None:
        self.clientId = clientId
        self.devices: list[V4DeviceInfo] = list(devices or [])

    def get_device(self, slotId: str) -> V4DeviceInfo | None:
        return next((device for device in self.devices if device.get("slotId") == slotId), None)

    getDevice = get_device

    def dispatch(self, data: Any) -> bool:
        if self._update_devices_from_event(data):
            return True
        if not V4Rpc.is_response(data):
            return False
        return self._update_devices_from_response(data.get("result"))

    def replace_devices(self, devices: list[dict[str, Any]]) -> None:
        self.devices[:] = [dict(device) for device in devices]  # type: ignore[list-item]

    replaceDevices = replace_devices

    def upsert_device(self, device: V4DeviceInfo) -> None:
        index = self._find_device_index(device["slotId"])
        if index == -1:
            self.devices.append(device)
        else:
            self.devices[index] = device

    upsertDevice = upsert_device

    def remove_device(self, slotId: str) -> None:
        index = self._find_device_index(slotId)
        if index != -1:
            self.devices.pop(index)

    removeDevice = remove_device

    def patch_devices(
        self, added: list[V4DeviceInfo] | None = None, removed: list[str] | None = None
    ) -> None:
        for device in added or []:
            self.upsert_device(device)
        for slotId in removed or []:
            self.remove_device(slotId)

    patchDevices = patch_devices

    def patch_slots(self, slots: list[dict[str, Any]] | None = None) -> None:
        for slot in slots or []:
            slotId = slot.get("slotId")
            if not isinstance(slotId, str):
                continue
            index = self._find_device_index(slotId)
            if index == -1:
                continue

            current = self.devices[index]
            next_device = dict(current)
            next_device["props"] = _merge_patch(
                current.get("props") or {},
                slot.get("props") or {},
            )
            next_device["slotState"] = _merge_patch(
                current.get("slotState") or {},
                slot.get("slotState") or {},
            )
            self.devices[index] = next_device  # type: ignore[assignment]

    patchSlots = patch_slots

    def destroy(self) -> None:
        self.devices.clear()

    def _update_devices_from_event(self, data: Any) -> bool:
        if not (_is_record(data) and data.get("t") == "ev" and isinstance(data.get("ev"), str)):
            return False

        if data["ev"] == "devices.snapshot":
            self.replace_devices(data.get("devices") or [])
            return True
        if data["ev"] == "devices.patch":
            self.patch_devices(data.get("added") or [], data.get("removed") or [])
            return True
        if data["ev"] == "slots.patch":
            self.patch_slots(data.get("slots") or [])
            return True
        return False

    def _update_devices_from_response(self, result: Any) -> bool:
        if not (_is_record(result) and isinstance(result.get("devices"), list)):
            return False
        self.replace_devices(result["devices"])
        return True

    def _find_device_index(self, slotId: str) -> int:
        for index, device in enumerate(self.devices):
            if device.get("slotId") == slotId:
                return index
        return -1


@dataclass
class _PendingResponse:
    clientId: str
    requestId: str
    future: asyncio.Future[Any]
    timer: asyncio.TimerHandle | None = None
    settled: bool = False


class V4Rpc:
    def __init__(
        self,
        send_frame: Any,
        *,
        responseTimeout: int | None = None,
        response_timeout: int | None = None,
        **_: Any,
    ) -> None:
        self._pending: dict[str, _PendingResponse] = {}
        self._send_frame = send_frame
        self._response_timeout = response_timeout if response_timeout is not None else responseTimeout

    def create_request(self, method: str, data: Any = None) -> dict[str, Any]:
        requestId = self._create_request_id()
        request = {"t": "req", "reqId": requestId, "requestId": requestId, "m": method}
        if data is not None:
            request["data"] = data
        return request

    createRequest = create_request

    def send(
        self, clientId: str, data: Any, options: dict[str, Any] | None = None
    ) -> asyncio.Future[Any]:
        if not clientId:
            raise create_named_error("socket-clientId", "发送协议数据需要指定 clientId")

        loop = asyncio.get_running_loop()
        requestId = self.get_request_id(data) or self._create_request_id()

        if _is_record(data):
            payload = {**data, "requestId": requestId, "reqId": requestId}
        else:
            payload = {
                "t": "req",
                "requestId": requestId,
                "reqId": requestId,
                "m": "custom",
                "data": data,
            }

        waitable_request_id = self.get_request_id(payload)
        if waitable_request_id is None:
            future: asyncio.Future[Any] = loop.create_future()
            future.set_exception(create_named_error("socket-command", "当前消息没有可等待响应"))
            return future

        key = self._pending_key(clientId, waitable_request_id)
        future = loop.create_future()
        pending = _PendingResponse(clientId, waitable_request_id, future)
        pending.timer = loop.call_later(
            self._response_timeout_seconds(options),
            self._timeout_pending,
            key,
            pending,
        )
        self._pending[key] = pending

        print(f"## {payload}")

        try:
            self._send_frame({"type": "message", "clientId": clientId, "data": payload})
        except Exception as error:
            self._pending.pop(key, None)
            self._reject_pending(pending, error)

        return future

    def send_operate(
        self, clientId: str, data: dict[str, Any], options: dict[str, Any] | None = None
    ) -> asyncio.Future[Any]:
        timeout = self._operate_response_timeout(data, options)
        return self.send(
            clientId,
            self.create_request("device.op", data),
            None if timeout is None else {"timeout": timeout},
        )

    sendOperate = send_operate

    def resolve_response(self, clientId: str, response: dict[str, Any]) -> None:
        requestId = self.get_request_id(response)
        if not requestId:
            return

        key = self._pending_key(clientId, requestId)
        entry = self._pending.pop(key, None)
        if entry is None:
            return

        if response.get("error"):
            self._reject_pending(
                entry,
                create_named_error("socket-v4-response", response.get("error") or "V4 指令执行失败"),
            )
            return

        if entry.settled:
            return
        entry.settled = True
        if entry.timer is not None:
            entry.timer.cancel()
        entry.future.set_result(response.get("result"))

    resolveResponse = resolve_response

    def reject_client_pending(self, clientId: str) -> None:
        for key, entry in list(self._pending.items()):
            if entry.clientId != clientId:
                continue
            self._pending.pop(key, None)
            self._reject_pending(entry, create_named_error("socket-disconnected", "被控方已断开"))

    rejectClientPending = reject_client_pending

    def reject_all_pending(self, error: Exception) -> None:
        for entry in list(self._pending.values()):
            self._reject_pending(entry, error)
        self._pending.clear()

    rejectAllPending = reject_all_pending

    @staticmethod
    def is_response(data: Any) -> bool:
        return _is_record(data) and data.get("t") == "resp"

    isResponse = is_response

    @staticmethod
    def get_request_id(data: Any) -> str | None:
        if not _is_record(data):
            return None
        requestId = data.get("requestId")
        if isinstance(requestId, str):
            return requestId
        reqId = data.get("reqId")
        return reqId if isinstance(reqId, str) else None

    getRequestId = get_request_id

    def _create_request_id(self) -> str:
        stamp = base36(int(time.time() * 1000))
        suffix = "".join(choices(ascii_lowercase + digits, k=6))
        return f"v4-{stamp}-{suffix}"

    def _response_timeout_seconds(self, options: dict[str, Any] | None = None) -> float:
        option_timeout = (options or {}).get("timeout")
        timeout = (
            option_timeout
            if option_timeout is not None
            else self._response_timeout
            if self._response_timeout is not None
            else DEFAULT_RESPONSE_TIMEOUT
        )
        return timeout / 1000

    def _operate_response_timeout(
        self, data: dict[str, Any], options: dict[str, Any] | None = None
    ) -> int | float | None:
        option_timeout = (options or {}).get("timeout")
        if option_timeout is not None:
            return option_timeout
        if self._response_timeout is not None:
            return None

        duration = data.get("d")
        if isinstance(duration, int | float) and duration > DEFAULT_RESPONSE_TIMEOUT:
            return duration + 1000
        return None

    def _pending_key(self, clientId: str, requestId: str) -> str:
        return f"{clientId}\0{requestId}"

    def _timeout_pending(self, key: str, pending: _PendingResponse) -> None:
        self._pending.pop(key, None)
        self._reject_pending(pending, create_named_error("socket-response-timeout", "等待响应超时"))

    @staticmethod
    def _reject_pending(entry: _PendingResponse, error: Exception) -> None:
        if entry.settled:
            return
        entry.settled = True
        if entry.timer is not None:
            entry.timer.cancel()
        if not entry.future.done():
            entry.future.set_exception(error)


class DglabSocketV4(DglabSocketBase):
    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self._targetId: str | None = None
        self._client_map: dict[str, V4Client] = {}
        self.rpc = V4Rpc(self.send_frame, **options)
        self._server_ping_task: asyncio.Task[None] | None = None
        self._missed_server_pongs = 0

    @property
    def targetId(self) -> str | None:
        return self._targetId

    @property
    def target_id(self) -> str | None:
        return self._targetId

    @property
    def clientIds(self) -> list[str]:
        return list(self._client_map)

    @property
    def client_ids(self) -> list[str]:
        return self.clientIds

    @property
    def clients(self) -> list[V4Client]:
        return list(self._client_map.values())

    def get_client(self, clientId: str) -> V4Client | None:
        return self._client_map.get(clientId)

    getClient = get_client

    async def request_devices(self, clientId: str) -> dict[str, Any]:
        result = await self.rpc.send(clientId, self.rpc.create_request("devices.get"))
        client = self._ensure_client(clientId)
        if client.dispatch({"t": "resp", "result": result}):
            self.emit("devices", client.devices, client.clientId)
        return result

    requestDevices = request_devices

    def send(
        self, clientId: str, data: Any, options: dict[str, Any] | None = None
    ) -> asyncio.Future[Any]:
        return self.rpc.send(clientId, data, options)

    def ping(
        self, clientId: str, options: dict[str, Any] | None = None
    ) -> asyncio.Future[Any]:
        return self.rpc.send(clientId, self.rpc.create_request("ping"), options)

    async def disconnect(self, code: int | None = None, reason: str | None = None) -> None:
        self._stop_server_ping()
        await super().disconnect(code, reason)

    def add_intensity(
        self,
        clientId: str,
        slotId: str,
        channel: V4Channel | int,
        value: int | float,
        options: V4OperateOptions | None = None,
    ) -> asyncio.Future[Any]:
        return self.rpc.send_operate(
            clientId,
            {
                **self._create_operate_base(slotId, channel, options),
                "t": V4ActionType.ADD_INTENSITY,
                "v": value,
            },
            options,
        )

    addIntensity = add_intensity

    def reduce_strength(
        self,
        clientId: str,
        slotId: str,
        channel: V4Channel | int,
        value: int | float,
        options: V4OperateOptions | None = None,
    ) -> asyncio.Future[Any]:
        return self.add_intensity(clientId, slotId, channel, -value, options)

    reduceStrength = reduce_strength

    def set_temp_intensity(
        self,
        clientId: str,
        slotId: str,
        channel: V4Channel | int,
        value: int | float,
        duration: int | float,
        options: V4OperateOptions | None = None,
    ) -> asyncio.Future[Any]:
        return self.rpc.send_operate(
            clientId,
            {
                **self._create_operate_base(slotId, channel, options),
                "t": V4ActionType.SET_TEMP_INTENSITY,
                "v": value,
                "d": duration,
            },
            options,
        )

    setTempIntensity = set_temp_intensity

    def reset_intensity(
        self,
        clientId: str,
        slotId: str,
        channel: V4Channel | int,
        options: V4OperateOptions | None = None,
    ) -> asyncio.Future[Any]:
        return self.rpc.send_operate(
            clientId,
            {
                **self._create_operate_base(slotId, channel, options),
                "t": V4ActionType.SET_INTENSITY,
                "v": 0,
            },
            options,
        )

    resetIntensity = reset_intensity

    def send_pulse(
        self,
        clientId: str,
        slotId: str,
        channel: V4Channel | int,
        duration: int | float,
        frames: list[list[int]] | list[str],
        options: V4AppendPulseDataOptions | None = None,
    ) -> asyncio.Future[Any]:
        options = options or {}
        payload = {
            **self._create_operate_base(slotId, channel, options),
            "t": V4ActionType.APPEND_PULSE_DATA,
            "d": duration,
            "v": frames,
        }
        if "version" in options:
            payload["ver"] = options["version"]
        if "seq" in options:
            payload["seq"] = options["seq"]
        return self.rpc.send_operate(clientId, payload, options)

    sendPulse = send_pulse

    def clear_pulse(
        self, clientId: str, slotId: str, channel: V4Channel | int
    ) -> asyncio.Future[Any]:
        return self.clear_operate(clientId, {"slotId": slotId, "channel": channel})

    clearPulse = clear_pulse

    def clear_operate(
        self, clientId: str, options: V4ClearOperateOptions | None = None
    ) -> asyncio.Future[Any]:
        data = None
        if options:
            slot_id = options.get("slot_id") or options.get("slotId")  # type: ignore[typeddict-item]
            if slot_id:
                data = {"s": slot_id, "c": options.get("channel")}
        return self.rpc.send(clientId, self.rpc.create_request("device.op.clear", data))

    clearOperate = clear_operate

    def _handle_protocol_message(self, text: str, raw: Any) -> None:
        try:
            frame = __import__("json").loads(text)
        except Exception:
            return
        if not _is_record(frame):
            return

        self.emit("frame", frame)
        frame_type = frame.get("type")

        if frame_type == "hello":
            self._handle_hello(frame)
        elif frame_type == "client_attached":
            clientId = frame.get("clientId")
            if isinstance(clientId, str):
                self._set_state(DglabSocketState.PAIRED)
                self._ensure_client(clientId)
                self.emit("client-attached", clientId)
        elif frame_type == "client_disconnected":
            clientId = frame.get("clientId")
            if isinstance(clientId, str):
                self._handle_client_disconnected(clientId)
        elif frame_type == "message":
            self._handle_message_frame(frame)
        elif frame_type == "pong":
            self._missed_server_pongs = 0
        elif frame_type == "idle_timeout":
            self.handle_error(create_named_error("socket-idle-timeout", "控制方空闲超时"))
        elif frame_type == "error":
            self.handle_error(
                create_named_error("socket-v4-server", frame.get("message") or frame.get("code") or "V4 服务端错误")
            )

    def _on_socket_closed(self, _event: DglabSocketCloseEvent) -> None:
        self._stop_server_ping()
        self._targetId = None
        for client in self._client_map.values():
            client.destroy()
        self._client_map.clear()
        self.rpc.reject_all_pending(create_named_error("socket-disconnected", "WebSocket 已断开"))

    def _get_connected_result(self) -> dict[str, Any] | None:
        if self._targetId is None:
            return None
        return {"targetId": self._targetId}

    def _handle_hello(self, frame: dict[str, Any]) -> None:
        clientId = frame.get("clientId")
        if not isinstance(clientId, str):
            return
        self._targetId = clientId
        self._set_state(DglabSocketState.WAITING_FOR_PEER)
        self._start_server_ping()
        self._resolve_active_connect({"targetId": clientId})

    def _handle_client_disconnected(self, clientId: str) -> None:
        self._detach_client(clientId)
        self.rpc.reject_client_pending(clientId)
        if not self._client_map:
            self._set_state(DglabSocketState.WAITING_FOR_PEER)
        self.emit("client-disconnected", clientId)

    def _handle_message_frame(self, frame: dict[str, Any]) -> None:
        clientId = frame.get("clientId")
        if not isinstance(clientId, str):
            return
        data = frame.get("data")
        self.emit("data", data, clientId)

        if self._is_custom_action_event(data):
            self.emit("action", data["action"])

        client = self._client_map.get(clientId)
        for event in self._create_device_events(data, client):
            self.emit("device", event, clientId)

        if client is not None and client.dispatch(data):
            self.emit("devices", client.devices, client.clientId)

        if V4Rpc.is_response(data):
            self.rpc.resolve_response(clientId, data)

    def _ensure_client(self, clientId: str) -> V4Client:
        client = self._client_map.get(clientId)
        if client is None:
            client = V4Client(clientId)
            self._client_map[clientId] = client
        return client

    def _detach_client(self, clientId: str) -> None:
        client = self._client_map.pop(clientId, None)
        if client is not None:
            client.destroy()

    def _start_server_ping(self) -> None:
        self._stop_server_ping()
        self._missed_server_pongs = 0
        self._server_ping_task = asyncio.create_task(self._run_server_ping())

    def _stop_server_ping(self) -> None:
        task = self._server_ping_task
        self._server_ping_task = None
        self._missed_server_pongs = 0
        if task is None or task.done():
            return

        current_task: asyncio.Task[Any] | None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        if task is not current_task:
            task.cancel()

    async def _run_server_ping(self) -> None:
        try:
            while True:
                await asyncio.sleep(SERVER_PING_INTERVAL)
                if self._missed_server_pongs >= MAX_MISSED_SERVER_PONGS:
                    await self._disconnect_for_server_ping_timeout()
                    return

                try:
                    self.send_frame({"type": "ping"})
                    self._missed_server_pongs += 1
                except Exception as error:
                    self.handle_error(error)
                    await self._disconnect_for_server_ping_timeout()
                    return
        except asyncio.CancelledError:
            raise

    async def _disconnect_for_server_ping_timeout(self) -> None:
        self._stop_server_ping()
        self.handle_error(create_named_error("socket-ping-timeout", "服务端 ping 超时"))
        await super().disconnect(1000, "ping_timeout")

    def _create_device_events(
        self, data: Any, client: V4Client | None
    ) -> list[dict[str, Any]]:
        if not (_is_record(data) and data.get("t") == "ev"):
            return []

        if data.get("ev") == "devices.snapshot":
            devices = data.get("devices") if isinstance(data.get("devices"), list) else []
            changed = [
                event
                for device in devices
                if isinstance(device, dict)
                for event in [self._create_device_upsert_event(device, client)]
                if event is not None
            ]
            next_slot_ids = {
                device.get("slotId") for device in devices if isinstance(device.get("slotId"), str)
            }
            removed = [
                {"slotId": device["slotId"], "removed": True}
                for device in (client.devices if client is not None else [])
                if device.get("slotId") not in next_slot_ids
            ]
            return changed + removed

        if data.get("ev") == "devices.patch":
            added = data.get("added") if isinstance(data.get("added"), list) else []
            removed = data.get("removed") if isinstance(data.get("removed"), list) else []
            changed = [
                event
                for device in added
                if isinstance(device, dict)
                for event in [self._create_device_upsert_event(device, client)]
                if event is not None
            ]
            return changed + [
                {"slotId": slot_id, "removed": True}
                for slot_id in removed
                if isinstance(slot_id, str)
            ]

        if data.get("ev") == "slots.patch":
            slots = data.get("slots") if isinstance(data.get("slots"), list) else []
            return [slot for slot in slots if isinstance(slot, dict)]

        return []

    def _create_device_upsert_event(
        self, device: dict[str, Any], client: V4Client | None
    ) -> dict[str, Any] | None:
        slotId = device.get("slotId")
        if not isinstance(slotId, str):
            return None

        previous = client.get_device(slotId) if client is not None else None
        if previous is None:
            return dict(device)

        event: dict[str, Any] = {"slotId": slotId}
        if previous.get("name") != device.get("name"):
            event["name"] = device.get("name")
        if previous.get("type") != device.get("type"):
            event["type"] = device.get("type")
        props_patch = _create_patch_value(previous.get("props"), device.get("props"))
        if props_patch is not _DEVICE_PATCH_UNCHANGED:
            event["props"] = props_patch
        slot_state_patch = _create_patch_value(
            previous.get("slotState"),
            device.get("slotState"),
        )
        if slot_state_patch is not _DEVICE_PATCH_UNCHANGED:
            event["slotState"] = slot_state_patch

        return event if len(event) > 1 else None

    @staticmethod
    def _is_custom_action_event(data: Any) -> bool:
        return (
            _is_record(data)
            and data.get("t") == "ev"
            and data.get("ev") == "custom.action"
            and isinstance(data.get("action"), int | float)
        )

    @staticmethod
    def _create_operate_base(
        slotId: str, channel: V4Channel | int, options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        options = options or {}
        payload = {
            "s": slotId,
            "c": int(channel),
        }
        if "priority" in options:
            payload["p"] = options["priority"]
        if "immediate" in options:
            payload["im"] = options["immediate"]
        return payload


def base36(number: int) -> str:
    alphabet = digits + ascii_lowercase
    if number == 0:
        return "0"
    result = ""
    while number:
        number, index = divmod(number, 36)
        result = alphabet[index] + result
    return result
