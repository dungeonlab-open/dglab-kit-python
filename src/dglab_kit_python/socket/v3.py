from __future__ import annotations

import json
from typing import Any

from dglab_kit_python.errors import create_named_error
from dglab_kit_python.socket.base import DglabSocketBase
from dglab_kit_python.types import (
    DglabSocketCloseEvent,
    DglabSocketDeviceType,
    DglabSocketState,
    V3Channel,
    V3WaveOptions,
)


class DglabSocketV3(DglabSocketBase):
    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self._targetId: str | None = None
        self._paired_target_id: str | None = None
        self._device: dict[str, Any] | None = None

    @property
    def targetId(self) -> str | None:
        return self._targetId

    @property
    def target_id(self) -> str | None:
        return self._targetId

    @property
    def pairedClientId(self) -> str | None:
        return self._paired_target_id

    @property
    def paired_client_id(self) -> str | None:
        return self._paired_target_id

    def send(self, data: Any) -> None:
        if not self._targetId or not self._paired_target_id:
            raise create_named_error("socket-target", "V3 尚未完成配对")

        payload = dict(data) if isinstance(data, dict) else {"data": data}
        payload.update({"clientId": self._targetId, "targetId": self._paired_target_id})
        self.send_frame(payload)

    def add_strength(self, channel: V3Channel | int, step: int = 1) -> None:
        command_type = 2 if step >= 0 else 1
        count = max(1, abs(step))
        channel_value = int(channel)

        if count > 200:
            raise create_named_error("socket-v3", "单次调整强度过大")

        for _ in range(count):
            self.send({"type": command_type, "channel": channel_value, "message": "set channel"})

    addStrength = add_strength

    def reduce_strength(self, channel: V3Channel | int, step: int = 1) -> None:
        self.add_strength(channel, -step)

    reduceStrength = reduce_strength

    def set_strength(self, channel: V3Channel | int, strength: int | float) -> None:
        self.send(
            {
                "type": 3,
                "channel": int(channel),
                "strength": strength,
                "message": "set channel",
            }
        )

    setStrength = set_strength

    def clear_pulse(self, channel: V3Channel | int) -> None:
        self.send({"type": 4, "channel": int(channel), "message": "clear"})

    clearPulse = clear_pulse

    def send_pulse(self, options: V3WaveOptions) -> None:
        data = options["data"]
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")) if isinstance(data, list) else data
        channel = options["channel"]
        self.send(
            {
                "type": "clientMsg",
                "channel": channel,
                "time": options["time"],
                "message": f"{channel}:{payload}",
            }
        )

    sendPulse = send_pulse

    def _handle_protocol_message(self, text: str, raw: Any) -> None:
        try:
            frame = json.loads(text)
        except Exception:
            self.emit("data", text)
            return

        if not isinstance(frame, dict):
            self.emit("data", frame)
            return

        self.emit("frame", frame)
        frame_type = frame.get("type")

        if frame_type == "bind":
            clientId = frame.get("clientId")
            targetId = frame.get("targetId")
            if not isinstance(clientId, str):
                return
            if "targetId" not in frame or targetId == "":
                self._handle_initial_bind(clientId)
                return
            if isinstance(targetId, str) and frame.get("message") == "200":
                self._handle_pair_bind(targetId)
                return
        elif frame_type == "break":
            self._paired_target_id = None
            self._device = None
            self._set_state(DglabSocketState.WAITING_FOR_PEER)
            targetId = frame.get("targetId")
            if isinstance(targetId, str):
                self._emit_device(
                    {"type": DglabSocketDeviceType.COYOTE_030, "removed": True},
                    targetId,
                )
                self.emit("client-disconnected", targetId)
            return
        elif frame_type == "msg" or frame_type == 4:
            self._handle_forward_message(frame)
            return
        elif frame_type == "error":
            self.handle_error(
                create_named_error(
                    "socket-v3-server",
                    frame.get("message") if isinstance(frame.get("message"), str) else "V3 服务端错误",
                )
            )
            return

        self.emit(
            "data",
            frame.get("message") if "message" in frame else frame,
            frame.get("targetId") if isinstance(frame.get("targetId"), str) else None,
        )

    def _on_socket_closed(self, _event: DglabSocketCloseEvent) -> None:
        self._targetId = None
        self._paired_target_id = None
        self._device = None

    def _get_connected_result(self) -> dict[str, Any] | None:
        if self._targetId is None:
            return None
        return {"targetId": self._targetId}

    def _handle_initial_bind(self, clientId: str) -> None:
        self._targetId = clientId
        self._set_state(DglabSocketState.WAITING_FOR_PEER)
        self._resolve_active_connect({"targetId": clientId})

    def _handle_pair_bind(self, targetId: str) -> None:
        self._paired_target_id = targetId
        self._set_state(DglabSocketState.PAIRED)
        self._device = self._create_device()
        self._emit_device(self._device, targetId)
        self.emit("client-attached", targetId)

    def _handle_forward_message(self, frame: dict[str, Any]) -> None:
        message = frame.get("message")
        if isinstance(message, str):
            action = self._parse_action_message(message)
            if action is not None:
                self.emit("action", action)

            device = self._parse_device_message(message)
            if device is not None:
                self._emit_device(device)

        self.emit(
            "data",
            frame.get("message") if "message" in frame else frame,
            frame.get("targetId") if isinstance(frame.get("targetId"), str) else None,
        )

    @staticmethod
    def _parse_action_message(message: str) -> int | None:
        import re

        match = re.fullmatch(r"feedback-(\d+)", message)
        if match is None:
            return None
        return int(match.group(1))

    def _parse_device_message(self, message: str) -> dict[str, Any] | None:
        import re

        match = re.fullmatch(r"strength-(\d+)-(\d+)-(\d+)-(\d+)", message)
        if match is None or self._paired_target_id is None:
            return None

        a_strength, b_strength, a_soft_limit, b_soft_limit = [int(value) for value in match.groups()]
        next_props = {
            "strength": {"A": a_strength, "B": b_strength},
            "softLimit": {"A": a_soft_limit, "B": b_soft_limit},
        }
        previous_props = (self._device or {}).get("props") or {}
        props: dict[str, Any] = {}

        if (previous_props.get("strength") or {}).get("A") != a_strength:
            props.setdefault("strength", {})["A"] = a_strength
        if (previous_props.get("strength") or {}).get("B") != b_strength:
            props.setdefault("strength", {})["B"] = b_strength
        if (previous_props.get("softLimit") or {}).get("A") != a_soft_limit:
            props.setdefault("softLimit", {})["A"] = a_soft_limit
        if (previous_props.get("softLimit") or {}).get("B") != b_soft_limit:
            props.setdefault("softLimit", {})["B"] = b_soft_limit

        self._device = {
            **(self._device or self._create_device()),
            "props": next_props,
        }

        if not props:
            return None
        return {"type": DglabSocketDeviceType.COYOTE_030, "props": props}

    def _emit_device(self, device: dict[str, Any], client_id: str | None = None) -> None:
        target_id = client_id or self._paired_target_id
        if target_id is None:
            return
        self.emit("device", device, target_id)

    @staticmethod
    def _create_device() -> dict[str, Any]:
        return {
            "type": DglabSocketDeviceType.COYOTE_030,
        }
