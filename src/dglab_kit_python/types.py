from __future__ import annotations

from enum import Enum, IntEnum
from typing import Any, Literal, NotRequired, TypedDict


class DglabSocketVersion(str, Enum):
    V3 = "v3"
    V4 = "v4"


class DglabSocketState(str, Enum):
    Idle = "idle"
    Connecting = "connecting"
    WaitingForPeer = "waiting_for_peer"
    Paired = "paired"
    Disconnected = "disconnected"

    IDLE = "idle"
    CONNECTING = "connecting"
    WAITING_FOR_PEER = "waiting_for_peer"
    PAIRED = "paired"
    DISCONNECTED = "disconnected"


class DglabSocketDeviceType(str, Enum):
    COYOTE_020 = "COYOTE_020"
    COYOTE_030 = "COYOTE_030"
    BMTR_1 = "BMTR_1"
    OVC_1 = "OVC_1"


class DglabSocketConnectResult(TypedDict):
    targetId: str


class DglabSocketCloseEvent(TypedDict):
    code: int
    reason: str
    wasClean: bool
    event: Any


class V4ActionType(IntEnum):
    AppendPulseData = 0
    AddIntensity = 3
    SetTempIntensity = 4
    SetIntensity = 7

    APPEND_PULSE_DATA = 0
    ADD_INTENSITY = 3
    SET_TEMP_INTENSITY = 4
    SET_INTENSITY = 7


class V4Channel(IntEnum):
    A = 0
    B = 1


class V3Channel(IntEnum):
    A = 1
    B = 2


class V4DeviceDescriptor(TypedDict):
    slotId: str
    name: str
    type: DglabSocketDeviceType


class V4DeviceInfo(V4DeviceDescriptor, total=False):
    props: dict[str, Any]
    slotState: dict[str, Any]


class V4DevicesGetResult(TypedDict):
    devices: list[V4DeviceDescriptor]


class V4OperateOptions(TypedDict, total=False):
    timeout: int
    priority: Literal[0, 1, 2]
    immediate: bool


class V4AppendPulseDataOptions(V4OperateOptions, total=False):
    version: int
    seq: int


class V4ClearOperateOptions(TypedDict, total=False):
    slot_id: str
    slotId: str
    channel: NotRequired[V4Channel | int]


class V3WaveOptions(TypedDict):
    channel: Literal["A", "B"]
    time: int | float
    data: str | list[str]


DGLAB_SOCKET_VERSION = DglabSocketVersion
DGLAB_SOCKET_STATE = DglabSocketState
DGLAB_SOCKET_DEVICE_TYPE = DglabSocketDeviceType
