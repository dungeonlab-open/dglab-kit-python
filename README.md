# DGLAB KIT - Python

DGLAB KIT 面向 **DG-LAB App** 的 Python SDK。它使用 `asyncio` 通过 WebSocket 与 App 配对，并控制 App 暴露的本地设备

SDK 提供两类能力：

- **Socket SDK**：连接 V3 / V4 Socket 服务，完成 App 配对、设备发现、强度控制、波形下发、任务清理和动作反馈接收
- **Waveform SDK**：内置郊狼与负鼠波形数据，可直接用于 V3 / V4 波形操作

> 推荐优先使用 V4。V4 支持 `1 控制方 : N App 被控方`、设备列表同步和请求/响应等待；V3 仅用于兼容旧版控制端

## 目录

- [运行要求](#运行要求)
- [安装](#安装)
- [核心概念](#核心概念)
- [快速开始：V4 控制 APP](#快速开始v4-控制-app)
- [生成 APP 配对二维码](#生成-app-配对二维码)
- [Socket SDK API](#socket-sdk-api)
- [V4 常用操作](#v4-常用操作)
- [手动传输模式](#手动传输模式)
- [V3 旧协议](#v3-旧协议)
- [Waveform SDK](#waveform-sdk)
- [V4 协议参考](#v4-协议参考)
- [常见问题](#常见问题)

## 运行要求

- Python 3.13 或更高版本
- 已有自己的 WebSocket 实现时，可使用[手动传输模式](#手动传输模式)

## 安装

在项目中安装 SDK：

```bash
pip install dglab-kit-python
```

## 核心概念

| 名称         | 说明                                                     |
|------------|--------------------------------------------------------|
| 控制方        | 运行 `DglabSocket` 的 Python 程序，例如服务端或桌面控制器               |
| App 被控方    | 连接到当前控制方的 DG-LAB APP                                   |
| `targetId` | 控制方连接成功后获得的配对 ID；App 需要使用它接入                           |
| `clientId` | V4 中某个 App 被控方的连接 ID；向指定 App 下发指令时必须传入                 |
| `slotId`   | App 暴露的设备 ID；设备操作通过它定位设备                               |
| `channel`  | 设备通道。V4 使用 `V4Channel.A` / `V4Channel.B`，也可传 `0` / `1` |

控制方只需连接 Socket 服务；App 使用控制方的 `targetId` 接入。连接关系、设备更新和全部协议细节请参阅 [dglab-kit V4 协议参考](https://github.com/dungeonlab-open/dglab-kit/blob/main/README.md#v4-%E5%8D%8F%E8%AE%AE%E5%8F%82%E8%80%83)

## 快速开始：V4 控制 APP

以下示例连接 V4 服务，取得配对 ID，等待 App 接入，读取设备并执行强度与波形操作

```python
import asyncio

from dglab_kit_python import (
    COYOTE_WAVEFORM,
    COYOTE_WAVEFORMS,
    DGLAB_SOCKET_STATE,
    DglabSocket,
    V4Channel,
)


async def main() -> None:
    socket = DglabSocket(
        url="ws://127.0.0.1:9998",
        connect_timeout=8_000,
        response_timeout=8_000,
    )

    socket.on("state", lambda state, previous: print(f"{previous} -> {state}"))
    socket.on("error", lambda error: print("Socket 错误:", error))
    socket.on("devices", lambda devices, client_id: print(client_id, devices))

    async def on_client_attached(client_id: str) -> None:
        result = await socket.request_devices(client_id)
        devices = result["devices"]
        if not devices:
            return

        slot_id = devices[0]["slotId"]
        await socket.reset_intensity(client_id, slot_id, V4Channel.A)
        await socket.add_intensity(client_id, slot_id, V4Channel.A, 5)
        await socket.set_temp_intensity(client_id, slot_id, V4Channel.A, 30, 3_000)
        await socket.send_pulse(
            client_id,
            slot_id,
            V4Channel.A,
            1_000,
            COYOTE_WAVEFORMS[COYOTE_WAVEFORM.BUBBLE]["raw"],
        )

    socket.on("client-attached", on_client_attached)

    result = await socket.connect()
    print("控制方 ID:", result["targetId"])
    assert socket.state is DGLAB_SOCKET_STATE.WAITING_FOR_PEER

    # 保持连接以接收事件。
    await asyncio.Event().wait()


asyncio.run(main())
```

监听函数既可以是普通函数，也可以是 `async def`。异步监听函数会被调度到当前事件循环

## 生成 APP 配对二维码

SDK 的 `connect()` 返回 `targetId` 后，请按照 [dglab-kit 的配对二维码文档](https://github.com/dungeonlab-open/dglab-kit#%E7%94%9F%E6%88%90-app-%E9%85%8D%E5%AF%B9%E4%BA%8C%E7%BB%B4%E7%A0%81) 生成二维码或配对链接并交给 App

## Socket SDK API

### 构造函数

```python
from dglab_kit_python import DGLAB_SOCKET_VERSION, DglabSocket

socket = DglabSocket(
    url=None,
    protocols=None,
    connect_timeout=None,
    response_timeout=None,
    version=DGLAB_SOCKET_VERSION.V4,
)
```

| 选项                 | 默认值                       | 说明                            |
|--------------------|---------------------------|-------------------------------|
| `url`              | `None`                    | WebSocket 服务地址；省略时使用手动传输模式    |
| `protocols`        | `None`                    | 一个 WebSocket 子协议字符串或子协议列表     |
| `connect_timeout`  | `8_000` ms                | `connect()` 等待协议握手的最长时间       |
| `response_timeout` | `8_000` ms                | V4 请求等待响应的默认超时时间              |
| `version`          | `DGLAB_SOCKET_VERSION.V4` | `V4` / `"v4"` 或 `V3` / `"v3"` |

构造函数也接受 `connectTimeout` 与 `responseTimeout` 两个驼峰别名；若两种写法同时出现，以 snake_case 的值为准

### `connect()` 返回值

```python
result = await socket.connect()
target_id = result["targetId"]
```

| 字段         | 说明                      |
|------------|-------------------------|
| `targetId` | 当前控制方 ID；App 被控方需要使用它接入 |

连接成功后再次调用 `connect()` 会直接返回当前连接结果

### Socket 状态

| 状态                                    | 说明                     |
|---------------------------------------|------------------------|
| `DGLAB_SOCKET_STATE.IDLE`             | 未连接                    |
| `DGLAB_SOCKET_STATE.CONNECTING`       | 正在建立连接                 |
| `DGLAB_SOCKET_STATE.WAITING_FOR_PEER` | 已连接，正在等待 App 接入        |
| `DGLAB_SOCKET_STATE.PAIRED`           | 至少一个 V4 App，或 V3 对端已配对 |
| `DGLAB_SOCKET_STATE.DISCONNECTED`     | 已断开                    |

`DglabSocketState` 也提供对应的 PascalCase 成员名；`DGLAB_SOCKET_STATE` 是该枚举类的兼容别名

### 通用事件

`DglabSocket` 是事件发射器。`on()`、`once()`、`off()` 与 `remove_all_listeners()` 均返回当前 Socket，可用于链式调用；`emit()` 可发布应用自定义事件

| 事件                    | 监听函数参数                   | 说明                                                |
|-----------------------|--------------------------|---------------------------------------------------|
| `state`               | `(state, previous)`      | Socket 状态变化                                       |
| `open`                | `(event)`                | WebSocket 传输层已打开                                  |
| `close`               | `(event)`                | 传输层已关闭；事件含 `code`、`reason`、`wasClean` 和原始 `event` |
| `error`               | `(error)`                | 传输、超时或协议错误                                        |
| `message`             | `(text, raw)`            | 收到原始消息，尚未解析                                       |
| `frame`               | `(frame)`                | 已解析 JSON 帧                                        |
| `data`                | `(data, client_id=None)` | 收到应用数据                                            |
| `action`              | `(action)`               | 收到 App 动作通知                                       |
| `device`              | `(device, client_id)`    | 设备新增、变化或移除                                        |
| `devices`             | `(devices, client_id)`   | V4 当前完整设备列表变化                                     |
| `client-attached`     | `(client_id)`            | App 被控方已接入                                        |
| `client-disconnected` | `(client_id)`            | App 被控方已断开                                        |

### 通用方法

| 方法或属性                                                                                           | 返回值                | 说明                        |
|-------------------------------------------------------------------------------------------------|--------------------|---------------------------|
| `state`                                                                                         | `DglabSocketState` | 当前连接状态                    |
| `await connect()`                                                                               | `dict`             | 建立连接并等待协议握手               |
| `await disconnect(code=None, reason=None)`                                                      | `None`             | 断开连接，并取消尚未完成的 `connect()` |
| `await destroy(code=None, reason=None)`                                                         | `None`             | 断开连接并移除全部监听函数             |
| `set_sender(sender)` / `setSender(sender)`                                                      | Socket             | 设置手动传输模式的发送函数             |
| `send_raw(data)` / `sendRaw(data)`                                                              | `None`             | 发送原始文本或二进制数据              |
| `send_frame(frame)` / `sendFrame(frame)`                                                        | `None`             | 将值 JSON 编码后发送             |
| `handle_open(event=None)` / `handleOpen(event)` / `handleSocketOpen(event)`                     | `None`             | 转交传输层打开事件                 |
| `handle_message(data)` / `handleMessage(data)` / `handleSocketMessage(data)`                    | `None`             | 转交入站传输消息                  |
| `handle_close(event_or_code=None, reason=None)` / `handleClose(...)` / `handleSocketClose(...)` | `None`             | 转交传输层关闭事件                 |
| `handle_error(error)` / `handleError(error)` / `handleSocketError(error)`                       | `None`             | 转交传输层错误                   |
| `on(event, listener)`                                                                           | Socket             | 注册监听函数                    |
| `once(event, listener)`                                                                         | Socket             | 注册仅触发一次的监听函数              |
| `off(event, listener)`                                                                          | Socket             | 移除一个匹配的监听函数               |
| `remove_all_listeners(event=None)`                                                              | Socket             | 移除指定事件或全部事件的监听函数          |
| `emit(event, *args)`                                                                            | `bool`             | 调用监听函数；返回是否存在监听函数         |

### V4 属性与方法

V4 支持多个 App 同时接入，因此所有面向 App 的方法都需要传入目标 `client_id`

| 方法或属性                                                                                                      | 返回值              | 说明                                              |
|------------------------------------------------------------------------------------------------------------|------------------|-------------------------------------------------|
| `target_id` / `targetId`                                                                                   | `str \| None`    | 当前控制方 ID                                      |
| `client_ids` / `clientIds`                                                                                 | `list[str]`      | 已接入 App ID 列表                                   |
| `clients`                                                                                                  | `list[V4Client]` | 已接入 App 的状态对象列表                                 |
| `rpc`                                                                                                      | `V4Rpc`          | Socket 使用的请求/响应辅助对象                             |
| `get_client(client_id)` / `getClient(client_id)`                                                           | `V4Client \| None` | 获取指定 App 状态                                  |
| `await request_devices(client_id)` / `await requestDevices(client_id)`                                     | `dict`           | 请求设备列表、更新缓存并触发 `devices`                        |
| `send(client_id, data, options=None)`                                                                      | `asyncio.Future` | 发送自定义 V4 请求并等待响应                                |
| `ping(client_id, options=None)`                                                                            | `asyncio.Future` | 向 App 发送 ping；结果为 App 收到请求时的本地时间戳              |
| `reset_intensity(client_id, slot_id, channel, options=None)` / `resetIntensity(...)`                       | `asyncio.Future` | 将指定通道强度重置为零                                     |
| `add_intensity(client_id, slot_id, channel, value, options=None)` / `addIntensity(...)`                    | `asyncio.Future` | 增加强度                                            |
| `reduce_strength(client_id, slot_id, channel, value, options=None)` / `reduceStrength(...)`                | `asyncio.Future` | 减少强度                                            |
| `set_temp_intensity(client_id, slot_id, channel, value, duration, options=None)` / `setTempIntensity(...)` | `asyncio.Future` | 设置临时强度，`duration` 单位为毫秒                         |
| `send_pulse(client_id, slot_id, channel, duration, frames, options=None)` / `sendPulse(...)`               | `asyncio.Future` | 下发波形，`frames` 为 `list[str]` 或 `list[list[int]]` |
| `clear_pulse(client_id, slot_id, channel)` / `clearPulse(...)`                                             | `asyncio.Future` | 清理指定设备通道的全部任务；等价于按通道调用 `clear_operate()`          |
| `clear_operate(client_id, options=None)` / `clearOperate(...)`                                             | `asyncio.Future` | 清理全部、指定设备或指定通道的任务                               |

### V4 操作选项

`reset_intensity()`、`add_intensity()`、`reduce_strength()`、`set_temp_intensity()` 和 `send_pulse()` 的 `options` 支持以下字段：

| 选项          | 类型            | 说明                      |
|-------------|---------------|-------------------------|
| `timeout`   | `int`         | 本次请求等待响应的超时时间，单位为毫秒     |
| `priority`  | `0`、`1` 或 `2` | 任务优先级                   |
| `immediate` | `bool`        | 是否立即替换匹配的现有任务           |
| `version`   | `int`         | 仅 `send_pulse()`：波形格式版本 |
| `seq`       | `int`         | 仅 `send_pulse()`：波形序列号  |

`send()` 和 `ping()` 的 `options` 仅支持 `timeout`。`clear_operate()` 的 `options` 用来指定 `slot_id` / `slotId` 和 `channel`，不使用上述操作选项

### `V4Client`

`V4Client` 表示一个 V4 App 被控方，具有 `clientId` 字符串属性和可变的 `devices` 列表。它通常由 `DglabSocketV4` 管理

| 方法                                                              | 说明                    |
|-----------------------------------------------------------------|-----------------------|
| `get_device(slot_id)` / `getDevice(slot_id)`                    | 按设备 ID 查找设备           |
| `dispatch(data)`                                                | 合并支持的事件或响应；返回设备状态是否变化 |
| `replace_devices(devices)` / `replaceDevices(devices)`          | 替换设备列表                |
| `upsert_device(device)` / `upsertDevice(device)`                | 按设备 ID 新增或替换设备        |
| `remove_device(slot_id)` / `removeDevice(slot_id)`              | 按设备 ID 移除设备           |
| `patch_devices(added=None, removed=None)` / `patchDevices(...)` | 应用设备新增和移除             |
| `patch_slots(slots=None)` / `patchSlots(slots)`                 | 合并属性与设备状态更新           |
| `destroy()`                                                     | 清空设备列表                |

### `V4Rpc`

`V4Rpc(send_frame, response_timeout=None)` 是底层请求跟踪器。每个 V4 Socket 都在 `socket.rpc` 创建一个实例；直接创建时传入发送外层服务器帧的函数。构造函数也接受 `responseTimeout` 别名

| 方法                                                                 | 说明                   |
|--------------------------------------------------------------------|----------------------|
| `create_request(method, data=None)` / `createRequest(...)`         | 创建自动分配请求 ID 的 V4 请求  |
| `send(client_id, data, options=None)`                              | 发送请求，并返回对应结果的 Future |
| `send_operate(client_id, data, options=None)` / `sendOperate(...)` | 发送设备操作请求             |
| `resolve_response(client_id, response)` / `resolveResponse(...)`   | 完成匹配的待处理响应           |
| `reject_client_pending(client_id)` / `rejectClientPending(...)`    | 拒绝已断开 App 的全部待处理请求   |
| `reject_all_pending(error)` / `rejectAllPending(error)`            | 拒绝全部待处理请求            |
| `is_response(data)` / `isResponse(data)`                           | 判断数据是否为 V4 响应        |
| `get_request_id(data)` / `getRequestId(data)`                      | 读取请求 ID              |

### 导出类型与错误

以下名称可直接从 `dglab_kit_python` 导入：

| 名称                                                 | 说明                                  |
|----------------------------------------------------|-------------------------------------|
| `DglabSocket`                                      | 版本选择门面类；`raw` 属性为实际的 V3 或 V4 Socket |
| `DglabSocketBase`、`DglabSocketV3`、`DglabSocketV4`  | 基类及协议专用实现                           |
| `DglabSocketVersion`、`DGLAB_SOCKET_VERSION`        | 版本枚举及兼容别名                           |
| `DglabSocketState`、`DGLAB_SOCKET_STATE`            | 状态枚举及兼容别名                           |
| `DglabSocketDeviceType`、`DGLAB_SOCKET_DEVICE_TYPE` | 设备类型枚举及兼容别名                         |
| `V3Channel`、`V4Channel`、`V4ActionType`             | 通道与 V4 动作类型枚举                       |
| `DglabError`、`create_named_error`                  | SDK 错误类及错误工厂函数                      |

`DglabError.name` 以 `DGLAB-` 开头。连接或响应超时、断开连接、缺少被控方、非法发送和远端错误都可能抛出或以该异常拒绝 Future。`create_named_error(name, message)` 可创建同样格式的 SDK 错误

以下 `TypedDict` 可从 `dglab_kit_python.types` 导入并用于类型标注：`DglabSocketConnectResult`、`DglabSocketCloseEvent`、`V4DeviceDescriptor`、`V4DeviceInfo`、`V4DevicesGetResult`、`V4OperateOptions`、`V4AppendPulseDataOptions`、`V4ClearOperateOptions` 和 `V3WaveOptions`

## V4 常用操作

### 请求设备列表

```python
result = await socket.request_devices(client_id)

for device in result["devices"]:
    print(device["slotId"], device["name"], device["type"])
```

设备列表也会通过 `devices` 事件自动维护。使用 `socket.get_client(client_id)` 可读取当前缓存

### 探测 APP 链路

```python
from time import perf_counter

started_at = perf_counter()
received_at = await socket.ping(client_id)
rtt_ms = (perf_counter() - started_at) * 1_000

print("RTT:", rtt_ms, "ms")
print("App 收到 ping 时的本地时间戳:", received_at)
```

`ping()` 的结果是 App 收到请求时的本地时间戳；RTT 需要像上例一样由控制方按请求往返耗时计算

### 强度控制

```python
await socket.reset_intensity(client_id, slot_id, V4Channel.A)
await socket.add_intensity(client_id, slot_id, V4Channel.A, 5)
await socket.reduce_strength(client_id, slot_id, V4Channel.B, 3)
```

### 临时强度

```python
await socket.set_temp_intensity(
    client_id,
    slot_id,
    V4Channel.A,
    30,    # 临时强度。
    3_000, # 持续时间，单位为毫秒。
)
```

### 下发波形

```python
from dglab_kit_python import COYOTE_WAVEFORM, COYOTE_WAVEFORMS, V4Channel

frames = COYOTE_WAVEFORMS[COYOTE_WAVEFORM.BUBBLE]["raw"]
await socket.send_pulse(client_id, slot_id, V4Channel.A, 1_000, frames)
```

### 清理任务

```python
# 清理某个 App 的全部任务
await socket.clear_operate(client_id)

# 清理指定设备的全部通道
await socket.clear_operate(client_id, {"slot_id": slot_id})

# 清理指定设备的指定通道
await socket.clear_operate(client_id, {"slot_id": slot_id, "channel": V4Channel.A})
```

### 发送自定义请求

SDK 未封装的 V4 RPC 可通过 `send()` 调用。`send()` 会补充请求 ID，并等待具有相同请求 ID 的响应。以下示例直接发送 `devices.get`；通常应优先使用等价的 `request_devices()` 封装：

```python
result = await socket.send(
    client_id,
    {"t": "req", "m": "devices.get"},
    {"timeout": 5_000},
)

print(result)
```

## 手动传输模式

不传 `url` 时，SDK 不会主动创建 WebSocket。绑定发送函数后，将传输层生命周期事件转交给 SDK。SDK 负责协议解析、状态、事件和 V4 请求等待；应用负责网络传输

```python
import asyncio
from typing import Any

from dglab_kit_python import DglabSocket


async def main() -> None:
    socket = DglabSocket()

    def transport_send(data: str | bytes | bytearray | memoryview) -> None:
        # 转发到传输层。
        print(data)

    socket.set_sender(transport_send)
    connect_task = asyncio.create_task(socket.connect())

    # 传输层打开后调用。
    socket.handle_open()

    # 将入站消息交给 SDK。
    def on_transport_message(data: Any) -> None:
        socket.handle_message(data)

    # 协议握手完成后连接任务返回。
    result = await connect_task
    print(result["targetId"])

    def on_transport_close(code: int, reason: str) -> None:
        socket.handle_close(code, reason)

    def on_transport_error(error: Exception) -> None:
        socket.handle_error(error)


asyncio.run(main())
```

`send_raw()` 接受 `str`、`bytes`、`bytearray` 或 `memoryview`。`send_frame()` 会将 Python 值编码为紧凑 JSON 后发送。手动传输层必须保持出入站消息的顺序

## V3 旧协议

V3 为旧版单 App 协议，使用 `DGLAB_SOCKET_VERSION.V3` 显式开启。通道使用 `V3Channel.A` / `V3Channel.B`，也可传 `1` / `2`。V3 控制方法是同步的：只发送数据，返回 `None`

```python
import asyncio

from dglab_kit_python import (
    COYOTE_WAVEFORM,
    COYOTE_WAVEFORMS,
    DGLAB_SOCKET_VERSION,
    DglabSocket,
    V3Channel,
)


async def main() -> None:
    socket = DglabSocket(
        version=DGLAB_SOCKET_VERSION.V3,
        url="wss://ws.dungeon-lab.cn/",
    )

    def on_client_attached(client_id: str) -> None:
        socket.set_strength(V3Channel.A, 20)
        socket.add_strength(V3Channel.A, 3)
        socket.reduce_strength(V3Channel.B, 1)
        socket.send_pulse(
            {
                "channel": "A",
                "time": 5,
                "data": COYOTE_WAVEFORMS[COYOTE_WAVEFORM.BUBBLE]["raw"],
            }
        )

    socket.on("client-attached", on_client_attached)
    socket.on("action", lambda action: print("动作:", action))
    result = await socket.connect()
    print("控制方 ID:", result["targetId"])
    await asyncio.Event().wait()


asyncio.run(main())
```

### V3 方法

| 方法或属性                                                      | 说明                                          |
|------------------------------------------------------------|---------------------------------------------|
| `target_id` / `targetId`                                   | 当前控制方 ID                                    |
| `paired_client_id` / `pairedClientId`                      | 已配对 App ID                                  |
| `send(data)`                                               | 配对后发送协议文档定义的 V3 自定义数据                       |
| `add_strength(channel, step=1)` / `addStrength(...)`       | 增加强度；绝对步数超过 200 时抛出 `DglabError`            |
| `reduce_strength(channel, step=1)` / `reduceStrength(...)` | 减少强度                                        |
| `set_strength(channel, strength)` / `setStrength(...)`     | 设置强度                                        |
| `send_pulse(options)` / `sendPulse(options)`               | 下发波形。`options` 使用 `channel`、`time` 和 `data` |
| `clear_pulse(channel)` / `clearPulse(channel)`             | 清理通道波形                                      |

V3 线协议的具体格式请查看 [V3 协议参考](https://github.com/dungeonlab-open/dglab-kit/blob/main/reference/WebSocket%20V3%E5%8D%8F%E8%AE%AE%E6%96%87%E6%A1%A3.md)

## Waveform SDK

每个波形字典均包含本地化的 `label` 和原始帧 `raw`。使用 `COYOTE_WAVEFORM` 或 `OVC_WAVEFORM` 的枚举成员作为字典键

```python
from dglab_kit_python import COYOTE_WAVEFORM, COYOTE_WAVEFORMS

waveform = COYOTE_WAVEFORMS[COYOTE_WAVEFORM.BUBBLE]
print(waveform["label"]["cn"])
frames = waveform["raw"]
```

可从 `dglab_kit_python` 导入 `COYOTE_WAVEFORM`、`COYOTE_WAVEFORMS`、`OVC_WAVEFORM` 和 `OVC_WAVEFORMS`。需要具体枚举类时，可从 `dglab_kit_python.waveform` 导入 `CoyoteWaveform` 和 `OvcWaveform`

## V4 协议参考

请阅读 [dglab-kit V4 协议参考](https://github.com/dungeonlab-open/dglab-kit/blob/main/README.md#v4-%E5%8D%8F%E8%AE%AE%E5%8F%82%E8%80%83)

## 常见问题

### App 一直没有接入

确认控制方 `connect()` 已成功返回当前 `targetId`，并按[配对二维码文档](#生成-app-配对二维码)将其交给 App。服务地址和配对参数请以 [V4 协议参考](#v4-协议参考) 为准

### 能收到 App 接入，但控制不了设备

先调用 `await socket.request_devices(client_id)`，确认返回了可用的 `slotId`，再使用该 `slotId` 和正确的通道调用 V4 操作。操作 Future 必须使用 `await`，以便接收远端错误

### V4 请求超时或被控方断开

监听 `error` 和 `client-disconnected`。可在构造函数中设置 `response_timeout`，或为单次请求传入 `{"timeout": 毫秒}`；被控方断开时，等待该被控方的 Future 会以 `DglabError` 失败
