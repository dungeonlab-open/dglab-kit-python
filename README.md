<div align="center">

<img src="https://raw.githubusercontent.com/dungeonlab-open/dglab-kit-python/main/dglab.webp" alt="DG-LAB Logo" width="128" />

# DGLAB KIT - Python

English | [简体中文](https://github.com/dungeonlab-open/dglab-kit-python/blob/main/README_CN.md)

</div>

DGLAB KIT is a Python SDK for the **DG-LAB App**. It uses `asyncio` to pair with the app over WebSocket and control the local devices exposed by the app.

The SDK provides two main components:

- **Socket SDK**: Connect to V3 / V4 Socket servers for app pairing, device discovery, Channel Intensity control, Pulse delivery, task cleanup, and action feedback.
- **Pulse SDK**: Built-in Coyote and Opossum Pulse data, ready to use with V3 / V4 Pulse operations.

> Use V4 for new integrations. V4 supports one controller connected to multiple app clients, device list synchronization, and request/response handling. V3 is provided for compatibility with legacy controllers.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Core Concepts](#core-concepts)
- [Quick Start: Control the App with V4](#quick-start-control-the-app-with-v4)
- [Generate an App Pairing QR Code](#generate-an-app-pairing-qr-code)
- [Socket SDK API](#socket-sdk-api)
- [Common V4 Operations](#common-v4-operations)
- [Manual Transport Mode](#manual-transport-mode)
- [Legacy V3 Protocol](#legacy-v3-protocol)
- [Pulse SDK](#pulse-sdk)
- [V4 Protocol Reference](#v4-protocol-reference)
- [Troubleshooting](#troubleshooting)

## Requirements

- Python 3.13 or later.
- If you already have a WebSocket implementation, use [Manual Transport Mode](#manual-transport-mode).

## Installation

Install the SDK in your project:

```bash
pip install dglab-kit-python
```

## Core Concepts

| Term | Description |
|------|-------------|
| Controller | The Python application running `DglabSocket`, such as a server or desktop controller. |
| App client | The DG-LAB App attached to the current controller. |
| `targetId` | The pairing ID assigned when the controller connects. The app uses it to attach. |
| `clientId` | The connection ID of a specific V4 app client. Required when sending commands to that app. |
| `slotId` | The ID of a device exposed by the app. Used to identify the device in device operations. |
| `channel` | The device channel. V4 uses `V4Channel.A` / `V4Channel.B`, or `0` / `1`. |

The controller connects to the Socket server, and the app attaches using the controller's `targetId`. For the connection topology, device updates, and complete protocol details, see the [dglab-kit V4 protocol reference](https://github.com/dungeonlab-open/dglab-kit/blob/main/README.md#v4-protocol-reference).

## Quick Start: Control the App with V4

This example connects to a V4 server, obtains a pairing ID, waits for an app to attach, discovers devices, and performs Channel Intensity and Pulse operations.

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
    socket.on("error", lambda error: print("Socket error:", error))
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
    print("Controller ID:", result["targetId"])
    assert socket.state is DGLAB_SOCKET_STATE.WAITING_FOR_PEER

    # Keep the connection alive to receive events.
    await asyncio.Event().wait()


asyncio.run(main())
```

Listeners can be regular functions or `async def` functions. Async listeners are scheduled on the current event loop.

## Generate an App Pairing QR Code

Once `connect()` returns a `targetId`, follow the [dglab-kit pairing QR code guide](https://github.com/dungeonlab-open/dglab-kit/blob/main/README.md#generate-an-app-pairing-qr-code) to generate a QR code or pairing link for the app.

## Socket SDK API

### Constructor

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

| Option | Default | Description |
|--------|---------|-------------|
| `url` | `None` | WebSocket server URL. Omit to use manual transport mode. |
| `protocols` | `None` | A WebSocket subprotocol string or list of subprotocols. |
| `connect_timeout` | `8_000` ms | Maximum time `connect()` waits for the protocol handshake. |
| `response_timeout` | `8_000` ms | Default V4 response timeout. |
| `version` | `DGLAB_SOCKET_VERSION.V4` | `V4` / `"v4"` or `V3` / `"v3"`. |

The constructor also accepts the camelCase aliases `connectTimeout` and `responseTimeout`. If both naming styles are supplied, the snake_case value takes precedence.

### `connect()` Result

```python
result = await socket.connect()
target_id = result["targetId"]
```

| Field | Description |
|-------|-------------|
| `targetId` | The current controller ID. App clients use it to attach. |

Calling `connect()` again after a successful connection returns the current connection result immediately.

### Socket States

| State | Description |
|-------|-------------|
| `DGLAB_SOCKET_STATE.IDLE` | Not connected. |
| `DGLAB_SOCKET_STATE.CONNECTING` | Establishing the connection. |
| `DGLAB_SOCKET_STATE.WAITING_FOR_PEER` | Connected and waiting for an app to attach. |
| `DGLAB_SOCKET_STATE.PAIRED` | At least one V4 app client, or the V3 peer, is paired. |
| `DGLAB_SOCKET_STATE.DISCONNECTED` | Disconnected. |

`DglabSocketState` also provides equivalent PascalCase member names. `DGLAB_SOCKET_STATE` is a compatibility alias for this enum class.

### Common Events

`DglabSocket` is an event emitter. `on()`, `once()`, `off()`, and `remove_all_listeners()` return the current socket for chaining. Use `emit()` to publish custom application events.

| Event | Listener Arguments | Description |
|-------|--------------------|-------------|
| `state` | `(state, previous)` | Socket state changed. |
| `open` | `(event)` | WebSocket transport opened. |
| `close` | `(event)` | Transport closed. The event includes `code`, `reason`, `wasClean`, and the original `event`. |
| `error` | `(error)` | Transport, timeout, or protocol error. |
| `message` | `(text, raw)` | Raw message received, before parsing. |
| `frame` | `(frame)` | Parsed JSON frame. |
| `data` | `(data, client_id=None)` | Application data received. |
| `action` | `(action)` | App action notification received. |
| `device` | `(device, client_id)` | A device was added, changed, or removed. |
| `devices` | `(devices, client_id)` | The current complete V4 device list changed. |
| `client-attached` | `(client_id)` | An app client attached. |
| `client-disconnected` | `(client_id)` | An app client disconnected. |

### Common Methods

| Method or Property | Returns | Description |
|--------------------|---------|-------------|
| `state` | `DglabSocketState` | Current connection state. |
| `await connect()` | `dict` | Connect and wait for the protocol handshake. |
| `await disconnect(code=None, reason=None)` | `None` | Disconnect and cancel any pending `connect()` call. |
| `await destroy(code=None, reason=None)` | `None` | Disconnect and remove all listeners. |
| `set_sender(sender)` / `setSender(sender)` | Socket | Set the send function for manual transport mode. |
| `send_raw(data)` / `sendRaw(data)` | `None` | Send raw text or binary data. |
| `send_frame(frame)` / `sendFrame(frame)` | `None` | JSON-encode a value and send it. |
| `handle_open(event=None)` / `handleOpen(event)` / `handleSocketOpen(event)` | `None` | Forward a transport open event. |
| `handle_message(data)` / `handleMessage(data)` / `handleSocketMessage(data)` | `None` | Forward an incoming transport message. |
| `handle_close(event_or_code=None, reason=None)` / `handleClose(...)` / `handleSocketClose(...)` | `None` | Forward a transport close event. |
| `handle_error(error)` / `handleError(error)` / `handleSocketError(error)` | `None` | Forward a transport error. |
| `on(event, listener)` | Socket | Register a listener. |
| `once(event, listener)` | Socket | Register a listener that runs once. |
| `off(event, listener)` | Socket | Remove one matching listener. |
| `remove_all_listeners(event=None)` | Socket | Remove listeners for one event, or all listeners. |
| `emit(event, *args)` | `bool` | Invoke listeners and return whether any listeners were registered. |

### V4 Properties and Methods

V4 supports multiple app clients, so every method targeting an app requires that app's `client_id`.

| Method or Property | Returns | Description |
|--------------------|---------|-------------|
| `target_id` / `targetId` | `str \| None` | Current controller ID. |
| `client_ids` / `clientIds` | `list[str]` | IDs of attached app clients. |
| `clients` | `list[V4Client]` | State objects for attached app clients. |
| `rpc` | `V4Rpc` | The socket's request/response helper. |
| `get_client(client_id)` / `getClient(client_id)` | `V4Client \| None` | Get the state of a specific app client. |
| `await request_devices(client_id)` / `await requestDevices(client_id)` | `dict` | Request the device list, update the cache, and emit `devices`. |
| `send(client_id, data, options=None)` | `asyncio.Future` | Send a custom V4 request and wait for its response. |
| `ping(client_id, options=None)` | `asyncio.Future` | Ping the app; returns the app's local timestamp when it received the request. |
| `reset_intensity(client_id, slot_id, channel, options=None)` / `resetIntensity(...)` | `asyncio.Future` | Reset Channel Intensity to zero. |
| `add_intensity(client_id, slot_id, channel, value, options=None)` / `addIntensity(...)` | `asyncio.Future` | Increase Channel Intensity. |
| `reduce_strength(client_id, slot_id, channel, value, options=None)` / `reduceStrength(...)` | `asyncio.Future` | Decrease Channel Intensity. |
| `set_temp_intensity(client_id, slot_id, channel, value, duration, options=None)` / `setTempIntensity(...)` | `asyncio.Future` | Apply a Temporary Intensity Change. `duration` is in milliseconds. |
| `send_pulse(client_id, slot_id, channel, duration, frames, options=None)` / `sendPulse(...)` | `asyncio.Future` | Send a Pulse. `frames` is a `list[str]` or `list[list[int]]`. |
| `clear_pulse(client_id, slot_id, channel)` / `clearPulse(...)` | `asyncio.Future` | Clear all tasks on the specified device channel. Equivalent to calling `clear_operate()` for that channel. |
| `clear_operate(client_id, options=None)` / `clearOperate(...)` | `asyncio.Future` | Clear all tasks, or tasks for a specific device or channel. |

### V4 Operation Options

The `options` argument for `reset_intensity()`, `add_intensity()`, `reduce_strength()`, `set_temp_intensity()`, and `send_pulse()` supports the following fields:

| Option | Type | Description |
|--------|------|-------------|
| `timeout` | `int` | Response timeout for this request, in milliseconds. |
| `priority` | `0`, `1`, or `2` | Task priority. |
| `immediate` | `bool` | Whether to replace matching existing tasks immediately. |
| `version` | `int` | Pulse format version; `send_pulse()` only. |
| `seq` | `int` | Pulse sequence number; `send_pulse()` only. |

The `options` argument for `send()` and `ping()` supports only `timeout`. For `clear_operate()`, `options` specifies `slot_id` / `slotId` and `channel`; the operation options above do not apply.

### `V4Client`

`V4Client` represents a V4 app client. It has a string `clientId` property and a mutable `devices` list, and is typically managed by `DglabSocketV4`.

| Method | Description |
|--------|-------------|
| `get_device(slot_id)` / `getDevice(slot_id)` | Find a device by ID. |
| `dispatch(data)` | Merge supported events or responses; returns whether device state changed. |
| `replace_devices(devices)` / `replaceDevices(devices)` | Replace the device list. |
| `upsert_device(device)` / `upsertDevice(device)` | Add or replace a device by ID. |
| `remove_device(slot_id)` / `removeDevice(slot_id)` | Remove a device by ID. |
| `patch_devices(added=None, removed=None)` / `patchDevices(...)` | Apply device additions and removals. |
| `patch_slots(slots=None)` / `patchSlots(slots)` | Merge property and device state updates. |
| `destroy()` | Clear the device list. |

### `V4Rpc`

`V4Rpc(send_frame, response_timeout=None)` is the low-level request tracker. Every V4 socket creates an instance at `socket.rpc`. When creating one directly, provide a function that sends outer server frames. The constructor also accepts the `responseTimeout` alias.

| Method | Description |
|--------|-------------|
| `create_request(method, data=None)` / `createRequest(...)` | Create a V4 request with an automatically assigned request ID. |
| `send(client_id, data, options=None)` | Send a request and return a Future for its result. |
| `send_operate(client_id, data, options=None)` / `sendOperate(...)` | Send a device operation request. |
| `resolve_response(client_id, response)` / `resolveResponse(...)` | Resolve the matching pending response. |
| `reject_client_pending(client_id)` / `rejectClientPending(...)` | Reject all pending requests for a disconnected app client. |
| `reject_all_pending(error)` / `rejectAllPending(error)` | Reject all pending requests. |
| `is_response(data)` / `isResponse(data)` | Check whether data is a V4 response. |
| `get_request_id(data)` / `getRequestId(data)` | Read the request ID. |

### Exported Types and Errors

The following names can be imported directly from `dglab_kit_python`:

| Name | Description |
|------|-------------|
| `DglabSocket` | Version-selecting facade. Its `raw` property exposes the underlying V3 or V4 socket. |
| `DglabSocketBase`, `DglabSocketV3`, `DglabSocketV4` | Base class and protocol-specific implementations. |
| `DglabSocketVersion`, `DGLAB_SOCKET_VERSION` | Version enum and compatibility alias. |
| `DglabSocketState`, `DGLAB_SOCKET_STATE` | State enum and compatibility alias. |
| `DglabSocketDeviceType`, `DGLAB_SOCKET_DEVICE_TYPE` | Device type enum and compatibility alias. |
| `V3Channel`, `V4Channel`, `V4ActionType` | Channel and V4 action type enums. |
| `DglabError`, `create_named_error` | SDK error class and error factory. |

`DglabError.name` starts with `DGLAB-`. Connection or response timeouts, disconnections, missing app clients, invalid sends, and remote errors may raise this exception or cause a Future to fail with it. Use `create_named_error(name, message)` to create an SDK error in the same format.

The following `TypedDict` types are available from `dglab_kit_python.types` for type annotations: `DglabSocketConnectResult`, `DglabSocketCloseEvent`, `V4DeviceDescriptor`, `V4DeviceInfo`, `V4DevicesGetResult`, `V4OperateOptions`, `V4AppendPulseDataOptions`, `V4ClearOperateOptions`, and `V3WaveOptions`.

## Common V4 Operations

### Request the Device List

```python
result = await socket.request_devices(client_id)

for device in result["devices"]:
    print(device["slotId"], device["name"], device["type"])
```

The device list is also maintained automatically through `devices` events. Use `socket.get_client(client_id)` to access the current cache.

### Measure App Round-Trip Latency

```python
from time import perf_counter

started_at = perf_counter()
received_at = await socket.ping(client_id)
rtt_ms = (perf_counter() - started_at) * 1_000

print("RTT:", rtt_ms, "ms")
print("App local timestamp when ping was received:", received_at)
```

`ping()` returns the app's local timestamp when it received the request. Calculate round-trip time on the controller by measuring the elapsed time, as shown above.

### Channel Intensity Control

```python
await socket.reset_intensity(client_id, slot_id, V4Channel.A)
await socket.add_intensity(client_id, slot_id, V4Channel.A, 5)
await socket.reduce_strength(client_id, slot_id, V4Channel.B, 3)
```

### Temporary Intensity Change

```python
await socket.set_temp_intensity(
    client_id,
    slot_id,
    V4Channel.A,
    30,    # Temporary intensity.
    3_000, # Duration in milliseconds.
)
```

### Send a Pulse

```python
from dglab_kit_python import COYOTE_WAVEFORM, COYOTE_WAVEFORMS, V4Channel

frames = COYOTE_WAVEFORMS[COYOTE_WAVEFORM.BUBBLE]["raw"]
await socket.send_pulse(client_id, slot_id, V4Channel.A, 1_000, frames)
```

### Clear Tasks

```python
# Clear all tasks for an app client
await socket.clear_operate(client_id)

# Clear tasks on all channels of a specific device
await socket.clear_operate(client_id, {"slot_id": slot_id})

# Clear tasks on a specific device channel
await socket.clear_operate(client_id, {"slot_id": slot_id, "channel": V4Channel.A})
```

### Send a Custom Request

Use `send()` for V4 RPCs without a dedicated SDK method. It adds a request ID and waits for a response with the same ID. This example sends `devices.get` directly; for typical use, prefer the equivalent `request_devices()` method:

```python
result = await socket.send(
    client_id,
    {"t": "req", "m": "devices.get"},
    {"timeout": 5_000},
)

print(result)
```

## Manual Transport Mode

When `url` is omitted, the SDK does not create a WebSocket connection. Bind a send function and forward transport lifecycle events to the SDK. The SDK handles protocol parsing, state, events, and V4 response tracking; your application handles network transport.

```python
import asyncio
from typing import Any

from dglab_kit_python import DglabSocket


async def main() -> None:
    socket = DglabSocket()

    def transport_send(data: str | bytes | bytearray | memoryview) -> None:
        # Forward data to the transport.
        print(data)

    socket.set_sender(transport_send)
    connect_task = asyncio.create_task(socket.connect())

    # Call when the transport opens.
    socket.handle_open()

    # Forward incoming messages to the SDK.
    def on_transport_message(data: Any) -> None:
        socket.handle_message(data)

    # The connection task returns after the protocol handshake completes.
    result = await connect_task
    print(result["targetId"])

    def on_transport_close(code: int, reason: str) -> None:
        socket.handle_close(code, reason)

    def on_transport_error(error: Exception) -> None:
        socket.handle_error(error)


asyncio.run(main())
```

`send_raw()` accepts `str`, `bytes`, `bytearray`, or `memoryview`. `send_frame()` encodes Python values as compact JSON before sending. A manual transport must preserve the order of incoming and outgoing messages.

## Legacy V3 Protocol

Enable the legacy, single-app V3 protocol explicitly with `DGLAB_SOCKET_VERSION.V3`. Use `V3Channel.A` / `V3Channel.B`, or `1` / `2`, for channels. V3 control methods are synchronous: they send data and return `None`.

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
    socket.on("action", lambda action: print("Action:", action))
    result = await socket.connect()
    print("Controller ID:", result["targetId"])
    await asyncio.Event().wait()


asyncio.run(main())
```

### V3 Methods

| Method or Property | Description |
|--------------------|-------------|
| `target_id` / `targetId` | Current controller ID. |
| `paired_client_id` / `pairedClientId` | ID of the paired app client. |
| `send(data)` | Send custom V3 data as defined by the protocol, after pairing. |
| `add_strength(channel, step=1)` / `addStrength(...)` | Increase Channel Intensity. Raises `DglabError` if the absolute step exceeds 200. |
| `reduce_strength(channel, step=1)` / `reduceStrength(...)` | Decrease Channel Intensity. |
| `set_strength(channel, strength)` / `setStrength(...)` | Set Channel Intensity. |
| `send_pulse(options)` / `sendPulse(...)` | Send a Pulse. `options` contains `channel`, `time`, and `data`. |
| `clear_pulse(channel)` / `clearPulse(channel)` | Clear Pulses on the channel. |

For V3 usage and message parameters, see the [legacy V3 protocol reference](https://github.com/dungeonlab-open/dglab-kit/blob/main/README.md#legacy-v3-protocol).

## Pulse SDK

Each Pulse dictionary contains a localized `label` and raw frames in `raw`. Use members of `COYOTE_WAVEFORM` or `OVC_WAVEFORM` as dictionary keys.

```python
from dglab_kit_python import COYOTE_WAVEFORM, COYOTE_WAVEFORMS

waveform = COYOTE_WAVEFORMS[COYOTE_WAVEFORM.BUBBLE]
print(waveform["label"]["cn"])
frames = waveform["raw"]
```

Import `COYOTE_WAVEFORM`, `COYOTE_WAVEFORMS`, `OVC_WAVEFORM`, and `OVC_WAVEFORMS` from `dglab_kit_python`. For the enum classes themselves, import `CoyoteWaveform` and `OvcWaveform` from `dglab_kit_python.waveform`.

## V4 Protocol Reference

See the [dglab-kit V4 protocol reference](https://github.com/dungeonlab-open/dglab-kit/blob/main/README.md#v4-protocol-reference).

## Troubleshooting

### The App Does Not Attach

Confirm that the controller's `connect()` call returned the current `targetId`, then pass it to the app as described in the [pairing QR code guide](#generate-an-app-pairing-qr-code). See the [V4 protocol reference](#v4-protocol-reference) for server URLs and pairing parameters.

### The App Attaches, but Device Control Does Not Work

Call `await socket.request_devices(client_id)` first and confirm that the response contains a valid `slotId`. Use that ID and the correct channel for V4 operations. Always `await` operation Futures to receive remote errors.

### A V4 Request Times Out or the App Client Disconnects

Listen for `error` and `client-disconnected`. Set `response_timeout` in the constructor, or pass `{"timeout": milliseconds}` for an individual request. If an app client disconnects, its pending Futures fail with `DglabError`.
