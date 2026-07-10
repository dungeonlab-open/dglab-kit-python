from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

Listener = Callable[..., Any]


class EventEmitter:
    def __init__(self) -> None:
        self._listeners: dict[str, list[Listener]] = defaultdict(list)

    def on(self, event: str, listener: Listener) -> EventEmitter:
        self._listeners[event].append(listener)
        return self

    def once(self, event: str, listener: Listener) -> EventEmitter:
        def wrapper(*args: Any) -> Any:
            self.off(event, wrapper)
            return listener(*args)

        self.on(event, wrapper)
        return self

    def off(self, event: str, listener: Listener) -> EventEmitter:
        listeners = self._listeners.get(event)
        if not listeners:
            return self
        try:
            listeners.remove(listener)
        except ValueError:
            pass
        if not listeners:
            self._listeners.pop(event, None)
        return self

    def emit(self, event: str, *args: Any) -> bool:
        listeners = list(self._listeners.get(event, ()))
        for listener in listeners:
            result = listener(*args)
            if inspect.isawaitable(result):
                self._schedule(result)
        return bool(listeners)

    def remove_all_listeners(self, event: str | None = None) -> EventEmitter:
        if event is None:
            self._listeners.clear()
        else:
            self._listeners.pop(event, None)
        return self

    @staticmethod
    def _schedule(awaitable: Awaitable[Any]) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(awaitable)
            return
        loop.create_task(awaitable)
