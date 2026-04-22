# biochar_app/scripts/cache.py

from __future__ import annotations

from collections.abc import Callable, Hashable

import pandas as pd
import psutil


def sizeof_df(df: pd.DataFrame) -> int:
    """
    Return the approximate in-memory size of a DataFrame in bytes,
    summing all its columns (deep=True counts object-dtypes accurately).
    """
    return int(df.memory_usage(deep=True).sum())


def available_memory_bytes() -> int:
    """
    Return how many bytes of RAM are currently available on this machine.
    """
    return int(psutil.virtual_memory().available)


class MemoryBoundedCache:
    def __init__(
        self,
        max_bytes: int,
        size_fn: Callable[[pd.DataFrame], int] = sizeof_df,
    ) -> None:
        self.max_bytes: int = max_bytes
        self.size_fn: Callable[[pd.DataFrame], int] = size_fn
        self._data: dict[Hashable, tuple[pd.DataFrame, int]] = {}
        self._order: list[Hashable] = []
        self._used: int = 0

    def get(self, key: Hashable) -> pd.DataFrame | None:
        item = self._data.get(key)
        if item is not None:
            self._order.remove(key)
            self._order.insert(0, key)
            return item[0]
        return None

    def set(self, key: Hashable, value: pd.DataFrame) -> None:
        size = self.size_fn(value)

        if size > self.max_bytes:
            return

        existing = self._data.get(key)
        if existing is not None:
            _, old_size = existing
            self._used -= old_size
            self._order.remove(key)
            del self._data[key]

        while self._used + size > self.max_bytes and self._order:
            old = self._order.pop()
            _, old_size = self._data.pop(old)
            self._used -= old_size

        self._data[key] = (value, size)
        self._order.insert(0, key)
        self._used += size