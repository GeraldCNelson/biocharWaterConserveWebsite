# biochar_app/cache.py

import threading
from collections import OrderedDict
import pandas as pd
import psutil
from pympler import asizeof

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
    return psutil.virtual_memory().available

class MemoryBoundedCache:
    def __init__(self, max_bytes: int, size_fn=sizeof_df):
        self.max_bytes = max_bytes
        self.size_fn = size_fn
        self._data = {}            # key -> (value, size_in_bytes)
        self._order = []           # LRU list of keys
        self._used = 0

    def get(self, key):
        item = self._data.get(key)
        if item:
            # move to front of LRU
            self._order.remove(key)
            self._order.insert(0, key)
            return item[0]
        return None

    def set(self, key, value):
        size = self.size_fn(value)
        # if single item bigger than whole cache, skip
        if size > self.max_bytes:
            return
        # evict until we have room
        while self._used + size > self.max_bytes and self._order:
            old = self._order.pop()
            _, old_size = self._data.pop(old)
            self._used -= old_size
        # store new
        self._data[key] = (value, size)
        self._order.insert(0, key)
        self._used += size