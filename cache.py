"""Small bounded TTL cache (size cap + expiry) used by the scanner and the API."""
import time
from collections import OrderedDict


class TTLCache:
    def __init__(self, maxsize: int, ttl: float, clock=time.monotonic):
        self.maxsize = max(1, int(maxsize))
        self.ttl = float(ttl)
        self._clock = clock
        self._data = OrderedDict()

    def get(self, key, default=None):
        entry = self._data.get(key)
        if entry is None:
            return default
        stamp, value = entry
        if self._clock() - stamp > self.ttl:
            self._data.pop(key, None)
            return default
        return value

    def set(self, key, value):
        self._data.pop(key, None)
        self._data[key] = (self._clock(), value)
        while len(self._data) > self.maxsize:
            self._data.popitem(last=False)

    def clear(self):
        self._data.clear()

    def __len__(self):
        return len(self._data)
