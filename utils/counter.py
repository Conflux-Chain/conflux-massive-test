import threading

class AtomicCounter:
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.value += 1
            return self.value

    def get(self):
        with self._lock:
            return self.value
        
_counters = {}
_global_lock = threading.Lock()

def get_global_counter(key: str):
    with _global_lock:
        if key not in _counters:
            _counters[key] = AtomicCounter()
        return _counters[key]