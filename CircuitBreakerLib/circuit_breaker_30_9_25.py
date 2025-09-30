import time
import threading
import asyncio
import os
import yaml
import logging
import concurrent.futures
from enum import Enum
from collections import deque
from typing import Callable, Optional, Any, Dict


# ----------------
# Circuit breaker states
# ----------------
class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(
        self,
        on_state_change: Optional[Callable[[str, State, State, dict], None]] = None,
        on_event: Optional[Callable[[str, str, dict], None]] = None,
        config_file: str = "CircuitBreaker.yaml",
    ):
        self._services: Dict[str, dict] = {}
        self._locks: Dict[str, threading.RLock] = {}
        self._async_locks: Dict[str, asyncio.Lock] = {}
        self._on_state_change = on_state_change
        self._on_event = on_event
        self.config_file = os.path.join(os.getenv("SERVICE_PATH", "."), config_file)
        self._cfg_cache = {}
        self._cfg_mtime = None
        self._log = logging.getLogger("CircuitBreaker")

    # ----------------
    # Load config dynamically
    # ----------------
    def _load_config(self, project: str, service: str):
        try:
            mtime = os.path.getmtime(self.config_file)
            if self._cfg_mtime != mtime:
                with open(self.config_file, "r") as f:
                    full_config = yaml.safe_load(f) or {}
                self._cfg_cache = full_config
                self._cfg_mtime = mtime
        except FileNotFoundError:
            self._cfg_cache = {}

        cb_config = self._cfg_cache.get(project, {}).get(service, {})
        return {
            "failure_threshold": cb_config.get("failure_threshold", 5),
            "recovery_timeout": cb_config.get("recovery_timeout", 30),
            "window_seconds": cb_config.get("window_seconds", 60),
            "half_open_max_calls": cb_config.get("half_open_max_calls", 1),
            "response_timeout": cb_config.get("response_timeout", None),
        }

    # ----------------
    # Get/init service state
    # ----------------
    def _get_state(self, key: str):
        if key not in self._services:
            self._services[key] = {
                "state": State.CLOSED,
                "fail_times": deque(),
                "half_open_inflight": 0,
                "opened_at": None,
            }
        return self._services[key]

    # ----------------
    # State transitions
    # ----------------
    def _set_state(self, key: str, new_state: State, ctx: dict):
        state = self._get_state(key)
        old = state["state"]
        if old != new_state:
            state["state"] = new_state
            if self._on_state_change:
                self._on_state_change(key, old, new_state, ctx)

    def _prune_failures(self, key: str, cfg: dict):
        now = time.time()
        state = self._get_state(key)
        while state["fail_times"] and state["fail_times"][0] < now - cfg["window_seconds"]:
            state["fail_times"].popleft()

    def _check_state(self, key: str, cfg: dict):
        state = self._get_state(key)
        now = time.time()

        if state["state"] == State.OPEN and state["opened_at"] is not None:
            if now - state["opened_at"] >= cfg["recovery_timeout"]:
                self._set_state(key, State.HALF_OPEN, {"ts": now})
                state["half_open_inflight"] = 0

        if state["state"] == State.HALF_OPEN:
            if state["half_open_inflight"] >= cfg["half_open_max_calls"]:
                raise CircuitOpenError(f"Circuit '{key}' is HALF_OPEN. Max probe reached.")

        if state["state"] == State.OPEN:
            raise CircuitOpenError(f"Circuit '{key}' is OPEN. Retry later")

    def _record_failure(self, key: str, cfg: dict):
        state = self._get_state(key)
        now = time.time()
        state["fail_times"].append(now)
        self._prune_failures(key, cfg)

        if state["state"] == State.HALF_OPEN or len(state["fail_times"]) >= cfg["failure_threshold"]:
            state["opened_at"] = now
            self._set_state(key, State.OPEN, {"ts": now})

    def _record_success(self, key: str, cfg: dict):
        state = self._get_state(key)
        if state["state"] in [State.OPEN, State.HALF_OPEN]:
            self._set_state(key, State.CLOSED, {"ts": time.time()})
            state["fail_times"].clear()
            state["half_open_inflight"] = 0

    # ----------------
    # Sync decorator
    # ----------------
    def __call__(self, project: str, service: str, fallback: Optional[Callable[..., Any]] = None):
        def decorator(func: Callable):
            def wrapper(*args, **kwargs):
                cfg = self._load_config(project, service)
                key = f"{project}:{service}"
                lock = self._locks.setdefault(key, threading.RLock())
                with lock:
                    self._check_state(key, cfg)
                    state = self._get_state(key)
                    if state["state"] == State.HALF_OPEN:
                        state["half_open_inflight"] += 1

                try:
                    if cfg["response_timeout"]:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(func, *args, **kwargs)
                            result = future.result(timeout=cfg["response_timeout"])
                    else:
                        result = func(*args, **kwargs)

                    with lock:
                        self._record_success(key, cfg)
                    return result
                except Exception:
                    with lock:
                        self._record_failure(key, cfg)
                    if fallback:
                        return fallback(*args, **kwargs)
                    raise
                finally:
                    with lock:
                        state = self._get_state(key)
                        if state["state"] == State.HALF_OPEN and state["half_open_inflight"] > 0:
                            state["half_open_inflight"] -= 1
            return wrapper
        return decorator

    # ----------------
    # Async decorator
    # ----------------
    def async_wrap(self, project: str, service: str, fallback: Optional[Callable[..., Any]] = None):
        def decorator(func: Callable):
            async def wrapper(*args, **kwargs):
                cfg = self._load_config(project, service)
                key = f"{project}:{service}"
                alock = self._async_locks.setdefault(key, asyncio.Lock())
                async with alock:
                    self._check_state(key, cfg)
                    state = self._get_state(key)
                    if state["state"] == State.HALF_OPEN:
                        state["half_open_inflight"] += 1
                try:
                    if cfg["response_timeout"]:
                        result = await asyncio.wait_for(func(*args, **kwargs), timeout=cfg["response_timeout"])
                    else:
                        result = await func(*args, **kwargs)

                    async with alock:
                        self._record_success(key, cfg)
                    return result
                except Exception:
                    async with alock:
                        self._record_failure(key, cfg)
                    if fallback:
                        return fallback(*args, **kwargs)
                    raise
                finally:
                    async with alock:
                        state = self._get_state(key)
                        if state["state"] == State.HALF_OPEN and state["half_open_inflight"] > 0:
                            state["half_open_inflight"] -= 1
            return wrapper
        return decorator

    # ----------------
    # Utility
    # ----------------
    def status(self, project: str, service: str) -> str:
        key = f"{project}:{service}"
        return self._get_state(key)["state"].value
