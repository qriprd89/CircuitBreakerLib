import time
import threading
import asyncio
from enum import Enum
from collections import deque
from typing import Callable, Optional, Any, Deque
import os
import yaml


# ----------------
# Circuit breaker states
# ----------------
class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    pass


# ----------------
# CircuitBreaker class
# ----------------
class CircuitBreaker:
    def __init__(self, fallback: Optional[Callable[..., Any]] = None):
        """
        Initialize a circuit breaker manager.
        Config is loaded dynamically from CircuitBreaker.yaml
        """
        service_path = os.getenv("SERVICE_PATH", ".")
        self.config_path = os.path.join(service_path, "CircuitBreaker.yaml")
        self._fallback = fallback

        # State storage per service
        self._services = {}
        self._lock = threading.RLock()
        self._async_lock = asyncio.Lock()

    # ----------------
    # Load config dynamically from YAML
    # ----------------
    def _load_config(self, service: str):
        with open(self.config_path, "r") as f:
            full_config = yaml.safe_load(f) or {}
        cb_config = full_config.get("circuit_breakers", {}).get(service, {})

        return {
            "failure_threshold": cb_config.get("failure_threshold", 5),
            "recovery_timeout": cb_config.get("recovery_timeout", 30),
            "window_seconds": cb_config.get("window_seconds", 60),
            "half_open_max_calls": cb_config.get("half_open_max_calls", 1),
        }

    # ----------------
    # Get or init service state
    # ----------------
    def _get_state(self, service: str):
        if service not in self._services:
            self._services[service] = {
                "state": State.CLOSED,
                "fail_times": deque(),
                "half_open_inflight": 0,
                "opened_at": None,
            }
        return self._services[service]

    # ----------------
    # Remove old failures
    # ----------------
    def _prune_failures(self, service: str, cfg: dict):
        now = time.time()
        state = self._get_state(service)
        while state["fail_times"] and state["fail_times"][0] < now - cfg["window_seconds"]:
            state["fail_times"].popleft()

    # ----------------
    # Check state before call
    # ----------------
    def _check_state(self, service: str, cfg: dict):
        state = self._get_state(service)
        now = time.time()

        if state["state"] == State.OPEN and state["opened_at"] is not None:
            if now - state["opened_at"] >= cfg["recovery_timeout"]:
                state["state"] = State.HALF_OPEN
                state["half_open_inflight"] = 0

        if state["state"] == State.HALF_OPEN:
            if state["half_open_inflight"] >= cfg["half_open_max_calls"]:
                raise CircuitOpenError(f"Circuit '{service}' is HALF_OPEN. Max probe reached.")

        if state["state"] == State.OPEN:
            raise CircuitOpenError(f"Circuit '{service}' is OPEN. Retry after {cfg['recovery_timeout']}s")

    # ----------------
    # Record failure
    # ----------------
    def _record_failure(self, service: str, cfg: dict):
        state = self._get_state(service)
        now = time.time()
        state["fail_times"].append(now)
        self._prune_failures(service, cfg)

        if state["state"] == State.HALF_OPEN or len(state["fail_times"]) >= cfg["failure_threshold"]:
            state["state"] = State.OPEN
            state["opened_at"] = now

    # ----------------
    # Record success
    # ----------------
    def _record_success(self, service: str):
        state = self._get_state(service)
        if state["state"] in [State.OPEN, State.HALF_OPEN]:
            state["state"] = State.CLOSED
            state["fail_times"].clear()
            state["half_open_inflight"] = 0

    # ----------------
    # Sync decorator
    # ----------------
    def __call__(self, service: str):
        def decorator(func: Callable):
            def wrapper(*args, **kwargs):
                cfg = self._load_config(service)
                with self._lock:
                    self._check_state(service, cfg)
                    state = self._get_state(service)
                    if state["state"] == State.HALF_OPEN:
                        state["half_open_inflight"] += 1

                try:
                    result = func(*args, **kwargs)
                except Exception:
                    with self._lock:
                        self._record_failure(service, cfg)
                    if self._fallback:
                        return self._fallback(*args, **kwargs)
                    raise
                else:
                    with self._lock:
                        self._record_success(service)
                    return result
            return wrapper
        return decorator

    # ----------------
    # Async decorator
    # ----------------
    def async_wrap(self, service: str):
        def decorator(func: Callable):
            async def wrapper(*args, **kwargs):
                cfg = self._load_config(service)
                async with self._async_lock:
                    self._check_state(service, cfg)
                    state = self._get_state(service)
                    if state["state"] == State.HALF_OPEN:
                        state["half_open_inflight"] += 1
                try:
                    result = await func(*args, **kwargs)
                except Exception:
                    async with self._async_lock:
                        self._record_failure(service, cfg)
                    if self._fallback:
                        return await self._fallback(*args, **kwargs)
                    raise
                else:
                    async with self._async_lock:
                        self._record_success(service)
                    return result
            return wrapper
        return decorator

    # ----------------
    # Utility: Get current state
    # ----------------
    def status(self, service: str) -> str:
        state = self._get_state(service)
        return state["state"].value
