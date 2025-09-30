import socket
import time
import threading
import os
import yaml
import logging
from enum import Enum
from collections import deque
from typing import Callable, Optional, Any, Dict
import concurrent.futures

# Circuit state
class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitOpenError(Exception):
    pass

class ResponseTimeoutError(Exception):
    """Raised when the API call exceeds the configured response timeout."""
    pass

# Circuit Breake
class CircuitBreaker:
    def __init__(
        self,
        on_state_change: Optional[Callable[[str, State, State, dict], None]] = None,
        config_file: str = "CircuitBreaker.yaml",
    ):
        self._circuits: Dict[str, dict] = {}       # API-level states
        self._locks: Dict[str, threading.RLock] = {}
        self._on_state_change = on_state_change
        self.config_file = os.path.join(os.getenv("SERVICE_PATH", "."), config_file)
        self._cfg_cache = {}
        self._cfg_mtime = None
        self._log = logging.getLogger("CircuitBreaker")

    # Load config
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
            "half_open_max_calls": cb_config.get("half_open_max_calls", 1),
            "window_seconds": cb_config.get("window_seconds", 60),
            "response_timeout": cb_config.get("response_timeout", None),
            "service_port": cb_config.get("service_port", None),
            "local_host": cb_config.get("local_host", "localhost"),
            "docker_host": cb_config.get("docker_host", "host.docker.internal"),
        }

    # Circuit keys & states
    def _circuit_key(self, project: str, service: str, api: str):
        return f"{project}:{service}:{api}"

    def _service_prefix(self, project: str, service: str):
        return f"{project}:{service}:"

    def _get_state(self, key: str):
        if key not in self._circuits:
            self._circuits[key] = {
                "state": State.CLOSED,
                "fail_times": deque(),
                "half_open_inflight": 0,
                "opened_at": None,
            }
        return self._circuits[key]

    def _set_state(self, key: str, new_state: State, ctx: dict):
        state = self._get_state(key)
        old = state["state"]
        if old != new_state:
            state["state"] = new_state
            if new_state == State.OPEN:
                state["opened_at"] = ctx.get("ts", time.time())
            if old == State.HALF_OPEN and new_state == State.CLOSED:
                state["half_open_inflight"] = 0
                state["fail_times"].clear()
            if self._on_state_change:
                self._on_state_change(key, old, new_state, ctx)

    # Prune old failures
    def _prune_failures(self, key: str, cfg: dict):
        now = time.time()
        state = self._get_state(key)
        while state["fail_times"] and state["fail_times"][0] < now - cfg["window_seconds"]:
            state["fail_times"].popleft()

    def _is_docker(self):
        return os.path.exists("/.dockerenv") or os.path.exists("/proc/self/cgroup")
        
    # Service-level port check with recovery timeout
    def _check_service_port(self, project: str, service: str, cfg: dict):
        port = cfg.get("service_port")
        if not port:
            return True

        service_prefix = self._service_prefix(project, service)
        now = time.time()

        # Track last down timestamp per service
        if not hasattr(self, "_last_down"):
            self._last_down = {}
        last_down_ts = self._last_down.get(service_prefix)
        recovery_timeout = cfg.get("recovery_timeout", 30)

        if last_down_ts and now - last_down_ts < recovery_timeout:
            # Skip immediate retry, raise CircuitOpenError
            raise CircuitOpenError(
                f"Service '{project}:{service}' port {port} DOWN (within recovery_timeout)"
            )

        try:
            host = cfg.get("docker_host") if self._is_docker() else cfg.get("local_host")
            with socket.create_connection((host, port), timeout=1):
                # Service is UP, reset last_down timestamp
                self._last_down.pop(service_prefix, None)
                return True
        except Exception:
            # Mark circuits as OPEN
            for circuit_key in list(self._circuits.keys()):
                if circuit_key.startswith(service_prefix):
                    self._set_state(circuit_key, State.OPEN, {
                        "ts": now,
                        "reason": f"port_{port}_down"
                    })

            # Create dummy if none exist
            if not any(k.startswith(service_prefix) for k in self._circuits):
                dummy_key = f"{project}:{service}:_dummy_api"
                self._get_state(dummy_key)
                self._set_state(dummy_key, State.OPEN, {"ts": now, "reason": f"port_{port}_down"})

            # Record down timestamp for recovery_timeout logic
            self._last_down[service_prefix] = now
            raise CircuitOpenError(f"Service '{project}:{service}' port {port} is DOWN")


    # API-level state check
    def _check_state(self, key: str, cfg: dict):
        state = self._get_state(key)
        now = time.time()

        if state["state"] == State.OPEN and state["opened_at"]:
            if now - state["opened_at"] >= cfg["recovery_timeout"]:
                self._set_state(key, State.HALF_OPEN, {"ts": now})
                state["half_open_inflight"] = 0
            else:
                raise CircuitOpenError(f"Circuit '{key}' is OPEN. Retry later.")

        if state["state"] == State.HALF_OPEN:
            if state["half_open_inflight"] >= cfg["half_open_max_calls"]:
                raise CircuitOpenError(f"Circuit '{key}' is HALF_OPEN. Max probe reached.")

    # Record API failure
    def _record_failure(self, key: str, cfg: dict):
        state = self._get_state(key)
        now = time.time()
        state["fail_times"].append(now)
        self._prune_failures(key, cfg)

        if state["state"] == State.HALF_OPEN or len(state["fail_times"]) >= cfg["failure_threshold"]:
            self._set_state(key, State.OPEN, {"ts": now, "reason": "api_failure_threshold"})

    # Record API success
    def _record_success(self, key: str, cfg: dict):
        state = self._get_state(key)
        if state["state"] in [State.OPEN, State.HALF_OPEN]:
            self._set_state(key, State.CLOSED, {"ts": time.time()})

    # Sync decorator
    def __call__(self, project: str, service: str, api: str,fallback: Optional[Callable[..., Any]]=None):
        def decorator(func: Callable):
            def wrapper(*args, **kwargs):
                key = self._circuit_key(project, service, api)
                cfg = self._load_config(project, service)
                lock = self._locks.setdefault(key, threading.RLock())

                with lock:
                    # 1️⃣ Check service port
                    self._check_service_port(project, service, cfg)
                    # 2️⃣ Check API-level circuit state
                    self._check_state(key, cfg)
                    state = self._get_state(key)
                    if state["state"] == State.HALF_OPEN:
                        state["half_open_inflight"] += 1

                try:
                    # Call wrapped function with optional timeout
                    if cfg["response_timeout"]:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(func, *args, **kwargs)
                            try:
                                result = future.result(timeout=cfg["response_timeout"])
                            except concurrent.futures.TimeoutError:
                                with lock:
                                    self._record_failure(key, cfg)
                                # 🔴 Raise proper timeout error
                                # raise ResponseTimeoutError(project, service, api, cfg["response_timeout"])
                                raise ResponseTimeoutError(f"Request timed out after {cfg['response_timeout']} seconds while waiting for a response.")
                    else:
                        result = func(*args, **kwargs)

                    # Record success
                    with lock:
                        self._record_success(key, cfg)
                    return result

                except ResponseTimeoutError:
                    # Already recorded failure above
                    if fallback:
                        return fallback(*args, **kwargs)
                    raise

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
    
    # def __call__(self, project: str, service: str, api: str, fallback: Optional[Callable[..., Any]] = None):
    #     def decorator(func: Callable):
    #         def wrapper(*args, **kwargs):
    #             key = self._circuit_key(project, service, api)
    #             cfg = self._load_config(project, service)
    #             lock = self._locks.setdefault(key, threading.RLock())

    #             with lock:
    #                 # 1️⃣ Check service port
    #                 self._check_service_port(project, service, cfg)
    #                 # 2️⃣ Check API-level circuit state
    #                 self._check_state(key, cfg)
    #                 state = self._get_state(key)
    #                 if state["state"] == State.HALF_OPEN:
    #                     state["half_open_inflight"] += 1

    #             try:
    #                 # Call the wrapped function with optional timeout
    #                 if cfg["response_timeout"]:
    #                     with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    #                         future = executor.submit(func, *args, **kwargs)
    #                         result = future.result(timeout=cfg["response_timeout"])
    #                 else:
    #                     result = func(*args, **kwargs)

    #                 # Record API success
    #                 with lock:
    #                     self._record_success(key, cfg)
    #                 return result

    #             except concurrent.futures.TimeoutError:
    #                 with lock:
    #                     self._record_failure(key, cfg)
    #                 raise
    #             except Exception:
    #                 with lock:
    #                     self._record_failure(key, cfg)
    #                 if fallback:
    #                     return fallback(*args, **kwargs)
    #                 raise
    #             finally:
    #                 with lock:
    #                     state = self._get_state(key)
    #                     if state["state"] == State.HALF_OPEN and state["half_open_inflight"] > 0:
    #                         state["half_open_inflight"] -= 1
    #         return wrapper
    #     return decorator

    # Utility
    def status(self, project: str, service: str, api: str) -> str:
        key = self._circuit_key(project, service, api)
        return self._get_state(key)["state"].value
