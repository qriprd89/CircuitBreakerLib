import asyncio
import time
from CircuitBreakerLib.circuit_breaker import CircuitBreaker, CircuitOpenError

# ---------------------------
# Initialize Circuit Breaker
# ---------------------------
cb = CircuitBreaker()
PROJECT = "Analytika"
SERVICE = "AuthUserService"
SERVICE_API = "call_auth_sync"

# ---------------------------
# Simulated sync API call
# ---------------------------
@cb(PROJECT, SERVICE,SERVICE_API)
def call_auth_sync(should_fail=False):
    """Simulated sync call: fails if should_fail=True"""
    if should_fail:
        raise Exception("Simulated failure")
    return {"status": "ok_sync"}

# ---------------------------
# Simulated async API call
# ---------------------------
@cb.async_wrap(PROJECT, SERVICE)
async def call_auth_async(should_fail=False):
    """Simulated async call: fails if should_fail=True"""
    if should_fail:
        raise Exception("Simulated failure")
    await asyncio.sleep(0.1)
    return {"status": "ok_async"}

# ---------------------------
# Test sync breaker
# ---------------------------
def test_sync():
    print("\n=== SYNC TEST ===")
    # First 3 calls fail to open the circuit
    for i in range(3):
        try:
            res = call_auth_sync(should_fail=True)
            print(f"[SYNC] call {i+1} ✅ result:", res)
        except Exception as e:
            print(f"[SYNC] call {i+1} ❌ FAILED:", e)

    # Next 3 calls should trigger circuit open
    for i in range(3, 6):
        try:
            res = call_auth_sync()
            print(f"[SYNC] call {i+1} ✅ result:", res)
        except CircuitOpenError as e:
            print(f"[SYNC] call {i+1} ⛔ BLOCKED:", e)
        except Exception as e:
            print(f"[SYNC] call {i+1} ❌ FAILED:", e)

    print("Final breaker state:", cb.status(PROJECT, SERVICE))

# ---------------------------
# Test async breaker
# ---------------------------
async def test_async():
    print("\n=== ASYNC TEST ===")
    # First 3 calls fail to open the circuit
    for i in range(3):
        try:
            res = await call_auth_async(should_fail=True)
            print(f"[ASYNC] call {i+1} ✅ result:", res)
        except Exception as e:
            print(f"[ASYNC] call {i+1} ❌ FAILED:", e)

    # Next 3 calls should trigger circuit open
    for i in range(3, 6):
        try:
            res = await call_auth_async()
            print(f"[ASYNC] call {i+1} ✅ result:", res)
        except CircuitOpenError as e:
            print(f"[ASYNC] call {i+1} ⛔ BLOCKED:", e)
        except Exception as e:
            print(f"[ASYNC] call {i+1} ❌ FAILED:", e)

    print("Final breaker state:", cb.status(PROJECT, SERVICE))

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    # Run sync test
    test_sync()

    # Run async test
    asyncio.run(test_async())

# ===================================
breaker = CircuitBreaker()

@breaker("Analytika", "AuthUserService", "login")
def login():
    print("Calling API...")
    raise Exception("Service failure")

# First call → fails and opens circuit
try:
    login()
except Exception as e:
    print("First call failed:", e)

# Second call → blocked immediately
try:
    login()
except CircuitOpenError as e:
    print("Second call blocked:", e)

print("Circuit status:", breaker.status("Analytika", "AuthUserService", "login"))

=========================
breaker = CircuitBreaker()

@breaker.async_wrap("Analytika", "AuthUserService", "login")
async def async_login():
    print("Calling async API...")
    raise Exception("Service failure")

async def main():
    try:
        await async_login()
    except Exception as e:
        print("First async call failed:", e)

    try:
        await async_login()
    except CircuitOpenError as e:
        print("Second async call blocked:", e)

import asyncio
asyncio.run(main())
