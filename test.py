import asyncio
import time
from CircuitBreakerLib.circuit_breaker import CircuitBreaker, CircuitOpenError

# ---------------------------
# Initialize Circuit Breaker
# ---------------------------
cb = CircuitBreaker()
PROJECT = "Analytika"
SERVICE = "AuthUserService"

# ---------------------------
# Simulated sync API call
# ---------------------------
@cb(PROJECT, SERVICE)
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
