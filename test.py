import random
import time
from CircuitBreakerLib.circuit_breaker import CircuitBreaker, CircuitOpenError

breaker = CircuitBreaker()

@breaker("name_service")
def call_name_service():
    print("calling name_service...")
    raise RuntimeError("Fail!")   # trigger failure

@breaker("auth_service")
def call_auth_service():
    print("auth_service works!")
    return "AUTH OK"


@breaker.async_wrap("client_service")
async def call_client_service():
    print("client service down...")
    raise RuntimeError("Boom")

# ----------------
# Run sync
# ----------------
try:
    for _ in range(3):
        call_name_service()
except Exception as e:
    print("name_service breaker:", e)

print("auth result:", call_auth_service())
print("name_service state:", breaker.status("name_service"))

# ----------------
# Run async
# ----------------
import asyncio
async def run():
    try:
        for _ in range(3):
            await call_client_service()
    except Exception as e:
        print("client_service breaker:", e)

    print("client_service state:", breaker.status("client_service"))

asyncio.run(run())