# @self.breaker(self.schema_name, self.service_name, self.endpoint_name)
# def do_request():
#     # Authorization
#     if self.service_info.requires_auth:
#         auth_header = headers.get('Authorization')
#         valid, token_or_resp = self.user_authorization(
#             auth_header, client_ip, self.service_info.endpoint_path,
#             request_method, self.schema_name
#         )
#         if not valid:
#             return token_or_resp
#         validated_token = JWTUtils.modify_token(token_or_resp, self.service_info.jwt_secret_key)
#         headers['Authorization'] = f"Bearer {validated_token}"

#     # Request data
#     request_data, files = self.extract_request_data(request)

#     # Redis cache check
#     response = self.get_response_from_redis(client_ip, url)
#     if response:
#         return response

#     # Forward request
#     response = self.forward_request(request_method, url, headers, query_params,
#         request.content_type, request_data, files, self.service_name  )
#     return self.process_response(client_ip, self.endpoint_name, request_method, response, url)
#     # Execute the request safely
# try:
#     return do_request()  # Will NOT execute if circuit is OPEN
# except CircuitOpenError as e:
#     print("_e___________1__________",e)
#     # Circuit is OPEN → skip API call
#     error_message = f"Circuit Breaker OPEN for {self.service_name}.{self.endpoint_name}: {e}"
#     self.publish_log(client_ip, self.endpoint_name, request_method.upper(), 503, error_message)
#     return JsonResponse({"code": 503, "message": str(e)}, status=503)

# import asyncio
# import time
# from CircuitBreakerLib.circuit_breaker_30_9_25 import CircuitBreaker, CircuitOpenError

# # ---------------------------
# # Initialize Circuit Breaker
# # ---------------------------
# cb = CircuitBreaker()
# PROJECT = "Analytika"
# SERVICE = "AuthUserService"
# SERVICE_API = "call_auth_sync"

# # ---------------------------
# # Simulated sync API call
# # ---------------------------
# @cb(PROJECT, SERVICE,SERVICE_API)
# def call_auth_sync(should_fail=False):
#     """Simulated sync call: fails if should_fail=True"""
#     if should_fail:
#         raise Exception("Simulated failure")
#     return {"status": "ok_sync"}

# # ---------------------------
# # Simulated async API call
# # ---------------------------
# @cb.async_wrap(PROJECT, SERVICE)
# async def call_auth_async(should_fail=False):
#     """Simulated async call: fails if should_fail=True"""
#     if should_fail:
#         raise Exception("Simulated failure")
#     await asyncio.sleep(0.1)
#     return {"status": "ok_async"}

# # ---------------------------
# # Test sync breaker
# # ---------------------------
# def test_sync():
#     print("\n=== SYNC TEST ===")
#     # First 3 calls fail to open the circuit
#     for i in range(3):
#         try:
#             res = call_auth_sync(should_fail=True)
#             print(f"[SYNC] call {i+1} ✅ result:", res)
#         except Exception as e:
#             print(f"[SYNC] call {i+1} ❌ FAILED:", e)

#     # Next 3 calls should trigger circuit open
#     for i in range(3, 6):
#         try:
#             res = call_auth_sync()
#             print(f"[SYNC] call {i+1} ✅ result:", res)
#         except CircuitOpenError as e:
#             print(f"[SYNC] call {i+1} ⛔ BLOCKED:", e)
#         except Exception as e:
#             print(f"[SYNC] call {i+1} ❌ FAILED:", e)

#     print("Final breaker state:", cb.status(PROJECT, SERVICE))

# # ---------------------------
# # Test async breaker
# # ---------------------------
# async def test_async():
#     print("\n=== ASYNC TEST ===")
#     # First 3 calls fail to open the circuit
#     for i in range(3):
#         try:
#             res = await call_auth_async(should_fail=True)
#             print(f"[ASYNC] call {i+1} ✅ result:", res)
#         except Exception as e:
#             print(f"[ASYNC] call {i+1} ❌ FAILED:", e)

#     # Next 3 calls should trigger circuit open
#     for i in range(3, 6):
#         try:
#             res = await call_auth_async()
#             print(f"[ASYNC] call {i+1} ✅ result:", res)
#         except CircuitOpenError as e:
#             print(f"[ASYNC] call {i+1} ⛔ BLOCKED:", e)
#         except Exception as e:
#             print(f"[ASYNC] call {i+1} ❌ FAILED:", e)

#     print("Final breaker state:", cb.status(PROJECT, SERVICE))

# # ---------------------------
# # Main
# # ---------------------------
# if __name__ == "__main__":
#     # Run sync test
#     test_sync()

#     # Run async test
#     asyncio.run(test_async())

# # ===================================
# breaker = CircuitBreaker()

# @breaker("Analytika", "AuthUserService", "login")
# def login():
#     print("Calling API...")
#     raise Exception("Service failure")

# # First call → fails and opens circuit
# try:
#     login()
# except Exception as e:
#     print("First call failed:", e)

# # Second call → blocked immediately
# try:
#     login()
# except CircuitOpenError as e:
#     print("Second call blocked:", e)

# print("Circuit status:", breaker.status("Analytika", "AuthUserService", "login"))

# =========================
# breaker = CircuitBreaker()

# @breaker.async_wrap("Analytika", "AuthUserService", "login")
# async def async_login():
#     print("Calling async API...")
#     raise Exception("Service failure")

# async def main():
#     try:
#         await async_login()
#     except Exception as e:
#         print("First async call failed:", e)

#     try:
#         await async_login()
#     except CircuitOpenError as e:
#         print("Second async call blocked:", e)

# import asyncio
# asyncio.run(main())
