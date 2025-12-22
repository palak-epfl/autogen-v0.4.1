# palaks_priority_dispatcher.py
import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

PALAK_QUEUE: asyncio.PriorityQueue = asyncio.PriorityQueue()
_dispatcher_task: Optional[asyncio.Task] = None
PALAK_record_step = {}

@dataclass
class RequestItem:
    send: Callable[[], Awaitable]
    future: asyncio.Future
    request_id: Optional[str]

async def dispatcher():
    while True:
        priority, item = await PALAK_QUEUE.get()

        if item is None:
            break

        asyncio.create_task(_handle_item(item))

async def _handle_item(item: RequestItem):
    try:
        result = await item.send()
        item.future.set_result(result)
    except Exception as e:
        item.future.set_exception(e)

def ensure_dispatcher():
    global _dispatcher_task
    if _dispatcher_task is None or _dispatcher_task.done():
        loop = asyncio.get_running_loop()
        _dispatcher_task = loop.create_task(dispatcher())

async def enqueue_request(
    send: Callable[[], Awaitable],
    future: asyncio.Future,
    request_id: Optional[str] = None,
    is_high_priority: bool = False,
):
    # Lower number = higher priority
    priority = 0 if is_high_priority else 1
    await PALAK_QUEUE.put((priority, RequestItem(send, future, request_id)))










##### PALAK: possibly wrong
# # palaks_poisson_dispatcher.py
# import asyncio
# import random
# from dataclasses import dataclass
# from typing import Awaitable, Callable, Optional

# # POISSON_RATE: Optional[float] = 32.0  # Average requests per second
# POISSON_RATE: Optional[float] = 10  # Average requests per second
# PALAK_QUEUE = asyncio.PriorityQueue()  # Priority queue instead of FIFO queue
# _dispatcher_task = None

# @dataclass
# class RequestItem:
#     send: Callable[[], Awaitable]
#     future: asyncio.Future
#     request_id: Optional[str]


# async def dispatcher():
#     while True:
#         _, item = await PALAK_QUEUE.get()
#         if item is None:
#             break  # Sentinel for graceful shutdown

#         ##### PALAK: THIS IS WRONG
#         # # Poisson delay between requests
#         # if POISSON_RATE and POISSON_RATE > 0:
#         #     await asyncio.sleep(random.expovariate(POISSON_RATE))

#         try:
#             result = await item.send()
#             item.future.set_result(result)
#         except Exception as e:
#             item.future.set_exception(e)

# def ensure_dispatcher():
#     global _dispatcher_task
#     if _dispatcher_task is None or _dispatcher_task.done():
#         loop = asyncio.get_running_loop()
#         _dispatcher_task = loop.create_task(dispatcher())

# async def enqueue_request(
#     send: Callable[[], Awaitable],
#     future: asyncio.Future,
#     request_id: Optional[str] = None,
#     is_high_priority: bool = False,
# ):
#     priority = 0 if is_high_priority else 1
#     print("PALAK: priority: ", priority)
#     await PALAK_QUEUE.put((priority, RequestItem(send=send, future=future, request_id=request_id)))











# #### 2 levels of priority
# async def enqueue_request(
#     send: Callable[[], Awaitable],
#     future: asyncio.Future,
#     custom_request_id: str,
#     likely_success_task_ids: set[str]
# ):
#     """
#     Enqueue a request with multi-level priority:
#       1. success likelihood
#       2. agent type
#       3. FIFO order
#     """
#     # ---- Level 1: success priority ----
#     is_likely_success = custom_request_id in likely_success_task_ids
#     success_priority = 0 if is_likely_success else 1

#     # ---- Level 2: agent priority ----
#     agent_name = extract_agent_name(custom_request_id)  # implement this
#     agent_priority = 0 if agent_name in ["filesurfer", "websurfer"] else 1

#     # ---- Level 3: FIFO ----
#     count_val = next(_counter)

#     # ---- Put in priority queue ----
#     await PALAK_QUEUE.put(
#         ((success_priority, agent_priority, count_val), RequestItem(send=send, future=future, request_id=custom_request_id))
#     )




