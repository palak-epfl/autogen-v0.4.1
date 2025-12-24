# palaks_global_dispatcher.py
import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional
from multiprocessing import Manager, Process, Queue
import time

# -----------------------------
# Shared global queue for all processes
# -----------------------------
manager = Manager()
GLOBAL_QUEUE: Queue = manager.Queue()

@dataclass
class RequestItem:
    send: Callable[[], Awaitable]
    future: asyncio.Future
    request_id: Optional[str]

# -----------------------------
# Async helper to wrap blocking multiprocessing queue
# -----------------------------
async def _async_get(queue: Queue):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, queue.get)

async def _handle_item(item: RequestItem):
    try:
        result = await item.send()
        item.future.set_result(result)
    except Exception as e:
        item.future.set_exception(e)

async def dispatcher():
    """
    Continuously consume items from the global queue and run them.
    """
    while True:
        print(f"GLOBAL_QUEUE size before get: {GLOBAL_QUEUE.qsize()}")
        priority, item = await _async_get(GLOBAL_QUEUE)
        print(f"GLOBAL_QUEUE got item: {item.request_id}, priority: {priority}")
        asyncio.create_task(_handle_item(item))

# -----------------------------
# Enqueue request from any process
# -----------------------------
async def enqueue_request(
    send: Callable[[], Awaitable],
    future: asyncio.Future,
    request_id: Optional[str] = None,
    is_high_priority: bool = False,
):
    """
    Wrap a request into RequestItem and put it in the global queue.
    Lower priority number = higher priority
    """
    priority = 0 if is_high_priority else 1
    GLOBAL_QUEUE.put((priority, RequestItem(send, future, request_id)))
