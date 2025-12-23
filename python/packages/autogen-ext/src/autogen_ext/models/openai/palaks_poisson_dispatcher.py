# palaks_priority_dispatcher.py
import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

PALAK_QUEUE: asyncio.PriorityQueue = asyncio.PriorityQueue()
_dispatcher_task: Optional[asyncio.Task] = None
PALAK_record_step_per_task = {}
STEP_THRESHOLD = 30

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

    try:
        # task_id_from_request_id = custom_request_id.split(':', 1)[0].rsplit('_', 1)[-1]
        task_id_from_request_id = custom_request_id.rsplit("_", 1)[-1]
    except Exception:
        task_id_from_request_id = none

    if task_id_from_request_id not in PALAK_record_step_per_task:
        PALAK_record_step_per_task[task_id_from_request_id] = 0   
    PALAK_record_step_per_task[task_id_from_request_id] += 1

    print(f"PALAK: current step count of task_id {task_id_from_request_id} is {PALAK_record_step_per_task[task_id_from_request_id]}")

    if PALAK_record_step_per_task[task_id_from_request_id] > 30:
        priority = 1
    else:
        priority = 0

    # Lower number = higher priority
    # priority = 0 if is_high_priority else 1
    await PALAK_QUEUE.put((priority, RequestItem(send, future, request_id)))
