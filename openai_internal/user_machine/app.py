import asyncio
import inspect
import json
import logging
import os
import sys
import time
import traceback
import uuid

import traitlets
from ace_client.ace_types.user_machine_types import *
from jupyter_client import AsyncKernelClient, AsyncKernelManager, AsyncMultiKernelManager
from pydantic import parse_obj_as
from quart import (
    Quart,
    abort,
    copy_current_websocket_context,
    jsonify,
    make_response,
    request,
    send_from_directory,
    websocket,
)

from . import routes, run_jupyter

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(name)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d,%H:%M:%S",
    stream=sys.stdout,
)

os.chdir(os.path.expanduser("~"))

_MAX_UPLOAD_SIZE = 1024 * 1024 * 1024
_MAX_JUPYTER_MESSAGE_SIZE = 10 * 1024 * 1024
_MAX_KERNELS = 20

app = Quart(__name__)
app.config["MAX_CONTENT_LENGTH"] = _MAX_UPLOAD_SIZE

jupyter_config = traitlets.config.get_config()
# Don't set c.KernelManager.autorestart to False because doing so will
# disable the add_restart_callback mechanism, which we depend on to
# detect kernel deaths.
# Set c.KernelRestarter.restart_limit to 0 so that the kernel will not
# attempt to restart if the kernel process dies unexpectedly.
jupyter_config.KernelRestarter.restart_limit = 0
_MULTI_KERNEL_MANAGER = AsyncMultiKernelManager(config=jupyter_config)

_response_to_callback_from_kernel_futures = {}

_timeout_at = {}
_timeout = {}
_timeout_task = None

_kernel_queue = None
_fill_kernel_queue_task = None
_first_kernel_started = False

# kickoff a background job that will kill any kernels that have been running for too long
async def _kill_old_kernels():
    while True:
        await asyncio.sleep(2.0)
        for kernel_id in list(_timeout_at.keys()):
            if time.monotonic() > _timeout_at[kernel_id]:
                logger.info(
                    f"Killing kernel {kernel_id} due to timeout, {_timeout_at[kernel_id]}, {_timeout[kernel_id]}"
                )
                await _delete_kernel(kernel_id)


async def _fill_kernel_queue():
    global _first_kernel_started, _kernel_queue
    try:
        # With a maxsize of 1, we effectively have two warmed-up kernels:
        # one in the queue, another blocked on queue.put call.
        _kernel_queue = asyncio.Queue(maxsize=1)
        # Prepare a single kernel a head of time, pull a future off of the queue and
        # assign it to that kernel. This way, we can avoid the overhead of starting
        # a new kernel for evdery request.
        kernel_id = None
        kernel_manager = None
        while True:
            logger.info("Create new kernel for pool: Preparing")
            if len(_timeout_at.keys()) >= _MAX_KERNELS:
                logger.info(f"Too many kernels ({_MAX_KERNELS}). Deleting oldest kernel.")
                sort_key = lambda kernel_id: _timeout_at[kernel_id]
                kernels_to_delete = sorted(_timeout_at.keys(), key=sort_key, reverse=True)[_MAX_KERNELS - 1 :]
                for kernel_id in kernels_to_delete:
                    logger.info(f"Deleting kernel {kernel_id}")
                    await _delete_kernel(kernel_id)

            logger.info(f"Create new kernel for pool: Making new kernel")
            start_time = time.monotonic()

            kernel_id = await _MULTI_KERNEL_MANAGER.start_kernel()
            kernel_manager = _MULTI_KERNEL_MANAGER.get_kernel(kernel_id)
            client = kernel_manager.client()
            client.start_channels()
            await client.wait_for_ready()
            client.stop_channels()
            del client

            end_time = time.monotonic()
            logger.info(f"Create new kernel for pool: Done making new kernel in {end_time - start_time:.3f} seconds")
            logger.info(f"Create new kernel for pool: New kernel id {kernel_id} in {id(_kernel_queue)}")

            _first_kernel_started = True
            await _kernel_queue.put(kernel_id)
    except Exception as e:
        logger.error(f"Error while filling queue: {e}", exc_info=True)
        raise


async def _delete_kernel(kernel_id):
    kernel_ids = _MULTI_KERNEL_MANAGER.list_kernel_ids()
    if kernel_id in kernel_ids:
        kernel_manager = _MULTI_KERNEL_MANAGER.get_kernel(str(kernel_id))
        await kernel_manager.shutdown_kernel()
        _MULTI_KERNEL_MANAGER.remove_kernel(str(kernel_id))
    del _timeout_at[kernel_id]
    del _timeout[kernel_id]


@app.before_serving
async def startup():
    global _timeout_task, _fill_kernel_queue_task
    _timeout_task = asyncio.create_task(_kill_old_kernels())
    _fill_kernel_queue_task = asyncio.create_task(_fill_kernel_queue())


@app.after_serving
async def shutdown():
    _timeout_task.cancel()  # type: ignore
    _fill_kernel_queue_task.cancel()  # type: ignore


async def _SEND_CALLBACK_REQUEST(name, **kwargs):
    raise NotImplementedError()


async def _forward_callback_from_kernel(name):
    if request.remote_addr != "127.0.0.1":
        abort(403)
    request_id = str(uuid.uuid4())
    call = MethodCall(
        request_id=request_id,
        object_reference=ObjectReference(type="callbacks", id=""),
        method=name,
        args=[],
        kwargs=request.args,
    )
    logger.info(f"Forwarding callback request from kernel. {call}")
    _response_to_callback_from_kernel_futures[request_id] = asyncio.Future()
    await _SEND_CALLBACK_REQUEST(call.json())
    response = await _response_to_callback_from_kernel_futures[request_id]
    logger.info(f"Forwarding callback response to kernel: {response}.")
    del _response_to_callback_from_kernel_futures[request_id]
    return jsonify({"value": response})


app.register_blueprint(routes.get_blueprint(_forward_callback_from_kernel), url_prefix="/")


def _respond_to_callback_from_kernel(response):
    logger.info("Received callback response.")
    _response_to_callback_from_kernel_futures[response.request_id].set_result(response.value)


async def _send(response):
    message = response.json()
    logger.info(f"Sending response: {type(response)}, {len(message)}")
    if len(message) > _MAX_JUPYTER_MESSAGE_SIZE:
        raise UserMachineResponseTooLarge(
            f"Message of type {type(response)} is too large: {len(message)}"
        )
    await websocket.send(message)
    logger.info("Response sent.")


@app.route("/check_liveness", methods=["GET"])
async def check_liveness():
    code = """
    print(f'{400+56}')
    100+23
    """
    logger.info("Health check: running...")
    start_time = time.monotonic()
    kernel_id = await _create_kernel(timeout=30.0)
    kernel_manager = _MULTI_KERNEL_MANAGER.get_kernel(kernel_id)
    assert isinstance(kernel_manager, AsyncKernelManager)
    try:
        execute_result, error_stacktrace, stream_text = await run_jupyter.async_run_code(
            kernel_manager, code, shutdown_kernel=False
        )
        status_code = 200
        status = "live"
        if error_stacktrace is not None:
            status_code = 500
            status = "error"
        elif execute_result != {"text/plain": "123"} or stream_text != "456\n":
            status_code = 500
            status = "unexpected_result"
        result = {
            "execute_result": execute_result,
            "stream_text": stream_text,
            "error_stacktrace": error_stacktrace,
            "status": status,
        }
    finally:
        await _delete_kernel(kernel_id)
    end_time = time.monotonic()
    logger.info(f"Health check took {end_time - start_time} seconds and returned {result}")
    return await make_response(jsonify(result), status_code)


@app.route("/check_startup", methods=["GET"])
async def check_startup():
    if not _first_kernel_started:
        logger.info("Failed health check")
        return await make_response(
            jsonify({"status": "failed", "reason": "kernel queue is not initialized"}), 500
        )
    logger.info("Passed health check")
    return await make_response(jsonify({"status": "started"}))


@app.route("/upload", methods=["POST"])
async def upload():
    logger.info("Upload request")
    upload_file_request = parse_obj_as(
        UploadFileRequest, json.loads((await request.form)["upload_request"])
    )
    file = (await request.files)["file"]
    await file.save(upload_file_request.destination)
    logger.info(f"Upload request complete. {upload_file_request}")
    return jsonify({})


@app.route("/download/<path:path>", methods=["GET"])
async def download(path):
    logger.info(f"Download request. {path}")
    response = await make_response(await send_from_directory("/", path))
    return response


@app.route("/check_file/<path:path>", methods=["GET"])
async def check_file(path):
    path = "/" + path
    logger.info(f"Check file request. {path}")
    exists = os.path.isfile(path)
    size = os.path.getsize(path) if exists else 0
    response = CheckFileResponse(exists=exists, size=size, too_large=False)
    response = await make_response(jsonify(response.dict()))
    return response


async def _create_kernel(timeout: float):
    assert _kernel_queue is not None, "Queue should be initialized once health checks pass."
    kernel_id = await _kernel_queue.get()
    _timeout[kernel_id] = timeout
    _timeout_at[kernel_id] = time.monotonic() + timeout
    return kernel_id


@app.route("/kernel", methods=["POST"])
async def create_kernel():
    create_kernel_request = parse_obj_as(CreateKernelRequest, await request.get_json())
    logger.info(f"Create kernel request. {create_kernel_request}")
    timeout = create_kernel_request.timeout
    kernel_id = await _create_kernel(timeout=timeout)
    logger.info(f"Got kernel id from queue. {create_kernel_request}")
    return jsonify(CreateKernelResponse(kernel_id=kernel_id).dict())


@app.websocket("/channel")
async def channel():
    @copy_current_websocket_context
    async def send_callback_request(json):
        await websocket.send(json)

    logger.info("Setting global forward function.")
    global _SEND_CALLBACK_REQUEST
    _SEND_CALLBACK_REQUEST = send_callback_request

    clients = {}
    # There are 3 sources of messages:
    # 1. The API server: proxied requests from the user machine
    # 2. The Jupyter client: return value of async Jupyter client invocations
    # 3. The kernel (user code): callback requests via http
    # The first two maps to the two following variables. The third is handled by
    # the send_callback_request function.
    #
    # Similarly, there are also 3 corresponding sinks of messages.
    recv_from_api_server = asyncio.create_task(websocket.receive())
    recv_from_jupyter = None
    try:
        while True:
            logger.info(f"Waiting for message. {recv_from_api_server}, {recv_from_jupyter}")
            done, _ = await asyncio.wait(
                [task for task in [recv_from_api_server, recv_from_jupyter] if task is not None],
                return_when=asyncio.FIRST_COMPLETED,
            )
            logger.info(f"Got messages for {done}.")
            if recv_from_api_server in done:
                done_future = recv_from_api_server
                recv_from_api_server = asyncio.create_task(websocket.receive())
                request = parse_obj_as(UserMachineRequest, json.loads(done_future.result()))
                logger.info(f"Received message from API server. {request}")
                if isinstance(request, RegisterActivityRequest):
                    logger.info(f"Registering activity. {request}")
                    _timeout_at[request.kernel_id] = time.monotonic() + _timeout[request.kernel_id]
                elif isinstance(request, MethodCallReturnValue):
                    _respond_to_callback_from_kernel(request)
                elif isinstance(request, MethodCall):

                    async def run(request):
                        try:
                            object_reference = request.object_reference
                            if object_reference.type == "multi_kernel_manager":
                                referenced_object = _MULTI_KERNEL_MANAGER
                            elif object_reference.type == "kernel_manager":
                                referenced_object = _MULTI_KERNEL_MANAGER.get_kernel(
                                    object_reference.id
                                )
                            elif object_reference.type == "client":
                                referenced_object = clients[object_reference.id]
                            else:
                                raise Exception(
                                    f"Unknown object reference type: {object_reference.type}"
                                )
                            logger.info(
                                f"Method call: {request.method} args: {request.args} kwargs: {request.kwargs}"
                            )
                            value = getattr(referenced_object, request.method)(
                                *request.args, **request.kwargs
                            )
                            if inspect.isawaitable(value):
                                value = await value
                            return (request.request_id, value, None)
                        except Exception as e:
                            return (request.request_id, None, e)

                    assert recv_from_jupyter is None
                    recv_from_jupyter = asyncio.create_task(run(request))
            if recv_from_jupyter in done:
                done_future = recv_from_jupyter
                recv_from_jupyter = None
                request_id, value, e = done_future.result()
                if e is None:
                    logger.info(f"Received result from Jupyter. {value}")
                    if isinstance(value, AsyncKernelClient):
                        client_id = str(uuid.uuid4())
                        clients[client_id] = value
                        result = MethodCallObjectReferenceReturnValue(
                            request_id=request_id,
                            object_reference=ObjectReference(type="client", id=client_id),
                        )
                    elif isinstance(value, AsyncKernelManager):
                        result = MethodCallObjectReferenceReturnValue(
                            request_id=request_id,
                            object_reference=ObjectReference(
                                type="kernel_manager", id=value.kernel_id
                            ),
                        )
                    else:
                        result = MethodCallReturnValue(request_id=request_id, value=value)
                else:
                    logger.info(f"Received result from Jupyter. Exception: {value}")
                    result = MethodCallException(
                        request_id=request_id,
                        type=type(e).__name__,
                        value=str(e),
                        traceback=traceback.format_tb(e.__traceback__),
                    )
                    # Drop reference to e because traceback may hold onto a lot of memory
                    del e
                await _send(result)
    except asyncio.CancelledError:
        logger.info("Websocket closed.")
        for client in clients.values():
            client.stop_channels()
        logger.info("All clients stopped.")
        return
