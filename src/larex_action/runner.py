# SPDX-License-Identifier: MIT
import asyncio
import logging
import multiprocessing
from multiprocessing.connection import Connection
from multiprocessing.context import SpawnProcess

from larex_actions import ActionCancelled, ActionContext, ActionInput

from .config import env
from .worker import WorkerRequest, WorkerResponse, WorkerStatus, worker

logger = logging.getLogger(__name__)


def _is_region_target(action_input: ActionInput) -> bool:
    selection = action_input.target_selection
    return selection is not None and selection.type == 'REGION'


def _get_selected_regions(ctx: ActionInput, page_id: str) -> list[str]:
    selection = ctx.target_selection
    if selection is None:
        return []
    regions: list[str] = []
    for page in selection.pages:
        if page.page_id != page_id:
            continue
        ids = list(page.region_ids)
        if not ids:
            raise ValueError(f'Region-targeted run for page {page_id} does not contain region ids')
        regions.extend(ids)
    return regions


def _calculate_progress(page: int, total: int) -> int:
    return int((page / total) * 100)


def _start_worker(*args: object) -> tuple[SpawnProcess, Connection]:
    """
    Spawn a new worker process.
    Args:
        *args: Extra arguments passed to the worker after its pipe connection.
    Returns:
        The worker process and the parent end of the pipe used to talk to it.
    """
    context = multiprocessing.get_context('spawn')
    connection, child_connection = context.Pipe()
    process = context.Process(target=worker, args=(child_connection, *args))
    
    process.start()
    child_connection.close()
    return process, connection


async def _check_cancelled(ctx: ActionContext) -> None:
    """
    Check if the action was cancelled from LAREX.
    Args:
        ctx: The ActionContext provided by the SDK.
    Raises:
        ActionCancelled: If the action was cancelled.
    """
    try:
        await ctx.check_cancelled()
    except ActionCancelled:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning(f'ctx.check_cancelled() raised an error: {exc}')


async def _receive(ctx: ActionContext, process: SpawnProcess, connection: Connection, timeout: int | None) -> object:
    """
    Wait for the next message from the worker.
    Args:
        ctx: The ActionContext provided by the SDK.
        process: The worker process to monitor.
        connection: The pipe connection used to receive the worker's message.
        timeout: Maximum time to wait in seconds, or None to wait indefinitely.
    Raises:
        TimeoutError: If the timeout is reached.
        RuntimeError: If the worker exits unexpectedly before sending a message.
    Returns:
        The received message.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout if timeout is not None else None
    next_cancel_check = 0.0
 
    while True:
        if loop.time() >= next_cancel_check:
            await _check_cancelled(ctx)
            next_cancel_check = loop.time() + 1.0
 
        if deadline is not None:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(f'Worker timed out after {timeout} seconds')
            poll_timeout = min(0.1, remaining)
        else:
            poll_timeout = 0.1
 
        if connection.poll(poll_timeout):
            try:
                return await asyncio.to_thread(connection.recv)
            except EOFError:
                await asyncio.to_thread(process.join, 1)
                raise RuntimeError(f'Worker exited unexpectedly with code {process.exitcode}') from None
        if not process.is_alive():
            if connection.poll(0):
                continue
            raise RuntimeError(f'Worker exited unexpectedly with code {process.exitcode}')


async def _wait_until_loaded(ctx: ActionContext, process: SpawnProcess, connection: Connection) -> None:
    """
    Wait for the worker to report that it finished loading. The first message of a worker must be
    a WorkerStatus: 'loaded' on success, 'error' if loading failed.
    Args:
        ctx: The ActionContext provided by the SDK.
        process: The freshly started worker process.
        connection: The pipe connection to the worker.
    Raises:
        WorkerLoadError: If loading failed, timed out, or the worker died while loading.
    """
    await ctx.heartbeat(status_message='Loading worker', raise_on_cancel=True)
    
    try:
        status = await _receive(ctx, process, connection, env.ACTION_LOADING_TIMEOUT)
    except TimeoutError:
        raise RuntimeError(f'Worker did not finish loading within {env.ACTION_LOADING_TIMEOUT} seconds') from None
    except RuntimeError as exc:
        raise RuntimeError(f'{exc} while loading') from None
 
    if isinstance(status, WorkerStatus):
        if status.type == 'loaded':
            logger.info('Worker loaded')
            return
        if status.type == 'error':
            raise RuntimeError(f'Worker failed to load: {status.message}')
    raise RuntimeError(f'Worker sent an unexpected message while loading: {type(status).__name__}')


async def _stop_worker(process: SpawnProcess, connection: Connection) -> None:
    """
    Stop the worker gracefully. If it does not exit within the grace period, terminate it.
    Args:
        process: The worker process to stop.
        connection: The pipe connection used to send data to the worker.
    """
    if not process.is_alive():
        await asyncio.to_thread(process.join)
        return
 
    try:
        connection.send(None)
    except (BrokenPipeError, EOFError, OSError):
        pass
 
    await asyncio.to_thread(process.join, 10)
 
    if process.is_alive():
        logger.warning('Worker did not stop gracefully; terminating process')
        process.terminate()
        await asyncio.to_thread(process.join)


async def _kill_worker(process: SpawnProcess, connection: Connection) -> None:
    """
    Forcefully stop a worker that is stuck on a page.
    A graceful stop is not possible here, because the worker is busy and would never read the stop signal.
    Args:
        process: The worker process to kill.
        connection: The pipe connection to the worker. It is closed afterwards.
    """
    if process.is_alive():
        logger.warning('Terminating stuck worker process')
        process.terminate()
        await asyncio.to_thread(process.join, 5)
        if process.is_alive():
            logger.warning('Worker ignored SIGTERM; killing process')
            process.kill()
    await asyncio.to_thread(process.join)
    connection.close()


async def _run(ctx: ActionContext) -> None:
    action_input = await ctx.pull_input()
    if not action_input.pages:
        await ctx.fail('Action input is empty')
    
    await ctx.heartbeat(status_message='Initialize action', raise_on_cancel=True)
    
    total = len(action_input.pages)
    logger.info(f'Starting worker for {total} pages(s)')
    
    spread = float(action_input.parameters.get('Spread', 0.0))
    region_processing = _is_region_target(action_input)
    
    args = (spread, )
    process, connection = _start_worker(*args)
    
    succeeded = 0
    restart_needed = False
    try:
        await _wait_until_loaded(ctx, process, connection)
        for i, page in enumerate(action_input.pages, start=1):
            await _check_cancelled(ctx)
            
            if restart_needed:
                logger.info(f'[{i}/{total}] Restarting worker after timeout')
                process, connection = _start_worker(*args)
                restart_needed = False
                await _wait_until_loaded(ctx, process, connection)
 
            await ctx.heartbeat(
                progress_percent=_calculate_progress(i - 1, total),
                status_message=f'{page.name} ({i}/{total})',
                raise_on_cancel=True
            )
            
            logger.debug(f'[{i}/{total}] Processing page {page.name}')
            results = ctx.result_builder()
            
            try:
                image = await ctx.download_bytes(page.images[0])
                xml = await ctx.download_bytes(page.xml[0])
                
                if region_processing:
                    regions = _get_selected_regions(action_input, page.id)
                else:
                    regions = []
                
                connection.send(WorkerRequest(image, xml, regions))
                
                result = await _receive(ctx, process, connection, env.ACTION_PROCESSING_TIMEOUT)
                if isinstance(result, WorkerStatus) and result.type == 'error':
                    message = f'[{i}/{total}] Action failed for {page.name}: {result.message}'
                    status = 'FAILED'
                    logger.error(message)
                elif isinstance(result, WorkerResponse):
                    results.add_xml_bytes(
                        page_id=page.id,
                        content=result.xml,
                        file_name=f'{page.name.split(".")[0] or "page"}.xml'
                    )
 
                    message = f'[{i}/{total}] Action finished for {page.name}'
                    status = 'SUCCEEDED'
                    logger.debug(message)
                    succeeded += 1
                else:
                    raise RuntimeError(f'Worker returned an unknown result type: {type(result).__name__}')
            
            except asyncio.CancelledError:
                raise
            
            except TimeoutError:
                logger.error(f'[{i}/{total}] Worker timed out after {env.ACTION_PROCESSING_TIMEOUT} seconds while processing {page.name}')
                if env.ACTION_TIMEOUT_MODE != 'fail_page':
                    raise
                message = (f'[{i}/{total}] Action timed out for {page.name} after {env.ACTION_PROCESSING_TIMEOUT} seconds')
                status = 'FAILED'
                await _kill_worker(process, connection)
                restart_needed = True
 
            except Exception as exc:
                message = f'[{i}/{total}] Action failed for {page.name}: {type(exc).__name__} {exc}'
                status = 'FAILED'
                logger.exception(message)
 
            await ctx.submit_page_results(page.id, results, message, page_status=status, reason=message)
            await ctx.heartbeat(progress_percent=_calculate_progress(i, total), raise_on_cancel=True)
        
        if not restart_needed:
            await _stop_worker(process, connection)
            if process.exitcode != 0:
                message = f'Worker exited with code {process.exitcode}'
                logger.error(message)
                await ctx.fail(message)
                return
 
        message = f'Successfully processed {succeeded}/{total} element(s)'
        logger.info(message)
        await ctx.complete(message=message)
    
    except (asyncio.CancelledError, ActionCancelled):
        logger.info('Action cancelled')
        raise
 
    except RuntimeError as exc:
        logger.error(str(exc))
        await ctx.fail(str(exc))
 
    except Exception:
        logger.exception('Unexpected action failure')
        await ctx.fail('Action failed')
 
    finally:
        if process.is_alive():
            logger.debug('Terminating action worker')
            process.terminate()
            await asyncio.to_thread(process.join)
        connection.close()


async def runner(ctx: ActionContext) -> None:
    async with env.RUN_SEMAPHORE:
        await _run(ctx)
