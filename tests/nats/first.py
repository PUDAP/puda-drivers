import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any
from puda_drivers.core import NATSMachineClient, ExecutionState
from puda_drivers.machines import First

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("puda_drivers").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

MACHINE_ID = "test-machine"


@asynccontextmanager
async def execution_lifecycle(client: NATSMachineClient, run_id: str, command: str, final_state: str = 'idle'):
    """
    Context manager for handling command execution lifecycle.
    
    Manages status updates and error handling for command execution:
    - Sets status to 'busy' at start
    - Sets status to final_state on success or 'error' on failure
    - Logs command completion or errors
    
    Args:
        client: NATS client instance
        run_id: Run ID for the command
        command: Command name
        final_state: Final state to set on success (default: 'idle')
    """
    logger.info("Executing: %s (run_id: %s)", command, run_id)
    await client.publish_status({'state': 'busy', 'run_id': run_id})
    
    try:
        yield  # This is where the actual command runs
        
        # Success path
        await client.publish_status({'state': final_state, 'run_id': None})
        await client.publish_log('INFO', f'Command {command} completed')
        
    except asyncio.CancelledError:
        # Cancellation path
        logger.info("Command %s (run_id: %s) was cancelled", command, run_id)
        await client.publish_status({'state': 'idle', 'run_id': None})
        await client.publish_log('INFO', f'Command {command} was cancelled')
        raise  # Re-raise CancelledError
        
    except Exception as e:
        # Failure path
        logger.error("Execution error: %s", e, exc_info=True)
        await client.publish_status({'state': 'error', 'run_id': None})
        await client.publish_log('ERROR', f'Command failed: {str(e)}')
        raise  # Re-raise to let the caller return False


async def main():
    # 1. Initialize Objects
    client = NATSMachineClient(
        servers=["nats://192.168.50.201:4222", "nats://192.168.50.201:4223", "nats://192.168.50.201:4224"],
        machine_id=MACHINE_ID
    )
    
    first_machine = First(
        qubot_port="/dev/ttyACM0",
        sartorius_port="/dev/ttyUSB0",
        camera_index=0,
    )
    
    # Shared execution state for cancellation
    exec_state = ExecutionState()

    # 2. Setup Handlers
    async def handle_execute(payload: Dict[str, Any]) -> bool:
        header = payload.get('header', {})
        command = header.get('command')
        run_id = header.get('run_id')
        params = payload.get('params', {})

        # Try to acquire execution lock
        if not await exec_state.acquire_execution(run_id):
            logger.warning("Cannot execute %s (run_id: %s): another command is running or cancelled", 
                         command, run_id)
            await client.publish_status({'state': 'error', 'run_id': None})
            await client.publish_log('ERROR', f'Cannot execute {command}: another command is running')
            return False

        try:
            async with execution_lifecycle(client, run_id, command):
                # A. Safe Dispatching
                # Check if command exists on the object
                #time.sleep(5)
                handler = getattr(first_machine, command, None)
                # replace handler for now with a dummy handler
                #handler = lambda **kwargs: True

                # Security: Ensure it's a method and not private (starts with _)
                if not callable(handler) or command.startswith('_'):
                    raise ValueError(f"Unknown or restricted command: {command}")

                # B. Execution
                # Wrap synchronous First machine methods in an executor so the async wrapper can be cancelled.
                # Note: The synchronous code in the executor will continue until it completes,
                # but we can cancel the async wrapper and prevent further status updates.
                async def execute_handler():
                    # Run the synchronous handler in a thread pool
                    loop = asyncio.get_event_loop()
                    return await loop.run_in_executor(None, lambda: handler(**params))
                
                # Create and track the task
                task = asyncio.create_task(execute_handler())
                exec_state.set_current_task(task)
                
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info("Handler execution cancelled (run_id: %s)", run_id)
                    raise
                    
            return True

        except asyncio.CancelledError:
            # Already handled by execution_lifecycle, just return False
            return False
        except Exception as e:
            logger.error("Execute handler error: %s", e, exc_info=True)
            return False
        finally:
            exec_state.release_execution()

    async def handle_pause(payload: Dict[str, Any]) -> bool:
        header = payload.get('header', {})
        run_id = header.get('run_id')
        command = header.get('command', 'pause')

        # Try to acquire execution lock
        if not await exec_state.acquire_execution(run_id):
            logger.warning("Cannot pause (run_id: %s): another command is running", run_id)
            await client.publish_status({'state': 'error', 'run_id': None})
            return False

        try:
            async with execution_lifecycle(client, run_id, command, final_state='paused'):
                # Pause command logic (if any machine-specific pause action needed)
                # For now, just the status update handled by context manager
                pass
            return True

        except Exception:
            return False
        finally:
            exec_state.release_execution()

    async def handle_cancel(payload: Dict[str, Any]) -> bool:
        header = payload.get('header', {})
        run_id = header.get('run_id')

        try:
            # Try to cancel the current execution
            cancelled = await exec_state.cancel_current_execution(run_id)
            
            if cancelled:
                logger.info("Successfully cancelled execution (run_id: %s)", run_id)
                await client.publish_status({'state': 'idle', 'run_id': None})
                await client.publish_log('INFO', f'Command cancelled (run_id: {run_id})')
                return True
            else:
                # No execution to cancel, or run_id doesn't match
                current_task = exec_state.get_current_task()
                current_run_id = exec_state.get_current_run_id()
                
                if current_task is None:
                    logger.warning("Cancel requested but no command is currently executing (run_id: %s)", run_id)
                    await client.publish_status({'state': 'idle', 'run_id': None})
                    await client.publish_log('WARNING', f'Cancel requested but no command running (run_id: {run_id})')
                else:
                    logger.warning("Cancel run_id %s doesn't match current run_id %s", 
                                 run_id, current_run_id)
                    await client.publish_status({'state': 'error', 'run_id': None})
                    await client.publish_log('ERROR', f'Cancel run_id mismatch (requested: {run_id}, current: {current_run_id})')
                return False

        except Exception as e:
            logger.error("Cancel handler error: %s", e, exc_info=True)
            return False

    # 3. Connect and Start Up
    if not await client.connect():
        logger.error("Failed to connect to NATS")
        return

    try:
        # Subscribe
        await client.subscribe_execute(handle_execute)
        await client.subscribe_pause(handle_pause)
        await client.subscribe_cancel(handle_cancel)

        # Start Hardware
        first_machine.startup()
        
        logger.info("Machine %s Ready. Publishing telemetry...", MACHINE_ID)
        await client.publish_status({'state': 'idle', 'run_id': None})

        # 4. Telemetry Loop (Keeps the script running)
        while True:
            await client.publish_heartbeat()
            await client.publish_position({'x': 10.5, 'y': 20.3, 'z': 5.0})
            await client.publish_health({'cpu': 45.2, 'mem': 60.1, 'temp': 35.0})
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        # 5. Cleanup
        logger.info("Shutting down...")
        first_machine.shutdown()
        await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass