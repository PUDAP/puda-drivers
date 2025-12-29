"""
Test script to send commands to a machine via NATS
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
import nats
from nats.js.client import JetStreamContext

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

MACHINE_ID = "test-machine"
NAMESPACE = "puda"
KV_BUCKET_NAME = f"MACHINE_STATE_{MACHINE_ID.replace('.', '-')}"


async def read_status(js: JetStreamContext, machine_id: str) -> dict:
    """
    Read machine status from KV store.
    
    Args:
        js: JetStream context
        machine_id: Machine identifier
    
    Returns:
        Dictionary with status data or None if not found
    """
    try:
        kv = await js.key_value(KV_BUCKET_NAME)
        entry = await kv.get(machine_id)
        
        if entry:
            status = json.loads(entry.value.decode())
            return status
        else:
            return None
    except Exception as e:
        logger.debug("Error reading status from KV store: %s", e)
        return None


async def wait_for_response(nc: nats.NATS, machine_id: str, run_id: str, command_id: str, timeout: float = 60.0) -> bool:
    """
    Wait for command response by subscribing to response messages.
    
    Returns True on success, False on error.
    Raises TimeoutError if timeout is reached.
    
    Args:
        nc: NATS connection (core NATS, not JetStream)
        machine_id: Machine identifier
        run_id: Run ID to wait for
        command_id: Command ID to wait for
        timeout: Maximum time to wait in seconds
    
    Returns:
        True if success, False if error
    """
    response_subject = f"{NAMESPACE}.{machine_id}.cmd.response"
    response_received = asyncio.Event()
    result = None
    
    async def message_handler(msg):
        """Handle response messages."""
        nonlocal result
        try:
            response = json.loads(msg.data.decode())
            resp_run_id = response.get('run_id')
            resp_command_id = response.get('command_id')
            status = response.get('status')
            
            # Check if this response matches our command
            if resp_run_id == run_id and resp_command_id == command_id:
                if status == 'success':
                    logger.info("Received response: Command %s completed successfully", command_id)
                    result = True
                elif status == 'error':
                    error_msg = response.get('error', 'Unknown error')
                    logger.error("Received response: Command %s failed - %s", command_id, error_msg)
                    result = False
                response_received.set()
        except Exception as e:
            logger.error("Error processing response message: %s", e)
    
    # Subscribe to response subject
    sub = await nc.subscribe(response_subject, cb=message_handler)
    logger.info("Waiting for response (run_id: %s, command_id: %s)...", run_id, command_id)
    
    try:
        # Wait for response with timeout
        try:
            await asyncio.wait_for(response_received.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            await sub.unsubscribe()
            raise TimeoutError(f"Timeout waiting for response after {timeout}s")
        
        # Unsubscribe and return result
        await sub.unsubscribe()
        return result
    except Exception:
        await sub.unsubscribe()
        raise


async def send_execute_command(
    nc: nats.NATS,
    js: JetStreamContext,
    machine_id: str,
    payload: dict,
    run_id: str,
    command_id: str
):
    """
    Send an execute command to a machine and wait for acknowledgment.
    
    Args:
        nc: NATS connection (for subscribing to responses)
        js: JetStream context (for publishing commands)
        machine_id: Machine identifier
        payload: Hardcoded command payload
        run_id: Run ID for the experiment
        command_id: Command ID
    
    Returns:
        True on ack (success), False on nak/term (error)
    """
    subject = f"{NAMESPACE}.{machine_id}.cmd.execute"
    
    command = payload.get('header', {}).get('command', 'unknown')
    parameters = payload.get('params', {})
    
    logger.info("Sending execute command to %s", subject)
    logger.info("Run ID: %s, Command ID: %s", run_id, command_id)
    logger.info("Command: %s", command)
    logger.info("Parameters: %s", parameters)
    
    # Publish to JetStream (execute commands use JetStream)
    pub_ack = await js.publish(
        subject,
        json.dumps(payload).encode()
    )
    
    logger.info("Command sent successfully. Sequence: %s", pub_ack.seq)
    logger.info("Stream: %s", pub_ack.stream)
    
    # Wait for response message
    try:
        result = await wait_for_response(nc, machine_id, run_id, command_id, timeout=120.0)
        return result
    except TimeoutError as e:
        logger.error("Timeout waiting for response: %s", e)
        return False
    

async def send_pause_command(
    nc: nats.NATS,
    js: JetStreamContext,
    machine_id: str,
    payload: dict,
    run_id: str
):
    """
    Send a pause command to a machine and wait for acknowledgment.
    
    Args:
        nc: NATS connection (for subscribing to responses)
        js: JetStream context (for publishing commands)
        machine_id: Machine identifier
        payload: Hardcoded pause command payload
        run_id: Run ID to pause
    
    Returns:
        True on ack (success), False on nak/term (error)
    """
    subject = f"{NAMESPACE}.{machine_id}.cmd.pause"
    command_id = payload.get('header', {}).get('command_id', 'pause')
    
    logger.info("Sending pause command to %s for run_id: %s", subject, run_id)
    
    pub_ack = await js.publish(
        subject,
        json.dumps(payload).encode()
    )
    
    logger.info("Pause command sent successfully. Sequence: %s", pub_ack.seq)
    
    # Wait for response message
    try:
        result = await wait_for_response(nc, machine_id, run_id, command_id, timeout=30.0)
        return result
    except TimeoutError as e:
        logger.error("Timeout waiting for pause response: %s", e)
        return False


async def send_cancel_command(
    nc: nats.NATS,
    js: JetStreamContext,
    machine_id: str,
    payload: dict,
    run_id: str
):
    """
    Send a cancel command to a machine and wait for acknowledgment.
    
    Args:
        nc: NATS connection (for subscribing to responses)
        js: JetStream context (for publishing commands)
        machine_id: Machine identifier
        payload: Hardcoded cancel command payload
        run_id: Run ID to cancel
    
    Returns:
        True on ack (success), False on nak/term (error)
    """
    subject = f"{NAMESPACE}.{machine_id}.cmd.cancel"
    command_id = payload.get('header', {}).get('command_id', 'cancel')
    
    logger.info("Sending cancel command to %s for run_id: %s", subject, run_id)
    
    pub_ack = await js.publish(
        subject,
        json.dumps(payload).encode()
    )
    
    logger.info("Cancel command sent successfully. Sequence: %s", pub_ack.seq)
    
    # Wait for response message
    try:
        result = await wait_for_response(nc, machine_id, run_id, command_id, timeout=30.0)
        return result
    except TimeoutError as e:
        logger.error("Timeout waiting for cancel response: %s", e)
        return False


async def main():
    """
    Main function to send commands to First machine.
    
    Commands are sent sequentially from the generated_sequence array,
    waiting for ACK before sending the next.
    Terminates on NAK/TERM (error state).
    """
    servers = ["nats://192.168.50.201:4222", "nats://192.168.50.201:4223", "nats://192.168.50.201:4224"]
    
    # Assume 'generated_sequence' comes from your MCP/LLM
    generated_sequence = [
        {
            "command": "load_deck",
            "params": {
                "deck_layout": { "A3": "opentrons_96_tiprack_300ul" }
            }
        },
        {
            "command": "attach_tip",
            "params": { "slot": "A3", "well": "G8" }
        },
        {
            "command": "aspirate_from",
            "params": { "slot": "C2", "well": "A1", "amount": 100 }
        }
    ]
    
    # Generate run_id for this experiment (same for all commands in experiment)
    run_id = "current_run_123"
    
    # Command counter (starts at 0, increments for each command)
    command_id = 0
    
    logger.info("=" * 60)
    logger.info("Sending execute commands to First machine")
    logger.info("Experiment Run ID: %s", run_id)
    logger.info("=" * 60)
    
    # Connect to NATS and keep connection open
    nc = None
    try:
        nc = await nats.connect(servers=servers)
        js = nc.jetstream()
        logger.info("Connected to NATS")
        
        for step in generated_sequence:
            command_id += 1
            
            # 1. Construct the full payload dynamically
            full_payload = {
                'header': {
                    'command': step['command'],
                    'version': '1.0',
                    'run_id': run_id,
                    'command_id': str(command_id),
                    'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                },
                'params': step['params']
            }

            # 2. Execute
            logger.info("--- Executing %s ---", step['command'])
            result = await send_execute_command(nc, js, MACHINE_ID, full_payload, run_id, str(command_id))
            
            if not result:
                error_msg = f"Command '{step['command']}' failed (NAK/TERM). Terminating."
                logger.error(error_msg)
                raise RuntimeError(error_msg)
        
        logger.info("=" * 60)
        logger.info("All commands completed successfully with run_id: %s", run_id)
        logger.info("Total commands sent: %d", command_id)
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("Error: %s", e, exc_info=True)
        return 1
    finally:
        if nc:
            await nc.close()
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

