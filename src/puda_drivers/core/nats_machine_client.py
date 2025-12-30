"""
NATS Client for Generic Machines
Handles commands, telemetry, and events following the puda.{machine_id}.{category}.{sub_category} pattern
"""
import asyncio
from contextlib import asynccontextmanager
import json
import logging
from typing import Dict, Any, Optional, Callable, Awaitable, Tuple
from datetime import datetime, timezone
import nats
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError

logger = logging.getLogger(__name__)


class ExecutionState:
    """
    Shared state for tracking command execution and cancellation.
    
    This class provides thread-safe access to:
    - Current executing task (for cancellation)
    - Execution lock (to prevent concurrent commands)
    - Current run_id (to match cancel with execute)
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._current_task: Optional[asyncio.Task] = None
        self._current_run_id: Optional[str] = None
        self._cancelled = False
    
    async def acquire_execution(self, run_id: str) -> bool:
        """
        Acquire the execution lock for a command.
        
        Args:
            run_id: Run ID of the command requesting execution
            
        Returns:
            True if execution can proceed, False if cancelled or another command is running
        """
        await self._lock.acquire()
        if self._cancelled:
            self._lock.release()
            return False
        self._current_run_id = run_id
        return True
    
    def release_execution(self):
        """Release the execution lock."""
        self._current_run_id = None
        self._current_task = None
        self._cancelled = False
        self._lock.release()
    
    def set_current_task(self, task: asyncio.Task):
        """Set the currently executing task (for cancellation)."""
        self._current_task = task
    
    def get_current_task(self) -> Optional[asyncio.Task]:
        """Get the currently executing task."""
        return self._current_task
    
    def get_current_run_id(self) -> Optional[str]:
        """Get the current run_id."""
        return self._current_run_id
    
    async def cancel_current_execution(self, run_id: Optional[str] = None) -> bool:
        """
        Cancel the currently executing command.
        
        Args:
            run_id: Optional run_id to match. If provided, only cancels if it matches.
            
        Returns:
            True if cancellation was successful, False if no execution to cancel
        """
        if self._current_task is None:
            return False
        
        # If run_id provided, only cancel if it matches
        if run_id is not None and self._current_run_id != run_id:
            logger.warning("Cancel run_id %s doesn't match current run_id %s", 
                         run_id, self._current_run_id)
            return False
        
        if not self._current_task.done():
            logger.info("Cancelling execution (run_id: %s)", self._current_run_id)
            self._cancelled = True
            self._current_task.cancel()
            return True
        
        return False


class NATSMachineClient:
    """
    NATS client for machines.
    
    Subject pattern: puda.{machine_id}.{category}.{sub_category}
    - Telemetry: core NATS (no JetStream)
    - Commands: JetStream with exactly-once delivery 
    - Events: JetStream for persistence
    """
    
    # Constants
    NAMESPACE = "puda"
    KEEP_ALIVE_INTERVAL = 25  # seconds
    CONSUMER_MAX_DELIVER = 3
    FETCH_TIMEOUT = 1.0  # seconds
    
    def __init__(self, servers: list[str], machine_id: str):
        """
        Initialize NATS client for machine.
        
        Args:
            servers: List of NATS server URLs (e.g., ["nats://localhost:4222"])
            machine_id: Machine identifier (e.g., "opentron")
        """
        self.servers = servers
        self.machine_id = machine_id
        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None
        self.kv = None
        
        # Generate subject and stream names
        self._init_subjects()
        
        # Subscriptions
        self._subscriptions = []
        self._js_subscriptions = []
        
        # Connection state
        self._is_connected = False
        self._reconnect_handlers = []
        
        # Queue control state
        self._pause_lock = asyncio.Lock()
        self._is_paused = False
        self._cancelled_run_ids = set()
    
    def _init_subjects(self):
        """Initialize all subject and stream names."""
        namespace = self.NAMESPACE
        machine_id_safe = self.machine_id.replace('.', '-')
        
        # Telemetry subjects (core NATS, no JetStream)
        self.tlm_heartbeat = f"{namespace}.{self.machine_id}.tlm.heartbeat"
        self.tlm_pos = f"{namespace}.{self.machine_id}.tlm.pos"
        self.tlm_health = f"{namespace}.{self.machine_id}.tlm.health"
        
        # Command subjects (JetStream, exactly-once)
        self.cmd_queue = f"{namespace}.{self.machine_id}.cmd.queue"
        self.cmd_immediate = f"{namespace}.{self.machine_id}.cmd.immediate"
        self.cmd_response = f"{namespace}.{self.machine_id}.cmd.response"
        
        # Command stream names (JetStream)
        self.cmd_stream_queue = f"CMD_QUEUE_{machine_id_safe}"
        self.cmd_stream_immediate = f"CMD_IMMEDIATE_{machine_id_safe}"
        
        # Event subjects (JetStream)
        self.evt_log = f"{namespace}.{self.machine_id}.evt.log"
        self.evt_alert = f"{namespace}.{self.machine_id}.evt.alert"
        self.evt_media = f"{namespace}.{self.machine_id}.evt.media"
        
        # Event stream names (JetStream)
        self.evt_stream_log = f"EVT_LOG_{machine_id_safe}"
        self.evt_stream_alert = f"EVT_ALERT_{machine_id_safe}"
        self.evt_stream_media = f"EVT_MEDIA_{machine_id_safe}"
        
        # KV bucket name for status
        self.kv_bucket_name = f"MACHINE_STATE_{machine_id_safe}"
    
    # ==================== HELPER METHODS ====================
    
    @staticmethod
    def _format_timestamp() -> str:
        """Format current timestamp as ISO 8601 UTC string."""
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    def _parse_message(self, data: bytes) -> Tuple[Dict[str, Any], Optional[str], Optional[str]]:
        """
        Parse message payload and extract header information.
        
        Returns:
            Tuple of (payload, run_id, command_id)
        """
        payload = json.loads(data.decode())
        header = payload.get('header', {})
        run_id = header.get('run_id')
        command_id = header.get('command_id', 'unknown')
        return payload, run_id, command_id
    
    async def _publish_telemetry(self, subject: str, data: Dict[str, Any]) -> bool:
        """Publish telemetry message to core NATS."""
        if not self.nc:
            logger.warning("NATS not connected, skipping %s", subject)
            return False
        
        try:
            message = {'timestamp': self._format_timestamp(), **data}
            await self.nc.publish(subject, json.dumps(message).encode())
            logger.debug("Published to %s", subject)
            return True
        except Exception as e:
            logger.error("Error publishing to %s: %s", subject, e)
            return False
    
    async def _publish_event(self, subject: str, stream_name: str, data: Dict[str, Any]) -> bool:
        """Publish event message to JetStream."""
        if not self.js:
            logger.warning("JetStream not available, skipping %s", subject)
            return False
        
        try:
            await self._ensure_stream(subject, stream_name)
            message = {'timestamp': self._format_timestamp(), **data}
            await self.js.publish(subject, json.dumps(message).encode())
            logger.debug("Published to %s", subject)
            return True
        except Exception as e:
            logger.error("Error publishing to %s: %s", subject, e)
            return False
    
    async def _get_or_create_kv_bucket(self):
        """Get or create KV bucket, handling errors gracefully."""
        if not self.js:
            return None
        
        try:
            return await self.js.create_key_value(bucket=self.kv_bucket_name)
        except Exception:
            try:
                return await self.js.key_value(self.kv_bucket_name)
            except Exception as e:
                logger.warning("Could not create or access KV bucket: %s", e)
                return None
    
    async def _cleanup_subscriptions(self):
        """Unsubscribe from all subscriptions."""
        for sub in self._subscriptions + self._js_subscriptions:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
        self._subscriptions.clear()
        self._js_subscriptions.clear()
    
    def _reset_connection_state(self):
        """Reset connection-related state."""
        self._is_connected = False
        self.js = None
        self.kv = None
    
    # ==================== CONNECTION MANAGEMENT ====================
    
    async def connect(self) -> bool:
        """Connect to NATS server and initialize JetStream with auto-reconnection."""
        try:
            self.nc = await nats.connect(
                servers=self.servers,
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
                error_cb=self._error_callback,
                disconnected_cb=self._disconnected_callback,
                reconnected_cb=self._reconnected_callback,
                closed_cb=self._closed_callback
            )
            self.js = self.nc.jetstream()
            self.kv = await self._get_or_create_kv_bucket()
            self._is_connected = True
            logger.info("Connected to NATS servers: %s", self.servers)
            return True
        except Exception as e:
            logger.error("Failed to connect to NATS: %s", e)
            self._reset_connection_state()
            return False
    
    async def _error_callback(self, error: Exception):
        """Callback for NATS errors."""
        logger.error("NATS error: %s", error)
    
    async def _disconnected_callback(self):
        """Callback when disconnected from NATS."""
        logger.warning("Disconnected from NATS servers")
        self._reset_connection_state()
    
    async def _reconnected_callback(self):
        """Callback when reconnected to NATS."""
        logger.info("Reconnected to NATS servers")
        self._is_connected = True
        
        if self.nc:
            self.js = self.nc.jetstream()
            self.kv = await self._get_or_create_kv_bucket()
            self._js_subscriptions.clear()
            await self._resubscribe_handlers()
    
    async def _resubscribe_handlers(self):
        """Re-subscribe to all handlers after reconnection."""
        subscribe_methods = {
            'queue': self.subscribe_queue,
            'immediate': self.subscribe_immediate,
        }
        
        for handler_info in self._reconnect_handlers:
            try:
                handler_type = handler_info['type']
                handler = handler_info['handler']
                subscribe_method = subscribe_methods.get(handler_type)
                
                if subscribe_method:
                    await subscribe_method(handler)
                else:
                    logger.warning("Unknown handler type: %s", handler_type)
            except Exception as e:
                logger.error("Failed to re-subscribe %s: %s", handler_type, e)
    
    async def _closed_callback(self):
        """Callback when connection is closed."""
        logger.info("NATS connection closed")
        self._reset_connection_state()
    
    async def disconnect(self):
        """Disconnect from NATS server."""
        await self._cleanup_subscriptions()
        if self.nc:
            await self.nc.close()
            self._reset_connection_state()
            logger.info("Disconnected from NATS")
    
    # ==================== TELEMETRY (Core NATS, no JetStream) ====================
    
    async def publish_heartbeat(self):
        """Publish heartbeat telemetry (timestamp only)."""
        await self._publish_telemetry(self.tlm_heartbeat, {})
    
    async def publish_position(self, coords: Dict[str, float]):
        """Publish real-time position coordinates."""
        await self._publish_telemetry(self.tlm_pos, coords)
    
    async def publish_health(self, vitals: Dict[str, Any]):
        """Publish system health vitals (CPU, memory, temperature, etc.)."""
        await self._publish_telemetry(self.tlm_health, vitals)
    
    async def publish_status(self, status_data: Dict[str, Any]):
        """
        Update machine status in KV store.
        
        Args:
            status_data: Dictionary with status data (e.g., {'state': 'idle', 'run_id': 'abc123'})
                - state: 'idle', 'busy', or 'error'
                - run_id: Current run_id if busy/error, None if idle
        """
        if not self.kv:
            logger.warning("KV store not available, skipping status update")
            return
        
        try:
            message = {'timestamp': self._format_timestamp(), **status_data}
            await self.kv.put(self.machine_id, json.dumps(message).encode())
            logger.info("Updated status in KV store: state=%s, run_id=%s", 
                       status_data.get('state'), status_data.get('run_id'))
        except Exception as e:
            logger.error("Error updating status in KV store: %s", e)
    
    # ==================== COMMANDS (JetStream, exactly-once with run_id) ====================
    
    async def _ensure_stream(self, subject: str, stream_name: str):
        """Ensure a JetStream stream exists for the given subject."""
        try:
            await self.js.stream_info(stream_name)
            logger.debug("Stream already exists: %s", stream_name)
        except NotFoundError:
            await self.js.add_stream(name=stream_name, subjects=[subject])
            logger.info("Created stream: %s for subject: %s", stream_name, subject)
        except Exception as e:
            logger.warning("Could not ensure stream %s: %s", stream_name, e)
    
    @asynccontextmanager
    async def _keep_message_alive(self, msg, interval: int = KEEP_ALIVE_INTERVAL):
        """
        Context manager that maintains a background task to reset the 
        redelivery timer (in_progress) while the block/machine is executing.
        """
        async def _heartbeat():
            while True:
                await asyncio.sleep(interval)
                try:
                    await msg.in_progress()
                    logger.debug("Reset redelivery timer via keep-alive")
                except Exception:
                    break

        task = asyncio.create_task(_heartbeat())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    
    async def _publish_command_response(
        self, 
        msg,
        status: str, 
        error: Optional[str] = None
    ):
        """
        Publish command response message with full original payload and appended response.
        
        Args:
            msg: NATS message
            status: Status of the command (success, error)
            error: Error message if status is error
        """
        if not self.nc:
            return
        
        try:
            payload, run_id, command_id = self._parse_message(msg.data)
            if not run_id:
                return
            
            # Append response to the original payload
            payload['response'] = {
                'status': status,
                'completed_at': self._format_timestamp(),
                'error': error
            }
            
            await self.nc.publish(self.cmd_response, json.dumps(payload).encode())
            logger.debug("Published command response: run_id=%s, command_id=%s, status=%s", 
                       run_id, command_id, status)
        except Exception as e:
            logger.error("Error publishing command response: %s", e)
    
    async def _process_message(
        self, 
        msg, 
        handler: Callable, 
        check_pause: bool = False
    ) -> None:
        """
        Handle the lifecycle of a single message: Parse -> Handle -> Ack/Nak/Term.
        
        Args:
            msg: NATS message
            handler: Handler function to process the message
            check_pause: If True, check pause state before processing (for queue messages)
        """
        try:
            # Parse payload
            payload, run_id, command_id = self._parse_message(msg.data)
            
            # Check if cancelled
            if run_id and run_id in self._cancelled_run_ids:
                logger.info("Skipping cancelled command: run_id=%s, command_id=%s", run_id, command_id)
                await msg.ack()
                return
            
            # Check if paused (for queue messages)
            if check_pause:
                async with self._pause_lock:
                    while self._is_paused:
                        await msg.in_progress()
                        await asyncio.sleep(1)
                        # Re-check cancelled state in case it was cancelled while paused
                        if run_id and run_id in self._cancelled_run_ids:
                            logger.info("Command cancelled while paused: run_id=%s", run_id)
                            await msg.ack()
                            return
            
            # Execute handler with auto-heartbeat (task might take a while for machine to complete)
            async with self._keep_message_alive(msg):
                success = await handler(payload)
            
            # Finalize message state
            if success:
                await msg.ack()
                await self._publish_command_response(msg, 'success')
                await self.publish_status({'state': 'idle', 'run_id': run_id})
            else:
                await msg.nak()
                await self._publish_command_response(msg, 'error', 'Handler returned False')
                await self.publish_status({'state': 'error', 'run_id': run_id})

        except json.JSONDecodeError as e:
            logger.error("JSON Decode Error. Terminating message.")
            await msg.term()
            await self._publish_command_response(msg, 'error', f'JSON decode error: {e}')
            await self.publish_status({'state': 'error', 'run_id': run_id})
        
        except Exception as e:
            logger.error("Handler failed: %s", e)
            await msg.nak()
            await self._publish_command_response(msg, 'error', str(e))
            await self.publish_status({'state': 'error', 'run_id': run_id})
    
    async def _process_immediate_command(self, msg, handler: Callable) -> None:
        """Process immediate commands (pause, cancel, unpause, etc.)."""
        try:
            payload, run_id, _ = self._parse_message(msg.data)
            # Ack immediately after successful parse
            await msg.ack()
            
            header = payload.get('header', {})
            command = header.get('command', '').lower()
            
            # Handle built-in commands
            if command == 'pause':
                async with self._pause_lock:
                    if not self._is_paused:
                        self._is_paused = True
                        logger.info("Queue paused")
                        await self.publish_status({'state': 'paused', 'run_id': run_id})
                await self._publish_command_response(msg, 'success')
                return
            
            elif command == 'unpause':
                async with self._pause_lock:
                    if self._is_paused:
                        self._is_paused = False
                        logger.info("Queue unpaused")
                        await self.publish_status({'state': 'idle', 'run_id': None})
                await self._publish_command_response(msg, 'success')
                return
            
            elif command == 'cancel':
                if run_id:
                    self._cancelled_run_ids.add(run_id)
                    logger.info("Cancelled all commands with run_id: %s", run_id)
                    await self.publish_status({'state': 'idle', 'run_id': None})
                await self._publish_command_response(msg, 'success')
                return
            
            # For other immediate commands, call the user-provided handler
            async with self._keep_message_alive(msg):
                success = await handler(payload)
            
            if success:
                await self._publish_command_response(msg, 'success')
            else:
                await self._publish_command_response(msg, 'error', 'Handler returned False')
        
        except json.JSONDecodeError as e:
            logger.error("JSON Decode Error in immediate command: %s", e)
            await msg.term()
            await self.publish_status({'state': 'error', 'run_id': None})
        
        except Exception as e:
            logger.error("Error processing immediate command: %s", e)
            await msg.nak()
            await self.publish_status({'state': 'error', 'run_id': None})
    
    async def _create_consumer(
        self, 
        stream_name: str, 
        consumer_name: str, 
        deliver_subject: Optional[str] = None
    ):
        """Create a pull/push consumer for a stream (idempotent)."""
        try:
            await self.js.add_consumer(
                stream_name,
                durable=consumer_name,
                config={
                    'deliver_policy': 'all',
                    'ack_policy': 'explicit',
                    'max_deliver': self.CONSUMER_MAX_DELIVER,
                    'deliver_subject': deliver_subject,  # None means pull consumer
                }
            )
        except Exception:
            pass  # Consumer likely exists
    
    def _register_handler(self, handler_type: str, handler: Callable):
        """Register a handler for reconnection."""
        if not any(h['type'] == handler_type for h in self._reconnect_handlers):
            self._reconnect_handlers.append({'type': handler_type, 'handler': handler})
    
    async def subscribe_queue(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[bool]]
    ):
        """Subscribe to main queue commands (standard commands, processed one by one, FIFO)."""
        if not self.js:
            logger.error("JetStream not available for queue commands")
            return
        
        try:
            # setup pull consumer
            consumer_name = f"{self.machine_id}_queue"
            await self._ensure_stream(self.cmd_queue, self.cmd_stream_queue)
            await self._create_consumer(self.cmd_stream_queue, consumer_name, deliver_subject=None)

            sub = await self.js.pull_subscribe(self.cmd_queue, durable=consumer_name)
            self._js_subscriptions.append(sub)
            
            async def consume_messages():
                """Continuously consume messages from a pull subscription."""
                while True:
                    try:
                        msgs = await sub.fetch(1, timeout=self.FETCH_TIMEOUT)
                        for msg in msgs:
                            await self._process_message(msg, handler, check_pause=True)
                    except TimeoutError:
                        continue
                    except Exception as e:
                        logger.error("Error consuming messages: %s", e)
                        await asyncio.sleep(1)
            
            asyncio.create_task(consume_messages())
            self._register_handler('queue', handler)
            logger.info("Subscribed to queue commands: %s", self.cmd_queue)
        
        except Exception as e:
            logger.error("Failed to subscribe to queue: %s", e)
    
    async def subscribe_immediate(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[bool]]
    ):
        """
        Subscribe to immediate queue commands using push subscription.
        Urgent commands like pause, cancel, etc. are processed immediately.
        """
        if not self.js:
            logger.error("JetStream not available for immediate commands")
            return
        
        try:
            # setup push consumer
            consumer_name = f"{self.machine_id}_immediate"
            deliver_subject = f"{self.cmd_immediate}.deliver"
            await self._ensure_stream(self.cmd_immediate, self.cmd_stream_immediate)
            await self._create_consumer(self.cmd_stream_immediate, consumer_name, deliver_subject=deliver_subject)

            async def message_handler(msg):
                """Handle immediate commands (pause, cancel, unpause, etc.)."""
                await self._process_immediate_command(msg, handler)
            
            sub = await self.js.subscribe(
                self.cmd_immediate,
                durable=consumer_name,
                cb=message_handler
            )
            self._js_subscriptions.append(sub)
            self._register_handler('immediate', handler)
            logger.info("Subscribed to immediate commands (push): %s", self.cmd_immediate)
        
        except Exception as e:
            logger.error("Failed to subscribe to immediate commands: %s", e)
    
    # ==================== EVENTS (JetStream) ====================
    
    async def publish_log(self, log_level: str, msg: str, **kwargs):
        """Publish log event."""
        await self._publish_event(
            self.evt_log,
            self.evt_stream_log,
            {'log_level': log_level, 'msg': msg, **kwargs}
        )
    
    async def publish_alert(self, alert_type: str, severity: str, **kwargs):
        """Publish alert event for critical issues."""
        await self._publish_event(
            self.evt_alert,
            self.evt_stream_alert,
            {'type': alert_type, 'severity': severity, **kwargs}
        )
    
    async def publish_media(self, media_url: str, media_type: str = "image", **kwargs):
        """Publish media event after uploading to object storage."""
        await self._publish_event(
            self.evt_media,
            self.evt_stream_media,
            {'media_url': media_url, 'media_type': media_type, **kwargs}
        )
