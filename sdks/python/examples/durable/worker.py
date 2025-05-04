import json
import logging
import hashlib
from typing import Dict, Any, Literal, Set
from pydantic import BaseModel, Field
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from hatchet_sdk import (
    DurableContext,
    Hatchet,
    UserEventCondition,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [WORKER] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize Hatchet and console
logger.info("Initializing Hatchet client")
hatchet = Hatchet(debug=True)
console = Console()

# Event keys for communication
COMMAND_EVENT_KEY = "worker:command"
WORKER_RESPONSE_KEY = "worker:response"


class CommandMessage(BaseModel):
    """Message sent from trigger to worker"""
    command: str = Field(
        ..., 
        description="Command to execute"
    )
    data: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Additional command data"
    )
    type: Literal["command"] = Field(
        default="command", 
        description="Message type"
    )

    def calculate_hash(self) -> str:
        """Calculate a hash of the command and data for verification"""
        content = f"{self.command}:{json.dumps(self.data, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:8]


class WorkerResponse(BaseModel):
    """Message sent from worker back to trigger"""
    message: str = Field(
        ..., 
        description="Response message"
    )
    data: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Additional response data"
    )
    type: Literal["response"] = Field(
        default="response", 
        description="Message type"
    )
    message_hash: str = Field(
        default="", 
        description="Hash of the original message"
    )


def create_command_table(command: 'CommandMessage') -> Table:
    """Create a rich table for displaying the command"""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green")
    
    # Add command info
    table.add_row("Command", command.command)
    table.add_row("Hash", command.calculate_hash())
    
    # Add data fields
    for key, value in command.data.items():
        table.add_row(key, str(value))
    
    return table


def create_response_table(response: 'WorkerResponse') -> Table:
    """Create a rich table for displaying the response"""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green")
    
    # Add basic info
    table.add_row("Message", response.message)
    table.add_row("Message Hash", response.message_hash)
    
    # Add data fields
    for key, value in response.data.items():
        table.add_row(key, str(value))
    
    return table


# Create workflow
logger.info("Creating communication workflow")
worker_workflow = hatchet.workflow(
    name="CommunicationWorkflow",
    input_validator=CommandMessage
)


@worker_workflow.durable_task()
async def communication_task(
    input: CommandMessage, 
    ctx: DurableContext
) -> None:
    """
    Single durable task that handles continuous communication
    
    Args:
        input: Initial command message
        ctx: Durable task context
    """
    logger.info(f"Starting worker with command: {input.command}")
    logger.info(f"Initial input data: {input.data}")
    
    # Set for deduplication
    processed_hashes: Set[str] = set()
    
    while True:  # Keep running until stop command
        try:
            # Stream ready status
            logger.info("Sending ready status")
            ready_response = WorkerResponse(
                message="Waiting for command...",
                data={"status": "ready"}
            )
            ctx.put_stream(json.dumps(ready_response.model_dump()))
            
            # Wait for next command
            logger.info("Waiting for next command...")
            event_data = await ctx.aio_wait_for(
                "command",
                UserEventCondition(event_key=COMMAND_EVENT_KEY),
            )
            
            # Parse command from event data
            try:
                # Log raw event data for debugging
                logger.info(f"Received raw event data: {event_data}")
                
                # --- MODIFIED PARSING LOGIC ---
                actual_payload = None
                if isinstance(event_data, dict) and 'CREATE' in event_data:
                    event_details = event_data.get('CREATE', {})
                    command_list = event_details.get(COMMAND_EVENT_KEY, [])
                    if command_list and isinstance(command_list, list):
                        # Assume the latest event is the one we want
                        actual_payload = command_list[-1] 
                        logger.info(f"Extracted payload: {actual_payload}")
                    else:
                        logger.warning(
                            f"Could not find command list in event_data['CREATE']: "
                            f"{event_details}"
                        )
                elif isinstance(event_data, dict) and "command" in event_data:
                    # Handle case where event might be the raw payload (less likely based on logs)
                    actual_payload = event_data
                    logger.info(
                        f"Using event_data directly as payload: {actual_payload}"
                    )
                elif isinstance(event_data, str):
                    try:
                        actual_payload = json.loads(event_data)
                        logger.info(f"Parsed JSON string payload: {actual_payload}")
                    except json.JSONDecodeError:
                        logger.error(f"Received non-JSON string event data: {event_data}")
                        # Potentially raise an error or handle as a simple command?
                        # For now, let's try to treat it as a basic command string
                        actual_payload = {"command": event_data, "data": {}, "type": "command"}
                        logger.warning(f"Treating non-JSON string as basic command: {actual_payload}")

                if not actual_payload:
                    logger.error(f"Failed to extract command payload from event_data: {event_data}")
                    # Skip this event or raise an error
                    continue # Skip processing this event

                # Validate and create command object
                logger.info(f"Validating payload: {actual_payload}")
                command = CommandMessage.model_validate(actual_payload)
                # --- END MODIFIED PARSING LOGIC ---

                msg_hash = command.calculate_hash()
                
                # Skip if already processed (deduplication)
                if msg_hash in processed_hashes:
                    logger.info(
                        f"Skipping duplicate command with hash: {msg_hash}"
                    )
                    continue
                
                processed_hashes.add(msg_hash)
                
                # Display received command
                table = create_command_table(command)
                console.print(
                    Panel(
                        table,
                        title="📥 Received Command",
                        border_style="blue"
                    )
                )
                
            except Exception as e:
                import traceback
                tb_str = traceback.format_exc()
                logger.error(
                    f"Failed to parse or validate command: {str(e)}\n{tb_str}"
                )
                # Decide if we should continue or break the loop on parse failure
                continue # Skip this malformed event
            
            # Handle stop command
            if command.command.lower() == "stop":
                logger.info("Received stop command, shutting down...")
                final_response = WorkerResponse(
                    message="Stopping worker...",
                    data={"status": "stopping"},
                    message_hash=msg_hash
                )
                
                # Display and send response
                table = create_response_table(final_response)
                console.print(
                    Panel(
                        table,
                        title="📤 Sending Response",
                        border_style="green"
                    )
                )
                ctx.put_stream(json.dumps(final_response.model_dump()))
                break
            
            # Process command
            logger.info(f"Processing command: {command.command}")
            
            # Create response with command verification
            response = WorkerResponse(
                message=f"Processed command: {command.command}",
                data={
                    "received_data": command.data,
                    "timestamp": "2024-03-21T10:00:00Z",
                    "command_verified": True,
                    "original_command": command.command
                },
                message_hash=msg_hash
            )
            
            # Display and send response
            table = create_response_table(response)
            console.print(
                Panel(
                    table,
                    title="📤 Sending Response",
                    border_style="green"
                )
            )
            ctx.put_stream(json.dumps(response.model_dump()))
            
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(
                f"Error processing command: {str(e)}\n{tb_str}"
            )
            error_response = WorkerResponse(
                message=f"Error processing command: {str(e)}",
                data={"error": str(e), "traceback": tb_str},
                message_hash="error"
            )
            
            # Display and send error response
            table = create_response_table(error_response)
            console.print(
                Panel(
                    table,
                    title="⚠️ Error Response",
                    border_style="red"
                )
            )
            ctx.put_stream(json.dumps(error_response.model_dump()))


def main() -> None:
    """Start the worker service"""
    try:
        logger.info("Starting worker service")
        worker = hatchet.worker(
            "communication-worker",
            workflows=[worker_workflow],
        )
        logger.info("Worker initialized, starting...")
        worker.start()
    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        logger.error(f"Failed to start worker: {str(e)}\n{tb_str}")
        raise


if __name__ == "__main__":
    main()
