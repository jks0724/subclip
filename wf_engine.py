import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from prefect import flow, task, get_run_logger
from prefect.states import State, Completed, Failed, Running
from prefect.futures import PrefectFuture
from prefect.context import get_run_context
from prefect.client.schemas import FlowRun, TaskRun
from prefect.events import emit_event
import websockets
import threading
from collections import defaultdict

# Import your existing modules
from Connection.pg import create_connection
from psycopg2 import sql
from Utility.utils import getCurrentUTCtime
from APIs.Common.logger import getWorkflowRunsLogger
from Connection.mongoConn import getConnection
from Email.service import email_service

# MongoDB setup
DN_db, DN_client = getConnection()
workspaceCollection = DN_db.get_collection("workspace")
workflowCollection = DN_db.get_collection("workflow")
jobRunCollection = DN_db.get_collection("jobRun")
userActivityCollection = DN_db.get_collection("userActivity")

# Import your existing data source modules
from CoreEngine.modularize import (
    bigquery, json_file, snowflake, MySql, postgres, googlesheet, 
    mariadb, aurorapostgresql, oracle, amazons3, yugabyte, 
    spanner, mongodb, supabase, singlestore, teradata, timescale, 
    yellowbrick, cockroachdb, cockroachdbaws, cockroachdbgcp, 
    clickhouse, csv, tsv
)
from CoreEngine.Transformations import (
    transformations, transformation_groupby, transformation_join,
    transformations_filter, transformations_selectcolumns
)

logger = getWorkflowRunsLogger


class WorkflowWebSocketManager:
    """Manages WebSocket connections for workflow status updates"""
    
    def __init__(self):
        self.connections: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.workflow_status: Dict[str, str] = {}
        
    async def register_connection(self, workflow_id: str, websocket):
        """Register a WebSocket connection for a workflow"""
        self.connections[workflow_id] = websocket
    
    async def unregister_connection(self, workflow_id: str):
        """Unregister a WebSocket connection"""
        if workflow_id in self.connections:
            del self.connections[workflow_id]
    
    async def broadcast_status(self, workflow_id: str, message: Dict[str, Any]):
        """Broadcast status update to WebSocket"""
        if workflow_id in self.connections:
            try:
                await self.connections[workflow_id].send(json.dumps(message))
            except websockets.exceptions.ConnectionClosed:
                await self.unregister_connection(workflow_id)
    
    async def broadcast_to_all(self, message: Dict[str, Any]):
        """Broadcast message to all connected WebSockets"""
        for workflow_id, websocket in list(self.connections.items()):
            try:
                await websocket.send(json.dumps(message))
            except websockets.exceptions.ConnectionClosed:
                await self.unregister_connection(workflow_id)

# Global WebSocket manager
ws_manager = WorkflowWebSocketManager()


class PrefectWorkflowState:
    """Enhanced workflow state management for Prefect"""
    
    def __init__(self):
        self.completed_nodes: Dict[str, Any] = {}
        self.failed_nodes: Dict[str, str] = {}
        self.running_nodes: Dict[str, PrefectFuture] = {}
        self.node_storage_paths: Dict[str, str] = {}
        self.node_metadata: Dict[str, Dict[str, Any]] = {}
        self.workflow_start_time: Optional[datetime] = None
        self.workflow_status: str = "Pending"

# Global state instance
workflow_state = PrefectWorkflowState()


# Enhanced task registry with async support
@task(name="postgres_extract", retries=1, retry_delay_seconds=30)
async def postgres_extract_task(node_config: Dict[str, Any], **params) -> Dict[str, Any]:
    """Prefect task wrapper for PostgreSQL extraction"""
    node_id = params["node_id"]
    node_name = params["node_name"]
    workflow_run_id = params["workflow_run_id"]
    
    logger = get_run_logger()
    logger.info(f"Starting PostgreSQL extraction for node {node_id}")
    
    try:
        # Send status update
        await send_node_status_update(
            workflow_run_id, node_id, "Running", "Started extraction"
        )
        
        # Call your existing postgres extraction function
        result = await postgres.postgres_extract(node_config, node_id, node_name, **params)
        
        # Update workflow state
        workflow_state.completed_nodes[node_id] = result
        workflow_state.node_storage_paths[node_id] = result.get("storage_path", "")
        
        await send_node_status_update(
            workflow_run_id, node_id, "Completed", 
            f"Extracted {result.get('row_count', 0)} rows"
        )
        
        return result
        
    except Exception as ex:
        workflow_state.failed_nodes[node_id] = str(ex)
        await send_node_status_update(
            workflow_run_id, node_id, "Failed", str(ex)
        )
        raise


@task(name="postgres_load", retries=1, retry_delay_seconds=30)
async def postgres_load_task(
    node_config: Dict[str, Any], 
    parent_results: List[Dict[str, Any]] = None, 
    **params
) -> Dict[str, Any]:
    """Prefect task wrapper for PostgreSQL loading"""
    node_id = params["node_id"]
    node_name = params["node_name"]
    workflow_run_id = params["workflow_run_id"]
    
    logger = get_run_logger()
    logger.info(f"Starting PostgreSQL loading for node {node_id}")
    
    try:
        await send_node_status_update(
            workflow_run_id, node_id, "Running", "Started loading"
        )
        
        # Get storage path from parent results
        storage_path = None
        if parent_results and len(parent_results) > 0:
            storage_path = parent_results[0].get("storage_path")
        
        # Call your existing postgres loading function
        result = await postgres.postgres_load(
            node_config, node_id, node_name, storage_path, **params
        )
        
        # Update workflow state
        workflow_state.completed_nodes[node_id] = result
        
        await send_node_status_update(
            workflow_run_id, node_id, "Completed", 
            f"Loaded {result.get('row_count', 0)} rows"
        )
        
        return result
        
    except Exception as ex:
        workflow_state.failed_nodes[node_id] = str(ex)
        await send_node_status_update(
            workflow_run_id, node_id, "Failed", str(ex)
        )
        raise


# Registry mapping connector types to their Prefect tasks
EXTRACTION_TASKS = {
    "postgresql": postgres_extract_task,
    "gcp-bigquery": bigquery.start_bigquery_extraction_task,
    "snowflake": snowflake.start_snowflake_extraction_task,
    "mysql": MySql.start_mySql_extraction_task,
    # Add all your other extractors here...
}

LOADER_TASKS = {
    "postgresql": postgres_load_task,
    "gcp-bigquery": bigquery.start_bigquery_loader_task,
    "snowflake": snowflake.start_snowflake_loader_task,
    "mysql": MySql.start_mySql_loader_task,
    # Add all your other loaders here...
}


async def send_node_status_update(
    workflow_run_id: str, 
    node_id: str, 
    status: str, 
    message: str = ""
):
    """Send node status update via WebSocket"""
    update_message = {
        "workflowRunId": workflow_run_id,
        "nodeId": node_id,
        "status": status,
        "message": message,
        "timestamp": datetime.now().isoformat()
    }
    
    await ws_manager.broadcast_status(workflow_run_id, update_message)


async def send_workflow_status_update(
    workflow_run_id: str, 
    workflow_status: str, 
    message: str = ""
):
    """Send workflow-level status update via WebSocket"""
    update_message = {
        "workflowRunId": workflow_run_id,
        "workflowStatus": workflow_status,
        "message": message,
        "timestamp": datetime.now().isoformat()
    }
    
    await ws_manager.broadcast_status(workflow_run_id, update_message)


def get_node_kind(sequence: int, dependencies: Dict[str, List[str]], node_id: str) -> str:
    """Determine if node is extract, transform, or load based on dependencies"""
    # Root nodes (no dependencies) are extractors
    if not dependencies.get(node_id, []):
        return "extract"
    
    # Check if this node has children (nodes that depend on it)
    has_children = any(node_id in deps for deps in dependencies.values())
    
    # If it has both parents and children, it's a transformation/intermediate
    if has_children:
        return "transform"  # Could be load then extract for intermediate DB nodes
    
    # Leaf nodes (no children) are loaders
    return "load"


def get_prefect_task(node_info: Dict[str, Any], kind: str):
    """Get the appropriate Prefect task for a node"""
    connector_type = node_info["connector_type"]
    
    if kind == "extract":
        if connector_type in EXTRACTION_TASKS:
            return EXTRACTION_TASKS[connector_type]
    elif kind in ["load", "transform"]:
        if connector_type in LOADER_TASKS:
            return LOADER_TASKS[connector_type]
    
    raise ValueError(f"No {kind} task found for connector type: {connector_type}")


def group_nodes_by_level(dependencies: Dict[str, List[str]]) -> Dict[int, List[str]]:
    """Group nodes by their dependency level for staged execution"""
    levels = {}
    node_levels = {}
    
    def calculate_level(node_id: str) -> int:
        if node_id in node_levels:
            return node_levels[node_id]
        
        deps = dependencies.get(node_id, [])
        if not deps:
            level = 0
        else:
            level = max(calculate_level(dep) for dep in deps) + 1
        
        node_levels[node_id] = level
        return level
    
    # Calculate levels for all nodes
    for node_id in dependencies.keys():
        level = calculate_level(node_id)
        if level not in levels:
            levels[level] = []
        levels[level].append(node_id)
    
    return levels


async def update_database_status(
    workflow_run_id: str,
    workflow_id: str,
    node_id: str = None,
    status: str = "Running",
    row_count: int = 0,
    **params
):
    """Update database with node/workflow status"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        if node_id:
            # Update node status
            node_update_query = sql.SQL(
                "UPDATE NODE_RUN_DETAILS SET node_status = %s WHERE workflow_run_id = %s AND node_id = %s"
            )
            cursor.execute(node_update_query, (status, workflow_run_id, node_id))
        
        # Update workflow status
        workflow_update_query = sql.SQL(
            "UPDATE WORKFLOW_RUN_STATS SET run_status = %s WHERE workflow_run_id = %s"
        )
        cursor.execute(workflow_update_query, (status, workflow_run_id))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error updating database status: {str(ex)}")


async def handle_workflow_completion(
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    status: str,
    **params
):
    """Handle workflow completion - success or failure"""
    try:
        # Send final status update
        await send_workflow_status_update(workflow_run_id, status)
        
        # Update database
        await update_database_status(
            workflow_run_id, workflow_id, status=status
        )
        
        # Send email notification if configured
        job_run_dict = params.get("job_run_dict", {})
        workflows = job_run_dict.get("workflows", [{}])
        notify_email = workflows[0].get("notifyEmail", "")
        on_success_notify = workflows[0].get("onSuccessNotify", False)
        
        if on_success_notify and notify_email:
            email_to = [notify_email] if isinstance(notify_email, str) else notify_email
            await email_service.send_workflow_status_notification(
                workflow_name=workflow_name,
                workflow_status=status,
                email_to=email_to,
                workflow_details=f"Workflow {workflow_name} {status.lower()}"
            )
        
        # Final log message
        completion_message = f"Workflow {workflow_name} {status.lower()} successfully"
        logger.info(completion_message)
        
        # Update workflow state
        workflow_state.workflow_status = status
        
    except Exception as ex:
        logger.error(f"Error handling workflow completion: {str(ex)}")


@flow(name="dynamic_workflow_execution", retries=1)
async def execute_dynamic_workflow(
    job_run_dict: Dict[str, Any],
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    token_dict: Dict[str, Any],
    **flow_params
) -> Dict[str, Any]:
    """
    Main Prefect flow for dynamic workflow execution
    Supports up to 20 levels and parallel execution within levels
    """
    logger = get_run_logger()
    logger.info(f"Starting dynamic workflow: {workflow_name}")
    
    # Initialize workflow state
    workflow_state.workflow_start_time = datetime.now()
    workflow_state.workflow_status = "Running"
    
    try:
        # Send initial status
        await send_workflow_status_update(workflow_run_id, "Running", "Workflow started")
        
        # Extract workflow configuration
        workflows = job_run_dict.get("workflows", [])
        if not workflows:
            raise ValueError("No workflows found in job_run_dict")
        
        workflow_config = workflows[0]
        dependencies = workflow_config.get("dependencies", {})
        nodes = {node["nodeId"]: node for node in workflow_config.get("nodes", [])}
        input_params = json.loads(job_run_dict.get("inputParams", "{}"))
        
        # Group nodes by dependency level (supports up to 20 levels)
        levels = group_nodes_by_level(dependencies)
        max_levels = 20
        
        if len(levels) > max_levels:
            raise ValueError(f"Workflow exceeds maximum supported levels ({max_levels})")
        
        logger.info(f"Workflow has {len(levels)} levels with nodes: {levels}")
        
        # Execute nodes level by level
        node_results = {}
        
        for level in sorted(levels.keys()):
            level_nodes = levels[level]
            logger.info(f"Processing level {level} with nodes: {level_nodes}")
            
            # Create tasks for all nodes at this level (parallel execution)
            level_futures = []
            
            for node_id in level_nodes:
                if node_id not in nodes:
                    logger.warning(f"Node {node_id} not found in nodes configuration")
                    continue
                
                node_info = nodes[node_id]
                node_config = input_params.get(node_id, {})
                
                # Determine task type
                kind = get_node_kind(node_info.get("sequence", 1), dependencies, node_id)
                
                # Get the appropriate Prefect task
                prefect_task = get_prefect_task(node_info, kind)
                
                # Prepare task parameters
                task_params = {
                    "node_id": node_id,
                    "node_name": node_info.get("nodeName", f"Node_{node_id}"),
                    "workflow_run_id": workflow_run_id,
                    "workflow_id": workflow_id,
                    "workflow_name": workflow_name,
                    "job_run_dict": job_run_dict,
                    "token_dict": token_dict,
                    **flow_params
                }
                
                # Get parent results for this node
                parent_node_ids = dependencies.get(node_id, [])
                parent_results = [node_results[pid] for pid in parent_node_ids if pid in node_results]
                
                # Submit task based on type
                if kind == "extract":
                    future = prefect_task.submit(node_config, **task_params)
                else:  # load or transform
                    future = prefect_task.submit(node_config, parent_results, **task_params)
                
                level_futures.append((node_id, future))
                workflow_state.running_nodes[node_id] = future
            
            # Wait for all tasks at this level to complete
            level_results = {}
            for node_id, future in level_futures:
                try:
                    result = await future.result()
                    level_results[node_id] = result
                    workflow_state.completed_nodes[node_id] = result
                    
                    # Remove from running nodes
                    if node_id in workflow_state.running_nodes:
                        del workflow_state.running_nodes[node_id]
                        
                except Exception as ex:
                    logger.error(f"Node {node_id} failed: {str(ex)}")
                    workflow_state.failed_nodes[node_id] = str(ex)
                    
                    # Send failure notification
                    await send_node_status_update(workflow_run_id, node_id, "Failed", str(ex))
                    raise
            
            # Add level results to overall results
            node_results.update(level_results)
            
            logger.info(f"Level {level} completed successfully")
        
        # All levels completed successfully
        await handle_workflow_completion(
            workflow_run_id, workflow_id, workflow_name, "Completed", 
            job_run_dict=job_run_dict
        )
        
        execution_summary = {
            "workflow_run_id": workflow_run_id,
            "workflow_name": workflow_name,
            "status": "Completed",
            "levels_processed": len(levels),
            "nodes_completed": len(workflow_state.completed_nodes),
            "execution_time": (datetime.now() - workflow_state.workflow_start_time).total_seconds(),
            "results": node_results
        }
        
        logger.info(f"Workflow completed successfully: {execution_summary}")
        return execution_summary
        
    except Exception as ex:
        logger.error(f"Workflow failed: {str(ex)}")
        
        # Handle workflow failure
        await handle_workflow_completion(
            workflow_run_id, workflow_id, workflow_name, "Failed",
            job_run_dict=job_run_dict
        )
        
        # Re-raise the exception
        raise


# Enhanced workflow runner function (replaces start_long_running_task)
async def start_prefect_workflow(
    job_run_dict: Dict[str, Any],
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    token_dict: Dict[str, Any],
    module: str = "workflows",
    from_job: bool = False,
    **additional_params
) -> Any:
    """
    Start a Prefect workflow (replaces the Redis RQ approach)
    
    Args:
        job_run_dict: Job run configuration
        workflow_run_id: Unique workflow run identifier
        workflow_id: Workflow identifier
        workflow_name: Workflow name
        token_dict: User token information
        module: Module type ("workflows" or "jobs")
        from_job: Whether this is part of a job
        **additional_params: Additional parameters
    
    Returns:
        Prefect flow run result
    """
    try:
        logger.info(f"Starting Prefect workflow: {workflow_name}")
        
        # Prepare flow parameters
        flow_params = {
            "module": module,
            "from_job": from_job,
            "workflow_log_path": f"/logs/{workflow_name}_{workflow_run_id}.log",
            **additional_params
        }
        
        # Run the Prefect flow
        flow_run = await execute_dynamic_workflow(
            job_run_dict=job_run_dict,
            workflow_run_id=workflow_run_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            token_dict=token_dict,
            **flow_params
        )
        
        logger.info(f"Prefect workflow completed: {workflow_name}")
        return flow_run
        
    except Exception as ex:
        logger.error(f"Error starting Prefect workflow {workflow_name}: {str(ex)}")
        raise


# WebSocket server for real-time updates
async def websocket_handler(websocket, path):
    """Handle WebSocket connections for real-time workflow updates"""
    try:
        # Extract workflow_id from path
        workflow_id = path.strip("/")
        await ws_manager.register_connection(workflow_id, websocket)
        
        logger.info(f"WebSocket connected for workflow: {workflow_id}")
        
        # Keep connection alive
        async for message in websocket:
            # Handle incoming messages if needed
            pass
            
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"WebSocket disconnected for workflow: {workflow_id}")
    except Exception as ex:
        logger.error(f"WebSocket error: {str(ex)}")
    finally:
        if 'workflow_id' in locals():
            await ws_manager.unregister_connection(workflow_id)


def start_websocket_server(host="localhost", port=8765):
    """Start WebSocket server for workflow updates"""
    import websockets
    
    logger.info(f"Starting WebSocket server on {host}:{port}")
    return websockets.serve(websocket_handler, host, port)


# Usage example and integration points
if __name__ == "__main__":
    # Example usage
    sample_job_run_dict = {
        "jobRunId": str(uuid.uuid4()),
        "workflow_id": "test-workflow",
        "workflows": [{
            "workflowRunId": str(uuid.uuid4()),
            "workflow_name": "Test Workflow",
            "dependencies": {
                "extract_node": [],
                "load_node": ["extract_node"]
            },
            "nodes": [
                {
                    "nodeId": "extract_node",
                    "nodeName": "Extract Data",
                    "nodeType": "postgresql",
                    "sequence": 1
                },
                {
                    "nodeId": "load_node", 
                    "nodeName": "Load Data",
                    "nodeType": "postgresql",
                    "sequence": 2
                }
            ],
            "notifyEmail": "admin@example.com",
            "onSuccessNotify": True
        }],
        "inputParams": json.dumps({
            "extract_node": {
                "connector_type": "postgresql",
                "sequence": 1,
                "host-name": "localhost",
                "db": "test_db",
                "table": "source_table"
            },
            "load_node": {
                "connector_type": "postgresql", 
                "sequence": 2,
                "host-name": "localhost",
                "db": "test_db",
                "table": "target_table"
            }
        })
    }
    
    token_dict = {
        "UserId": "user123",
        "fullName": "Test User",
        "infoId": "info123"
    }
    
    # This would replace your original start_long_running_task call
    asyncio.run(start_prefect_workflow(
        job_run_dict=sample_job_run_dict,
        workflow_run_id=str(uuid.uuid4()),
        workflow_id="test-workflow-id",
        workflow_name="Test Workflow",
        token_dict=token_dict
    ))
