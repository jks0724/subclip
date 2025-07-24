import asyncio
import json
import uuid
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from fastapi import HTTPException, status

# Prefect imports
from prefect import get_client
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment
from prefect.exceptions import PrefectException

# Your existing imports
from Connection.pg import create_connection
from psycopg2 import sql
from Utility.utils import getCurrentUTCtime, updateModelObject
from APIs.Common.logger import getWorkflowRunsLogger
from UserManagement.Roles.service import Roles
from Connection.mongoConn import getConnection

# Import the Prefect workflow components
from prefect_workflow_engine import (
    execute_dynamic_workflow,
    start_prefect_workflow,
    ws_manager,
    PrefectWorkflowState
)

# MongoDB setup
DN_db, DN_client = getConnection()
workspaceCollection = DN_db.get_collection("workspace")
workflowCollection = DN_db.get_collection("workflow")
jobRunCollection = DN_db.get_collection("jobRun")
userActivityCollection = DN_db.get_collection("userActivity")


class PrefectWorkflowService:
    """
    Enhanced workflow service using Prefect instead of Redis RQ
    Maintains same interface as original WorkflowRun class
    """
    
    def __init__(self):
        self.logger = getWorkflowRunsLogger
        self.workflow_states: Dict[str, PrefectWorkflowState] = {}
        
    async def addWorkflowRun(
        self,
        payload,
        user_obj: Dict[str, Any],
        from_job: bool = False,
        job_run_id: Optional[str] = None,
        module: str = "workflows"
    ) -> Dict[str, Any]:
        """
        Enhanced workflow runner using Prefect (replaces Redis RQ version)
        
        Args:
            payload: Workflow run payload
            user_obj: User information
            from_job: Whether this is part of a job
            job_run_id: Job run ID if part of job
            module: Module type ("workflows" or "jobs")
            
        Returns:
            Dict containing workflow run information
        """
        try:
            self.logger.info(
                f"Starting Prefect workflow run - User: {user_obj['fullName']}, "
                f"fromJob: {from_job}, jobRunId: {job_run_id}"
            )
            
            # Validate required fields
            if not (payload.job_id and payload.folder_id and payload.workflow_id):
                raise Exception("Please enter FOLDER ID, JOB ID, and WORKFLOW ID")
            
            # Get workflow details
            workflow = await workflowCollection.find_one({
                "workspace_id": payload.workspace_id,
                "folder_id": payload.folder_id,
                "job_id": payload.job_id,
                "workflow_id": payload.workflow_id
            })
            
            if not workflow:
                raise Exception(
                    f"Workflow not found for folder_id: {payload.folder_id}, "
                    f"job_id: {payload.job_id}, workflow_id: {payload.workflow_id}"
                )
            
            # Check workflow status
            if workflow.get("status") != "Active":
                raise Exception("Workflow is inactive and cannot be run")
            
            if workflow.get("runStatus") == "Running":
                raise Exception(
                    f"Workflow {workflow['workflow_name']} is already running"
                )
            
            # Check user permissions
            roles_instance = Roles()
            user_id = user_obj.get("UserId")
            
            if not user_id:
                self.logger.warning(f"User ID not found: {user_id}")
                return False
            
            required_role = await roles_instance.checkRolebyUserId(user_id, "Workflow Run")
            if not required_role:
                raise Exception("Permission Denied for Workflow Run")
            
            # Prepare workflow run data
            payload = updateModelObject(payload, ["created_at"])
            
            if not from_job:
                job_run_id = str(uuid.uuid4())
                setattr(payload, "jobRunId", job_run_id)
            
            workflow_run_id = str(uuid.uuid4())
            
            # Build workflow run dictionary
            workflow_run_dict = await self._build_workflow_run_dict(
                payload, workflow, workflow_run_id, user_obj, job_run_id
            )
            
            # Insert workflow run into database
            await jobRunCollection.insert_one(workflow_run_dict)
            
            # Update PostgreSQL workflow run stats
            await self._insert_workflow_run_stats(
                workflow, workflow_run_id, job_run_id, user_obj
            )
            
            # Update workflow collection status
            await self._update_workflow_status(
                workflow["workflow_id"], job_run_id, workflow_run_id, 
                user_obj["UserId"], "Running"
            )
            
            # Log user activity
            await self._log_user_activity(
                user_obj, workflow_run_dict, workflow["workflow_name"]
            )
            
            # Start Prefect workflow execution
            workflow_execution_result = await self._start_prefect_execution(
                workflow_run_dict, workflow_run_id, workflow["workflow_id"],
                workflow["workflow_name"], user_obj, module, from_job
            )
            
            self.logger.info(f"Prefect workflow started successfully: {workflow_run_id}")
            
            from bson import json_util
            return json.loads(json_util.dumps({"id": workflow_run_dict}))
            
        except Exception as ex:
            self.logger.error(f"addWorkflowRun failed: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))

    async def _build_workflow_run_dict(
        self,
        payload,
        workflow: Dict[str, Any],
        workflow_run_id: str,
        user_obj: Dict[str, Any],
        job_run_id: str
    ) -> Dict[str, Any]:
        """Build the workflow run dictionary"""
        workflow_run_dict = payload.dict()
        workflow_run_dict["created_by"] = user_obj["UserId"]
        workflow_run_dict["runBy"] = user_obj["UserId"]
        workflow_run_dict["runStatus"] = "Running"
        workflow_run_dict["status"] = "Active"
        workflow_run_dict["category"] = "workflows"
        workflow_run_dict["startTimestamp"] = workflow_run_dict["created_at"]
        workflow_run_dict["folder_name"] = workflow.get("folder_name", "")
        workflow_run_dict["workspace_name"] = workflow.get("workspace_name", "")
        workflow_run_dict["job_name"] = workflow.get("job_name", "")
        workflow_run_dict["inputParams"] = workflow.get("connectionInformation_v1", "{}")

        # Process node information
        input_params = json.loads(workflow_run_dict["inputParams"])
        from Utility.utils import getNodesAndActvity
        from Connection.mongoConn import getConnection
        DN_db, _ = getConnection()
        connector_collection = DN_db.get_collection("connectors")
        
        node_activity, nodes = await getNodesAndActvity(input_params, connector_collection)

        if not workflow_run_dict["workflows"]:
            workflow_run_dict["workflows"].append({})

        # Populate workflow details
        workflow_run_dict["workflows"][0].update({
            "workflow_id": workflow["workflow_id"],
            "workflow_name": workflow["workflow_name"],
            "workflowRunId": workflow_run_id,
            "nodes": nodes,
            "nodeActivity": node_activity,
            "startTimestamp": workflow_run_dict["created_at"],
            "end_timestamp": None,
            "dependencies": json.loads(workflow.get("dependencies", "{}")),
            "notifyEmail": workflow.get("notifyEmail", ""),
            "onFailureNotify": workflow.get("onFailureNotify", ""),
            "onSuccessNotify": workflow.get("onSuccessNotify", "")
        })

        return workflow_run_dict

    async def _insert_workflow_run_stats(
        self,
        workflow: Dict[str, Any],
        workflow_run_id: str,
        job_run_id: str,
        user_obj: Dict[str, Any]
    ):
        """Insert workflow run statistics into PostgreSQL"""
        utc_time = getCurrentUTCtime()
        
        data_params = (
            workflow.get("workspace_id"),
            workflow.get("workspace_name", ""),
            workflow.get("folder_id"),
            workflow.get("folder_name", ""),
            workflow.get("job_id"),
            job_run_id,
            workflow.get("job_name", ""),
            workflow["workflow_id"],
            workflow_run_id,
            workflow["workflow_name"],
            "Started",
            user_obj["UserId"],
            user_obj["fullName"],
            utc_time,
            user_obj.get("photoURL", ""),
        )
        
        connection = create_connection()
        query = sql.SQL("""
            INSERT INTO WORKFLOW_RUN_STATS(
                workspace_id, workspace_name, folder_id, folder_name, job_id, 
                job_run_id, job_name, workflow_id, workflow_run_id, workflow_name, 
                run_status, run_by_id, run_by, start_timestamp, user_photourl
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        with connection.cursor() as cursor:
            cursor.execute(query, data_params)
        connection.commit()
        connection.close()

    async def _update_workflow_status(
        self,
        workflow_id: str,
        job_run_id: str,
        workflow_run_id: str,
        user_id: str,
        status: str
    ):
        """Update workflow status in workflow collection"""
        utc_time = getCurrentUTCtime()
        
        await workflowCollection.update_one(
            {"workflow_id": workflow_id},
            {
                "$set": {
                    "jobRunId": job_run_id,
                    "workflowRunId": workflow_run_id,
                    "startTimestamp": utc_time,
                    "end_timestamp": None,
                    "sourceRows": None,
                    "targetRows": None,
                    "runStatus": status,
                    "updated_by": user_id,
                    "runBy": user_id,
                    "updated_at": utc_time
                }
            }
        )
        
        # Also update PostgreSQL
        connection = create_connection()
        update_query = sql.SQL("""
            UPDATE WORKFLOW_RUN_STATS 
            SET run_status = %s 
            WHERE job_run_id = %s AND workflow_run_id = %s
        """)
        
        with connection.cursor() as cursor:
            cursor.execute(update_query, (status, job_run_id, workflow_run_id))
        connection.commit()
        connection.close()

    async def _log_user_activity(
        self,
        user_obj: Dict[str, Any],
        workflow_run_dict: Dict[str, Any],
        workflow_name: str
    ):
        """Log user activity for workflow run"""
        from Utility.constants import UserActivityConstants, ObjectTypeConstants
        
        await userActivityCollection.insert_one({
            "histId": str(uuid.uuid4()),
            "UserId": user_obj["UserId"],
            "infoId": user_obj["infoId"],
            "action": "Run",
            "object_id": workflow_run_dict["jobRunId"],
            "object_name": workflow_name,
            "data": {"current_data": workflow_run_dict},
            "created_at": workflow_run_dict["created_at"],
            "category": UserActivityConstants.WORKFLOWRUN,
            "object_type": ObjectTypeConstants.WORKFLOWRUN,
            "status": "Successful",
        })

    async def _start_prefect_execution(
        self,
        workflow_run_dict: Dict[str, Any],
        workflow_run_id: str,
        workflow_id: str,
        workflow_name: str,
        user_obj: Dict[str, Any],
        module: str,
        from_job: bool
    ) -> Any:
        """Start Prefect workflow execution"""
        try:
            # Create token dictionary
            token_dict = {
                "UserId": user_obj["UserId"],
                "fullName": user_obj["fullName"],
                "infoId": user_obj["infoId"],
                "photoURL": user_obj.get("photoURL", "")
            }
            
            # Start the Prefect workflow
            flow_run = await start_prefect_workflow(
                job_run_dict=workflow_run_dict,
                workflow_run_id=workflow_run_id,
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                token_dict=token_dict,
                module=module,
                from_job=from_job
            )
            
            # Store workflow state
            workflow_state = PrefectWorkflowState()
            workflow_state.workflow_start_time = datetime.now()
            workflow_state.workflow_status = "Running"
            self.workflow_states[workflow_run_id] = workflow_state
            
            return flow_run
            
        except Exception as ex:
            self.logger.error(f"Failed to start Prefect execution: {str(ex)}")
            raise

    async def killWorkflowRunById(
        self,
        job_run_id: str,
        user_obj: Dict[str, Any]
    ) -> int:
        """
        Kill/cancel a running workflow using Prefect
        (Replaces the Redis RQ killWorkRunById method)
        """
        try:
            if not job_run_id:
                raise Exception("Please enter field JOBRUN ID")

            # Get workflow run information
            workflow_run_obj = await jobRunCollection.find_one({"jobRunId": job_run_id})
            
            if not workflow_run_obj:
                raise Exception(f"Workflow run not found: {job_run_id}")

            folder_id = workflow_run_obj["folder_id"]
            job_id = workflow_run_obj["job_id"]
            workflow_id = workflow_run_obj["workflow_id"]
            
            cancelled_workflows = []
            
            # Cancel each workflow in the job
            for workflow in workflow_run_obj["workflows"]:
                workflow_run_id = workflow["workflowRunId"]
                workflow_name = workflow["workflow_name"]
                
                self.logger.info(f"Cancelling workflow {workflow_name} with ID: {workflow_run_id}")
                
                try:
                    # Cancel Prefect flow run
                    await self._cancel_prefect_flow(workflow_run_id)
                    
                    # Update database status
                    await self._update_cancelled_workflow_status(
                        workflow_id, workflow_run_id, job_run_id, user_obj["UserId"]
                    )
                    
                    # Send cancellation notification via WebSocket
                    await ws_manager.broadcast_status(workflow_run_id, {
                        "workflowRunId": workflow_run_id,
                        "workflowStatus": "Stopped",
                        "message": f"Workflow {workflow_name} was cancelled by user",
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    cancelled_workflows.append(workflow_run_id)
                    
                except Exception as workflow_ex:
                    self.logger.error(f"Failed to cancel workflow {workflow_run_id}: {str(workflow_ex)}")
                    continue

            # Update job run status
            utc_time = getCurrentUTCtime()
            
            # Update workflow collection
            await workflowCollection.update_one(
                {"workflow_id": workflow_id},
                {
                    "$set": {
                        "runStatus": "Stopped",
                        "end_timestamp": utc_time,
                        "updated_by": user_obj["UserId"],
                        "updated_at": utc_time
                    }
                }
            )
            
            # Update job run collection
            update_dict = {
                "workflows.$[].nodes.$[].status": "Stopped",
                "runStatus": "Stopped",
                "end_timestamp": utc_time,
                "updated_by": user_obj["UserId"],
                "updated_at": utc_time
            }
            
            # Update node activities to stopped
            dependencies = workflow_run_obj["workflows"][0].get("nodeActivity", {})
            for node_key in dependencies.keys():
                update_dict[f"workflows.$[].nodeActivity.{node_key}.status"] = "Stopped"
            
            await jobRunCollection.update_one(
                {"jobRunId": job_run_id},
                {"$set": update_dict}
            )
            
            self.logger.info(f"Successfully cancelled workflow run: {job_run_id}")
            return status.HTTP_200_OK
            
        except Exception as ex:
            self.logger.error(f"killWorkflowRunById failed: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))

    async def _cancel_prefect_flow(self, workflow_run_id: str):
        """Cancel a Prefect flow run"""
        try:
            async with get_client() as client:
                # Find the flow run by name or tags
                flow_runs = await client.read_flow_runs(
                    flow_run_filter={
                        "name": {"any_": [f"workflow_run_{workflow_run_id}"]}
                    }
                )
                
                for flow_run in flow_runs:
                    if flow_run.state.is_running():
                        await client.set_flow_run_state(
                            flow_run.id,
                            state_type="CANCELLED",
                            message="Cancelled by user request"
                        )
                        self.logger.info(f"Cancelled Prefect flow run: {flow_run.id}")
                        
        except Exception as ex:
            self.logger.error(f"Failed to cancel Prefect flow: {str(ex)}")
            raise

    async def _update_cancelled_workflow_status(
        self,
        workflow_id: str,
        workflow_run_id: str,
        job_run_id: str,
        user_id: str
    ):
        """Update database status for cancelled workflow"""
        utc_time = getCurrentUTCtime()
        
        # Update PostgreSQL workflow run stats
        connection = create_connection()
        query = sql.SQL("""
            UPDATE WORKFLOW_RUN_STATS 
            SET run_status = %s, end_timestamp = %s
            WHERE workflow_run_id = %s AND job_run_id = %s
        """)
        
        with connection.cursor() as cursor:
            cursor.execute(query, ("Stopped", utc_time, workflow_run_id, job_run_id))
        connection.commit()
        connection.close()
        
        # Update workflow state
        if workflow_run_id in self.workflow_states:
            self.workflow_states[workflow_run_id].workflow_status = "Stopped"

    async def getWorkflowRunWithFilters(
        self, 
        user_obj: Dict[str, Any], 
        is_download: bool = False, 
        **payload
    ) -> Dict[str, Any]:
        """
        Enhanced method to get workflow runs with filters
        (Maintains compatibility with existing API)
        """
        try:
            self.logger.info(f"Getting workflow runs with filters: {payload}")
            
            # Use existing PostgreSQL view for consistency
            connection = create_connection()
            query = sql.SQL("SELECT * FROM vw_dashboard_job_runs_table")
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            
            # Process results with timezone adjustment
            user_utc_offset = user_obj["user_utc_offset"]
            from Utility.utils import extract_info_from_user_utc_offset, time_zone_value_mapping
            
            utc_offset_info = extract_info_from_user_utc_offset(user_utc_offset)
            hour_minutes_value = (
                utc_offset_info["hours"] + 
                time_zone_value_mapping[utc_offset_info["minutes"]]
            )
            
            items = []
            for result in results:
                from datetime import timedelta
                
                start_timestamp = None
                end_timestamp = None
                
                if result[3]:  # start_timestamp
                    start_timestamp = (result[3] + timedelta(hours=hour_minutes_value)).strftime('%Y-%m-%d %H:%M:%S.%f%z')
                if result[4]:  # end_timestamp
                    end_timestamp = (result[4] + timedelta(hours=hour_minutes_value)).strftime('%Y-%m-%d %H:%M:%S.%f%z')
                
                item = {
                    "_id": {
                        "job_name": result[0],
                        "folder_name": result[2],
                        "workspace_name": result[1],
                        "startTimestamp": start_timestamp,
                        "end_timestamp": end_timestamp,
                        "runStatus": result[5],
                        "recordsProcessed": result[9],
                        "workspace_id": result[10],
                        "folder_id": result[11],
                        "job_id": result[12],
                        "runBy": {
                            "id": result[7],
                            "name": result[6],
                            "photoURL": result[8]
                        }
                    },
                    "sortField": start_timestamp
                }
                items.append(item)

            total_docs = {"active": len(items), "inactive": 0, "totalDocs": len(items)}
            
            from bson import json_util
            return json.loads(json_util.dumps({
                "Items": items, 
                "total_records": total_docs
            }))
            
        except Exception as ex:
            self.logger.error(f"getWorkflowRunWithFilters failed: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))

    async def getWorkflowRunStats(
        self, 
        user_obj: Dict[str, Any], 
        **payload
    ) -> Dict[str, Any]:
        """
        Get workflow run statistics
        (Enhanced version with Prefect integration)
        """
        try:
            self.logger.info("Getting workflow run statistics")
            
            # Extract parameters
            rows = payload.get("rows", 20)
            sort_val = payload.get("sort_val", "start_timestamp DESC")
            search_val = payload.get("search_val", "")
            num_of_rows_to_skip = payload.get("num_of_rows_to_skip", 0)
            filter_type = payload.get("filter_type", "")
            filter_start_date = payload.get("filter_start_date", "")
            filter_end_date = payload.get("filter_end_date", "")
            user_utc_offset = user_obj["user_utc_offset"]

            # Build base query
            base_query = """
                SELECT 
                    workspace_id, workspace_name, folder_id, folder_name, 
                    job_id, job_run_id, job_name, workflow_id, workflow_run_id, 
                    workflow_name, run_status, run_by_id, run_by, 
                    to_char(start_timestamp, 'YYYY-MM-DD HH24:MI:SS.MS') as start_timestamp, 
                    to_char(end_timestamp, 'YYYY-MM-DD HH24:MI:SS.MS') as end_timestamp  
                FROM WORKFLOW_RUN_STATS
            """
            
            count_query = "SELECT COUNT(*) FROM WORKFLOW_RUN_STATS"
            
            # Add filters
            where_conditions = []
            
            if filter_type:
                from Utility.utils import extract_info_from_user_utc_offset, getDateFilters
                
                utc_offset_info = extract_info_from_user_utc_offset(user_utc_offset)
                start_date, end_date = getDateFilters(
                    filter_type, filter_start_date, filter_end_date, utc_offset_info
                )
                
                date_filter = f"""
                    start_timestamp >= {start_date} AND start_timestamp <= {end_date} AND 
                    end_timestamp >= {start_date} AND end_timestamp <= {end_date}
                """
                where_conditions.append(date_filter)
            
            if search_val:
                search_filter = f"""
                    (workspace_name LIKE '%{search_val}%' OR 
                     folder_name LIKE '%{search_val}%' OR 
                     job_name LIKE '%{search_val}%' OR 
                     workflow_name LIKE '%{search_val}%')
                """
                where_conditions.append(search_filter)
            
            # Apply WHERE clause
            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
                base_query += where_clause
                count_query += where_clause
            
            # Add ordering and pagination
            import re
            sort_val = re.sub("[^A-Za-z0-9_,]+", "", sort_val)
            sort_val = sort_val.replace("1", " ASC").replace("-1", " DESC")
            
            if len(sort_val) > 2:
                base_query += f" ORDER BY {sort_val}"
            else:
                base_query += " ORDER BY start_timestamp DESC"
            
            base_query += f" LIMIT {rows} OFFSET {num_of_rows_to_skip}"
            
            # Execute queries
            connection = create_connection()
            cursor = connection.cursor()
            
            # Get count
            cursor.execute(count_query)
            total_records = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
            
            # Get data
            cursor.execute(base_query)
            columns = [desc[0] for desc in cursor.description]
            runstats_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            cursor.close()
            connection.close()
            
            # Generate breadcrumbs
            breadcrumb_data = self._generate_breadcrumbs(runstats_data)
            
            from bson import json_util
            return json.loads(json_util.dumps({
                "Items": runstats_data,
                "total_records": total_records,
                "breadcrumbs": breadcrumb_data,
            }))
            
        except Exception as ex:
            self.logger.error(f"getWorkflowRunStats failed: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))

    def _generate_breadcrumbs(self, data: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Generate breadcrumb navigation data"""
        breadcrumb_data = []
        
        for item in data:
            crumb_list = [
                {
                    "workspace_name": item["workspace_name"],
                    "workspace_id": item["workspace_id"],
                },
                {
                    "folder_name": item["folder_name"],
                    "folder_id": item["folder_id"],
                },
                {
                    "workflow_name": item["workflow_name"],
                    "workflow_id": item["workflow_id"],
                }
            ]
            breadcrumb_data.append(crumb_list)
        
        return breadcrumb_data

    async def getWorkflowRunLogById(
        self, 
        workflow_id: str, 
        workflow_run_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get workflow run logs by ID
        (Enhanced with Prefect logging integration)
        """
        try:
            self.logger.info(f"Getting logs for workflow {workflow_id}, run {workflow_run_id}")
            
            if not workflow_id:
                raise Exception("Please enter field WORKFLOW ID")
            
            # Try to get logs from Prefect first
            prefect_logs = await self._get_prefect_logs(workflow_run_id)
            if prefect_logs:
                return prefect_logs
            
            # Fallback to file-based logs (existing implementation)
            from CoreEngine.modularize.utils import download_workflow_log_file
            
            local_storage_file_path = await download_workflow_log_file(workflow_id, workflow_run_id)
            
            if not local_storage_file_path:
                return []
            
            # Process log file
            import os
            from Utility.utils import changeLogTimeFormat
            
            log_data = []
            messages = []
            
            with open(local_storage_file_path, 'r') as file:
                file_data = file.read()
                file_data_list = file_data.split("\n")
                
                log_time = ""
                for line in file_data_list:
                    if not line:
                        continue
                    
                    msg_type = (
                        "Success" if ":: INFO ::" in line
                        else "Failed" if ":: ERROR ::" in line
                        else "Success"
                    )
                    
                    parts = (
                        line.split(":: INFO ::") if ":: INFO ::" in line
                        else line.split(":: ERROR ::") if ":: ERROR ::" in line
                        else ["", line]
                    )
                    
                    if parts[0]:
                        log_time = parts[0]
                    
                    parts[0] = log_time
                    
                    if len(parts) > 1 and parts[1] not in messages and "Result" not in parts[1]:
                        messages.append(parts[1])
                        log_data.append({
                            "timestamp": changeLogTimeFormat(parts[0].split(",")[0]),
                            "message": parts[1],
                            "msgType": msg_type,
                        })
            
            # Cleanup
            os.remove(local_storage_file_path)
            return log_data
            
        except Exception as ex:
            self.logger.error(f"getWorkflowRunLogById failed: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))

    async def _get_prefect_logs(self, workflow_run_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get logs from Prefect for a workflow run"""
        try:
            async with get_client() as client:
                # Find flow runs matching the workflow_run_id
                flow_runs = await client.read_flow_runs(
                    flow_run_filter={
                        "name": {"any_": [f"workflow_run_{workflow_run_id}"]}
                    }
                )
                
                if not flow_runs:
                    return None
                
                logs = []
                for flow_run in flow_runs:
                    # Get logs for this flow run
                    flow_logs = await client.read_logs(
                        log_filter={
                            "flow_run_id": {"any_": [flow_run.id]}
                        }
                    )
                    
                    for log in flow_logs:
                        logs.append({
                            "timestamp": log.timestamp.isoformat(),
                            "message": log.message,
                            "msgType": "Failed" if log.level >= 40 else "Success",  # ERROR level = 40
                            "level": log.level,
                            "logger": log.name
                        })
                
                # Sort by timestamp
                logs.sort(key=lambda x: x["timestamp"])
                return logs
                
        except Exception as ex:
            self.logger.error(f"Failed to get Prefect logs: {str(ex)}")
            return None


# Global service instance
prefect_workflow_service = PrefectWorkflowService()


# WebSocket endpoints for real-time workflow monitoring
async def setup_workflow_websocket_monitoring():
    """Setup WebSocket server for workflow monitoring"""
    import websockets
    
    async def websocket_handler(websocket, path):
        """Handle WebSocket connections"""
        workflow_id = path.strip("/")
        await ws_manager.register_connection(workflow_id, websocket)
        
        try:
            await websocket.wait_closed()
        finally:
            await ws_manager.unregister_connection(workflow_id)
    
    # Start WebSocket server
    server = await websockets.serve(websocket_handler, "localhost", 8765)
    prefect_workflow_service.logger.info("WebSocket server started on ws://localhost:8765")
    
    return server


# Example usage and integration
if __name__ == "__main__":
    import asyncio
    
    # Example of how to use the new service
    async def example_usage():
        service = PrefectWorkflowService()
        
        # Mock payload and user data
        class MockPayload:
            def __init__(self):
                self.workspace_id = "ws1"
                self.folder_id = "folder1"
                self.job_id = "job1"
                self.workflow_id = "workflow1"
                self.workflows = []
            
            def dict(self):
                return {
                    "workspace_id": self.workspace_id,
                    "folder_id": self.folder_id,
                    "job_id": self.job_id,
                    "workflow_id": self.workflow_id,
                    "workflows": self.workflows
                }
        
        user_obj = {
            "UserId": "user123",
            "fullName": "Test User",
            "infoId": "info123",
            "photoURL": "https://example.com/photo.jpg"
        }
        
        payload = MockPayload()
        
        try:
            # Start a workflow
            result = await service.addWorkflowRun(
                payload=payload,
                user_obj=user_obj,
                from_job=False
            )
            print(f"Workflow started: {result}")
            
        except Exception as ex:
            print(f"Error: {ex}")
    
    # Run example
    asyncio.run(example_usage())
