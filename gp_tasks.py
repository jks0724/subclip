import asyncio
import json
import pandas as pd
import psycopg2
from datetime import datetime
from typing import Dict, Any, List, Optional
from prefect import task, get_run_logger
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO
import uuid

# Import your existing utilities
from Connection.pg import create_connection
from psycopg2 import sql
from CoreEngine.modularize.utils import (
    initialize_gcs_client,
    initialize_aws_client,
    get_size,
    get_column_size
)


@task(name="postgres_extract_prefect", retries=2, retry_delay_seconds=30)
async def postgres_extract_task(
    config: Dict[str, Any], 
    node_id: str,
    node_name: str,
    **task_params
) -> Dict[str, Any]:
    """
    Enhanced Prefect task for PostgreSQL data extraction
    
    Args:
        config: PostgreSQL connection and extraction configuration
        node_id: Unique identifier for the node
        node_name: Human-readable name for the node
        **task_params: Additional parameters including workflow context
    
    Returns:
        Dict containing extraction results and metadata
    """
    logger = get_run_logger()
    workflow_run_id = task_params.get("workflow_run_id")
    workflow_id = task_params.get("workflow_id")
    job_run_dict = task_params.get("job_run_dict", {})
    
    # Initialize tracking variables
    row_count = 0
    total_size_bytes = 0
    start_timestamp = datetime.utcnow()
    
    logger.info(f"Starting PostgreSQL extraction for node {node_id} ({node_name})")
    
    try:
        # Database connection setup
        meta_connection = create_connection()
        meta_cursor = meta_connection.cursor()
        
        # Extract configuration from job_run_dict
        input_params = json.loads(job_run_dict.get("inputParams", "{}"))
        node_config = input_params.get(node_id, {})
        workflows = job_run_dict.get("workflows", [{}])
        dependencies = workflows[0].get("dependencies", {})
        
        # Node metadata
        connector_type = node_config.get("connector_type", "postgresql")
        node_sequence = node_config.get("sequence", 1)
        node_type = "source"
        
        # Calculate parent and child relationships
        parent_node_ids = dependencies.get(node_id, [])
        parent_node_names = [input_params.get(pid, {}).get("node_name", "") for pid in parent_node_ids]
        parent_node_types = [input_params.get(pid, {}).get("connector_type", "") for pid in parent_node_ids]
        parent_node_sequences = [input_params.get(pid, {}).get("sequence", 0) for pid in parent_node_ids]
        
        child_node_ids = [child_id for child_id, parents in dependencies.items() if node_id in parents]
        child_node_names = [input_params.get(cid, {}).get("node_name", "") for cid in child_node_ids]
        child_node_types = [input_params.get(cid, {}).get("connector_type", "") for cid in child_node_ids]
        child_node_sequences = [input_params.get(cid, {}).get("sequence", 0) for cid in child_node_ids]
        
        # Insert node run details
        node_run_details_params = (
            node_id, node_name, connector_type, node_type,
            workflow_run_id, workflow_id, workflows[0].get("workflow_name", ""),
            job_run_dict.get("folder_id"), job_run_dict.get("folder_name"),
            job_run_dict.get("job_id"), job_run_dict.get("job_name"),
            job_run_dict.get("workspace_id"), job_run_dict.get("workspace_name"),
            job_run_dict.get("jobRunId"), start_timestamp, node_sequence,
            parent_node_ids, parent_node_names, parent_node_types, parent_node_sequences,
            child_node_ids, child_node_names, child_node_types, child_node_sequences
        )
        
        insert_query = sql.SQL("""
            INSERT INTO NODE_RUN_DETAILS(
                node_id, node_name, connector_type, node_type, workflow_run_id, 
                workflow_id, workflow_name, folder_id, folder_name, job_id, job_name, 
                workspace_id, workspace_name, job_run_id, node_start_timestamp, node_sequence,
                parent_node_ids, parent_node_names, parent_node_types, parent_node_sequences,
                child_node_ids, child_node_names, child_node_types, child_node_sequences
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        meta_cursor.execute(insert_query, node_run_details_params)
        meta_connection.commit()
        
        # Update status to Running
        update_query = sql.SQL("""
            UPDATE NODE_RUN_DETAILS 
            SET node_status = 'Running'
            WHERE workflow_run_id = %s AND job_run_id = %s AND node_id = %s
        """)
        meta_cursor.execute(update_query, (workflow_run_id, job_run_dict.get("jobRunId"), node_id))
        meta_connection.commit()
        
        logger.info(f"Data extraction from PostgreSQL started on node {node_name}")
        
        # Establish PostgreSQL connection for data extraction
        pg_connection = psycopg2.connect(
            host=config["host-name"],
            dbname=config["db"],
            user=config["username"],
            password=config["password"],
        )
        pg_cursor = pg_connection.cursor()
        
        # Build query based on configuration
        filter_clause = config.get("filter", "").strip()
        sql_query = config.get("sql", "").strip()
        
        if filter_clause:
            if filter_clause.endswith(';'):
                filter_clause = filter_clause[:-1]
            query = f"SELECT * FROM {config['table']} {filter_clause}"
        elif sql_query:
            if sql_query.endswith(';'):
                sql_query = sql_query[:-1]
            query = sql_query
        else:
            query = f"SELECT * FROM {config['table']}"
        
        logger.info(f"Executing query: {query}")
        
        # Execute query and fetch results
        pg_cursor.execute(query)
        result = pg_cursor.fetchall()
        row_count = pg_cursor.rowcount if pg_cursor.rowcount > 0 else 0
        colnames = [desc[0] for desc in pg_cursor.description]
        
        # Get column information for size calculation
        pg_cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{config['table']}'
        """)
        columns_info = pg_cursor.fetchall()
        column_types = {col[0]: col[1] for col in columns_info}
        
        # Calculate data size
        row_size = sum(get_column_size(column_types.get(col, '')) for col in colnames)
        total_size_bytes = row_size * row_count
        readable_total_size = get_size(total_size_bytes)
        
        logger.info(f"Query executed successfully. Rows: {row_count}, Size: {readable_total_size}")
        
        # Process storage and upload
        storage_path = await process_extraction_storage(
            result, colnames, config, workflow_run_id, node_id, logger
        )
        
        pg_cursor.close()
        pg_connection.close()
        
        # Update completion status
        end_timestamp = datetime.utcnow()
        execution_time = (end_timestamp - start_timestamp).total_seconds()
        
        completion_params = (
            readable_total_size, end_timestamp, row_count, row_count,
            f'{{{storage_path}}}' if storage_path else '{}',
            execution_time, workflow_run_id, job_run_dict.get("jobRunId"), node_id
        )
        
        completion_query = sql.SQL("""
            UPDATE NODE_RUN_DETAILS
            SET node_status = 'Completed', parent_storage_size = %s, node_end_timestamp = %s,
                source_rows = %s, target_rows = %s, output_storage_path = %s, total_run_time_sec = %s
            WHERE workflow_run_id = %s AND job_run_id = %s AND node_id = %s
        """)
        
        meta_cursor.execute(completion_query, completion_params)
        meta_connection.commit()
        
        logger.info(f"PostgreSQL extraction completed for node {node_id}")
        
        return {
            "node_id": node_id,
            "node_name": node_name,
            "status": "Completed",
            "row_count": row_count,
            "storage_path": storage_path,
            "execution_time": execution_time,
            "data_size": readable_total_size,
            "type": "extract"
        }
        
    except Exception as ex:
        # Handle extraction failure
        end_timestamp = datetime.utcnow()
        execution_time = (end_timestamp - start_timestamp).total_seconds()
        
        error_params = (
            end_timestamp, row_count, row_count, execution_time,
            workflow_run_id, job_run_dict.get("jobRunId"), node_id
        )
        
        error_query = sql.SQL("""
            UPDATE NODE_RUN_DETAILS
            SET node_status = 'Failed', node_end_timestamp = %s, 
                source_rows = %s, target_rows = %s, total_run_time_sec = %s
            WHERE workflow_run_id = %s AND job_run_id = %s AND node_id = %s
        """)
        
        if 'meta_cursor' in locals():
            meta_cursor.execute(error_query, error_params)
            meta_connection.commit()
        
        logger.error(f"PostgreSQL extraction failed for node {node_id}: {str(ex)}")
        raise
        
    finally:
        if 'meta_cursor' in locals():
            meta_cursor.close()
        if 'meta_connection' in locals():
            meta_connection.close()


@task(name="postgres_load_prefect", retries=2, retry_delay_seconds=30)
async def postgres_load_task(
    config: Dict[str, Any],
    parent_results: List[Dict[str, Any]] = None,
    node_id: str = None,
    node_name: str = None,
    **task_params
) -> Dict[str, Any]:
    """
    Enhanced Prefect task for PostgreSQL data loading
    
    Args:
        config: PostgreSQL connection and loading configuration
        parent_results: Results from parent extraction tasks
        node_id: Unique identifier for the node
        node_name: Human-readable name for the node
        **task_params: Additional parameters including workflow context
    
    Returns:
        Dict containing loading results and metadata
    """
    logger = get_run_logger()
    workflow_run_id = task_params.get("workflow_run_id")
    workflow_id = task_params.get("workflow_id")
    job_run_dict = task_params.get("job_run_dict", {})
    
    # Initialize tracking variables
    total_rows_loaded = 0
    total_size_bytes = 0
    num_files_loaded = 0
    start_timestamp = datetime.utcnow()
    
    logger.info(f"Starting PostgreSQL loading for node {node_id} ({node_name})")
    
    try:
        # Database connection setup
        meta_connection = create_connection()
        meta_cursor = meta_connection.cursor()
        
        # Extract configuration
        input_params = json.loads(job_run_dict.get("inputParams", "{}"))
        node_config = input_params.get(node_id, {})
        workflows = job_run_dict.get("workflows", [{}])
        dependencies = workflows[0].get("dependencies", {})
        
        # Node metadata
        connector_type = node_config.get("connector_type", "postgresql")
        node_sequence = node_config.get("sequence", 2)
        node_type = "target"
        
        # Calculate relationships
        parent_node_ids = dependencies.get(node_id, [])
        parent_node_names = [input_params.get(pid, {}).get("node_name", "") for pid in parent_node_ids]
        parent_node_types = [input_params.get(pid, {}).get("connector_type", "") for pid in parent_node_ids]
        parent_node_sequences = [input_params.get(pid, {}).get("sequence", 0) for pid in parent_node_ids]
        
        child_node_ids = [child_id for child_id, parents in dependencies.items() if node_id in parents]
        child_node_names = [input_params.get(cid, {}).get("node_name", "") for cid in child_node_ids]
        child_node_types = [input_params.get(cid, {}).get("connector_type", "") for cid in child_node_ids]
        child_node_sequences = [input_params.get(cid, {}).get("sequence", 0) for cid in child_node_ids]
        
        # Insert node run details
        node_run_details_params = (
            node_id, node_name, connector_type, node_type,
            workflow_run_id, workflow_id, workflows[0].get("workflow_name", ""),
            job_run_dict.get("folder_id"), job_run_dict.get("folder_name"),
            job_run_dict.get("job_id"), job_run_dict.get("job_name"),
            job_run_dict.get("workspace_id"), job_run_dict.get("workspace_name"),
            job_run_dict.get("jobRunId"), start_timestamp, node_sequence,
            parent_node_ids, parent_node_names, parent_node_types, parent_node_sequences,
            child_node_ids, child_node_names, child_node_types, child_node_sequences
        )
        
        insert_query = sql.SQL("""
            INSERT INTO NODE_RUN_DETAILS(
                node_id, node_name, connector_type, node_type, workflow_run_id, 
                workflow_id, workflow_name, folder_id, folder_name, job_id, job_name, 
                workspace_id, workspace_name, job_run_id, node_start_timestamp, node_sequence,
                parent_node_ids, parent_node_names, parent_node_types, parent_node_sequences,
                child_node_ids, child_node_names, child_node_types, child_node_sequences
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        meta_cursor.execute(insert_query, node_run_details_params)
        meta_connection.commit()
        
        # Update status to Running
        update_query = sql.SQL("""
            UPDATE NODE_RUN_DETAILS 
            SET node_status = 'Running'
            WHERE workflow_run_id = %s AND job_run_id = %s AND node_id = %s
        """)
        meta_cursor.execute(update_query, (workflow_run_id, job_run_dict.get("jobRunId"), node_id))
        meta_connection.commit()
        
        logger.info(f"Data loading started on PostgreSQL node {node_name}")
        
        # Get storage path from parent results
        storage_path = None
        if parent_results and len(parent_results) > 0:
            storage_path = parent_results[0].get("storage_path")
        
        if not storage_path:
            raise ValueError("No storage path provided from parent extraction")
        
        # Process field mapping
        field_mapping = node_config.get('field_mapping', [])
        target_columns = [field['target'] for field in field_mapping if field.get('selected', True)]
        
        # Load data from storage
        total_rows_loaded, total_size_bytes = await process_loading_storage(
            config, node_id, storage_path, field_mapping, target_columns, logger
        )
        
        # Update completion status
        end_timestamp = datetime.utcnow()
        execution_time = (end_timestamp - start_timestamp).total_seconds()
        readable_total_size = get_size(total_size_bytes)
        
        completion_params = (
            total_size_bytes, end_timestamp, total_rows_loaded, total_rows_loaded,
            f'{{{storage_path}}}' if storage_path else '{}',
            execution_time, workflow_run_id, job_run_dict.get("jobRunId"), node_id
        )
        
        completion_query = sql.SQL("""
            UPDATE NODE_RUN_DETAILS
            SET node_status = 'Completed', parent_storage_size = %s, node_end_timestamp = %s,
                source_rows = %s, target_rows = %s, input_storage_path = %s, total_run_time_sec = %s
            WHERE workflow_run_id = %s AND job_run_id = %s AND node_id = %s
        """)
        
        meta_cursor.execute(completion_query, completion_params)
        meta_connection.commit()
        
        logger.info(f"PostgreSQL loading completed for node {node_id}")
        
        # Determine if this is an intermediate node that needs extraction
        result_data = {
            "node_id": node_id,
            "node_name": node_name,
            "status": "Completed",
            "row_count": total_rows_loaded,
            "storage_path": storage_path if child_node_ids else None,
            "execution_time": execution_time,
            "data_size": readable_total_size,
            "type": "load"
        }
        
        # If this node has children, it's an intermediate node
        if child_node_ids:
            result_data["status"] = "LoadCompleted"
            result_data["child_nodes"] = child_node_ids
            # For intermediate database nodes, we might need to trigger extraction
            # This would be handled by the main workflow orchestrator
        
        return result_data
        
    except Exception as ex:
        # Handle loading failure
        end_timestamp = datetime.utcnow()
        execution_time = (end_timestamp - start_timestamp).total_seconds()
        
        error_params = (
            end_timestamp, total_rows_loaded, total_rows_loaded, execution_time,
            workflow_run_id, job_run_dict.get("jobRunId"), node_id
        )
        
        error_query = sql.SQL("""
            UPDATE NODE_RUN_DETAILS
            SET node_status = 'Failed', node_end_timestamp = %s,
                source_rows = %s, target_rows = %s, total_run_time_sec = %s
            WHERE workflow_run_id = %s AND job_run_id = %s AND node_id = %s
        """)
        
        if 'meta_cursor' in locals():
            meta_cursor.execute(error_query, error_params)
            meta_connection.commit()
        
        logger.error(f"PostgreSQL loading failed for node {node_id}: {str(ex)}")
        raise
        
    finally:
        if 'meta_cursor' in locals():
            meta_cursor.close()
        if 'meta_connection' in locals():
            meta_connection.close()


async def process_extraction_storage(
    result: List[tuple],
    colnames: List[str],
    config: Dict[str, Any],
    workflow_run_id: str,
    node_id: str,
    logger
) -> str:
    """
    Process and store extracted data to cloud storage
    
    Args:
        result: Query results from database
        colnames: Column names
        config: Storage configuration
        workflow_run_id: Workflow run identifier
        node_id: Node identifier
        logger: Logger instance
    
    Returns:
        str: Storage path where data was stored
    """
    storage_type = "gcs"  # Can be made configurable
    
    if storage_type == "gcs":
        if "gcs_creds" in config:
            storage_client, storage_path_prefix = initialize_gcs_client(config)
    elif storage_type == "aws":
        if "aws_access_key" in config and "aws_secret_key" in config:
            storage_client, storage_path_prefix = initialize_aws_client(config)
    else:
        raise ValueError("Cloud provider not specified in config")
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    folder_path = f"workflow_data/temp/{workflow_run_id}/{node_id}/{timestamp}"
    full_storage_path = f"{storage_path_prefix}/{folder_path}"
    
    logger.info(f"Storing data to: {full_storage_path}")
    
    # Handle empty result set
    if not result or len(result) == 0:
        logger.info("No data to extract - result set is empty")
        csv_string = ','.join(colnames) + '\n'  # Just headers
        destination_blob_name = f"{folder_path}/{config['table']}_0.csv"
        
        if storage_type == "gcs":
            bucket = storage_client.get_bucket(config["bucket_name"])
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(csv_string, content_type="text/csv")
        elif storage_type == "aws":
            storage_client.put_object(
                Bucket=config["bucket_name"],
                Key=destination_blob_name,
                Body=csv_string.encode('utf-8')
            )
        
        simplified_blob_name = destination_blob_name.rsplit("/", 1)[0]
        return f"{storage_path_prefix}/{simplified_blob_name}/"
    
    # Process data in chunks
    chunk_size = 10000  # Configurable chunk size
    csv_data_df = pd.DataFrame(result, columns=colnames)
    csv_data_chunks = [csv_data_df.iloc[i:i + chunk_size] for i in range(0, len(csv_data_df), chunk_size)]
    
    logger.info(f"Processing {len(csv_data_chunks)} chunks")
    
    for idx, chunk in enumerate(csv_data_chunks):
        csv_string = chunk.to_csv(index=False)
        destination_blob_name = f"{folder_path}/{config['table']}_{idx}.csv"
        
        if storage_type == "gcs":
            bucket = storage_client.get_bucket(config["bucket_name"])
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(csv_string, content_type="text/csv")
        elif storage_type == "aws":
            storage_client.put_object(
                Bucket=config["bucket_name"],
                Key=destination_blob_name,
                Body=csv_string.encode('utf-8')
            )
        
        logger.info(f"Uploaded chunk {idx} to {destination_blob_name}")
    
    simplified_blob_name = destination_blob_name.rsplit("/", 1)[0]
    return f"{storage_path_prefix}/{simplified_blob_name}/"


async def process_loading_storage(
    config: Dict[str, Any],
    node_id: str,
    storage_path: str,
    field_mapping: List[Dict[str, Any]],
    target_columns: List[str],
    logger
) -> Tuple[int, int]:
    """
    Process and load data from cloud storage to PostgreSQL
    
    Args:
        config: PostgreSQL and storage configuration
        node_id: Node identifier
        storage_path: Path to stored data
        field_mapping: Field mapping configuration
        target_columns: Target column names
        logger: Logger instance
    
    Returns:
        Tuple[int, int]: (total_rows_loaded, total_size_bytes)
    """
    storage_type = "gcs"  # Can be made configurable
    total_rows_loaded = 0
    total_size_bytes = 0
    
    if storage_type == "gcs":
        storage_client, storage_path_prefix = initialize_gcs_client(config)
    elif storage_type == "aws":
        storage_client, storage_path_prefix = initialize_aws_client(config)
    else:
        raise ValueError("Cloud provider not specified in config")
    
    # Parse storage path
    parts = storage_path.split("/")
    bucket_name = parts[2]
    folder_name = "/".join(parts[3:-1])
    
    # Get bucket and list files
    if storage_type == "gcs":
        bucket = storage_client.get_bucket(config["bucket_name"])
        blobs = sorted(
            bucket.list_blobs(prefix=folder_name),
            key=lambda x: int(x.name.split("_")[-1].split(".")[0])
        )
    
    # PostgreSQL connection setup
    schema = config.get("schema", "public")
    table = f"{schema}.{config['table']}"
    
    pg_connection = psycopg2.connect(
        host=config["host-name"],
        database=config["db"],
        user=config["username"],
        password=config["password"],
    )
    pg_cursor = pg_connection.cursor()
    
    # Truncate table if configured
    if config.get("truncateTable", False):
        truncate_query = f"TRUNCATE TABLE {table};"
        pg_cursor.execute(truncate_query)
        pg_connection.commit()
        logger.info(f"Table {table} truncated")
    
    # Prepare INSERT query
    columns_str = ", ".join(target_columns)
    placeholders = ", ".join(["%s"] * len(target_columns))
    insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
    
    logger.info(f"INSERT query: {insert_query}")
    
    # Process each file
    for blob in blobs:
        if not blob.name.endswith('.csv'):
            continue
            
        # Download and process file
        csv_data = blob.download_as_text()
        total_size_bytes += len(csv_data.encode('utf-8'))
        
        # Skip if only headers
        lines = csv_data.splitlines()
        if len(lines) <= 1:
            continue
        
        # Process CSV data
        import csv as csv_module
        csv_reader = csv_module.reader(lines)
        next(csv_reader)  # Skip header
        
        # Batch insert
        batch_size = 10000
        batch_data = []
        file_rows = 0
        
        for row in csv_reader:
            # Apply field mapping if configured
            if field_mapping:
                selected_indices = [i for i, field in enumerate(field_mapping) if field.get('selected', True)]
                mapped_row = [row[i] if i < len(row) else None for i in selected_indices]
            else:
                mapped_row = row
            
            batch_data.append(mapped_row)
            file_rows += 1
            
            # Execute batch when size reached
            if len(batch_data) >= batch_size:
                pg_cursor.executemany(insert_query, batch_data)
                pg_connection.commit()
                batch_data = []
        
        # Insert remaining data
        if batch_data:
            pg_cursor.executemany(insert_query, batch_data)
            pg_connection.commit()
        
        total_rows_loaded += file_rows
        logger.info(f"Loaded {file_rows} rows from {blob.name}")
    
    pg_cursor.close()
    pg_connection.close()
    
    logger.info(f"Total rows loaded: {total_rows_loaded}")
    return total_rows_loaded, total_size_bytes


# Additional utility tasks for workflow management
@task(name="workflow_status_updater")
async def update_workflow_status(
    workflow_run_id: str,
    workflow_id: str,
    status: str,
    **context
):
    """Update workflow status in database and send notifications"""
    logger = get_run_logger()
    
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        update_query = sql.SQL("""
            UPDATE WORKFLOW_RUN_STATS 
            SET run_status = %s, end_timestamp = %s
            WHERE workflow_run_id = %s AND workflow_id = %s
        """)
        
        cursor.execute(update_query, (status, datetime.utcnow(), workflow_run_id, workflow_id))
        connection.commit()
        
        cursor.close()
        connection.close()
        
        logger.info(f"Workflow {workflow_run_id} status updated to {status}")
        
    except Exception as ex:
        logger.error(f"Failed to update workflow status: {str(ex)}")
        raise


@task(name="cleanup_resources")
async def cleanup_workflow_resources(workflow_run_id: str, **context):
    """Clean up temporary resources after workflow completion"""
    logger = get_run_logger()
    
    try:
        # Add cleanup logic here
        # - Remove temporary files
        # - Clean up storage paths
        # - Update final statistics
        
        logger.info(f"Resources cleaned up for workflow {workflow_run_id}")
        
    except Exception as ex:
        logger.error(f"Failed to cleanup resources: {str(ex)}")
        # Don't raise exception as cleanup is not critical
