# Prefect 3 Integration Guide

## Overview

This guide explains how to migrate from Redis RQ to Prefect 3 for your dynamic workflow engine. The migration maintains the same API interfaces while providing enhanced orchestration, monitoring, and scalability.

## Architecture Changes

### Before (Redis RQ)
```
API Request → Redis Queue → Worker Process → Database Updates → WebSocket Notifications
```

### After (Prefect 3)
```
API Request → Prefect Flow → Prefect Tasks → Database Updates → WebSocket Notifications
```

## Key Benefits

1. **Enhanced Orchestration**: Support for complex dependencies up to 20 levels
2. **Better Monitoring**: Built-in UI and logging
3. **Improved Error Handling**: Automatic retries and failure recovery
4. **Scalability**: Dynamic scaling of workers
5. **Observability**: Rich metrics and tracing

## Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Prefect Server

```bash
# Option 1: Local development
prefect server start

# Option 2: Using Docker
docker-compose up -d
```

### 3. Run Deployment Setup

```bash
python prefect_deployment_setup.py --full-setup
```

### 4. Start Workers

```bash
# Start default worker
prefect worker start --pool default-worker-pool

# Start specialized workers
prefect worker start --pool postgres-worker-pool
prefect worker start --pool priority-worker-pool
```

## Code Changes Required

### 1. Replace WorkflowRun Service

**Before (workflow_runs.py):**
```python
from Workflow_Runs.service import WorkflowRun
workflow_service = WorkflowRun()

# Start workflow
result = await workflow_service.addWorkflowRun(
    payload, user_obj, manager, redisWFQueue, redisConn, fromJob, job_run_id
)
```

**After:**
```python
from prefect_workflow_service import prefect_workflow_service

# Start workflow
result = await prefect_workflow_service.addWorkflowRun(
    payload, user_obj, from_job=fromJob, job_run_id=job_run_id
)
```

### 2. Update Task Definitions

**Before (postgres.py):**
```python
def start_postgres_extraction_task(connectorInfo, nodeId, nodeName, redisQueue, **params):
    redisQueue.enqueue("postgres_extract", args=(...), kwargs=params)
```

**After:**
```python
from prefect_postgres_tasks import postgres_extract_task

# Task is automatically handled by Prefect flow orchestration
# No manual enqueuing required
```

### 3. WebSocket Integration

The WebSocket functionality is enhanced and built into the new service:

```python
from prefect_workflow_engine import ws_manager

# WebSocket connections are automatically managed
# Status updates are sent via the enhanced workflow engine
```

## Configuration

### Environment Variables

Create a `.env` file based on `.env.template`:

```bash
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_DB=workflow_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Prefect Configuration
PREFECT_API_URL=http://localhost:4200/api

# Cloud Storage
GCS_CREDENTIALS_PATH=/path/to/gcs-key.json
```

### Work Pool Configuration

Three work pools are created by default:

1. **default-worker-pool**: General workflows (10 concurrent)
2. **postgres-worker-pool**: Database-specific workflows (5 concurrent) 
3. **priority-worker-pool**: Critical workflows (3 concurrent)

## Workflow Features

### 1. Multi-Level Dependencies (Up to 20 Levels)

```python
dependencies = {
    "level_0_node": [],
    "level_1_node_a": ["level_0_node"],
    "level_1_node_b": ["level_0_node"],
    "level_2_node": ["level_1_node_a", "level_1_node_b"],
    # ... up to 20 levels
}
```

### 2. Parallel Execution

Nodes at the same level execute in parallel automatically:

```python
# These will run in parallel
dependencies = {
    "extract_postgres": [],
    "extract_mysql": [],
    "extract_bigquery": [],
    "transform_data": ["extract_postgres", "extract_mysql", "extract_bigquery"]
}
```

### 3. Enhanced Error Handling

```python
@task(name="postgres_extract", retries=2, retry_delay_seconds=30)
async def postgres_extract_task(config, **params):
    # Automatic retries on failure
    # Enhanced error reporting
    # Graceful failure handling
```

### 4. Real-time Monitoring

- **Prefect UI**: http://localhost:4200
- **WebSocket Updates**: Real-time status via WebSocket
- **Detailed Logs**: Enhanced logging with context

## API Compatibility

The migration maintains full API compatibility:

### Starting Workflows

```python
# Same interface as before
POST /workflow_runs/
{
    "workspace_id": "ws1",
    "folder_id": "folder1", 
    "job_id": "job1",
    "workflow_id": "workflow1"
}
```

### Killing Workflows

```python
# Same interface as before
DELETE /workflow_runs/{job_run_id}
```

### Getting Workflow Status

```python
# Same interface as before
GET /workflow_runs/stats
GET /workflow_runs/{job_run_id}/logs
```

## Migration Steps

### Phase 1: Parallel Deployment

1. Deploy Prefect components alongside existing Redis RQ
2. Test with non-critical workflows
3. Validate functionality and performance

### Phase 2: Gradual Migration

1. Route new workflows to Prefect
2. Keep existing workflows on Redis RQ
3. Monitor and compare performance

### Phase 3: Full Migration

1. Route all workflows to Prefect
2. Decommission Redis RQ workers
3. Remove Redis RQ dependencies

## Monitoring & Observability

### 1. Prefect UI Dashboard

Access at http://localhost:4200:
- Flow run status
- Task execution details
- Performance metrics
- Error tracking

### 2. WebSocket Real-time Updates

```javascript
// Connect to WebSocket for real-time updates
const ws = new WebSocket('ws://localhost:8765/workflow-run-id');
ws.onmessage = function(event) {
    const update = JSON.parse(event.data);
    console.log('Workflow update:', update);
};
```

### 3. Enhanced Logging

```python
# Structured logging with context
logger.info("Node processing started", extra={
    "node_id": node_id,
    "workflow_run_id": workflow_run_id,
    "execution_context": context
})
```

## Troubleshooting

### Common Issues

1. **Prefect Server Not Running**
   ```bash
   prefect server start
   ```

2. **Worker Not Connected**
   ```bash
   prefect worker start --pool default-worker-pool
   ```

3. **Database Connection Issues**
   - Check environment variables
   - Verify database connectivity
   - Review connection strings

4. **WebSocket Connection Failed**
   - Ensure WebSocket server is running
   - Check firewall settings
   - Verify port availability

### Debugging

1. **Check Prefect Logs**
   ```bash
   prefect config view
   prefect flow-run logs <flow-run-id>
   ```

2. **Validate Deployments**
   ```bash
   prefect deployment ls
   prefect work-pool ls
   ```

3. **Test Connectivity**
   ```bash
   python prefect_deployment_setup.py
   # Select option to run test workflow
   ```

## Performance Considerations

### Scaling Workers

```bash
# Scale horizontally
prefect worker start --pool default-worker-pool --name worker-1
prefect worker start --pool default-worker-pool --name worker-2
prefect worker start --pool default-worker-pool --name worker-3
```

### Resource Limits

Configure resource limits in deployment:

```python
deployment = Deployment.build_from_flow(
    flow=execute_dynamic_workflow,
    name="resource-limited-workflow",
    infrastructure=Process(
        memory_limit="2Gi",
        cpu_limit="1000m"
    )
)
```

### Database Optimization

1. **Connection Pooling**: Use connection pooling for database connections
2. **Batch Operations**: Batch database updates where possible
3. **Indexing**: Ensure proper indexing on workflow tables

## Security Considerations

### 1. Secret Management

Use Prefect Secret blocks for sensitive data:

```python
from prefect.blocks.system import Secret

postgres_password = Secret.load("postgres-password")
```

### 2. Network Security

- Use TLS for Prefect API communication
- Secure WebSocket connections with authentication
- Implement proper firewall rules

### 3. Access Control

- Configure Prefect RBAC if using Prefect Cloud
- Implement API authentication
- Use environment-specific configurations

## Migration Checklist

- [ ] Prefect server installed and running
- [ ] Database connections configured
- [ ] Secrets and environment variables set
- [ ] Deployments created successfully
- [ ] Workers started and connected
- [ ] WebSocket server running
- [ ] Test workflows executed successfully
- [ ] Monitoring and logging validated
- [ ] Error handling tested
- [ ] Performance benchmarked
- [ ] Security measures implemented
- [ ] Documentation updated
- [ ] Team training completed

## Support & Resources

### Documentation
- [Prefect 3 Documentation](https://docs.prefect.io/)
- [Prefect Concepts](https://docs.prefect.io/concepts/)
- [Prefect Tutorials](https://docs.prefect.io/tutorials/)

### Community
- [Prefect Slack Community](https://prefect.io/slack)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
- [Prefect Discourse](https://discourse.prefect.io/)

### Getting Help

If you encounter issues during migration:

1. Check the troubleshooting section above
2. Review Prefect logs and documentation
3. Search existing GitHub issues
4. Post questions in the Prefect community Slack

## Conclusion

This migration to Prefect 3 provides significant improvements in workflow orchestration, monitoring, and scalability while maintaining full compatibility with your existing API. The enhanced features support complex multi-level workflows with improved error handling and observability.

The migration can be done gradually, allowing you to validate functionality and performance before fully transitioning from Redis RQ to Prefect.
