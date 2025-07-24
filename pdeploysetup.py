#!/usr/bin/env python3
"""
Prefect 3 Deployment Setup for Dynamic Workflow Engine
This script sets up Prefect deployments and configurations to replace Redis RQ
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Dict, Any

from prefect import serve
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule as ServerIntervalSchedule
from prefect.blocks.system import Secret
from prefect.blocks.notifications import SlackWebhook
from prefect.settings import PREFECT_API_URL, PREFECT_API_KEY

# Import your workflow components
from prefect_workflow_engine import execute_dynamic_workflow
from prefect_postgres_tasks import postgres_extract_task, postgres_load_task


class PrefectDeploymentManager:
    """
    Manages Prefect deployments for the dynamic workflow engine
    """
    
    def __init__(self):
        self.deployment_configs = []
        
    async def setup_secrets(self):
        """Setup required secrets in Prefect"""
        print("Setting up Prefect secrets...")
        
        # Database connection secrets
        db_secrets = {
            "postgres-host": os.getenv("POSTGRES_HOST", "localhost"),
            "postgres-db": os.getenv("POSTGRES_DB", "neondb"),
            "postgres-user": os.getenv("POSTGRES_USER", "postgres"),
            "postgres-password": os.getenv("POSTGRES_PASSWORD", "password"),
            "gcs-credentials": os.getenv("GCS_CREDENTIALS_PATH", "/path/to/gcs-key.json"),
            "aws-access-key": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "aws-secret-key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "mongodb-uri": os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
            "redis-host": os.getenv("REDIS_HOST", "localhost"),
            "redis-port": os.getenv("REDIS_PORT", "6379"),
        }
        
        for secret_name, secret_value in db_secrets.items():
            if secret_value:
                try:
                    secret_block = Secret(value=secret_value)
                    await secret_block.save(name=secret_name, overwrite=True)
                    print(f"✓ Secret '{secret_name}' configured")
                except Exception as ex:
                    print(f"✗ Failed to configure secret '{secret_name}': {ex}")

    async def setup_notification_blocks(self):
        """Setup notification blocks for workflow alerts"""
        print("Setting up notification blocks...")
        
        # Slack webhook for notifications
        slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if slack_webhook_url:
            try:
                slack_block = SlackWebhook(url=slack_webhook_url)
                await slack_block.save(name="workflow-notifications", overwrite=True)
                print("✓ Slack notification block configured")
            except Exception as ex:
                print(f"✗ Failed to configure Slack notifications: {ex}")

    def create_deployment_configs(self):
        """Create deployment configurations for different workflow types"""
        
        # Main dynamic workflow deployment
        main_deployment = {
            "name": "dynamic-workflow-execution",
            "flow": execute_dynamic_workflow,
            "description": "Main deployment for dynamic workflow execution supporting up to 20 levels",
            "tags": ["workflow", "dynamic", "production"],
            "parameters": {
                "job_run_dict": {},
                "workflow_run_id": "",
                "workflow_id": "",
                "workflow_name": "",
                "token_dict": {}
            },
            "work_pool_name": "default-worker-pool",
            "work_queue_name": "workflow-queue"
        }
        
        # PostgreSQL specific deployment
        postgres_deployment = {
            "name": "postgres-workflow",
            "flow": execute_dynamic_workflow,
            "description": "Specialized deployment for PostgreSQL workflows",
            "tags": ["postgresql", "database", "etl"],
            "parameters": {
                "job_run_dict": {},
                "workflow_run_id": "",
                "workflow_id": "",
                "workflow_name": "",
                "token_dict": {}
            },
            "work_pool_name": "postgres-worker-pool",
            "work_queue_name": "postgres-queue"
        }
        
        # High-priority workflow deployment
        priority_deployment = {
            "name": "priority-workflow-execution",
            "flow": execute_dynamic_workflow,
            "description": "High-priority workflow execution for critical processes",
            "tags": ["workflow", "priority", "critical"],
            "parameters": {
                "job_run_dict": {},
                "workflow_run_id": "",
                "workflow_id": "",
                "workflow_name": "",
                "token_dict": {}
            },
            "work_pool_name": "priority-worker-pool",
            "work_queue_name": "priority-queue"
        }
        
        self.deployment_configs = [
            main_deployment,
            postgres_deployment,
            priority_deployment
        ]

    async def create_deployments(self):
        """Create Prefect deployments"""
        print("Creating Prefect deployments...")
        
        deployments = []
        
        for config in self.deployment_configs:
            try:
                deployment = Deployment.build_from_flow(
                    flow=config["flow"],
                    name=config["name"],
                    description=config["description"],
                    tags=config["tags"],
                    parameters=config["parameters"],
                    work_pool_name=config.get("work_pool_name"),
                    work_queue_name=config.get("work_queue_name"),
                    path="./",
                    entrypoint="prefect_workflow_engine.py:execute_dynamic_workflow"
                )
                deployments.append(deployment)
                print(f"✓ Deployment '{config['name']}' configured")
                
            except Exception as ex:
                print(f"✗ Failed to create deployment '{config['name']}': {ex}")
        
        # Apply all deployments
        if deployments:
            try:
                deployment_ids = await asyncio.gather(
                    *[deployment.apply() for deployment in deployments]
                )
                print(f"✓ Successfully created {len(deployment_ids)} deployments")
                return deployment_ids
            except Exception as ex:
                print(f"✗ Failed to apply deployments: {ex}")
                return []

    async def setup_work_pools(self):
        """Setup work pools for different workflow types"""
        print("Setting up work pools...")
        
        from prefect.client import get_client
        
        work_pool_configs = [
            {
                "name": "default-worker-pool",
                "type": "process",
                "description": "Default worker pool for general workflows",
                "concurrency_limit": 10
            },
            {
                "name": "postgres-worker-pool", 
                "type": "process",
                "description": "Specialized worker pool for PostgreSQL workflows",
                "concurrency_limit": 5
            },
            {
                "name": "priority-worker-pool",
                "type": "process", 
                "description": "High-priority worker pool for critical workflows",
                "concurrency_limit": 3
            }
        ]
        
        async with get_client() as client:
            for pool_config in work_pool_configs:
                try:
                    # Check if work pool exists
                    existing_pools = await client.read_work_pools()
                    if any(pool.name == pool_config["name"] for pool in existing_pools):
                        print(f"ℹ Work pool '{pool_config['name']}' already exists")
                        continue
                    
                    # Create work pool
                    work_pool = await client.create_work_pool(
                        work_pool=pool_config
                    )
                    print(f"✓ Work pool '{pool_config['name']}' created")
                    
                except Exception as ex:
                    print(f"✗ Failed to create work pool '{pool_config['name']}': {ex}")

    async def setup_monitoring_and_observability(self):
        """Setup monitoring and observability features"""
        print("Setting up monitoring and observability...")
        
        # This could include:
        # - Custom flow run hooks
        # - Metrics collection
        # - Dashboard configurations
        # - Alert rules
        
        monitoring_config = {
            "webhook_endpoints": {
                "workflow_start": os.getenv("WEBHOOK_WORKFLOW_START"),
                "workflow_complete": os.getenv("WEBHOOK_WORKFLOW_COMPLETE"),
                "workflow_failed": os.getenv("WEBHOOK_WORKFLOW_FAILED")
            },
            "metrics": {
                "enabled": True,
                "export_interval": 60,
                "include_task_metrics": True
            },
            "logging": {
                "level": "INFO",
                "format": "json",
                "include_context": True
            }
        }
        
        print("✓ Monitoring configuration prepared")
        return monitoring_config

    async def validate_deployment(self):
        """Validate the deployment setup"""
        print("Validating deployment setup...")
        
        from prefect.client import get_client
        
        validation_results = {
            "deployments": [],
            "work_pools": [],
            "secrets": [],
            "overall_status": "unknown"
        }
        
        try:
            async with get_client() as client:
                # Check deployments
                deployments = await client.read_deployments()
                target_deployments = [config["name"] for config in self.deployment_configs]
                
                for deployment in deployments:
                    if deployment.name in target_deployments:
                        validation_results["deployments"].append({
                            "name": deployment.name,
                            "status": "active" if deployment.is_schedule_active else "inactive",
                            "id": str(deployment.id)
                        })
                
                # Check work pools
                work_pools = await client.read_work_pools()
                for pool in work_pools:
                    validation_results["work_pools"].append({
                        "name": pool.name,
                        "type": pool.type,
                        "status": "ready"
                    })
                
                validation_results["overall_status"] = "healthy"
                print("✓ Deployment validation completed successfully")
                
        except Exception as ex:
            validation_results["overall_status"] = "unhealthy"
            print(f"✗ Deployment validation failed: {ex}")
        
        return validation_results

    async def run_test_workflow(self):
        """Run a test workflow to verify everything is working"""
        print("Running test workflow...")
        
        test_job_run_dict = {
            "jobRunId": "test-job-123",
            "workspace_id": "test-workspace",
            "folder_id": "test-folder", 
            "job_id": "test-job",
            "workspace_name": "Test Workspace",
            "folder_name": "Test Folder",
            "job_name": "Test Job",
            "workflows": [{
                "workflowRunId": "test-workflow-run-123",
                "workflow_name": "Test Workflow",
                "dependencies": {
                    "extract_node": [],
                    "load_node": ["extract_node"]
                },
                "nodes": [
                    {
                        "nodeId": "extract_node",
                        "nodeName": "Test Extract",
                        "nodeType": "postgresql",
                        "sequence": 1
                    },
                    {
                        "nodeId": "load_node",
                        "nodeName": "Test Load", 
                        "nodeType": "postgresql",
                        "sequence": 2
                    }
                ],
                "notifyEmail": "test@example.com",
                "onSuccessNotify": False
            }],
            "inputParams": """{
                "extract_node": {
                    "connector_type": "postgresql",
                    "sequence": 1,
                    "host-name": "localhost",
                    "db": "test_db",
                    "table": "test_table"
                },
                "load_node": {
                    "connector_type": "postgresql",
                    "sequence": 2,
                    "host-name": "localhost", 
                    "db": "test_db",
                    "table": "test_target_table"
                }
            }"""
        }
        
        token_dict = {
            "UserId": "test-user-123",
            "fullName": "Test User",
            "infoId": "test-info-123"
        }
        
        try:
            # Import the workflow starter
            from prefect_workflow_service import start_prefect_workflow
            
            # Start test workflow
            result = await start_prefect_workflow(
                job_run_dict=test_job_run_dict,
                workflow_run_id="test-workflow-run-123",
                workflow_id="test-workflow-id",
                workflow_name="Test Workflow",
                token_dict=token_dict
            )
            
            print(f"✓ Test workflow started successfully: {result}")
            return True
            
        except Exception as ex:
            print(f"✗ Test workflow failed: {ex}")
            return False


async def main():
    """Main deployment setup function"""
    print("🚀 Starting Prefect 3 Deployment Setup for Dynamic Workflow Engine")
    print("=" * 70)
    
    manager = PrefectDeploymentManager()
    
    try:
        # Step 1: Setup secrets
        await manager.setup_secrets()
        print()
        
        # Step 2: Setup notification blocks
        await manager.setup_notification_blocks()
        print()
        
        # Step 3: Create deployment configurations
        manager.create_deployment_configs()
        print("✓ Deployment configurations created")
        print()
        
        # Step 4: Setup work pools
        await manager.setup_work_pools()
        print()
        
        # Step 5: Create deployments
        deployment_ids = await manager.create_deployments()
        print()
        
        # Step 6: Setup monitoring
        monitoring_config = await manager.setup_monitoring_and_observability()
        print()
        
        # Step 7: Validate deployment
        validation_results = await manager.validate_deployment()
        print()
        
        # Step 8: Run test workflow (optional)
        test_input = input("Run test workflow? (y/N): ").lower().strip()
        if test_input == 'y':
            await manager.run_test_workflow()
            print()
        
        # Summary
        print("=" * 70)
        print("🎉 Prefect Deployment Setup Complete!")
        print(f"✓ Deployments created: {len(deployment_ids)}")
        print(f"✓ Work pools configured: {len(validation_results['work_pools'])}")
        print(f"✓ Overall status: {validation_results['overall_status']}")
        
        print("\n📋 Next Steps:")
        print("1. Start Prefect worker: prefect worker start --pool default-worker-pool")
        print("2. Access Prefect UI: prefect server start")
        print("3. Monitor workflows at: http://localhost:4200")
        print("4. Test workflow execution through your API")
        
        print("\n🔧 Configuration Files Generated:")
        print("- prefect_workflow_engine.py (Main workflow engine)")
        print("- prefect_postgres_tasks.py (PostgreSQL task implementations)")
        print("- prefect_workflow_service.py (Service layer integration)")
        print("- prefect_deployment_setup.py (This deployment script)")
        
    except Exception as ex:
        print(f"❌ Deployment setup failed: {ex}")
        sys.exit(1)


def create_docker_compose():
    """Create a Docker Compose file for easy development setup"""
    docker_compose_content = """
version: '3.8'

services:
  prefect-server:
    image: prefecthq/prefect:3-latest
    ports:
      - "4200:4200"
    environment:
      - PREFECT_SERVER_API_HOST=0.0.0.0
      - PREFECT_API_URL=http://localhost:4200/api
    command: prefect server start --host 0.0.0.0
    volumes:
      - prefect_data:/root/.prefect
  
  prefect-worker:
    image: prefecthq/prefect:3-latest
    environment:
      - PREFECT_API_URL=http://prefect-server:4200/api
    depends_on:
      - prefect-server
    command: prefect worker start --pool default-worker-pool
    volumes:
      - ./:/app
      - prefect_data:/root/.prefect
    working_dir: /app
  
  postgres:
    image: postgres:15
    environment:
      - POSTGRES_DB=workflow_db
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
  
  mongodb:
    image: mongo:7
    ports:
      - "27017:27017"
    environment:
      - MONGO_INITDB_ROOT_USERNAME=admin
      - MONGO_INITDB_ROOT_PASSWORD=admin
    volumes:
      - mongodb_data:/data/db

volumes:
  prefect_data:
  postgres_data:
  redis_data:
  mongodb_data:
"""
    
    with open("docker-compose.yml", "w") as f:
        f.write(docker_compose_content)
    
    print("✓ docker-compose.yml created")


def create_env_template():
    """Create environment template file"""
    env_content = """
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_DB=workflow_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# MongoDB Configuration
MONGODB_URI=mongodb://admin:admin@localhost:27017

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379

# Cloud Storage Configuration
GCS_CREDENTIALS_PATH=/path/to/gcs-key.json
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key

# Prefect Configuration
PREFECT_API_URL=http://localhost:4200/api
PREFECT_API_KEY=your_prefect_api_key

# Notification Configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK

# Webhook Configuration
WEBHOOK_WORKFLOW_START=http://localhost:8000/webhooks/workflow/start
WEBHOOK_WORKFLOW_COMPLETE=http://localhost:8000/webhooks/workflow/complete
WEBHOOK_WORKFLOW_FAILED=http://localhost:8000/webhooks/workflow/failed
"""
    
    with open(".env.template", "w") as f:
        f.write(env_content)
    
    print("✓ .env.template created")


def create_requirements():
    """Create requirements.txt file"""
    requirements_content = """
prefect>=3.0.0
psycopg2-binary>=2.9.0
pandas>=2.0.0
google-cloud-storage>=2.10.0
boto3>=1.26.0
redis>=4.5.0
pymongo>=4.3.0
fastapi>=0.100.0
uvicorn>=0.20.0
websockets>=11.0.0
python-multipart>=0.0.6
python-dotenv>=1.0.0
httpx>=0.25.0
asyncio>=3.4.3
"""
    
    with open("requirements.txt", "w") as f:
        f.write(requirements_content)
    
    print("✓ requirements.txt created")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Prefect 3 Deployment Setup")
    parser.add_argument("--create-docker", action="store_true", help="Create Docker Compose file")
    parser.add_argument("--create-env", action="store_true", help="Create environment template")
    parser.add_argument("--create-requirements", action="store_true", help="Create requirements.txt")
    parser.add_argument("--full-setup", action="store_true", help="Run full deployment setup")
    
    args = parser.parse_args()
    
    if args.create_docker:
        create_docker_compose()
    elif args.create_env:
        create_env_template()
    elif args.create_requirements:
        create_requirements()
    elif args.full_setup:
        create_docker_compose()
        create_env_template()
        create_requirements()
        asyncio.run(main())
    else:
        asyncio.run(main())

            "
