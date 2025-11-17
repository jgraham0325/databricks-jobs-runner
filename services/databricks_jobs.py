import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from typing import Optional, Dict, Any, Callable


class DatabricksJobsService:
    """Singleton service for interacting with Databricks Jobs API."""
    
    _instance = None
    _workspace_client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabricksJobsService, cls).__new__(cls)
        return cls._instance
    
    def _get_workspace_client(self) -> WorkspaceClient:
        """Get or create WorkspaceClient instance."""
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient(
                host=os.getenv("DATABRICKS_HOST"),
                client_id=os.getenv("DATABRICKS_CLIENT_ID"),
                client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
            )
        return self._workspace_client
    
    def get_job_id_by_name(self, job_name: str) -> Optional[str]:
        """
        Lookup job ID by job name.
        Returns the job ID if found, None otherwise.
        """
        try:
            w = self._get_workspace_client()
            jobs = list(w.jobs.list())
            
            for job in jobs:
                if job.settings and job.settings.name == job_name:
                    return str(job.job_id)
            
            return None
        except Exception as e:
            raise Exception(f"Error looking up job '{job_name}': {str(e)}")
    
    def run_job(self, job_id: str, job_parameters: dict) -> dict:
        """
        Trigger a Databricks job run with parameters.
        Returns the run information.
        """
        try:
            w = self._get_workspace_client()
            # run_now returns a Wait[Run] object, access the actual Run via .response
            # job_id must be an int according to the SDK
            run_response = w.jobs.run_now(job_id=int(job_id), job_parameters=job_parameters)
            run = run_response.response
            
            result = {
                "run_id": str(run.run_id)
            }
            # number_in_job may not always be available
            if hasattr(run, 'number_in_job') and run.number_in_job is not None:
                result["number_in_job"] = run.number_in_job
            return result
        except Exception as e:
            raise Exception(f"Error running job '{job_id}': {str(e)}")
    
    def wait_for_job_completion(
        self, 
        run_id: str, 
        timeout_seconds: int = 3600,
        poll_interval: int = 5,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, Any]:
        """
        Poll for job completion and return the final status.
        
        Args:
            run_id: The run ID to monitor
            timeout_seconds: Maximum time to wait (default: 1 hour)
            poll_interval: Seconds between status checks (default: 5)
            progress_callback: Optional callback function to report progress updates
        
        Returns:
            Dictionary with:
                - status: 'SUCCESS', 'FAILED', 'TIMEOUT', or 'CANCELLED'
                - state_message: Error message if failed, None otherwise
                - life_cycle_state: Final life cycle state
                - result_state: Final result state
        """
        try:
            w = self._get_workspace_client()
            start_time = time.time()
            
            while True:
                # Get current run status
                run_info = w.jobs.get_run(run_id=int(run_id))
                state = run_info.state
                life_cycle_state = state.life_cycle_state
                
                # Report progress if callback provided
                if progress_callback:
                    progress_callback(f"Job status: {life_cycle_state.value}")
                
                # Check if job has reached a terminal state
                if life_cycle_state in [
                    RunLifeCycleState.TERMINATED,
                    RunLifeCycleState.SKIPPED,
                    RunLifeCycleState.INTERNAL_ERROR
                ]:
                    result_state = state.result_state
                    state_message = getattr(state, 'state_message', None)
                    
                    if result_state == RunResultState.SUCCESS:
                        return {
                            "status": "SUCCESS",
                            "state_message": None,
                            "life_cycle_state": life_cycle_state.value,
                            "result_state": result_state.value,
                            "task_errors": []
                        }
                    else:
                        # Collect task-level errors
                        task_errors = []
                        if hasattr(run_info, 'tasks') and run_info.tasks:
                            for task in run_info.tasks:
                                if hasattr(task, 'state') and task.state:
                                    task_state = task.state
                                    task_result_state = getattr(task_state, 'result_state', None)
                                    if task_result_state and task_result_state != RunResultState.SUCCESS:
                                        task_error = {
                                            "task_key": getattr(task, 'task_key', 'unknown'),
                                            "state_message": getattr(task_state, 'state_message', None),
                                            "result_state": task_result_state.value if hasattr(task_result_state, 'value') else str(task_result_state)
                                        }
                                        task_errors.append(task_error)
                        
                        return {
                            "status": "FAILED",
                            "state_message": state_message,
                            "life_cycle_state": life_cycle_state.value,
                            "result_state": result_state.value if result_state else None,
                            "task_errors": task_errors
                        }
                
                # Check for timeout
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout_seconds:
                    return {
                        "status": "TIMEOUT",
                        "state_message": f"Job did not complete within {timeout_seconds} seconds",
                        "life_cycle_state": life_cycle_state.value,
                        "result_state": None,
                        "task_errors": []
                    }
                
                # Wait before polling again
                time.sleep(poll_interval)
                
        except Exception as e:
            raise Exception(f"Error waiting for job completion '{run_id}': {str(e)}")

