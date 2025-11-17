import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from typing import Optional, Dict, Any, Callable


class DatabricksJobsService:
    """Singleton service for interacting with Databricks Jobs API."""
    
    _instance = None
    _workspace_client = None
    _current_user = None
    _dab_target = None
    
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
    
    def _get_current_user(self) -> Optional[str]:
        """Get the current Databricks user name."""
        if self._current_user is None:
            try:
                w = self._get_workspace_client()
                current_user = w.current_user.me()
                # Extract username from user object (could be user_name or user_name property)
                if hasattr(current_user, 'user_name'):
                    self._current_user = current_user.user_name
                elif hasattr(current_user, 'userName'):
                    self._current_user = current_user.userName
                else:
                    # Fallback: try to get from the string representation
                    self._current_user = str(current_user)
            except Exception as e:
                # If we can't get the user, return None and fall back to pattern matching
                return None
        return self._current_user
    
    def _get_dab_target(self) -> str:
        """Get the DAB target (defaults to 'dev')."""
        if self._dab_target is None:
            # Check environment variable first, then default to 'dev'
            self._dab_target = os.getenv("DATABRICKS_BUNDLE_TARGET", "dev")
        return self._dab_target
    
    def _get_dab_prefix(self) -> Optional[str]:
        """Construct the DAB prefix pattern: '[target user]'."""
        target = self._get_dab_target()
        user = self._get_current_user()
        if user:
            return f"[{target} {user}]"
        return None
    
    def get_job_id_by_name(self, job_name: str) -> Optional[str]:
        """
        Lookup job ID by job name.
        Handles both exact matches and Databricks Asset Bundle (DAB) prefixed names.
        DAB adds prefixes like '[dev james_graham] job-name', so we match jobs for the current user.
        Prioritizes DAB-prefixed jobs over exact matches to ensure we use the deployed bundle version.
        Returns the job ID if found, None otherwise.
        """
        try:
            w = self._get_workspace_client()
            jobs = list(w.jobs.list())
            
            # Get the expected DAB prefix for the current user
            dab_prefix = self._get_dab_prefix()
            expected_full_name = f"{dab_prefix} {job_name}" if dab_prefix else None
            
            # PRIORITY 1: Try to match jobs with the current user's DAB prefix first
            # This ensures we use the DAB-deployed version when available
            if expected_full_name:
                for job in jobs:
                    if job.settings and job.settings.name == expected_full_name:
                        return str(job.job_id)
            
            # PRIORITY 2: Match any job ending with the expected name that has a DAB prefix
            # This handles cases where we can't determine the current user but can still find DAB jobs
            # We prefer ANY DAB-prefixed job over exact matches
            dab_prefixed_jobs = []
            for job in jobs:
                if job.settings and job.settings.name:
                    job_name_full = job.settings.name
                    # Match if job name ends with the expected name and has a DAB prefix format
                    # DAB format: '[target user] job-name'
                    if job_name_full.endswith(f"] {job_name}"):
                        # If we have a prefix, prioritize jobs matching our prefix
                        if dab_prefix and job_name_full.startswith(dab_prefix):
                            return str(job.job_id)
                        # Otherwise, collect all DAB-prefixed jobs
                        else:
                            dab_prefixed_jobs.append(job)
            
            # If we found any DAB-prefixed jobs (even if not matching our user), prefer them
            if dab_prefixed_jobs:
                # Return the first one found (could be enhanced to prefer current user if detectable)
                return str(dab_prefixed_jobs[0].job_id)
            
            # PRIORITY 3: Fallback to exact match (for backward compatibility)
            # Only used if no DAB-prefixed version is found
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

