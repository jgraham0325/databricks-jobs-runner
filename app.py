import os
import yaml
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv
import streamlit as st
from services.databricks_jobs import DatabricksJobsService

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Databricks Workflow Runner",
    page_icon="🚀",
    layout="wide"
)

# Initialize services
jobs_service = DatabricksJobsService()

# Constants
JOB_CONFIGS_DIR = Path("job_configs")


def load_job_configs() -> Dict[str, Path]:
    """Load all YAML config files from job_configs directory."""
    configs = {}
    if JOB_CONFIGS_DIR.exists():
        for config_file in JOB_CONFIGS_DIR.glob("*.yaml"):
            try:
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    if config and 'job_name' in config:
                        configs[config['job_name']] = config_file
            except Exception as e:
                st.error(f"Error loading config {config_file}: {e}")
    return configs


def load_job_config(job_name: str) -> Optional[Dict[str, Any]]:
    """Load a specific job config by name."""
    configs = load_job_configs()
    if job_name not in configs:
        return None
    
    try:
        with open(configs[job_name], 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Error loading config for {job_name}: {e}")
        return None


def validate_parameter(value: Any, param_config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a parameter value against its configuration."""
    param_type = param_config.get('type')
    validation = param_config.get('validation', {})
    
    # Check required fields
    if param_config.get('required', False) and (value is None or value == ''):
        return False, f"{param_config.get('label', param_config['name'])} is required"
    
    # Skip validation if value is empty and not required
    if not param_config.get('required', False) and (value is None or value == ''):
        return True, None
    
    # Type-specific validation
    if param_type == 'text':
        if 'max_length' in validation:
            if len(str(value)) > validation['max_length']:
                return False, f"Maximum length is {validation['max_length']} characters"
    
    elif param_type == 'integer':
        try:
            int_value = int(value)
            if 'min' in validation and int_value < validation['min']:
                return False, f"Minimum value is {validation['min']}"
            if 'max' in validation and int_value > validation['max']:
                return False, f"Maximum value is {validation['max']}"
        except ValueError:
            return False, "Must be a valid integer"
    
    elif param_type == 'decimal':
        try:
            float_value = float(value)
            if 'min' in validation and float_value < validation['min']:
                return False, f"Minimum value is {validation['min']}"
            if 'max' in validation and float_value > validation['max']:
                return False, f"Maximum value is {validation['max']}"
        except ValueError:
            return False, "Must be a valid number"
    
    elif param_type == 'date':
        if isinstance(value, date):
            date_value = value
        elif isinstance(value, str):
            try:
                date_value = datetime.strptime(value, '%Y-%m-%d').date()
            except ValueError:
                return False, "Must be a valid date (YYYY-MM-DD)"
        else:
            return False, "Must be a valid date"
        
        if 'min_date' in validation:
            min_date = datetime.strptime(validation['min_date'], '%Y-%m-%d').date()
            if date_value < min_date:
                return False, f"Date must be on or after {validation['min_date']}"
        
        if 'max_date' in validation:
            max_date = datetime.strptime(validation['max_date'], '%Y-%m-%d').date()
            if date_value > max_date:
                return False, f"Date must be on or before {validation['max_date']}"
    
    elif param_type == 'enum':
        options = param_config.get('options', [])
        if value not in options:
            return False, f"Must be one of: {', '.join(options)}"
    
    return True, None


def render_parameter_input(param_config: Dict[str, Any], key_prefix: str) -> Any:
    """Render the appropriate input widget for a parameter."""
    param_name = param_config['name']
    param_type = param_config.get('type', 'text')
    label = param_config.get('label', param_name)
    required = param_config.get('required', False)
    validation = param_config.get('validation', {})
    key = f"{key_prefix}_{param_name}"
    
    # Get default value from session state if available
    default_value = st.session_state.get(key, None)
    
    if param_type == 'text':
        max_length = validation.get('max_length', None)
        value = st.text_input(
            label=f"{label}{' *' if required else ''}",
            value=default_value if default_value else "",
            key=key,
            max_chars=max_length if max_length else None,
            help=f"Max length: {max_length}" if max_length else None
        )
        return value if value else None
    
    elif param_type == 'integer':
        min_val = validation.get('min', None)
        max_val = validation.get('max', None)
        value = st.number_input(
            label=f"{label}{' *' if required else ''}",
            value=int(default_value) if default_value is not None else (min_val if min_val else 0),
            min_value=min_val if min_val is not None else None,
            max_value=max_val if max_val is not None else None,
            step=1,
            key=key
        )
        return value if value is not None else None
    
    elif param_type == 'decimal':
        min_val = validation.get('min', None)
        max_val = validation.get('max', None)
        value = st.number_input(
            label=f"{label}{' *' if required else ''}",
            value=float(default_value) if default_value is not None else (min_val if min_val else 0.0),
            min_value=min_val if min_val is not None else None,
            max_value=max_val if max_val is not None else None,
            step=0.01,
            format="%.2f",
            key=key
        )
        return value if value is not None else None
    
    elif param_type == 'date':
        min_date_str = validation.get('min_date')
        max_date_str = validation.get('max_date')
        
        min_date = None
        max_date = None
        
        if min_date_str:
            min_date = datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if max_date_str:
            max_date = datetime.strptime(max_date_str, '%Y-%m-%d').date()
        
        default_date = None
        if default_value:
            if isinstance(default_value, str):
                default_date = datetime.strptime(default_value, '%Y-%m-%d').date()
            elif isinstance(default_value, date):
                default_date = default_value
        
        value = st.date_input(
            label=f"{label}{' *' if required else ''}",
            value=default_date,
            min_value=min_date,
            max_value=max_date,
            key=key
        )
        return value if value else None
    
    elif param_type == 'enum':
        options = param_config.get('options', [])
        default_index = 0
        if default_value and default_value in options:
            default_index = options.index(default_value)
        
        value = st.selectbox(
            label=f"{label}{' *' if required else ''}",
            options=options,
            index=default_index if default_value else 0,
            key=key
        )
        return value if value else None
    
    else:
        # Fallback to text input
        return st.text_input(
            label=f"{label}{' *' if required else ''}",
            value=default_value if default_value else "",
            key=key
        )


def render_job_form(config: Dict[str, Any]):
    """Render the form for a specific job configuration."""
    job_name = config.get('job_name', 'unknown')
    display_name = config.get('display_name', job_name)
    description = config.get('description', '')
    parameters = config.get('parameters', [])
    
    # Track if validation should be shown (only after submit attempt)
    validation_key = f"show_validation_{job_name}"
    show_validation = st.session_state.get(validation_key, False)
    
    # Back button
    if st.button("← Back to Job Selection", use_container_width=False):
        if "selected_job" in st.session_state:
            del st.session_state["selected_job"]
        if validation_key in st.session_state:
            del st.session_state[validation_key]
        st.rerun()
    
    st.title(display_name)
    if description:
        st.markdown(f"*{description}*")
    
    st.divider()
    
    # Store form values
    form_values = {}
    validation_errors = {}
    
    # Render all parameter inputs
    for param_config in parameters:
        param_name = param_config['name']
        value = render_parameter_input(param_config, f"job_{job_name}")
        form_values[param_name] = value
        
        # Only validate if we should show validation errors
        if show_validation:
            is_valid, error_msg = validate_parameter(value, param_config)
            if not is_valid:
                validation_errors[param_name] = error_msg
    
    # Display validation errors only if validation should be shown
    if show_validation and validation_errors:
        st.error("Please fix the following errors:")
        for param_name, error_msg in validation_errors.items():
            st.error(f"• {error_msg}")
    
    st.divider()
    
    # Submit button
    if st.button("🚀 Run Job", type="primary", use_container_width=True):
        # Validate all fields when submit is clicked
        validation_errors = {}
        for param_config in parameters:
            param_name = param_config['name']
            value = form_values.get(param_name)
            is_valid, error_msg = validate_parameter(value, param_config)
            if not is_valid:
                validation_errors[param_name] = error_msg
        
        # Set flag to show validation errors
        st.session_state[validation_key] = True
        
        if validation_errors:
            st.error("Please fix validation errors before submitting.")
            st.rerun()
        else:
            try:
                # Lookup job ID
                with st.spinner("Looking up job ID..."):
                    job_id = jobs_service.get_job_id_by_name(job_name)
                
                if not job_id:
                    st.error(f"Job '{job_name}' not found in Databricks workspace.")
                else:
                    # Prepare job parameters (convert dates to strings)
                    job_parameters = {}
                    for param_name, value in form_values.items():
                        if value is not None:
                            if isinstance(value, date):
                                job_parameters[param_name] = value.strftime('%Y-%m-%d')
                            else:
                                job_parameters[param_name] = str(value)
                    
                    # Submit job
                    with st.spinner(f"Submitting job '{display_name}'..."):
                        run_info = jobs_service.run_job(job_id, job_parameters)
                    
                    # Construct Databricks job run URL
                    databricks_host = os.getenv("DATABRICKS_HOST", "").rstrip('/')
                    job_run_url = f"{databricks_host}/#job/{job_id}/run/{run_info['run_id']}"
                    
                    # Poll for job completion
                    status_container = st.empty()
                    progress_text = st.empty()
                    
                    def update_progress(message: str):
                        progress_text.text(message)
                    
                    with status_container.container():
                        st.info(f"Job submitted! Run ID: {run_info['run_id']}\n\n🔗 [View Job Run in Databricks]({job_run_url})")
                    
                    # Wait for job completion
                    with st.spinner("Waiting for job to complete..."):
                        completion_status = jobs_service.wait_for_job_completion(
                            run_id=run_info['run_id'],
                            timeout_seconds=3600,
                            poll_interval=5,
                            progress_callback=update_progress
                        )
                    
                    # Clear progress text
                    progress_text.empty()
                    
                    # Display final result
                    if completion_status['status'] == 'SUCCESS':
                        st.success(f"✅ Job completed successfully!")
                        status_container.empty()
                        st.info(f"Run ID: {run_info['run_id']}\n\n🔗 [View Job Run in Databricks]({job_run_url})")
                    elif completion_status['status'] == 'FAILED':
                        st.error(f"❌ Job failed!")
                        status_container.empty()
                        
                        error_message = f"Run ID: {run_info['run_id']}"
                        if completion_status.get('state_message'):
                            error_message += f"\n\n**Error:** {completion_status['state_message']}"
                        if completion_status.get('result_state'):
                            error_message += f"\n**Result State:** {completion_status['result_state']}"
                        
                        # Display task-level errors if available
                        task_errors = completion_status.get('task_errors', [])
                        if task_errors:
                            error_message += "\n\n**Task Errors:**"
                            for task_error in task_errors:
                                error_message += f"\n- **Task:** {task_error.get('task_key', 'unknown')}"
                                if task_error.get('state_message'):
                                    error_message += f"\n  - {task_error['state_message']}"
                                if task_error.get('result_state'):
                                    error_message += f"\n  - State: {task_error['result_state']}"
                        
                        error_message += f"\n\n🔗 [View Job Run in Databricks]({job_run_url})"
                        st.error(error_message)
                    elif completion_status['status'] == 'TIMEOUT':
                        st.warning(f"⏱️ Job did not complete within the timeout period")
                        status_container.empty()
                        st.info(f"Run ID: {run_info['run_id']}\n\n🔗 [View Job Run in Databricks]({job_run_url})")
                    else:
                        st.warning(f"⚠️ Job status: {completion_status['status']}")
                        status_container.empty()
                        st.info(f"Run ID: {run_info['run_id']}\n\n🔗 [View Job Run in Databricks]({job_run_url})")
                    
                    # Clear form values and validation flag
                    for param_config in parameters:
                        param_name = param_config['name']
                        key = f"job_{job_name}_{param_name}"
                        if key in st.session_state:
                            del st.session_state[key]
                    if validation_key in st.session_state:
                        del st.session_state[validation_key]
            except Exception as e:
                st.error(f"Error submitting job: {str(e)}")


def main():
    """Main application entry point."""
    # Check for URL query parameter (Streamlit uses query params for routing)
    query_params = st.query_params
    
    # Get available job configs
    available_configs = load_job_configs()
    
    if not available_configs:
        st.error("No job configuration files found in 'job_configs' directory.")
        st.info("Please create YAML configuration files in the 'job_configs' directory.")
        return
    
    # Check if a specific job is requested via query parameter or session state
    # Priority: session state (for button clicks) > query params (for URL navigation)
    selected_job = st.session_state.get("selected_job", None)
    
    if not selected_job:
        job_param = query_params.get("job", None)
        if job_param:
            # Handle both list and string formats
            if isinstance(job_param, list):
                selected_job = job_param[0] if job_param else None
            else:
                selected_job = str(job_param) if job_param else None
    
    if selected_job:
        # Load and render specific job
        config = load_job_config(selected_job)
        if config:
            render_job_form(config)
        else:
            st.error(f"Job configuration '{selected_job}' not found.")
            st.info("Available jobs:")
            for job_name in available_configs.keys():
                st.markdown(f"- [{job_name}](?job={job_name})")
    else:
        # Show job selection page
        st.title("🚀 Databricks Workflow Runner")
        st.markdown("Select a job to run from the list below:")
        
        # Create dropdown for job selection
        job_names = list(available_configs.keys())
        selected_job_name = st.selectbox(
            "Select a job:",
            options=job_names,
            index=0
        )
        
        if selected_job_name:
            # Load and display job config preview
            config = load_job_config(selected_job_name)
            if config:
                st.divider()
                st.markdown(f"### {config.get('display_name', selected_job_name)}")
                if config.get('description'):
                    st.markdown(f"*{config.get('description')}*")
                
                # Show parameters preview
                parameters = config.get('parameters', [])
                if parameters:
                    st.markdown("**Parameters:**")
                    for param in parameters:
                        param_type = param.get('type', 'text')
                        param_label = param.get('label', param['name'])
                        required = param.get('required', False)
                        st.markdown(f"- {param_label} ({param_type}){' *' if required else ''}")
                
                # Button to go to job form
                if st.button("Configure and Run Job", type="primary", use_container_width=True):
                    st.session_state["selected_job"] = selected_job_name
                    st.rerun()


if __name__ == "__main__":
    main()

