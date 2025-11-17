# Databricks Jobs Runner

A Streamlit web application for running Databricks Jobs with dynamic parameter configuration. Define job parameters in YAML configuration files and submit jobs through an intuitive web interface.

This project is also configured as a **Databricks Asset Bundle**, allowing you to deploy and manage Databricks jobs and the Streamlit app itself using the Databricks CLI.

## Technologies Used

- **Streamlit**: Modern web framework for building data applications with Python
- **Databricks SDK**: Integration with Databricks Workspace and Jobs API
- **Databricks Asset Bundles**: Infrastructure-as-code for Databricks resources
- **Python-dotenv**: Environment variable management
- **PyYAML**: YAML configuration file parsing

## Databricks Asset Bundle

This project is configured as a Databricks Asset Bundle, containing:

- **App**: `databricks-jobs-runner` - The Streamlit web application
- **Jobs**: Two job definitions:
  - `alteryx-converted-job-1`: Job with date range and client ID parameters
  - `alteryx-converted-job-2`: Job with client name, financial year, and accounting period parameters

### Using the Asset Bundle

1. **Install Databricks CLI** (if not already installed):
   ```bash
   pip install databricks-cli
   ```

2. **Configure variables**:
   The bundle uses variables defined in `databricks.yml`. You can set them via environment variables or CLI:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_USER="your-email@example.com"
   ```

3. **Validate the bundle**:
   ```bash
   databricks bundle validate
   ```

4. **Deploy the bundle** (to dev target):
   ```bash
   databricks bundle deploy -t dev
   ```
   
   This will deploy both the Streamlit app and the job definitions to your Databricks workspace.

5. **Access the deployed app**:
   After deployment, you can access your Streamlit app in the Databricks workspace. The app will be available at:
   ```
   https://your-workspace.cloud.databricks.com/apps/[app-name]
   ```

6. **Run a job**:
   ```bash
   databricks bundle run alteryx-converted-job-1 -t dev
   ```

### Bundle Structure

- `databricks.yml`: Main bundle configuration file (defines the app and includes job definitions)
- `app.py`: Main Streamlit application file
- `resources/jobs/`: Directory containing job definitions
  - `alteryx-converted-job-1.yml`: First job definition
  - `alteryx-converted-job-2.yml`: Second job definition

**Note**: Before deploying, update the job definitions in `resources/jobs/` with your actual notebook paths and cluster configurations.

### Deploying the App

The Streamlit app is configured as a Databricks App resource in the bundle. When you run `databricks bundle deploy -t dev`, it will:

1. Upload all application files (including `app.py`, `services/`, `job_configs/`, etc.) to your Databricks workspace
2. Deploy the app so it can be accessed through the Databricks UI
3. Deploy all job definitions configured in the bundle

The app will use environment variables configured in your Databricks workspace. Make sure to set up the required environment variables (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, etc.) in your workspace settings.

### DAB Job Name Prefixes

When deploying jobs using Databricks Asset Bundles in development mode, DAB automatically prefixes job names with `[target user]` to prevent naming conflicts. For example:
- Base job name: `alteryx-converted-job-1`
- Deployed job name: `[dev james_graham] alteryx-converted-job-1`

The application automatically handles these prefixes by:
1. **Detecting the current user** from the Databricks workspace
2. **Matching jobs** that belong to the current user's deployment
3. **Preventing conflicts** when multiple users have deployed jobs with the same base name

**Configuration**:
- The DAB target defaults to `dev` but can be overridden with the `DATABRICKS_BUNDLE_TARGET` environment variable
- The current user is automatically detected from your Databricks workspace connection

This ensures that each user's app instance references only their own deployed jobs, even when multiple users have deployed the same bundle.

## Features

### 🎯 Core Functionality

- **Dynamic Job Configuration**: Define job parameters in YAML files
- **Multiple Input Types**: Support for text, integer, decimal, date, and enum inputs
- **Input Validation**: Configurable validation rules (min/max values, date ranges, text length)
- **Job Discovery**: Automatic job ID lookup by job name
- **URL Routing**: Direct access to specific jobs via URL query parameters

### 📝 Supported Input Types

- **Text Inputs**: Single-line text with optional max length validation
- **Integer Inputs**: Whole numbers with min/max validation
- **Decimal Inputs**: Floating-point numbers with min/max validation
- **Date Pickers**: Date selection with min/max date validation
- **Enums**: Dropdown selection from predefined options

### 🎨 User Experience

- **Job Selection**: Dropdown menu showing all available job configurations
- **Parameter Forms**: Dynamic form generation based on YAML configuration
- **Real-time Validation**: Immediate feedback on input validation errors
- **Job Submission**: One-click job submission with run ID tracking

## Getting Started

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables:

   - Copy `example.env` to `.env`
   - Fill in your actual Databricks credentials:
     - `DATABRICKS_HOST` (e.g., `https://your-workspace.cloud.databricks.com`)
     - `DATABRICKS_CLIENT_ID`
     - `DATABRICKS_CLIENT_SECRET`
   - Optionally set `DATABRICKS_BUNDLE_TARGET` (defaults to `dev`) to match your DAB deployment target

3. Create job configuration files:

   - Create YAML files in the `job_configs/` directory
   - See `job_configs/example-job.yaml` for a template

4. Run the application:

   ```bash
   streamlit run app.py
   ```

5. Open your browser to `http://localhost:8501`

## Job Configuration Format

Job configurations are defined in YAML files in the `job_configs/` directory. Each file should follow this structure:

```yaml
job_name: "my-job-name"
display_name: "My Job Display Name"
description: "Description of what this job does"

parameters:
  - name: "param1"
    type: "text"
    label: "Parameter 1"
    required: true
    validation:
      max_length: 100
  
  - name: "param2"
    type: "integer"
    label: "Parameter 2"
    required: true
    validation:
      min: 1
      max: 1000
  
  - name: "param3"
    type: "date"
    label: "Start Date"
    required: true
    validation:
      min_date: "2020-01-01"
      max_date: "2030-12-31"
  
  - name: "param4"
    type: "enum"
    label: "Status"
    required: true
    options:
      - "active"
      - "inactive"
      - "pending"
```

### Parameter Types

- **text**: Text input field
- **integer**: Integer number input
- **decimal**: Decimal/floating-point number input
- **date**: Date picker
- **enum**: Dropdown selection

### Validation Rules

- **max_length**: Maximum character length (text only)
- **min**: Minimum value (integer, decimal)
- **max**: Maximum value (integer, decimal)
- **min_date**: Minimum date (date only, format: YYYY-MM-DD)
- **max_date**: Maximum date (date only, format: YYYY-MM-DD)

## URL Routing

You can access specific jobs directly via URL query parameters:

- `http://localhost:8501/?job=my-job-name` - Loads the form for `my-job-name`

The app automatically detects the `job` query parameter and displays the corresponding job configuration form.

## Permissions

Your Databricks service principal needs the following permissions:

- `CAN MANAGE RUN` permission on the jobs you want to run

See [Control access to a job](https://docs.databricks.com/en/jobs/manage-access.html) for more information.

## Project Structure

```
.
├── app.py                      # Main Streamlit application
├── databricks.yml             # Databricks Asset Bundle configuration
├── services/
│   └── databricks_jobs.py     # Databricks Jobs service (singleton)
├── resources/
│   └── jobs/                  # Databricks Asset Bundle job definitions
│       ├── alteryx-converted-job-1.yml
│       └── alteryx-converted-job-2.yml
├── job_configs/               # YAML job configuration files (for Streamlit app)
│   ├── alteryx-converted-job-1.yaml
│   └── alteryx-converted-job-2.yaml
├── requirements.txt           # Python dependencies
├── example.env               # Example environment variables
└── README.md                 # This file
```
