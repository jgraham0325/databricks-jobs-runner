# Databricks Jobs Runner

A Streamlit web application for running Databricks Jobs with dynamic parameter configuration. Define job parameters in YAML configuration files and submit jobs through an intuitive web interface.

## Technologies Used

- **Streamlit**: Modern web framework for building data applications with Python
- **Databricks SDK**: Integration with Databricks Workspace and Jobs API
- **Python-dotenv**: Environment variable management
- **PyYAML**: YAML configuration file parsing

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
├── services/
│   └── databricks_jobs.py     # Databricks Jobs service (singleton)
├── job_configs/               # YAML job configuration files
│   └── example-job.yaml       # Example configuration
├── requirements.txt           # Python dependencies
├── example.env               # Example environment variables
└── README.md                 # This file
```
