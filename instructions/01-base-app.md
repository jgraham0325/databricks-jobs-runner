# Build the base of an app

Docs online:
https://apps-cookbook.dev/docs/category/streamlit

https://apps-cookbook.dev/docs/streamlit/workflows/workflows_run

Make me a simple Streamlit app (in the root of the project) that takes a set of input parameters from a config file, displays them on the screen to the user and allows them to fill them in and submit a Databricks job to run.

I want the following types of input values handled in my config files and UI:
- Date Pickers
- Text inputs
- Integer inputs
- Decimal inputs
- Enums (one or more of a set of defined values)

Allow me to define simple rules around validating these inputs, e.g. max length, max number, min number, min date, max date

The config file should also contain the name of the job to call. You might need to lookup the correct ID for that job. 

I want my config files to be in yaml format and sit in a subdirectory called "job_configs".

When I got to the app's root page, show a drop-down of the available jobs, based on the config files available.

I also want to be able to go directly to the inputs for a job based on the url, e.g. 
- http://myapp.com/job/alteryx-converted-job-1 will load the UI with inputs for alteryx-converted-job-1 as defined by it's config file.
- http://myapp.com/job/alteryx-converted-job-2 will load the UI with inputs for alteryx-converted-job-2 as defined by it's config file.

Use the Databricks SDK to trigger the jobs and pass the values of the parameters. Example: https://apps-cookbook.dev/docs/streamlit/workflows/workflows_run

Have the app load the env file.

don't worry about setting up virtual environments or packages, i will handle it.
