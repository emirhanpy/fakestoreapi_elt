# FakestoreAPI ELT Pipeline

## Project Overview
This project features an ELT (Extract, Load, Transform) pipeline designed to extract product data from the FakestoreAPI using Python. The extracted data is loaded into a JSON file, and the transformation phase is done using Apache Spark in Jupyter Notebooks. SparkSQL is used to run queries on the transformed data, and Airflow automates the entire ELT process.

## Features
- **Data Extraction**: Automatically fetches product data from the FakestoreAPI using HTTP requests in Python.
- **Data Loading**: The raw data is loaded into a JSON file for further processing.
- **Data Transformation**: The data is processed and transformed using Apache Spark within a Jupyter Notebook.
- **Data Querying**: After transforming the data, SparkSQL is used to run SQL-like queries on the data for analysis.
- **ELT Automation**: Airflow is used to automate the entire ELT process through a Directed Acyclic Graph (DAG), orchestrating data extraction, loading, and transformation.

## Requirements
- Python 3.x
- Apache Spark (with PySpark)
- Apache Airflow
- Required Python libraries:
  - `requests`
  - `pyspark`
  - `datetime`
  - `airflow`

## How It Works
1. **Data Extraction**: The project uses the `requests` library to pull product data from the FakestoreAPI. The data is fetched in JSON format.
2. **Data Loading**: The raw data is loaded into a local JSON file to serve as the source for the transformation phase.
3. **Data Transformation**: In the transformation phase, Apache Spark is used to clean, filter, and transform the data in a Jupyter Notebook environment.
4. **Querying with SparkSQL**: After the data is transformed, SparkSQL is used to run SQL-like queries on the data for deeper insights and analytics.
5. **ELT Automation with Airflow**: Airflow is set up to automate the entire ELT process by creating a DAG that runs the steps of extraction, loading, and transformation in sequence.

## Installation

To run this project, you'll need to install the following dependencies:
```bash
pip install requests pandas pyspark airflow
