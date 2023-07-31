from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import (SQLCheckOperator, BranchSQLOperator)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from cosmos.providers.dbt.task_group import DbtTaskGroup
from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro.options import *
from astro.files import File
from pendulum import datetime, duration
from io import StringIO
import pandas as pd
import requests
import logging

task_logger = logging.getLogger("airflow.task")

CONNECTION_ID = "postgres_default"
DB_NAME = "spdr_db"
SCHEMA_NAME = "spdr_schema"
TABLE_NAME = "spdr_gold_spots"
CSV_URL = "https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv"
DBT_PROJECT_NAME = "spdr_project"
POSTGRES_SPDR_TABLE = f'{SCHEMA_NAME}.{TABLE_NAME}'

# the path where the Astro dbt provider will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

# The path to dbt directory
DBT_ROOT_PATH = "/usr/local/airflow/dbt/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": duration(minutes = 1),
}

params = {
    'db_name': DB_NAME,
    'schema_name': SCHEMA_NAME,
    'table_name': TABLE_NAME
}

@aql.dataframe(columns_names_capitalization = "original")
def get_newest_data():
    data = requests.get(CSV_URL).content
    df = pd.read_csv(StringIO(data.decode('utf-8')), skiprows = 6)
    return df

@aql.dataframe(columns_names_capitalization = "original")
def transform_data (df: pd.DataFrame):
    # Remove backspace in columns' names
    df.columns = [col.strip() for col in df.columns.to_list()]

    df.drop (columns = 
        ['Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT',
        'Mid point of bid/ask spread at 4.15 p.m. NYT#'],
    inplace = True)
    
    # Rename all columns
    df.columns = ['Date', 'GLD Close', 'LBMA Gold Price', 'NAV Per GLD',
        'NAV Per share', 'Indicative Price', 'Daily Share Volume',
        'Total NAV Ounces', 'Total NAV Tonnes', 'Total NAV Value']
    
    # Transform and Parsing data
    df['LBMA Gold Price'] = df['LBMA Gold Price'].apply (lambda x: x.replace ('$', ''))
    df['Date'] = pd.to_datetime (df['Date'], format = '%d-%b-%Y', errors = 'coerce')
    df = df[df['Date'].isna() == False]
    
    for col in df.columns[1:]:
        df= df[df[col].str.contains ('[0-9]+', regex = True) == True]
        df[col] = df[col].astype('float64')
        
    cols = [col.lower().replace(' ', '_') for col in df.columns]
    df.columns = cols
    return df


with DAG(
    dag_id = 'spdr_ingest_dag',
    start_date = datetime(2023, 1, 1),
    max_active_runs = 3,
    schedule = "@once",
    default_args = default_args,
    template_searchpath = "usr/local/airflow/include/",
    catchup = False,
) as dag:
    first_task = EmptyOperator (task_id = 'first_task')
    
    get_newest_data = get_newest_data ()
    
    transform_data = transform_data (get_newest_data)
    
    spdr_table_check = BranchSQLOperator (
        task_id = 'spdr_table_check',
        conn_id = CONNECTION_ID,
        sql = 'sql/check_schema_exists.sql',
        params = params,
        follow_task_ids_if_true = ['get_newest_data'],
        follow_task_ids_if_false = ['spdr_create_metadata']
    ) 
        
    spdr_create_metadata = PostgresOperator (
        task_id = 'spdr_create_metadata',
        sql = 'sql/create_schema.sql',
        params = params,
    )
    
    # For the case data exists
    spdr_merge_data = aql.merge (
        task_id = 'spdr_merge_data',
        target_table = Table(name = TABLE_NAME, conn_id = CONNECTION_ID),
        source_table = transform_data,  
        target_conflict_columns = ['date'],  
        columns = ['date'],
        if_conflicts = 'update'
    )
    
    # For the case data doesn't exist
    spdr_append_data = aql.append (
        task_id = 'spdr_append_data',
        target_table = Table(name = TABLE_NAME, conn_id = CONNECTION_ID),
        source_table = transform_data,  
        columns = ['date'],
    )
    
    spdr_empty_dag = EmptyOperator (task_id = 'spdr_empty_dag')
    
    
    # Execute pipeline
    first_task >> spdr_table_check >> spdr_create_metadata >> get_newest_data >> transform_data >> spdr_append_data
    first_task >> spdr_table_check >> spdr_empty_dag >> get_newest_data >> transform_data >> spdr_merge_data