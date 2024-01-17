import os

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator,
)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)


# -----------------------------------------------------------------
@dag(
    dag_id="date_engineering_project",
    schedule="@once",
    start_date=pendulum.datetime(2023, 12, 10),
    tags=["data_engineering_project"],
)
def the2in1_dataengineer_project():
    """
    This project is made 2-in-1 because I don't want to repeat the ingesting data step.
    """

    # --------------------- Common tasks ---------------------
    # Extract data
    DATA_MONTH_YEAR = pendulum.now().subtract(months=3)
    DATA_YEAR = DATA_MONTH_YEAR.year
    DATA_MONTH = str(DATA_MONTH_YEAR.month).zfill(2)
    DATA_ENDPOINT = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    DATA_FILE_NAME = f"fhvhv_tripdata_{DATA_YEAR}-{DATA_MONTH}.parquet"
    DATA_URL = DATA_ENDPOINT + DATA_FILE_NAME
    DATA_DIR = os.path.join(os.path.expanduser("~"), "data")

    extract = BashOperator(
        task_id="NYC_TLC_extract",
        bash_command="wget -P $data_dir $data_url",
        env={"data_dir": DATA_DIR, "data_url": DATA_URL},
    )

    # Load data to GCS
    SRC_FILE = os.path.join(DATA_DIR, DATA_FILE_NAME)
    GCS_DST = "pq/"
    BUCKET_ID = Variable.get("BUCKET_ID")
    load_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_GCS",
        src=SRC_FILE,
        dst=GCS_DST,
        bucket=BUCKET_ID,
    )

    # ---------------- ETL - Cloud transform -----------------
    DATA_GCS_URL = f"gs://{BUCKET_ID}/{GCS_DST}{DATA_FILE_NAME}"
    DATAPROC_OUTPUT = f"gs://{BUCKET_ID}/dataproc_results/"
    REGION = Variable.get("REGION")
    CLUSTER_NAME = Variable.get("CLUSTER_NAME")
    PROJECT_ID = Variable.get("PROJECT_ID")
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        region=REGION,
        job={
            "reference": {
                "project_id": PROJECT_ID
            },
            "placement": {
                "cluster_name": CLUSTER_NAME,
            },
            "pyspark_job": {
                "main_python_file_uri": "gs://data-engineer-project-nhanart/pyspark_app/etl_transform.py",
                "args": [
                    f"--nyc_input_path={DATA_GCS_URL}",
                    f"--nyc_output_path={DATAPROC_OUTPUT}",
                    f"--nyc_year={DATA_YEAR}",
                    f"--nyc_month={DATA_MONTH}",
                ],
            }
        },
    )

    list_transformed_tables = GCSListObjectsOperator(
        task_id="list_transformed_tables",
        bucket=BUCKET_ID,
        prefix="dataproc_results/",
        match_glob="**/",
    )

    @task
    def create_kwargs(tab_path):
        ETL_BQ_DATASET_NAME = "etl_dataset"
        tab_name = os.path.basename(os.path.dirname(tab_path))
        return {
            "source_objects": tab_path + "*.parquet",
            "destination_project_dataset_table": f"{ETL_BQ_DATASET_NAME}.{tab_name}",
            "write_disposition": "WRITE_APPEND"
            if tab_name in ["datetime_dim", "fact_table"]
            else "WRITE_TRUNCATE",
        }

    list_kwargs = create_kwargs.expand(tab_path=list_transformed_tables.output)

    etl_gcs_to_bigquery = GCSToBigQueryOperator.partial(
        task_id="etl_gcs_to_bigquery",
        bucket=BUCKET_ID,
        source_format="PARQUET",
        autodetect=True,
    ).expand_kwargs(list_kwargs)

    # ---------------- ELT - Cloud transform -----------------
    ELT_BQ_DATASET_NAME = "elt_staging"
    TABLE_NAME = "raw_fact_data"
    elt_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="elt_gcs_to_bigquery",
        bucket=BUCKET_ID,
        source_objects=GCS_DST + DATA_FILE_NAME,
        destination_project_dataset_table=f"{ELT_BQ_DATASET_NAME}.{TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
    )
    
    DBT_SRC = os.path.join(os.path.expanduser("~"), "dbt_src", "elt_bigquery")
    START_DATE = DATA_MONTH_YEAR.start_of("month").strftime("%Y-%m-%d")
    END_DATE = DATA_MONTH_YEAR.add(months=1).start_of("month").strftime("%Y-%m-%d")
    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command="cd $DBT_SRC && ~/miniconda3/envs/de/bin/dbt run --vars \"{start_date: '$START_DATE', end_date: '$END_DATE'}\"",
        env={
            "DBT_SRC": DBT_SRC,
            "START_DATE": START_DATE,
            "END_DATE": END_DATE,
        },
    )
    # --------------------------------------------------------

    # ------------------- Task dependency --------------------
    # Common task
    extract >> load_to_gcs
    # ETL
    load_to_gcs >> submit_pyspark_job >> list_transformed_tables
    # ELT 
    load_to_gcs >> elt_gcs_to_bigquery >> dbt_transform


# Call dag
the2in1_dataengineer_project()
