import os
from datetime import datetime

from airflow import models
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from debussy_airflow.hooks.facebook_ads_hook import FacebookAdsHook
from debussy_airflow.hooks.storage_hook import GCSHook
from debussy_airflow.operators.facebook_ads_to_storage_operator import (
    FacebookAdsToStorageOperator,
)
from facebook_business.adobjects.adsinsights import AdsInsights

gcp_project = os.environ.get("GCP_PROJECT", "modular-aileron-191222")
project_env = "prod" if gcp_project == "dotzcloud-datalabs-production" else "dev"
bucket = f"dotz-datalake-{project_env}-l1-landing"
file_name = "facebook_ads_ad_{{ ts_nodash }}.csv"
gcs_object_path = f"facebook_ads_ad_api/{file_name}"
dataset = "temp"
table = "facebook_ads_ad"

default_args = {
    "owner": "debussy_framework_test",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

fields = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.clicks,
    AdsInsights.Field.impressions,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.outbound_clicks,
]

parameters = {
    "level": "ad",
    "limit": 1000,
    "time_range": {"since": "{{ ds }}", "until": "{{ ds }}"},
}
rest_to_storage_task_id = "facebook_ads_ad_api_to_gcs"

storage_hook = GCSHook(gcp_conn_id="google_cloud_default")
facebookads_hook = FacebookAdsHook(
    facebook_conn_id="facebook_default", api_version="v13.0"
)


def exists_file_in_bucket(task_id, **context):
    """short circuit if there is no file in bucket"""
    exist_file = context["ti"].xcom_pull(task_ids=task_id)
    return exist_file


with models.DAG(
    dag_id="test_debussy_framework_facebook_ads",
    default_args=default_args,
    description="Exporting data from the facebook ads campaigns",
    max_active_runs=5,
    schedule_interval=None,
    start_date=datetime(2019, 10, 8),
    catchup=True,
    tags=["debussy_framework", "test dag"],
) as dag:
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=dataset,
        table_id=table,
        schema_fields=[
            {
                "name": "account_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The ID number of your ad account, which groups your advertising activity. Your ad account includes your campaigns, ads and billing.",
            },
            {
                "name": "account_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The name of your ad account, which groups your advertising activity. Your ad account includes your campaigns, ads and billing.",
            },
            {
                "name": "campaign_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The name of the ad campaign you're viewing in reporting. Your campaign contains ad sets and ads.",
            },
            {
                "name": "campaign_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The unique ID number of the ad campaign you're viewing in reporting. Your campaign contains ad sets and ads.",
            },
            {
                "name": "adset_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The unique ID of the ad set you're viewing in reporting. An ad set is a group of ads that share the same budget, schedule, delivery optimization and targeting.",
            },
            {
                "name": "adset_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The name of the ad set you're viewing in reporting. An ad set is a group of ads that share the same budget, schedule, delivery optimization and targeting.",
            },
            {
                "name": "ad_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The unique ID of the ad you're viewing in reporting.",
            },
            {
                "name": "ad_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The name of the ad you're viewing in reporting.",
            },
            {
                "name": "clicks",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The number of clicks on your ads.",
            },
            {
                "name": "impressions",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The number of times your ads were on screen.",
            },
            {
                "name": "spend",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "The estimated total amount of money you've spent on your campaign, ad set or ad during its schedule. This metric is estimated.",
            },
            {
                "name": "date_start",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "The start date for your data. This is controlled by the date range you've selected for your reporting view.",
            },
            {
                "name": "date_stop",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "The end date for your data. This is controlled by the date range you've selected for your reporting view.",
            },
            {
                "name": "outbound_clicks",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The number of clicks on links that take people off Facebook-owned properties.",
            },
        ],
    )

    run_build_csv = FacebookAdsToStorageOperator(
        task_id=rest_to_storage_task_id,
        facebookads_hook=facebookads_hook,
        storage_hook=storage_hook,
        parameters=parameters,
        fields=fields,
        bucket_file_path=f"gs://{bucket}/{gcs_object_path}",
        file_name=file_name,
    )

    check_response_data = ShortCircuitOperator(
        task_id="check_exists_file",
        python_callable=exists_file_in_bucket,
        provide_context=True,
        op_kwargs={"task_id": rest_to_storage_task_id},
    )

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=bucket,
        source_objects=[gcs_object_path],
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        field_delimiter=",",
        destination_project_dataset_table=f"{dataset}.{table}",
        write_disposition="WRITE_APPEND",
    )

    create_table >> run_build_csv >> check_response_data >> load_csv
