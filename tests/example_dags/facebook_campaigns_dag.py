import os
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from debussy_airflow.hooks.facebook_ads import FacebookAdsHook
from debussy_airflow.hooks.storage_hook import GCSHook
from debussy_airflow.operators.facebook_ads_to_storage import \
    FacebookAdsToStorageOperator
from facebook_business.adobjects.adsinsights import AdsInsights

gcp_project = os.environ['GCP_PROJECT']
project_env = 'prod' if gcp_project == 'dotzcloud-datalabs-production' else 'dev'
bucket = f'dotz-datalake-{project_env}-l1-landing'
file_name = "facebook_ads_campaign_{{ ts_nodash }}.csv"
gcs_object_path = f"facebook_ads_campaign_api/{file_name}"
dataset = 'temp'
table = 'facebook_ads_campaign'

default_args = {
    'owner': 'debussy_framework_test',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

fields = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.clicks,
    AdsInsights.Field.impressions,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.conversions
]

parameters = {'level': 'campaign', 'limit': 1000, 'time_range': {'since': datetime(2020, 1, 26).strftime("%Y-%m-%d"),
                                                                 'until': datetime(2020, 2, 26).strftime("%Y-%m-%d")}}
rest_to_storage_task_id = 'facebook_ads_ad_api_to_gcs'

storage_hook = GCSHook(gcp_conn_id="google_cloud_default")
facebookads_hook = FacebookAdsHook(facebook_conn_id="facebook_growth", api_version='v13.0')


def exists_file_in_bucket(task_id, **context):
    """short circuit if there is no file in bucket"""
    exist_file = context['ti'].xcom_pull(task_ids=task_id)
    return exist_file


with models.DAG(
        dag_id="test_debussy_framework_facebook_campaigns",
        default_args=default_args,
        description="Exporting data from the facebook of campaigns",
        max_active_runs=5,
        schedule_interval=None,
        start_date=datetime(2019, 10, 8),
        catchup=True,
        tags=['debussy_framework', 'test dag'],

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
                "description": "The ID number of your ad account, which groups your advertising activity. Your ad account includes your campaigns, ads and billing."
            },
            {
                "name": "account_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The name of your ad account, which groups your advertising activity. Your ad account includes your campaigns, ads and billing."
            },
            {
                "name": "campaign_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The name of the ad campaign you're viewing in reporting. Your campaign contains ad sets and ads."
            },
            {
                "name": "campaign_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The unique ID number of the ad campaign you're viewing in reporting. Your campaign contains ad sets and ads."
            },
            {
                "name": "clicks",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The number of clicks on your ads."
            },
            {
                "name": "impressions",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "The number of times your ads were on screen."
            },
            {
                "name": "spend",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "The estimated total amount of money you've spent on your campaign, ad set or ad during its schedule. This metric is estimated."
            },
            {
                "name": "date_start",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "The start date for your data. This is controlled by the date range you've selected for your reporting view."
            },
            {
                "name": "date_stop",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "The end date for your data. This is controlled by the date range you've selected for your reporting view."
            },
            {
                "name": "conversions",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "The total number of conversions across all channels (i.e. website, mobile app, offline, and on-facebook) attributed to your ads."
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
        file_name=file_name
    )

    check_response_data = ShortCircuitOperator(
        task_id='check_exists_file',
        python_callable=exists_file_in_bucket,
        provide_context=True,
        op_kwargs={'task_id': rest_to_storage_task_id}
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=bucket,
        source_objects=[gcs_object_path],
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        field_delimiter=",",
        destination_project_dataset_table=f"{dataset}.{table}",
        write_disposition='WRITE_APPEND',
    )

    delay_task = BashOperator(
        task_id="delay_task",
        bash_command="sleep 10m",
        dag=dag
    )

    create_table >> run_build_csv >> check_response_data >> load_csv
