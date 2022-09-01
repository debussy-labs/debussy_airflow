
from airflow import DAG
from debussy_airflow.hooks.storage_hook import GCSHook
from debussy_airflow.operators.db_to_storage_operator import DatabaseToStorageOperator

from tests.test_tools import TestHookOperator, test_dag

gcs_hook = GCSHook()

bucket = 'gs://dotz-datalake-prod-artifacts/'
object_path = ''
data_string = 'test_string'


def assert_download_string(context):
    assert gcs_hook.download_string(bucket, object_path) == data_string


with test_dag(
    dag_id='test_debussy_storage_hook'
) as dag:

    storage_to_bt = DatabaseToStorageOperator(
        task_id='test_data_base_storage',
        db_hook="",
        query="",
        storage_hook=gcs_hook, bucket='', object_path='',
    )
    test_upload_task = TestHookOperator(
        task_id='test_upload_string',
        execute_fn=lambda bucket, object_path, data_string: gcs_hook.upload_string(
            bucket, object_path, data_string),
    )

    '''
    test_download_task = TestHookOperator(
        task_id='test_download_string',
        execute_fn=assert_download_string,
    )
    '''
