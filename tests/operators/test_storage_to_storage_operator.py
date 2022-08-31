from airflow import DAG
from test_dags.test_tools import test_dag
from debussy_airflow.hooks.storage_hook import StorageHookInterface
from debussy_airflow.operators.storage_to_storage import StorageToStorageOperator


class StorageTestHook(StorageHookInterface):
    def upload_file(self, remote_file_uri, local_file_uri, **kwargs):
        assert remote_file_uri == "storage://destination_uri/file"
        assert local_file_uri == self.local_file_uri

    def upload_string(self, remote_file_uri, data_string, **kwargs):
        raise NotImplementedError()

    def download_file(self, remote_file_uri, local_file_uri, **kwargs):
        assert remote_file_uri == "storage://origin_uri/file"
        self.local_file_uri = local_file_uri

    def download_string(self, remote_file_uri, **kwargs):
        raise NotImplementedError()

    def list_dir(self, remote_uri):
        return ['file', 'not_this_file']

    def exists_file(self):
        raise NotImplementedError()


hook_test = StorageTestHook()


with test_dag(
    dag_id='test_debussy_framework_storage_to_storage'
) as dag:

    test_storage_to_storage_operator = StorageToStorageOperator(
        origin_storage_hook=hook_test, origin_file_uri="storage://origin_uri/file",
        destination_storage_hook=hook_test, destination_file_uri="storage://destination_uri/file",
        temp_folder='/tmp',
        task_id="test_file_storage_to_storage_operator"
    )
    test_storage_to_storage_operator = StorageToStorageOperator(
        origin_storage_hook=hook_test, origin_file_uri="storage://origin_uri/",
        destination_storage_hook=hook_test, destination_file_uri="storage://destination_uri",
        temp_folder='/tmp',
        task_id="test_folder_storage_to_storage_operator",
        is_dir=True,
        file_filter_fn=lambda context, filename: filename == 'file'
    )
