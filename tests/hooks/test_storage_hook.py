import os
from pathlib import Path
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from debussy_airflow.hooks.storage_hook import StorageHookInterface, GCSHook, SFTPHook, S3Hook
from test_dags.test_tools import TestHookOperator, test_dag


sftp_storage_hook = SFTPHook(sftp_conn_id="sftp_debussy_test")
gcs_storage_hook = GCSHook(gcp_conn_id='google_cloud_debussy')
s3_storage_hook = S3Hook(aws_conn_id='aws_noverde')

upload_string_text = "texto teste"


def test_upload_file(context, storage_hook: StorageHookInterface, remote_file_uri, local_file_uri):
    storage_hook.upload_file(remote_file_uri=remote_file_uri, local_file_uri=local_file_uri)


def test_upload_string(context, storage_hook: StorageHookInterface, remote_file_uri, data_string):
    storage_hook.upload_string(remote_file_uri=remote_file_uri, data_string=data_string)


def test_download_file(context, storage_hook: StorageHookInterface, remote_file_uri, local_file_uri):
    local_path = Path(local_file_uri)
    local_path.resolve().parent.mkdir(parents=True, exist_ok=True)
    storage_hook.download_file(remote_file_uri=remote_file_uri, local_file_uri=local_file_uri)
    assert os.path.exists(local_file_uri)


def test_download_string(context, storage_hook: StorageHookInterface, remote_file_uri):
    str_data = storage_hook.download_string(remote_file_uri=remote_file_uri)
    assert str_data == upload_string_text


def test_list_dir(context, storage_hook: StorageHookInterface, remote_uri):
    files = sorted(storage_hook.list_dir(remote_uri=remote_uri))
    expected_file = sorted([
        'test_upload_file.txt',
        'test_upload_string.txt'
    ])
    print(f"test_list_dir: dir files={files}")
    assert expected_file == files


def test_list_root_dir(context, storage_hook: StorageHookInterface, remote_uri):
    dir_files = storage_hook.list_dir(remote_uri=remote_uri)
    expected_folder = 'test_debussy_framework_storage'
    print(f"test_list_root_dir: dir_files={dir_files}")
    assert expected_folder in dir_files


def test_list_invalid_dir(context, storage_hook: StorageHookInterface, remote_uri):
    files = storage_hook.list_dir(remote_uri=remote_uri)
    expected_file = []
    print(f"test_list_dir: file={files}")
    assert expected_file == files


with test_dag(
    dag_id='test_debussy_framework_storage'
) as dag:

    ############
    # GCS
    task_group_gcs = TaskGroup('gcs_tests')
    test_upload_file_gcs_hook = TestHookOperator(
        execute_fn=test_upload_file,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_file_uri": "gs://autodelete_dev_bucket/test_debussy_framework_storage/test_upload_file.txt",
            "local_file_uri": f"{os.path.dirname(__file__)}/test_storage_file.txt"
        },
        task_id='upload_file',
        task_group=task_group_gcs
    )

    test_download_file_gcs_hook = TestHookOperator(
        execute_fn=test_download_file,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_file_uri": "gs://autodelete_dev_bucket/test_debussy_framework_storage/test_upload_file.txt",
            "local_file_uri": "/tmp/gcs_hook/test_storage_file.txt",
        },
        task_id='download_file',
        task_group=task_group_gcs
    )

    test_upload_string_gcs_hook = TestHookOperator(
        execute_fn=test_upload_string,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_file_uri": ("gs://autodelete_dev_bucket/test_debussy_framework_storage/test_upload_string.txt"),
            "data_string": upload_string_text
        },
        task_id='upload_string',
        task_group=task_group_gcs
    )

    test_download_string_gcs_hook = TestHookOperator(
        execute_fn=test_download_string,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_file_uri": "gs://autodelete_dev_bucket/test_debussy_framework_storage/test_upload_string.txt",
        },
        task_id='download_string',
        task_group=task_group_gcs
    )

    test_list_dir_gcs_hook = TestHookOperator(
        execute_fn=test_list_dir,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_uri": "gs://autodelete_dev_bucket/test_debussy_framework_storage/"
        },
        task_id='list_dir',
        task_group=task_group_gcs
    )

    test_list_root_dir_gcs_hook = TestHookOperator(
        execute_fn=test_list_root_dir,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_uri": "gs://autodelete_dev_bucket/"
        },
        task_id='list_root_dir',
        task_group=task_group_gcs
    )

    test_list_invalid_dir_gcs_hook = TestHookOperator(
        execute_fn=test_list_invalid_dir,
        fn_kwargs={
            "storage_hook": gcs_storage_hook,
            "remote_uri": "gs://autodelete_dev_bucket/invalid_folder/"
        },
        task_id='list_invalid_dir',
        task_group=task_group_gcs
    )

    test_upload_file_gcs_hook >> test_download_file_gcs_hook
    test_upload_string_gcs_hook >> test_download_string_gcs_hook
    test_upload_file_gcs_hook >> test_list_dir_gcs_hook
    test_upload_string_gcs_hook >> test_list_dir_gcs_hook
    test_upload_file_gcs_hook >> test_list_root_dir_gcs_hook
    test_upload_string_gcs_hook >> test_list_root_dir_gcs_hook
    ############
    # SFTP
    task_group_sftp = TaskGroup('sftp_tests')
    test_upload_file_sftp_hook = TestHookOperator(
        execute_fn=test_upload_file,
        fn_kwargs={
            "storage_hook": sftp_storage_hook,
            "local_file_uri": f"{os.path.dirname(__file__)}/test_storage_file.txt",
            "remote_file_uri": ""
        },
        task_id='upload_file',
        task_group=task_group_sftp
    )

    test_upload_string_sftp_hook = TestHookOperator(
        execute_fn=test_upload_string,
        fn_kwargs={
            "storage_hook": sftp_storage_hook,
            "data_string": upload_string_text,
            "remote_file_uri": "test_upload_string.txt"
        },
        task_id='upload_string',
        task_group=task_group_sftp
    )

    ############
    # S3
    task_group_s3 = TaskGroup('s3_tests')
    test_upload_file_s3_hook = TestHookOperator(
        execute_fn=test_upload_file,
        fn_kwargs={
            "storage_hook": s3_storage_hook,
            "remote_file_uri": "s3://dotz-integracao-stg/test_debussy_framework_storage/test_upload_file.txt",
            "local_file_uri": f"{os.path.dirname(__file__)}/test_storage_file.txt"
        },
        task_id='upload_file',
        task_group=task_group_s3
    )

    test_download_file_s3_hook = TestHookOperator(
        execute_fn=test_download_file,
        fn_kwargs={
            "storage_hook": s3_storage_hook,
            "remote_file_uri": "s3://dotz-integracao-stg/test_debussy_framework_storage/test_upload_file.txt",
            "local_file_uri": "/tmp/s3_hook/test_upload_file.txt",
        },
        task_id='download_file',
        task_group=task_group_s3
    )

    test_upload_string_s3_hook = TestHookOperator(
        execute_fn=test_upload_string,
        fn_kwargs={
            "storage_hook": s3_storage_hook,
            "remote_file_uri": ("s3://dotz-integracao-stg/test_debussy_framework_storage/test_upload_string.txt"),
            "data_string": upload_string_text
        },
        task_id='upload_string',
        task_group=task_group_s3
    )

    test_download_string_s3_hook = TestHookOperator(
        execute_fn=test_download_string,
        fn_kwargs={
            "storage_hook": s3_storage_hook,
            "remote_file_uri": "s3://dotz-integracao-stg/test_debussy_framework_storage/test_upload_string.txt",
        },
        task_id='download_string',
        task_group=task_group_s3
    )

    test_list_dir_s3_hook = TestHookOperator(
        execute_fn=test_list_dir,
        fn_kwargs={
            "storage_hook": s3_storage_hook,
            "remote_uri": "s3://dotz-integracao-stg/test_debussy_framework_storage"
        },
        task_id='list_dir',
        task_group=task_group_s3
    )

    test_upload_file_s3_hook >> test_download_file_s3_hook
    test_upload_string_s3_hook >> test_download_string_s3_hook
    test_upload_file_s3_hook >> test_list_dir_s3_hook
    test_upload_string_s3_hook >> test_list_dir_s3_hook
