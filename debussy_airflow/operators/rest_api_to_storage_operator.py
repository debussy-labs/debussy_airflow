from typing import Callable, List

import requests
from airflow.hooks.http_hook import HttpHook as AirflowHttpHook
from airflow.models import BaseOperator
from debussy_airflow.hooks.http_hook import HttpHook as DebussyHttpHook
from debussy_airflow.hooks.storage_hook import StorageHookInterface


class RestApiToStorageOperator(BaseOperator):
    template_fields = (
        "httphook_kwargs",
        "raw_object_path",
        "object_path",
        "endpoint",
        "httphook_kwargs",
    )

    def __init__(
        self,
        http_hook: "HttpHook",
        endpoint: str,
        storage_hook: StorageHookInterface,
        bucket,
        object_path,
        response_transformer_callable: Callable = None,
        flag_save_raw_response: bool = False,
        raw_bucket=None,
        raw_object_path=None,
        httphook_kwargs=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.http_hook = http_hook
        self.endpoint = endpoint
        self.transformer = response_transformer_callable or self.response_transformer
        self.storage_hook = storage_hook
        self.bucket = bucket
        self.object_path = object_path
        self.flag_save_raw_response = flag_save_raw_response
        self.raw_bucket = raw_bucket
        self.raw_object_path = raw_object_path
        self.validate_params()
        self.raw_object_path = raw_object_path or object_path
        self.raw_bucket = raw_bucket or bucket
        self.httphook_kwargs = httphook_kwargs

    def validate_params(self):
        if not (
            isinstance(self.http_hook, AirflowHttpHook)
            or isinstance(self.http_hook, DebussyHttpHook)
        ):
            raise TypeError("http_hook must be an instance of HttpHook")
        if not isinstance(self.storage_hook, StorageHookInterface):
            raise TypeError(
                "storage_hook must be an instance of StorageHookInterface")
        if (
            self.flag_save_raw_response
            and self.raw_object_path is None
            and self.raw_bucket is None
        ):
            raise ValueError(
                "RestApiToStorageOperator: raw_object_path or raw_bucket "
                "must be set when flag_save_raw_response is True"
            )

    @staticmethod
    def response_transformer(response: requests.Response, **context) -> str:
        return response.text

    def upload_from_string(self, bucket, object_path, data_string):
        self.log.info(f"Uploading data to gs://{bucket}/{object_path}")
        self.storage_hook.upload_string(
            remote_file_uri=f"gs://{bucket}/{object_path}", data_string=data_string
        )

    def api_to_storage(
        self, endpoint, object_path, raw_object_path, httphook_kwargs, context
    ):
        httphook_kwargs = httphook_kwargs or {}
        response = self.http_hook.run(endpoint, **httphook_kwargs)
        if self.flag_save_raw_response:
            self.upload_from_string(
                self.raw_bucket, raw_object_path, response.text)
        data_string = self.transformer(response, **context)
        self.upload_from_string(self.bucket, object_path, data_string)
        return len(data_string)

    def execute(self, context):
        return self.api_to_storage(
            self.endpoint,
            self.object_path,
            self.raw_object_path,
            self.httphook_kwargs,
            context,
        )


class RestListApiToStorageOperator(RestApiToStorageOperator):
    template_fields = (
        "httphook_kwargs",
        "raw_object_paths",
        "object_paths",
        "endpoints",
    )

    def __init__(
        self,
        http_hook: "HttpHook",
        endpoints: List[str],
        storage_hook: StorageHookInterface,
        bucket,
        object_paths: List[str],
        response_transformer_callable: Callable = None,
        flag_save_raw_response: bool = False,
        raw_bucket=None,
        raw_object_paths: List = None,
        httphook_kwargs: List = None,
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        self.http_hook = http_hook
        self.endpoints = endpoints
        self.transformer = response_transformer_callable or self.response_transformer
        self.storage_hook = storage_hook
        self.bucket = bucket
        self.object_paths = object_paths
        self.flag_save_raw_response = flag_save_raw_response
        self.raw_bucket = raw_bucket
        self.raw_object_paths = raw_object_paths
        # NILTON AJUSTAR self.validate_params()
        self.raw_object_paths = raw_object_paths or object_paths
        self.raw_bucket = raw_bucket or bucket
        self.httphook_kwargs = httphook_kwargs

    def execute(self, context):
        for endpoint, object_path, raw_object_path, httphook_kwargs in zip(
            self.endpoints,
            self.object_paths,
            self.raw_object_paths,
            self.httphook_kwargs,
        ):
            self.api_to_storage(
                endpoint, object_path, raw_object_path, httphook_kwargs, context
            )
