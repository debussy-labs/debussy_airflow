import os
from typing import Callable
from uuid import uuid4

from airflow.models.baseoperator import BaseOperator
from debussy_airflow.hooks.storage_hook import StorageHookInterface

airflow_context_dict_type = dict
tmp_file_uri_type = str


class StorageToStorageOperator(BaseOperator):
    """
    Download data from origin and upload to destination
    """

    template_fields = ("origin_file_uri", "destination_file_uri")

    def __init__(
        self,
        origin_storage_hook: StorageHookInterface,
        origin_file_uri,
        destination_storage_hook: StorageHookInterface,
        destination_file_uri,
        temp_folder="/tmp",
        is_dir=False,
        file_filter_fn: Callable[[airflow_context_dict_type, str], bool] = None,
        file_transformer_fn: Callable[[tmp_file_uri_type], tmp_file_uri_type] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.origin_storage_hook = origin_storage_hook
        self.destination_storage_hook = destination_storage_hook
        self.origin_file_uri = origin_file_uri
        self.destination_file_uri = destination_file_uri
        self.temp_folder = temp_folder
        self.is_dir = is_dir
        self.file_filter_fn = file_filter_fn or (lambda context, file_name: True)
        self.file_transformer_fn = file_transformer_fn or (
            lambda tmp_file_uri: tmp_file_uri
        )
        self._temp_folder = temp_folder

    def file_to_file(self, origin_file_uri, destination_file_uri):
        self.log.info(
            f"StorageToStorageOperator - Retrieving data from {origin_file_uri}"
        )
        tmp_filepath = os.path.join(
            self._temp_folder, self.origin_storage_hook.basename(origin_file_uri)
        )
        self.origin_storage_hook.download_file(origin_file_uri, tmp_filepath)
        transformed_file_uri = self.file_transformer_fn(tmp_filepath)
        self.log.info(
            f"StorageToStorageOperator - Uploading data to {destination_file_uri}"
        )
        self.destination_storage_hook.upload_file(
            destination_file_uri, transformed_file_uri
        )

    def folder_to_folder(self, context):
        for file_name in self.origin_storage_hook.list_dir(self.origin_file_uri):
            if self.file_filter_fn(context, file_name):
                origin = os.path.join(self.origin_file_uri, file_name)
                destination = os.path.join(self.destination_file_uri, file_name)
                self.file_to_file(origin, destination)

    def execute(self, context):
        uri_to_name = self.origin_file_uri.replace("/", "__").replace("\\", "__")
        random_suffix_attached = f"{uri_to_name}-{uuid4()}"
        temp_folder = os.path.join(self.temp_folder, random_suffix_attached)
        self._temp_folder = temp_folder
        os.mkdir(temp_folder)
        if self.is_dir:
            self.folder_to_folder(context)
        else:
            self.file_to_file(self.origin_file_uri, self.destination_file_uri)
