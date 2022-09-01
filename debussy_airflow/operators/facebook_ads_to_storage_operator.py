import os

from airflow.models.baseoperator import BaseOperator
from debussy_airflow.hooks.facebook_ads_hook import FacebookAdsHook
from debussy_airflow.hooks.storage_hook import StorageHookInterface


class FacebookAdsToStorageOperator(BaseOperator):
    """
    Download data from facebook ads and upload to storage
    """

    template_fields = ["file_name", "parameters", "bucket_file_path"]

    def __init__(
        self,
        facebookads_hook: FacebookAdsHook,
        storage_hook: StorageHookInterface,
        parameters,
        fields: list,
        bucket_file_path,
        file_name,
        temp_folder="/tmp",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.facebookads_hook = facebookads_hook
        self.storage_hook = storage_hook
        self.parameters = parameters
        self.fields = (fields,)
        self.bucket_file_path = bucket_file_path
        self.file_name = file_name
        self.temp_folder = temp_folder

    def exists_file_tmp(self, file_name, temp_folder="/tmp", **kwargs):
        return os.path.exists(f"{temp_folder}/{file_name}")

    def remove_file_tmp(self, file_name, temp_folder="/tmp", **kwargs):
        os.remove(f"{temp_folder}/{file_name}")

    def execute(self, context):
        self.log.info("FacebookAdsToStorageOperator - Retrieving data")
        if self.exists_file_tmp(file_name=self.file_name):
            self.remove_file_tmp(file_name=self.file_name)

        tmp_filepath = f"{self.temp_folder}/{self.file_name}"
        self.facebookads_hook.build_file(
            tmp_filepath=tmp_filepath, parameters=self.parameters, fields=self.fields
        )
        self.log.info("FacebookAdsToStorageOperator - Uploading data")
        exists_file = False
        if self.exists_file_tmp(file_name=self.file_name):
            self.storage_hook.upload_file(self.bucket_file_path, tmp_filepath)
            exists_file = self.storage_hook.exists_file(
                file_path=self.bucket_file_path)
        return exists_file
