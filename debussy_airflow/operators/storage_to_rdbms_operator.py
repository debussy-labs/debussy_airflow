import os

import pandas as pd
from airflow.models.baseoperator import BaseOperator
from debussy_airflow.hooks.db_api_hook import DbApiHookInterface
from debussy_airflow.hooks.storage_hook import StorageHookInterface


class StorageToRdbmsOperator(BaseOperator):
    """
    Download data storage and tranform in the query for to execute in rdbms
    """

    template_fields = ["storage_file_uri", "table_name"]

    def __init__(
        self,
        dbapi_hook: DbApiHookInterface,
        storage_hook: StorageHookInterface,
        table_name,
        storage_file_uri,
        temp_folder="/tmp",
        **kwargs,
    ):
        self.dbapi_hook = dbapi_hook
        self.storage_hook = storage_hook
        self.storage_file_uri = storage_file_uri
        self.table_name = table_name
        self.temp_folder = temp_folder
        super().__init__(**kwargs)

    def execute(self, context):
        tmp_filepath = os.path.join(
            self.temp_folder, self.storage_hook.basename(self.storage_file_uri)
        )
        self.storage_hook.download_file(self.storage_file_uri, tmp_filepath)

        self.log.info("StorageToRdbmsOperator - Retrieving data of storage")
        dataset_table = pd.read_csv(
            tmp_filepath,
            encoding="utf-8",
            delimiter=",",
            low_memory=False,
            keep_default_na=False,
            na_values=None,
        )
        self.log.info("StorageToRdbmsOperator - Building insert query")
        query = self.dbapi_hook.build_upsert_query(table_name=self.table_name, dataset_table=dataset_table)

        return self.dbapi_hook.query_run(sql=query, autocommit=True)