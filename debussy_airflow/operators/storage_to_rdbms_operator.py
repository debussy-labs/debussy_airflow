import os

import pandas as pd
from airflow.models.baseoperator import BaseOperator
from debussy_airflow.hooks.db_api_hook import DbApiInterfaceHook
from debussy_airflow.hooks.storage_hook import StorageHookInterface


class StorageToRdbmsOperator(BaseOperator):
    """
    Download data storage and tranform in the query for to execute in rdbms
    """

    template_fields = ["storage_file_uri", "table_name"]

    def __init__(
        self,
        dbapi_hook: DbApiInterfaceHook,
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

    def build_insert_query(self, dataset_table: pd.DataFrame):
        query_records = []
        escaped_columns = map(lambda c: f"`{c}`", dataset_table.columns)
        query = f"REPLACE INTO {self.table_name}({','.join(escaped_columns)}) VALUES "
        query_records = []
        for records in dataset_table.values.tolist():
            query_records.append(str(records).replace(
                "[", "(").replace("]", ")"))
        query_records = ",".join(query_records) + ";"
        query += query_records
        return query

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
        query = self.build_insert_query(dataset_table)

        return self.dbapi_hook.query_run(sql=query, autocommit=True)
