from uuid import uuid4

from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models.baseoperator import BaseOperator
from debussy_airflow.hooks.storage_hook import StorageHookInterface


class DatabaseToStorageOperator(BaseOperator):
    """
    Fetch data from database using `query` and saves on `bucket` `object_path` paths
    """

    template_fields = ("query", "object_path")

    def __init__(
        self,
        db_hook: DbApiHook,
        query,
        storage_hook: StorageHookInterface,
        bucket,
        object_path,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_hook = db_hook
        self.storage_hook = storage_hook
        self.query = query
        self.bucket = bucket
        if not object_path.endswith(".parquet"):
            object_path = object_path + ".parquet"
        self.object_path = object_path

    def execute(self, context):
        self.log.info("RdbmstoStorageOperator - Retrieving records")
        records_df = self.db_hook.get_pandas_df(self.query)
        filepath = f"/tmp/DatabaseToStorageOperator_{uuid4()}.parquet"
        records_df.to_parquet(filepath)
        self.log.info("RdbmstoStorageOperator - Uploading records")
        self.storage_hook.upload_file(self.bucket, self.object_path, filepath)
