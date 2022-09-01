import MySQLdb as mysql
from typing import Dict

from abc import abstractmethod
from airflow.models import Connection
from airflow.hooks.dbapi_hook import DbApiHook as AirflowDbApiHook


class DbApiInterfaceHook(AirflowDbApiHook):
    @abstractmethod
    def query_run(self, sql: str, autocommit: bool):
        pass


class MySqlConnectorHook(DbApiInterfaceHook):
    def __init__(self, rdbms_conn_id: str = "rdbms_conn_id"):
        self.rdbms_conn_id = rdbms_conn_id
        self.conn_name_attr = rdbms_conn_id

    def _get_conn_config_mysql_client(self, conn: Connection) -> Dict:
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or "",
            "host": conn.host or "localhost",
            "db": conn.schema,
        }
        conn_config["port"] = int(conn.port) if conn.port else 3306
        return conn_config

    def get_conn(self):
        if self.rdbms_conn_id is None:
            raise ValueError("rdbms_conn_id is None")
        conn = self.get_connection(conn_id=self.rdbms_conn_id)
        conn_config = self._get_conn_config_mysql_client(conn)
        return mysql.connect(**conn_config)

    def query_run(self, sql: str, autocommit: bool):
        super().run(sql=sql, autocommit=autocommit)
