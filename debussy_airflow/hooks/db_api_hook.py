import pg8000 as pgsql
import MySQLdb as mysql
from typing import Dict
from pandas import DataFrame

from abc import abstractmethod
from airflow.models import Connection
from airflow.hooks.dbapi import DbApiHook


class DbApiHookInterface(DbApiHook):
    @abstractmethod
    def query_run(self, sql: str, autocommit: bool):
        pass

    @abstractmethod
    def build_upsert_query(self, table_name: str, dataset_table: DataFrame):
        pass


class MySqlConnectorHook(DbApiHookInterface):
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
    
    def build_upsert_query(self, table_name: str, dataset_table: DataFrame):
        if dataset_table.empty:
            return None
        
        escaped_columns = map(lambda c: f"`{c}`", dataset_table.columns)
        query = f"REPLACE INTO {table_name}({','.join(escaped_columns)}) VALUES "
        query_records = []
        for records in dataset_table.values.tolist():
            query_records.append(str(records).replace(
                "[", "(").replace("]", ")"))
        query_records = ",".join(query_records) + ";"
        query += query_records
        return query

    def query_run(self, sql: str, autocommit: bool):
        super().run(sql=sql, autocommit=autocommit)

class PostgreSQLConnectorHook(DbApiHookInterface):
    def __init__(self, rdbms_conn_id: str = "rdbms_conn_id"):
        self.rdbms_conn_id = rdbms_conn_id
        self.conn_name_attr = rdbms_conn_id

    def _build_insert_records(self, dataset_table_values: list):
        query_records = []
        for records in dataset_table_values:
            query_records.append(str(records).replace(
                "[", "(").replace("]", ")"))
        query_records = ",".join(query_records)
        return query_records

    def _build_update_records(self, columns: list):
        
        query_update_records = [f"{column}=excluded.{column}," for column in columns]
        query_update_records = f"{''.join(query_update_records)[:-1]};"
        return query_update_records

    def _get_conn_config_pgsql_client(self, conn: Connection) -> Dict:
        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
            "host": conn.host or "localhost",
            "database": conn.schema,
        }
        conn_config["port"] = int(conn.port) if conn.port else 5432
        return conn_config

    def _get_primary_key(self, table_name):
        pk_query=f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a 
            ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table_name}'::regclass
            AND i.indisprimary;
            """
        result = super().get_first(sql=pk_query)
        return result[0]

    def get_conn(self):
        if self.rdbms_conn_id is None:
            raise ValueError("rdbms_conn_id is None")
        conn = self.get_connection(conn_id=self.rdbms_conn_id)
        conn_config = self._get_conn_config_pgsql_client(conn)
        return pgsql.connect(**conn_config)

    """
        This build_upsert_query function use upsert feature statement for pgsql
    """
    def build_upsert_query(self, table_name: str, dataset_table: DataFrame):
        if dataset_table.empty:
            return None
        
        columns_name = ','.join(dataset_table.columns.tolist())
        query = f"INSERT INTO {table_name}({columns_name}) VALUES "
        dataset_table_records = dataset_table.values.tolist()        
        query += self._build_insert_records(dataset_table_records)
        query += f"\n ON CONFLICT ({self._get_primary_key(table_name)}) \n DO \n UPDATE SET {self._build_update_records(dataset_table.columns.tolist())}"
        return query

    def query_run(self, sql: str, autocommit: bool):
        super().run(sql=sql, autocommit=autocommit)