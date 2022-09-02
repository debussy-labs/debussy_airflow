from airflow import DAG

from tests.test_tools import TestConnectionsExistOperator, test_dag

connections = ("google_cloud_debussy", "http_dataform")

with test_dag("test_conn_ids") as dag:
    test_conn = TestConnectionsExistOperator(connections=connections)
