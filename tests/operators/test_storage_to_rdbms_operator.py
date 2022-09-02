import pandas as pd
from airflow import DAG
from debussy_airflow.hooks.db_api_hook import DbApiInterfaceHook, MySqlConnectorHook
from debussy_airflow.hooks.storage_hook import GCSHook
from debussy_airflow.operators.storage_to_rdbms_operator import StorageToRdbmsOperator
from MySQLdb._exceptions import OperationalError

from test_tools import TestExceptionOperator, TestHookOperator, test_dag


def test_query_run_empty_list(
    context, hook_dbapi: MySqlConnectorHook, sql: str, autocommit: bool
):

    assert (
        hook_dbapi.query_run(sql=sql, autocommit=autocommit) == None
        or len(hook_dbapi.query_run(sql=sql, autocommit=autocommit)) == 0
    )


def test_query_run_data_list(context, hook_dbapi: MySqlConnectorHook, sql, autocommit):
    assert len(hook_dbapi.query_run(sql=sql, autocommit=autocommit)) > 0
    assert "id_page" in hook_dbapi.query_run(sql=sql, autocommit=autocommit)
    assert "123456" in hook_dbapi.query_run(sql=sql, autocommit=autocommit)["id_lead"]


def test_build_query_operator(
    context, operator: StorageToRdbmsOperator, dataset_table: pd.DataFrame
):

    build_query = operator.build_insert_query(dataset_table=dataset_table)
    print(f"build_query: {build_query}")
    assert (
        "INSERT INTO teste.participant(id_page,id_lead,name_campain,created_at_lead,is_client_dotz,celular) VALUES "
        in build_query
    )


def get_id_page_incr(hook_dbapi: DbApiInterfaceHook):
    id_page_incr = hook_dbapi.query_run(
        sql="SELECT max(id_page)+1 FROM teste.participant", autocommit=True
    )
    print(f"VALOR ID {id_page_incr}")
    return id_page_incr


storage_hook = GCSHook(gcp_conn_id="google_cloud_default")
dbapi_hook = MySqlConnectorHook(rdbms_conn_id="db_teste_connection_id")


def get_pandas_dataset_participant_table(id_page_incr):
    dataset_table = pd.DataFrame(
        {
            "id_page": [f"{id_page_incr}"],
            "id_lead": ["123456"],
            "name_campain": ["nome da campanha"],
            "created_at_lead": ["2022-06-20"],
            "is_client_dotz": [True],
            "celular": [123456],
        }
    )

    return dataset_table


with test_dag(dag_id="test_debussy_framework_storage_to_rdbms") as dag:
    table_name = "teste.participant"

    test_empty_query_run_dbapi_hook = TestHookOperator(
        execute_fn=test_query_run_empty_list,
        fn_kwargs={
            "hook_dbapi": dbapi_hook,
            "sql": "SELECT * FROM teste.participant",
            "autocommit": True,
        },
        task_id="test_empty_query_run_dbapi_hook",
    )

    test_storage_to_rdbms_operator = StorageToRdbmsOperator(
        dbapi_hook=dbapi_hook,
        storage_hook=storage_hook,
        table_name=table_name,
        storage_file_uri="gs://dotz-datalake-dev-l3-reverse-etl/reverse_etl_unbounce/database_teste_participant_file.csv",
        task_id="test_debussy_framework_storage_to_rdbms_operator",
    )

    id_page_incr = get_id_page_incr(hook_dbapi=dbapi_hook)
    dataset_table = get_pandas_dataset_participant_table(id_page_incr=id_page_incr)
    test_build_query_storage_to_rdbms_operator = TestHookOperator(
        execute_fn=test_build_query_operator,
        fn_kwargs={
            "operator": test_storage_to_rdbms_operator,
            "dataset_table": dataset_table,
        },
        task_id="test_build_query_storage_to_rdbms_operator",
    )

    test_query_run_dbapi_hook = TestHookOperator(
        execute_fn=test_query_run_empty_list,
        fn_kwargs={
            "hook_dbapi": dbapi_hook,
            "sql": "SELECT * FROM teste.participant",
            "autocommit": True,
        },
        task_id="test_query_run_dbapi_hook",
    )

    test_sensor_exception = TestExceptionOperator(
        exception=OperationalError,
        operator=StorageToRdbmsOperator,
        op_kwargs={
            "storage_hook": storage_hook,
            "dbapi_hook": dbapi_hook,
            "table_name": table_name,
            "storage_file_uri": "gs://dotz-datalake-dev-l3-reverse-etl/reverse_etl_unbounce/database_teste_participant_bug_file.csv",
        },
    )
