import pandas as pd
from airflow import DAG
from debussy_airflow.hooks.db_api_hook import DbApiHookInterface, MySqlConnectorHook, PostgreSQLConnectorHook
from debussy_airflow.hooks.storage_hook import GCSHook
from debussy_airflow.operators.storage_to_rdbms_operator import StorageToRdbmsOperator

from tests.test_tools import TestHookOperator, test_dag


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
    context, hook_dbapi: DbApiHookInterface, dataset_table: pd.DataFrame, table_name: str
):

    build_query = hook_dbapi.build_upsert_query(table_name=table_name, dataset_table=dataset_table)
    print(f"build_query: {build_query}")
    assert ("REPLACE INTO public.test(`id_page`,`id_lead`,`name_campain`,`created_at_lead`,`is_client_dotz`,`celular`) VALUES " in build_query)


def get_id_page_incr(hook_dbapi: DbApiHookInterface):
    id_page_incr = hook_dbapi.query_run(
        sql="SELECT max(id_page)+1 FROM public.test", autocommit=True
    )
    print(f"VALOR ID {id_page_incr}")
    return id_page_incr


storage_hook = GCSHook(gcp_conn_id="google_cloud_default")
mysql_hook = MySqlConnectorHook(rdbms_conn_id="connection_mysql_dev")

pgsql_hook = PostgreSQLConnectorHook(rdbms_conn_id="connection_pgsql_dev")


def get_pandas_dataset_table_test(id_page_incr):
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
    table_name = "public.test"

    test_empty_query_run_dbapi_hook = TestHookOperator(
        execute_fn=test_query_run_empty_list,
        fn_kwargs={
            "hook_dbapi": mysql_hook,
            "sql": "SELECT * FROM public.test",
            "autocommit": True,
        },
        task_id="test_empty_query_run_mysql_hook",
    )

    test_storage_to_rdbms_operator = StorageToRdbmsOperator(
        dbapi_hook=mysql_hook,
        storage_hook=storage_hook,
        table_name=table_name,
        storage_file_uri="gs://dotz-datalake-dev-public/tests/mysql_table_test.csv",
        task_id="test_storage_to_mysql_operator",
    )

    dataset_table = get_pandas_dataset_table_test(id_page_incr=get_id_page_incr(hook_dbapi=mysql_hook))
    
    test_build_query_storage_to_rdbms_operator = TestHookOperator(
        execute_fn=test_build_query_operator,
        fn_kwargs={
            "hook_dbapi": mysql_hook,
            "dataset_table": dataset_table,
            "table_name": table_name
        },
        task_id="test_build_query_storage_to_mysql_operator",
    )

    test_query_run_dbapi_hook = TestHookOperator(
        execute_fn=test_query_run_empty_list,
        fn_kwargs={
            "hook_dbapi": mysql_hook,
            "sql": "SELECT * FROM public.test",
            "autocommit": True,
        },
        task_id="test_query_run_mysql_dbapi_hook",
    )

    table_name="public.test"
    test_storage_to_rdbms_operator = StorageToRdbmsOperator(
        dbapi_hook=pgsql_hook,
        storage_hook=storage_hook,
        table_name=table_name,
        storage_file_uri="gs://dotz-datalake-dev-public/tests/pgsql_table_test.csv",
        task_id="test_storage_to_pgsql_operator",
    )