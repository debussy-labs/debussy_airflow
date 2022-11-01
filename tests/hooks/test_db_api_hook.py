from uuid import uuid4
from airflow import DAG

from debussy_airflow.hooks.db_api_hook import (
    PostgreSQLConnectorHook,
    DbApiHookInterface
)
from tests.test_tools import TestHookOperator, test_dag

pgsql_hook = PostgreSQLConnectorHook(rdbms_conn_id="connection_pgsql_dev")

def test_query_run_empty_list(context, pgsql_hook:DbApiHookInterface, dql_statement: str):

    assert (
        pgsql_hook.query_run(sql=dql_statement, autocommit=False) == None
        or len(pgsql_hook.query_run(sql=dql_statement, autocommit=False)) == 0
    )
    
with test_dag("test_debussy_pgsql_connector_dag") as dag:
    test_http_run = TestHookOperator(
        task_id="DQL_pgsql_test", execute_fn=test_query_run_empty_list
    )
    query = ("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE FROM information_schema.COLUMNS"
            f" WHERE TABLE_NAME = 'test_2' AND TABLE_SCHEMA = 'public'")
    insert_query = f"INSERT INTO public.test_2(guid, pid, storecode, cpf_cnpj, valor, status, obs) VALUES('{uuid4()}', 0, '', '', 0, '', '');"
    test_http_run.fn_kwargs = {
        "pgsql_hook": pgsql_hook,
        "dql_statement": insert_query#"SELECT guid, pid, storecode, cpf_cnpj, valor, status, obs FROM public.fila_bonificacao;",
    }