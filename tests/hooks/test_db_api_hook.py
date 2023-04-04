# DAG
import logging
from uuid import uuid4


from debussy_airflow.hooks.db_api_hook import (
    PostgreSQLConnectorHook,
    DbApiHookInterface,
)
from tests.test_tools import TestHookOperator, test_dag

pgsql_hook = PostgreSQLConnectorHook(rdbms_conn_id="connection_pgsql_dev")


def test_exec_query(context, pgsql_hook: DbApiHookInterface, dql_statement: str):

    assert (
        pgsql_hook.query_run(sql=dql_statement, autocommit=False) == None
        or len(pgsql_hook.query_run(sql=dql_statement, autocommit=False)) == 0
    )


def test_get_first_record(context, pgsql_hook: DbApiHookInterface, dql_statement: str):
    first_record = pgsql_hook.get_first(sql=dql_statement)
    logging.info(f"valor: {first_record[0]}")
    logging.info(f"tipo: {type(first_record[0])}")
    assert first_record[0] != None


with test_dag("test_debussy_pgsql_connector_dag") as dag:
    test_insert_query_run = TestHookOperator(
        task_id="test_insert_query_run", execute_fn=test_exec_query
    )
    pk = uuid4()
    insert_query = f"INSERT INTO public.test_2(guid, pid, storecode, cpf_cnpj, valor, status, obs) VALUES('{pk}', 0, '', '', 0, '', '');"
    test_insert_query_run.fn_kwargs = {
        "pgsql_hook": pgsql_hook,
        "dql_statement": insert_query,
    }

    test_update_query_run = TestHookOperator(
        task_id="test_update_query_run", execute_fn=test_exec_query
    )

    update_query = f"UPDATE public.test_2 SET obs = 'updated field' WHERE guid='{pk}'"
    test_update_query_run.fn_kwargs = {
        "pgsql_hook": pgsql_hook,
        "dql_statement": update_query,
    }

    test_first_record = TestHookOperator(
        task_id="test_first_record", execute_fn=test_get_first_record
    )

    get_first_record_query = f"SELECT * FROM public.test_2"
    test_first_record.fn_kwargs = {
        "pgsql_hook": pgsql_hook,
        "dql_statement": get_first_record_query,
    }
