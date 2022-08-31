from airflow import DAG
import datetime as dt
from debussy_airflow.operators.basic import BasicOperator, StartOperator, FinishOperator
from tests.test_tools import test_dag

with test_dag(
    dag_id='test_debussy_framework_basic_operators'
) as dag:

    test_basic_perator = BasicOperator(
        phase="dag", step="begin_and_finish"
    )
    test_basic_start_operator = StartOperator(
        phase="dag"
    )

    test_basic_finish_operator = FinishOperator(
        phase="dag"
    )

    test_basic_start_operator_inner = StartOperator(
        phase="start_inner_dag"
    )

    test_basic_finish_operator_inner = FinishOperator(
        phase="finish_inner_dag"
    )

    test_basic_start_operator_inner >> test_basic_finish_operator_inner
