from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from tests.test_tools import test_dag, TestHook, TestExceptionOperator

hook = TestHook().set_method("print", lambda x: print(x))


with test_dag("test_debussy_tools_dag") as dag:
    dummy = DummyOperator(task_id="dummy_task")
    test_zero_division = TestExceptionOperator(
        task_id="raise_zero_division",
        operator=PythonOperator,
        op_kwargs={"python_callable": lambda: 1 / 0},
        exception=ZeroDivisionError,
    )

    test_test_exception_op = TestExceptionOperator(
        task_id="test_test_op",
        operator=TestExceptionOperator,
        op_kwargs={
            "task_id": "inner_test_op",
            "operator": PythonOperator,
            "op_kwargs": {"python_callable": lambda: 1 / 0, "task_id": "python_op"},
            "exception": RuntimeError,
        },
        exception=AirflowException,
    )
