# DAG
import random
from debussy_airflow.operators.reschedulable_operator import (
    BaseReschedulableOperator,
    PokeReturnValue,
    PythonReschedulableOperator,
    AirflowSensorTimeout,
)
from tests.test_tools import (
    test_dag,
    TestReschedulableOperator,
    TestExceptionReschedulableOperator,
)


class DummyReschedulableOperator(BaseReschedulableOperator):
    def setup(self, context):
        print("setting up something very important")

    def poke(self, context) -> PokeReturnValue:
        rand_val = random.random()
        if rand_val < 0.5:
            print(f"Not done yet: {rand_val}")
            return False
        message = f"My very important return value: : {rand_val}"
        print(message)
        return PokeReturnValue(is_done=True, xcom_value=message)


with test_dag(dag_id="test_debussy_reschedulable_operator") as dag:
    dummy_op = DummyReschedulableOperator(
        task_id="DummyReschedulableOperator", poke_interval=10, timeout=260
    )
    test_op = TestReschedulableOperator(
        operator=DummyReschedulableOperator,
        op_kwargs={
            "timeout": 60,
            "poke_interval": 30,
        },
    )

    python_reschedulable = PythonReschedulableOperator(
        task_id="PythonReschedulableOperator",
        setup_callable=lambda: True,
        poke_callable=lambda: True,
        poke_interval=10,
    )

    python_timeout = TestExceptionReschedulableOperator(
        PythonReschedulableOperator,
        op_kwargs={
            "setup_callable": lambda: True,
            "poke_callable": lambda: False,
            "timeout": 90,
            "poke_interval": 30,
        },
        exception=AirflowSensorTimeout,
    )
