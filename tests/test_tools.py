from typing import Callable, Iterable, Type
import datetime as dt

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException, AirflowException
from airflow.models.baseoperator import BaseOperator


def test_dag(dag_id):
    """
    simple dag definition for testing.
    for more complex definition instantiate your dag
    """
    default_args = {
        "owner": "debussy_framework_test",
        "retires": 0,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="Test dag",
        schedule_interval="0 5 * * *",
        catchup=False,
        start_date=dt.datetime(2022, 1, 1),
        max_active_tasks=3,
        max_active_runs=1,
        tags=["debussy_framework", "test dag"],
    )
    return dag


class TestHook(BaseHook):
    def __init__(self, **kwargs) -> None:
        super().__init__()

    def set_method(self, name, function_mock: Callable):
        """
        set the method `name` to be `function_mock`
        this is intended to be used on testing to mock a response from a hook
        """
        self.__dict__[name] = function_mock
        return self


class TestHookOperator(BaseOperator):
    template_fields = ["fn_kwargs"]

    def __init__(
        self, execute_fn: Callable, fn_kwargs=None, task_id="test_hook", **kwargs
    ):
        self.execute_fn = execute_fn
        self.fn_kwargs = fn_kwargs or {}

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        self.execute_fn(context, **self.fn_kwargs)


class TestOperator(BaseOperator):
    def __init__(self, operator: BaseOperator, op_kwargs=None, task_id=None, **kwargs):
        self.op_kwargs = op_kwargs or {}
        self.operator = operator
        self.task_id = task_id or f"TestOp_{self.operator.__qualname__}"
        super().__init__(task_id=self.task_id, **kwargs)

    def execute(self, context):
        op_kwargs = {"task_id": f"{self.task_id}_{self.operator.__qualname__}"}
        op_kwargs.update(self.op_kwargs)
        task = self.operator(**op_kwargs, dag=self.dag)
        task.pre_execute(context)
        ret = task.execute(context)
        task.post_execute(context)
        if self.do_xcom_push:
            return ret


class TestConnectionsExistOperator(BaseOperator):
    def __init__(
        self, connections: Iterable[str], task_id="connections_test", **kwargs
    ):
        self.connections = connections
        self.kwargs = kwargs

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        failed_conn = []
        for connection_id in self.connections:
            try:
                BaseHook.get_connection(connection_id)
                self.log.info(f"OK - The conn_id `{connection_id}` is defined")
            except AirflowNotFoundException:
                self.log.info(f"FAILED - The conn_id `{connection_id}` isn't defined")
                failed_conn.append(connection_id)
        if failed_conn:
            raise AirflowNotFoundException(
                f"FAILED - The conn_ids `{', '.join(failed_conn)}` isn't defined"
            )
        self.log.info("OK - All connections exist")


# operador que teste uma falha (exception)
class TestExceptionOperator(BaseOperator):
    # template_fields = ['op_kwargs']

    def __init__(
        self,
        operator: Type[BaseOperator],
        op_kwargs=None,
        exception: Exception = Exception,
        task_id=None,
        **kwargs,
    ):
        self.exception = exception
        self.op_kwargs = op_kwargs or {}
        self.operator = operator
        self.task_id = task_id or f"TestExcOp_{self.operator.__qualname__}"
        self.__task = None
        super().__init__(task_id=self.task_id, **kwargs)

    @property
    def task(self):
        if self.__task is None:
            op_kwargs = {"task_id": f"{self.task_id}_{self.operator.__qualname__}"}
            op_kwargs.update(self.op_kwargs)
            self.__task = self.operator(**op_kwargs, dag=self.dag)
        return self.__task

    # pre and post execute raise errors that i dont understand atm
    # def pre_execute(self, context):
    #     self.task.pre_execute(context)

    # def post_execute(self, context, result=None):
    #     self.task.post_execute(context, result)

    def execute(self, context):

        exception_raised = None
        try:
            ret = None
            # self.task.pre_execute(context)
            ret = self.task.execute(context)
            # self.task.post_execute(context, ret)
            exception_raised = False
        except self.exception as exc:
            exception_raised = True
            self.log.info(f"OK - Exception raised: {exc}")
        except Exception as exc:
            raise AirflowException(f"FAILED - Wrong exception raised: {exc}")

        if not exception_raised:
            raise AirflowException("FAILED - Exception not raised")
