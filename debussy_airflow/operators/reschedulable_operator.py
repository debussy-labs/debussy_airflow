import datetime
from datetime import timedelta
from typing import Any, Callable, Dict, Optional
import functools

from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
)
from airflow.models.taskreschedule import TaskReschedule
from airflow.utils import timezone
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.operator_helpers import determine_kwargs

try:
    from airflow.utils.context import Context
except ImportError:
    Context = Any
from airflow import settings


# from airflow 2.5 - backporting to 2.0.2
_MYSQL_TIMESTAMP_MAX = datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc)


# from airflow 2.5 - backporting to 2.0.2
@functools.lru_cache(maxsize=None)
def _is_metadatabase_mysql() -> bool:
    if settings.engine is None:
        raise AirflowException("Must initialize ORM first")
    return settings.engine.url.get_backend_name() == "mysql"


# from airflow 2.5 - backporting to 2.0.2
class PokeReturnValue:
    """
    Optional return value for poke methods.

    Sensors can optionally return an instance of the PokeReturnValue class in the poke method.
    If an XCom value is supplied when the sensor is done, then the XCom value will be
    pushed through the operator return value.
    :param is_done: Set to true to indicate the sensor can stop poking.
    :param xcom_value: An optional XCOM value to be returned by the operator.
    """

    def __init__(self, is_done: bool, xcom_value: Optional[Any] = None) -> None:
        self.xcom_value = xcom_value
        self.is_done = is_done

    def __bool__(self) -> bool:
        return self.is_done


class BaseReschedulableOperator(BaseSensorOperator):
    """
    Reschedulable operators are derived from this class and inherit these attributes.

    Reschedulable operators executes the setup step and the poke step keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on poke failure
    :param poke_interval: Time in seconds that the job should wait in
        between each try
    :param timeout: Time, in seconds before the task times out and fails.
    """

    ui_color: str = "#e6f1f2"

    def __init__(
        self,
        soft_fail=False,
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 8,
        **kwargs,
    ) -> None:
        if "mode" in kwargs.keys():
            raise ValueError(
                "mode arg is not allowed on ReschedulableOperator. It is always reschedule mode."
            )
        if "exponential_backoff" in kwargs.keys() or "max_wait" in kwargs.keys():
            self.log.warn(
                "exponential_backoff and/or max_wait has no effect and are ignored"
            )
        kwargs["mode"] = "reschedule"
        super().__init__(
            soft_fail=soft_fail, poke_interval=poke_interval, timeout=timeout, **kwargs
        )

    def setup(self, context: Context) -> Any:
        """Function defined by the ReschedulableOperator while deriving this class should override."""
        raise AirflowException("Override me.")

    def execute(self, context: Context) -> Any:
        started_at: datetime.datetime | float

        if not self.reschedule:
            raise RuntimeError(
                "ReschedulableOperator in a invalid state. It must be on reschedule mode"
            )
        # If reschedule, use first start date of current try
        task_reschedules = TaskReschedule.find_for_task_instance(context["ti"])
        if task_reschedules:
            started_at = task_reschedules[0].start_date
        else:
            started_at = timezone.utcnow()

        do_poke = bool(task_reschedules)
        do_setup = not do_poke

        # if it is the first time, run setup function
        if do_setup:
            setup_return = self.setup(context)
            # do not add the setup time into duration calculation
            start_date = timezone.utcnow()
            if setup_return:
                self.log.info(f"setup_function return value [{setup_return}]")
                self.log.warn("setup function return value are logged and ignored.")
        # else, run the poke function
        else:
            start_date = task_reschedules[0].start_date
            poke_return = self.poke(context)
            xcom_value = None
            if poke_return:
                if isinstance(poke_return, PokeReturnValue):
                    xcom_value = poke_return.xcom_value
                self.log.info("Success criteria met. Exiting.")
                return xcom_value
        started_at = start_date

        def run_duration() -> float:
            # We are in reschedule mode, then we have to compute diff
            # based on the time in a DB, so can't use time.monotonic
            return (timezone.utcnow() - start_date).total_seconds()

        log_dag_id = self.dag.dag_id if self.has_dag() else ""

        if do_poke and run_duration() > self.timeout:
            # If sensor is in soft fail mode but will be retried then
            # give it a chance and fail with timeout.
            # This gives the ability to set up non-blocking AND soft-fail sensors.
            message = (
                f"Sensor has timed out; run duration of {run_duration()} seconds exceeds "
                f"the specified timeout of {self.timeout}."
            )
            if self.soft_fail and not context["ti"].is_eligible_to_retry():
                raise AirflowSkipException(message)
            else:
                raise AirflowSensorTimeout(message)

        next_poke_interval = self._get_next_poke_interval(started_at, run_duration, 1)
        reschedule_date = timezone.utcnow() + timedelta(seconds=next_poke_interval)
        if _is_metadatabase_mysql() and reschedule_date > _MYSQL_TIMESTAMP_MAX:
            raise AirflowSensorTimeout(
                f"Cannot reschedule DAG {log_dag_id} to {reschedule_date.isoformat()} "
                f"since it is over MySQL's TIMESTAMP storage limit."
            )
        raise AirflowRescheduleException(reschedule_date)


class PythonReschedulableOperator(BaseReschedulableOperator):
    template_fields = ("setup_kwargs", "poke_kwargs", "templates_dict")

    def __init__(
        self,
        setup_callable: Callable,
        poke_callable: Callable,
        soft_fail=False,
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 8,
        setup_kwargs: Optional[Dict] = None,
        poke_kwargs: Optional[Dict] = None,
        templates_dict: Optional[Dict] = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(
            soft_fail=soft_fail,
            poke_interval=poke_interval,
            timeout=timeout,
            **kwargs,
        )
        self.setup_callable = setup_callable
        self.setup_kwargs = setup_kwargs or {}
        self.poke_callable = poke_callable
        self.poke_kwargs = poke_kwargs or {}
        self.templates_dict = templates_dict
        self.show_return_value_in_logs = show_return_value_in_logs
        # not supporting v_args for now
        self.op_args = []

    def setup(self, context):
        context.update(self.setup_kwargs)
        context["templates_dict"] = self.templates_dict
        self.setup_kwargs = determine_kwargs(self.setup_callable, self.op_args, context)

        self.log.info("Setup callable: %s", str(self.setup_callable))
        setup_ret = self.setup_callable(*self.op_args, **self.poke_kwargs)
        if self.show_return_value_in_logs:
            self.log.info("Setup Done. Returned value was: %s", setup_ret)
        else:
            self.log.info("Setup Done. Returned value not shown")
        return setup_ret

    def poke(self, context):
        context.update(self.poke_kwargs)
        context["templates_dict"] = self.templates_dict
        self.poke_kwargs = determine_kwargs(self.poke_callable, self.op_args, context)

        self.log.info("Poking callable: %s", str(self.poke_callable))
        poke_ret = self.poke_callable(*self.op_args, **self.poke_kwargs)

        if self.show_return_value_in_logs:
            self.log.info("Poke Done. Returned value was: %s", poke_ret)
        else:
            self.log.info("Poke Done. Returned value not shown")
        return poke_ret
