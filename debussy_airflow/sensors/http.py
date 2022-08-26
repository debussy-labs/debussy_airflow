from functools import partial

from airflow.sensors.http_sensor import HttpSensor


class DebussyHttpSensor(HttpSensor):
    """
    Its like HttpSensor but receives a hook instance instead of a connection id
    and allow for templated kwargs on extra_options (using partial)
    """

    template_fields = ("op_kwargs",)

    def __init__(
        self,
        endpoint,
        http_hook,
        method="GET",
        request_params=None,
        headers=None,
        response_check=None,
        extra_options=None,
        op_kwargs=None,
        *args,
        **kwargs
    ):
        super().__init__(
            endpoint=endpoint,
            http_conn_id="http_default",  # passes default http but the hook will be replaced
            method=method,
            request_params=request_params,
            headers=headers,
            response_check=response_check,
            extra_options=extra_options,
            *args,
            **kwargs
        )
        self.op_kwargs = op_kwargs
        self.hook = http_hook

    def poke(self, context):
        self.response_check = partial(self.response_check, **self.op_kwargs)
        return super().poke(context)
