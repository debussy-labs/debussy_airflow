
from airflow import DAG
from debussy_airflow.hooks.http import HttpHook
from debussy_airflow.operators.http import HTTPOperator
from debussy_airflow.sensors.http import DebussyHttpSensor
from test_dags.test_tools import test_dag


def check_response(response, *, expected_json_content):
    response_json = response.json()
    assert response_json == expected_json_content
    return True


conn_id = 'http_viacep'

expected_json_content = {
    "cep": "01001-000",
    "logradouro": "Praça da Sé",
    "complemento": "lado ímpar",
    "bairro": "Sé",
    "localidade": "São Paulo",
    "uf": "SP",
    "ibge": "3550308",
    "gia": "1004",
    "ddd": "11",
    "siafi": "7107"
}


http_hook = HttpHook(
    method='GET',
    http_conn_id=conn_id
)

with test_dag('test_debussy_framework_http') as dag:

    headers = {'Content-Type': 'application/json'}
    http_run = HTTPOperator(
        task_id='create_run',
        http_hook=http_hook,
        endpoint='01001000/json',
        headers=headers
    )

    sensor = DebussyHttpSensor(
        task_id='run_sensor',
        endpoint='01001000/json',
        http_hook=http_hook,
        headers=headers,
        response_check=check_response,
        op_kwargs={'expected_json_content': expected_json_content}
    )

    http_run >> sensor
