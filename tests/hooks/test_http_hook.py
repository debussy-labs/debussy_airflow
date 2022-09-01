import base64

from airflow import DAG
from debussy_airflow.hooks.http_hook import BearerHttpHook, HttpHook
from tests.test_tools import TestHookOperator, test_dag


def extract_echo_token(response):
    token = response.json()['args']['access_token']
    import logging
    logging.info(token)
    return token


conn_id = 'http_postman_echo'
http_hook = HttpHook(method='GET', http_conn_id=conn_id)
encoded_auth = base64.b64encode(b'postman:password').decode('ascii')
print(encoded_auth)
test_token = 'test_access_token'
bearer_hook = BearerHttpHook(
    method='GET',
    http_conn_id=conn_id,
    token_endpoint=f'/post?access_token={test_token}',
    extract_token_fn=extract_echo_token,
)


def assert_authentication(context, hook, endpoint, log):
    response = hook.run(endpoint=endpoint)
    log.info(response.text)
    assert response.json()['authenticated'] is True


def assert_http_hook(context, hook, endpoint, headers, data, log):
    response = hook.run(endpoint=endpoint, data=data, headers=headers)
    log.info(response.text)
    assert response.json()['args'] == {"name": "debussy"}
    log.info("OK - args test")
    assert response.json()['headers']['x-test'] == 'test_header'
    log.info("OK - x-test header test")
    assert response.json()[
        'headers']['authorization'] == f'Basic {encoded_auth}'
    log.info("OK - authorization header test")


def assert_bearer_hook(context, hook, endpoint, headers, data, log):
    response = hook.run(endpoint=endpoint, data=data, headers=headers)
    log.info(response.text)
    assert response.json()['args'] == {"name": "debussy"}
    log.info("OK - args test")
    assert response.json()['headers']['x-test'] == 'test_header'
    log.info("OK - x-test header test")
    assert response.json()[
        'headers']['authorization'] == f'Bearer {test_token}'
    log.info("OK - authorization header test")


with test_dag('test_debussy_http_dag') as dag:
    test_http_run = TestHookOperator(
        task_id='http_auth_test', execute_fn=assert_authentication)
    test_http_run.fn_kwargs = {'log': test_http_run.log,
                               'hook': http_hook, 'endpoint': '/basic-auth'}

    test_http_run = TestHookOperator(
        task_id='http_test', execute_fn=assert_http_hook)
    test_http_run.fn_kwargs = {'log': test_http_run.log, 'hook': http_hook, 'endpoint': '/get',
                               'headers': {'x-test': 'test_header'},
                               'data': {'name': 'debussy'}}

    test_http_run = TestHookOperator(
        task_id='bearer_test', execute_fn=assert_bearer_hook)
    test_http_run.fn_kwargs = {'log': test_http_run.log, 'hook': bearer_hook, 'endpoint': '/get',
                               'headers': {'x-test': 'test_header'},
                               'data': {'name': 'debussy'}}
