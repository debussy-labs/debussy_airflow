from airflow import DAG
from debussy_airflow.hooks.http_api import LinkPaginatedApiHook
from tests.test_tools import test_dag, TestHookOperator


def responses_handler(responses):
    pages = []
    for response in responses:
        page = response.json()['page']
        pages.append(page)
    return pages


conn_id = 'http_postman_echo'
link_paginated_hook = LinkPaginatedApiHook(method='GET', http_conn_id=conn_id,
                                           responses_handler_callable=responses_handler)


def assert_link_hook(context, hook, endpoint, data, log):
    response = hook.run(endpoint=endpoint, data=data)
    log.info(response)
    assert response == ['1', '2']


with test_dag('test_debussy_api_dag') as dag:
    test_link_hook = TestHookOperator(task_id='link_test', execute_fn=assert_link_hook)
    test_link_hook.fn_kwargs = {
        'log': test_link_hook.log, 'hook': link_paginated_hook, 'endpoint': '/response-headers',
        'data': {"link": '<https://postman-echo.com/response-headers?page=2>; rel="next"', 'page': '1'}
    }
