from abc import ABC, abstractmethod
from typing import Callable, List

import requests
from debussy_airflow.hooks.http_hook import HttpHook


class PaginatedApiHook(HttpHook, ABC):
    def __init__(self, method, http_conn_id, responses_handler_callable: Callable):
        super().__init__(method, http_conn_id)
        self.responses_handler_callable = responses_handler_callable

    @abstractmethod
    def request_pages(
        self, endpoint, data=None, headers=None, extra_options=None, **request_kwargs
    ):
        pass

    @staticmethod
    def responses_handler(responses: List[requests.Response]) -> requests.Response:
        """responses_handler_callable signature"""
        pass

    def run(
        self, endpoint, data=None, headers=None, extra_options=None, **request_kwargs
    ) -> requests.Response:
        responses = []
        for response_page in self.request_pages(
            endpoint, data, headers, extra_options, **request_kwargs
        ):
            responses.append(response_page)
        response = self.responses_handler_callable(responses)
        return response

    def fetch_page(
        self, endpoint, data=None, headers=None, extra_options=None, **request_kwargs
    ) -> requests.Response:
        return super().run(
            endpoint,
            data=data,
            headers=headers,
            extra_options=extra_options,
            **request_kwargs,
        )


class LinkPaginatedApiHook(PaginatedApiHook):
    def request_pages(
        self, endpoint, data=None, headers=None, extra_options=None, **request_kwargs
    ):
        link_next = {"url": endpoint}
        while link_next:
            # hook already have the base url and the link will double it if we do not remove
            base_url = self.base_url or ""
            endpoint = link_next["url"].replace(base_url, "")
            self.log.info(f"Fetching page from {endpoint}.")

            response = super().fetch_page(
                endpoint, data, headers, extra_options, **request_kwargs
            )
            # we dont need data querystring anymore as the next link already have it
            data = None
            link_next = response.links.get("next")
            yield response
