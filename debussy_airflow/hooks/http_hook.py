import functools
from typing import Any, Callable, Dict, Optional, Union

import tenacity
import requests
from requests import Response
from requests.auth import AuthBase, HTTPBasicAuth
from requests.models import PreparedRequest
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class OAuth(AuthBase):
    def __init__(self, username, password, scope, grant_type, content_type):
        self.username = username
        self.password = password
        self.scope = scope
        self.grant_type = grant_type
        self.content_type = content_type

    def prepare_body(self):
        body_data = {
            "username": self.username,
            "password": self.password,
            "scope": self.scope,
            "grant_type": self.grant_type,
        }
        return body_data

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Content-Type"] = self.content_type
        data = self.prepare_body()
        r.body = r._encode_params(data)
        return r


class BearerAuth(AuthBase):
    def __init__(self, get_token: Callable) -> None:
        self.get_token = get_token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Authorization"] = f"Bearer {self.get_token()}"
        return r


def bearer_factory(get_token: Callable, login, password):
    # bearer dont use login/password
    # but the http hook will pass it to instantiate the class
    # so we will use a functools partial to create a factory
    # this factory knows the get_token callable to instantiate the bearer auth
    # and will ignore the login adn password
    #
    return BearerAuth(get_token)


class HttpHook(BaseHook):
    """
    Interact with HTTP servers.
    :param method: the API method to be called
    :type method: str
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :type http_conn_id: str
    :param auth_type: The auth type for the service
    :type auth_type: AuthBase of python requests lib
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = HTTPBasicAuth,
    ) -> None:
        super().__init__("")
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Any = auth_type

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers: Optional[Dict[Any, Any]] = None) -> requests.Session:
        """
        Returns http session for use with requests
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        session = requests.Session()

        if self.http_conn_id:
            conn = self.get_connection(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.login:
                session.auth = self.auth_type(conn.login, conn.password)
            if conn.extra:
                try:
                    session.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning(
                        "Connection to %s has invalid extra field.", conn.host
                    )
        if headers:
            session.headers.update(headers)

        return session

    def run(
        self,
        headers: Optional[Dict] = None,
        endpoint: Optional[str] = None,
        json: Optional[Dict] = None,
        data: Optional[Union[Dict[str, Any], str]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""
        Performs the request
        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :type extra_options: dict
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        if (
            self.base_url
            and not self.base_url.endswith("/")
            and endpoint
            and not endpoint.startswith("/")
        ):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")

        if self.method == "GET":
            # GET uses params
            req = requests.Request(
                self.method, url, params=data, headers=headers, **request_kwargs
            )
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(
                self.method,
                url,
                data=data,
                headers=headers,
                json=json,
                **request_kwargs,
            )

        prepped_request = session.prepare_request(req)
        self.log.info("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response: requests.Response) -> None:
        """
        Checks the status code and raise an AirflowException exception on non 2XX or 3XX
        status codes
        :param response: A requests response object
        :type response: requests.response
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: Dict[Any, Any],
    ) -> Any:
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result
        :param session: the session to be used to execute the request
        :type session: requests.Session
        :param prepped_request: the prepared request generated in run()
        :type prepped_request: session.prepare_request
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        :type extra_options: dict
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get("check_response", True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warning("%s Tenacity will retry to execute the operation", ex)
            raise ex

    def run_with_advanced_retry(
        self, _retry_args: Dict[Any, Any], *args: Any, **kwargs: Any
    ) -> Any:
        """
        Runs Hook.run() with a Tenacity decorator attached to it. This is useful for
        connectors which might be disturbed by intermittent issues and should not
        instantly fail.
        :param _retry_args: Arguments which define the retry behavior.
            See Tenacity documentation at https://github.com/jd/tenacity
        :type _retry_args: dict
        .. code-block:: python
            hook = HttpHook(http_conn_id="my_conn", method="GET")
            retry_args = dict(
                wait=tenacity.wait_exponential(),
                stop=tenacity.stop_after_attempt(10),
                retry=requests.exceptions.ConnectionError,
            )
            hook.run_with_advanced_retry(endpoint="v1/test", _retry_args=retry_args)
        """
        self._retry_obj = tenacity.Retrying(**_retry_args)

        return self._retry_obj(self.run, *args, **kwargs)

    def test_connection(self):
        """Test HTTP Connection"""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:  # noqa pylint: disable=broad-except
            return False, str(e)


class BearerHttpHook(HttpHook):
    def __init__(
        self,
        *,
        token_endpoint,
        method="GET",
        http_conn_id="http_default",
        token_auth_type: AuthBase = HTTPBasicAuth,
        extract_token_fn: Callable[[Response], str] = None,
    ):
        self.method = method
        self.bearer_http_conn_id = http_conn_id
        self.token_endpoint = token_endpoint
        self.token_auth_type = token_auth_type
        self.extract_token_fn = extract_token_fn or self.extract_token
        self.__token = None
        bearer_auth_factory = functools.partial(bearer_factory, self.get_token)
        super().__init__(
            method=method, http_conn_id=http_conn_id, auth_type=bearer_auth_factory
        )

    @staticmethod
    def extract_token(response: Response):
        return response.json()["access_token"]

    def get_token(self) -> str:
        """
        Get API Access Token
        """
        if self.__token:
            return self.__token

        self.log.info("Getting API Token.")
        response = HttpHook(
            method="POST",
            http_conn_id=self.bearer_http_conn_id,
            auth_type=self.token_auth_type,
        ).run(endpoint=self.token_endpoint)
        self.__token = self.extract_token_fn(response)
        return self.__token

    def run(
        self,
        endpoint=None,
        data=None,
        headers=None,
        extra_options=None,
        **request_kwargs,
    ):
        return super().run(
            endpoint=endpoint,
            data=data,
            headers=headers,
            extra_options=extra_options,
            **request_kwargs,
        )
