from urllib.parse import urlparse

import aiohttp
from ckanapi.common import ActionShortcut, prepare_action, reverse_apicontroller_action
from ckanapi.errors import CKANAPIError
from ckanapi.version import __version__


class AsyncRemoteCKAN:
    base_url = "api/action/"

    def __init__(
        self, address, apikey=None, user_agent=None, get_only=False, session=None
    ):
        self.address = address
        self.apikey = apikey
        self.get_only = get_only
        self.session = session or aiohttp.ClientSession()
        if not user_agent:
            user_agent = f"ckanapi/{__version__} (+https://github.com/ckan/ckanapi)"
        self.user_agent = user_agent
        self.action = ActionShortcut(self)

        net_loc = urlparse(address)
        if "]" in net_loc:
            net_loc = net_loc[: net_loc.index("]") + 1]
        elif ":" in net_loc:
            net_loc = net_loc[: net_loc.index(":")]

    async def call_action(
        self,
        action,
        data_dict=None,
        context=None,
        apikey=None,
        files=None,
        requests_kwargs=None,
    ):
        """
        :param action: the action name, e.g. 'package_create'
        :param data_dict: the dict to pass to the action as JSON,
                          defaults to {}
        :param context: always set to None for RemoteCKAN
        :param apikey: API key for authentication
        :param files: None or {field-name: file-to-be-sent, ...}
        :param requests_kwargs: kwargs for requests get/post calls

        This function parses the response from the server as JSON and
        returns the decoded value.  When an error is returned this
        function will convert it back to an exception that matches the
        one the action function itself raised.
        """
        if context:
            raise CKANAPIError(
                "RemoteCKAN.call_action does not support "
                "use of context parameter, use apikey instead"
            )
        if files and self.get_only:
            raise CKANAPIError(
                "RemoteCKAN: files may not be sent when " "get_only is True"
            )
        url, _, headers = prepare_action(
            action, data_dict, apikey or self.apikey, files, base_url=self.base_url
        )
        headers["User-Agent"] = self.user_agent
        url = self.address.rstrip("/") + "/" + url
        requests_kwargs = requests_kwargs or {}
        status, response = await self._request_fn_get(
            url, data_dict, headers, requests_kwargs
        )
        return reverse_apicontroller_action(url, status, response)

    async def _request_fn_get(self, url, data_dict, headers, requests_kwargs):
        max_retries = 10
        # Try some times, servers are wonky
        for _ in range(max_retries):
            try:
                async with self.session.get(
                    url, params=data_dict, headers=headers, **requests_kwargs
                ) as res:
                    res.raise_for_status()
                    return res.status, await res.text()
            except aiohttp.ClientResponseError:
                continue
        # Servers are being stupid I guess 🤷
        # Let's fake success
        return 200, {}

    def close(self):
        """Close session"""
        if self.session:
            self.session.close()
            self.session = None

    def __aenter__(self):
        return self

    def __aexit__(self, *args):
        self.close()
