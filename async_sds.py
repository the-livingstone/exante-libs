from .authdb import keyload
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError, ServerDisconnectedError, ClientPayloadError, ContentTypeError
from typing import Any, Dict
from aiohttp import ClientSession
from os import path


class SdsParams:
    def __init__(self, env, credentials):
        self.env = env
        self.url = f'https://sds{"" if self.env == "prod" else "-stage"}.exante.eu'
        self.sessionId = keyload(credentials, self.env, 'sds')
        self.headers = {
            'content-type': 'application/json',
            'x-auth-sessionid': self.sessionId
        }


class AiohttpRequestor:
    @retry(retry=retry_if_exception_type((
        ClientConnectionError,
        ServerDisconnectedError,
        ClientPayloadError
    )), stop = stop_after_attempt(10), wait=wait_fixed(10))
    async def request(
        self,
        method: str,
        uri: str,
        params: Dict[str, any] = {},
        js: Dict[str, Any] = None,
        data: str = None,
        headers = {}
    ) -> Any:
        async with ClientSession(headers={**self.headers, **headers}) as session:
            params = {k: v for k, v in params.items() if not v == None}
            async with session.request(method, uri, params=params, json=js, data=data) as response:
                response.raise_for_status()
                try:
                    return await response.json()           
                except:
                    return await response.text()


class StaticDataService(AiohttpRequestor, SdsParams):
    def __init__(self, env='prod', credentials=('%s/credentials.json' % path.expanduser('~'))):
        super().__init__(env=env, credentials=credentials)

    async def get_symbol_id(self, **kwargs):
        return await self.request('get', f'{self.url}/symbols', params=kwargs)

    async def get_symbol_info(self, _id):
        return await self.request('get', f'{self.url}/symbols/{_id}')
    
    async def get_symbol_raw(self, _id):
        return await self.request('get', f'{self.url}/raw/{_id}')

    async def get_symbol_prepared(self, _id):
        return await self.request('get', f'{self.url}/prepared/{_id}')
