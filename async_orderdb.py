from typing import Any, Dict
from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientResponseError, ClientPayloadError
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import traceback

class OrderDB:

    def __init__(self, env: str='prod'):
        self.env = env
        self.url = f'http://orderdb.{env}.zorg.sh'
        self.headers = {'Content-Type': 'application/json'}
    
    @retry(
        retry=retry_if_exception_type((ClientResponseError, ClientPayloadError)),
        stop = stop_after_attempt(10), wait=wait_fixed(10))
    async def request(self, method: str, uri: str, params: Dict[str, Any] = None,
                             js: Dict[str, Any] = None) -> Any:
        async with ClientSession(connector=TCPConnector(limit=50, limit_per_host=30)) as session:
            params={k: str(v).lower() if isinstance(v,bool) else v for k,v in params.items() if not v in [None, '']}
            params.update({'allAccounts':'true'}) if not (params.get('clientOrder') or params.get('account'))  else {}
            async with session.request(method, uri, params=params, headers=self.headers) as response:
                try:
                    response.raise_for_status()
                    try:
                        return await response.json(content_type=None)
                    except:
                        return await response.text()
                except:
                    logging.error(traceback.format_exc())
                    raise

    
    async def get_orders(self, **kwargs):
        return await self.request('get', f'{self.url}/orders', params=kwargs)
    
    async def get_order(self, order_id):
        return await self.request('get', f'{self.url}/orders/{order_id}', params={})

    async def get_order_chain(self, parent_order_id):
        return  await self.request('get',f'{self.url}/orders', params={'clientOrder':parent_order_id})
