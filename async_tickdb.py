from decimal import Decimal
from typing import Any, Dict, Optional
from aiohttp import ClientSession
from os import path
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed
from libs.tickdb3 import TickDB3
from requests.utils import quote



class TickDB:
    def __init__(self,node: str='tickdb3', env='prod'):
        self.env = env
        self.domain = 'zorg.sh'
        self.url = f'http://{node}.{self.env}.{self.domain}'
        self.export_api = self.url+ '/quote_api/v1'
        self.quote_api_prefix = 'quote_api/v1'
        self.price_api_prefix = 'price_api/v1'
        self.import_api_prefix = '/v1/import'
        self.crossrate_api_prefix = 'crossrate_api/v1'
        self.nodes = TickDB3(env=env).get_nodes()
        self.quotes_nodes = [node.replace('tickdb_server@','') for node in self.nodes if 'tickdb_server@' in node]
        self.prices_nodes = [node.replace('tickdb_prices@','') for node in self.nodes if 'tickdb_prices@' in node]
        self.candles_nodes = [node.replace('tickdb_candles@','') for node in self.nodes if 'tickdb_candles@' in node]

    @retry(stop = stop_after_attempt(10), wait = wait_fixed(1))
    async def request(self, method: str, uri: str, params: Dict[str, Any] = None,
                        js: Dict[str, Any] = None, ndjson: str = None,
                        headers: Dict[str, Any] = None) -> Any:
        async with ClientSession(headers=headers) as session:
            if method == 'get':
                params = {k:v if not isinstance(v,(list,tuple)) else ','.join(v)  for k,v in params.items() if v!=None}
            async with session.request(method, uri, params=params, json=js, data=ndjson) as response:
                try:
                    return await response.json()
                except:
                    return await response.text()
    
    async def get_quotes(self, instrument, node=None, **kwargs):
        if node:
            return await self.request('get',node + self.quote_api_prefix+f'/export/symbols/{quote(instrument, safe = "")}/quotes', params = kwargs)
        else:
            return await self.request('get', self.export_api+f'/export/symbols/{quote(instrument, safe = "")}/quotes', params = kwargs)
    
    async def delete_quotes(self, instrument, node=None, **kwargs):
        if node:
            return await self.request('delete', node + self.import_api_prefix+f'/symbols/{quote(instrument, safe = "")}/quotes', params = kwargs)
        else:
            return await self.request('delete', self.url + self.import_api_prefix+f'/symbols/{quote(instrument, safe = "")}/quotes', params = kwargs)

    async def get_quote_candles(self, instrument, duration, **kwargs):
        return await self.request('get', self.export_api+f'/export/symbols/{quote(instrument, safe = "")}/quote_candles/{duration}/', params=kwargs)
    
    async def get_trades(self, instrument, **kwargs):
        return await self.request('get',self.export_api+f'/export/symbols/{quote(instrument, safe = "")}/trades', params = kwargs)

    async def get_trade_candles(self, instrument, duration, **kwargs):
        return await self.request('get', self.export_api+f'/export/symbols/{quote(instrument, safe = "")}/trade_candles/{duration}/', params=kwargs, headers = {"Content-Type": "application/x-ld-json"})
    
    async def delete_trades(self, instrument, **kwargs):
        return await self.request('delete',self.export_api+f'/export/symbols/{quote(instrument, safe = "")}/trades', params = kwargs)

    async def get_prices(self, instrument, **kwargs):
        return await self.request('get',self.export_api+f'/export/symbols/{quote(instrument, safe = "")}/prices', params = kwargs)
    
    async def post_prices(self, instrument, ndjson: str = None, jdata: Dict[str, Any] = None, **kwargs):
        endpoints = [f'{node}{self.import_api_prefix}/symbols/{quote(instrument, safe = "")}/price' for node in self.prices_nodes]
        return [await self.request('post', endpoint, ndjson=ndjson, js=jdata, params=kwargs, headers = {"Content-Type": "application/x-ld-json"}) for endpoint in endpoints]
    
    async def post_quotes(self, instrument, ndjson: str = None, jdata: Dict[str, Any] = None, **kwargs):
        endpoints = [f'{node}{self.import_api_prefix}/symbols/{quote(instrument, safe = "")}/quotes' for node in self.quotes_nodes]
        return [await self.request('post', endpoint, ndjson=ndjson, js=jdata, params=kwargs, headers = {"Content-Type": "application/x-ld-json"}) for endpoint in endpoints]
    
    async def post_quote_candles(self, instrument, duration, ndjson: str = None, jdata: Dict[str, Any] = None, **kwargs):
        endpoints = [f'{node}{self.import_api_prefix}/symbols/{quote(instrument, safe = "")}/qcandles/{duration}' for node in self.candles_nodes]
        return [await self.request('post', endpoint, ndjson=ndjson, js=jdata, params=kwargs, headers = {"Content-Type": "application/x-ld-json"}) for endpoint in endpoints]    
    
    async def post_trade_candles(self, instrument, duration, ndjson: str = None, jdata: Dict[str, Any] = None, **kwargs):
        endpoints = [f'{node}{self.import_api_prefix}/symbols/{quote(instrument, safe = "")}/tcandles/{duration}' for node in self.candles_nodes]
        return [await self.request('post', endpoint, ndjson=ndjson, js=jdata, params=kwargs, headers = {"Content-Type": "application/x-ld-json"}) for endpoint in endpoints]
    
    async def get_prices_snapshot(self, **kwargs):
        for node in self.prices_nodes:
            endpoint = f'{node}/{self.price_api_prefix}/prices/snapshot'
            return await self.request('get',endpoint, params=kwargs)
    
    async def get_crossrates_snapshot(self, **kwargs):
        return await self.request('get',f'{self.url}/{self.crossrate_api_prefix}/snapshot', params = kwargs or {})
    
    async def get_crossrate(self,**kwargs):
        return await self.request('get',f'{self.url}/{self.crossrate_api_prefix}/crossrate', params = kwargs)
