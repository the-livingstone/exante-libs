# from typing import Any, Dict 
# from aiohttp import ClientSession
# from os import path
# from tenacity import retry, stop_after_attempt, wait_fixed
# from .authdb import get_session, keyload
# import traceback
# from loguru import logger
# import json as json
# from requests.utils import quote

# class BackOffice():
#     def __init__(self, env='prod', user=None, password=None,
#                  credentials=('%s/credentials.json' % path.expanduser('~')), version='2.0'):
#         self.sessionId = None
#         self.env = env
#         self.version=version
#         if self.env == 'prod':
#             self.url = 'https://backoffice.exante.eu'
#         elif self.env == 'cprod':
#             self.url = 'https://backoffice.gozo.pro' #cprod.zorg.sh'
#         elif self.env == 'cstage':
#             self.url = 'https://backoffice-stage.gozo.pro'
#         else:
#             self.url = 'https://backoffice-{}.exante.eu'.format(self.env)
        
#         self.url = '{0}/api/v{1}'.format(self.url, self.version)

#         if user and password:
#             self.sessionId = get_session(user, password, 'backoffice', self.env)
#         else:
#             self.sessionId = keyload(credentials, self.env, 'backoffice')

#         self.headers = {'content-type': 'application/json',
#                         'x-use-historical-limits': 'false',
#                         'accept-encoding': 'gzip',
#                         'X-Auth-SessionId': self.sessionId}

                        
#     @retry(stop = stop_after_attempt(10), wait=wait_fixed(1))
#     async def request(self, method: str, uri: str, params: Dict[str, Any] = {},
#                              js: Dict[str, Any] = {}) -> Any:
#         async with ClientSession(headers=self.headers) as session:
#             try:
#                 if method == 'get':
#                     params = {k:v if not isinstance(v,(list,tuple)) else ','.join(v)  for k,v in params.items()}
#                 async with session.request(method, self.url+uri, params=params, json=js) as response:
#                     if response.status in [404]:
#                         return await response.json(content_type=None)
#                     elif response.status in [400]:
#                         return await response.text()
#                     elif response.ok:
#                         return await response.json(content_type=None)
#                     else:
#                         response.raise_for_status()
#             except:
#                 logger.error(traceback.format_exc() )
#                 raise
    
#     async def stream(self, method: str, uri:str, params: Dict[str, Any] = None,
#                              js: Dict[str, Any] = None) -> Any:
#         async with ClientSession(headers=self.headers) as session:
#             async with session.request(method, self.url+uri, params=params, json=js) as response:
#                 result = []
#                 async for line in response.content:
#                     item = json.loads(line.decode())
#                     if item.get('$type')=='sync':
#                         return result
#                     elif item.get('$type')=='heartbeat':
#                         continue
#                     result.append(item)
#                 #return [line.decode() async for line in response.content if line]
                    

#     async def account_permissions_post(self, account, jdata, **kwargs):
#         return await self.request('post',f'/accounts/{account}/permissions', js=jdata)
    
#     async def account_permissions_get(self, account, **kwargs):
#         return await self.request('get',f'/accounts/{account}/permissions', params=kwargs)
    
#     async def account_permissions_effective_get(self, account, **kwargs):
#         return await self.request('get',f'/accounts/{account}/permissions/effective', params=kwargs)
    
#     async def user_permissions_post(self, user, jdata, **kwargs):
#         return await self.request('post',f'/users/{quote(user, safe="")}/permissions', js=jdata)
    
#     async def user_permissions_get(self, user, **kwargs):
#         return await self.request('get',f'/users/{user}/permissions', params=kwargs)

#     async def user_permissions_sets_post(self, jdata, **kwargs):
#         return await self.request('post',f'/user_permissions_sets', js=jdata)
    
#     async def account_limits_post(self, account, jdata, **kwargs):
#         return await self.request('post',f'/accounts/{account}/limits',js=jdata)
    
#     async def permissions_sets_overrides_post(self, id, jdata, **kwargs):
#         return await self.request('post',f'/permissions/sets/{id}/overrides',js=jdata)
    
#     async def permissions_sets_get(self, **kwargs):
#         return await self.request('get',f'/permissions/sets', params=kwargs)
    
#     async def permissions_sets_overrides_get(self, id, **kwargs):
#         return await self.request('get',f'/permissions/sets/{id}/overrides')
    
#     async def trades_get(self, **kwargs):
#         return await self.request('get','/trades', params=kwargs)

#     async def trade_rollback(self, order_id, order_pos):
#         return await self.request('post',f'/trades/rollback/{order_id}/{order_pos}')
    
#     async def trades_post(self, jdata):
#         return await self.request('post','/trades', js=jdata)
    
#     async def account_trade_post(self, account, jdata):
#         return await self.request('post',f'/accounts/{account}/trade', js=jdata)
    
#     async def riskarrays_get(self, **kwargs):
#         return await self.request('get','/riskarrays', params=kwargs)
    
#     async def clients_get(self, **kwargs):
#         return await self.request('get',f'/clients', params=kwargs)
    
#     async def client_get(self, client_id, **kwargs):
#         return await self.request('get',f'/clients/{client_id}', params=kwargs)
    
#     async def account_get(self, account):
#         return await self.request('get',f'/accounts/{account}')
    
#     async def account_post(self, jdata, **kwargs):
#         return await self.request('post',f'/accounts',js=jdata)
    
#     async def account_update(self, account, jdata, **kwargs):
#         return await self.request('post',f'/accounts/{account}',js=jdata)

#     async def account_benchmark_get(self, account, **kwargs):
#         return await self.request('get',f'/accounts/{account}/benchmark')
    
#     async def account_benchmark_post(self, account, jdata, **kwargs):
#         return await self.request('post', f'/accounts/{account}/benchmark', js=jdata)
    
#     async def user_accounts_post(self, jdata):
#         return await self.request('post', f'/user_accounts', js=jdata)
    
#     async def account_users_get(self, account):
#         return await self.request('get','/user_accounts', params={'accountId': account})

#     async def global_summary(self, **kwargs):
#         return await self.request('get','/summary/EUR', params = kwargs)
    
#     async def global_summary_on_date(self, date, **kwargs):
#         return await self.request('get',f'/summary/{date}/EUR', params = kwargs)

#     async def default_permissions_post(self, jdata):
#         return await self.request('post', f'/permissions', js=jdata)

#     async def default_permissions_get(self, **kwargs):
#         return await self.request('get', f'/permissions', params = kwargs)
    
#     async def intermonth_spread_margin_get(self, **kwargs):
#         return await self.request('get','/intermonth_spread_margin', params = kwargs)

#     async def margin_settings_get(self, account_id):
#         return await self.request('get', f'/accounts/{account_id}/margin_settings')
    
#     async def margin_settings_post(self, account_id, jdata):
#         return await self.request('post', f'/accounts/{account_id}/margin_settings', js=jdata)
    
#     async def account_leverage_rates_get(self, account_id):
#         return await self.request('get', f'/accounts/{account_id}/rates/leverages')
    
#     async def account_leverage_rates_post(self, account_id, jdata):
#         #return self.post(f'/accounts/{account}/rates/leverages', jdata)
#         return await self.request('post', f'/accounts/{account_id}/rates/leverages', js=jdata)
    
#     async def user_accounts_get(self, **kwargs):
#         return await self.request('get', f'/user_accounts', params = kwargs)
    
#     async def access_link_delete(self, _id):
#         return await self.request('delete', f'/user_accounts/{_id}')
    
#     async def access_link_create(self, jdata):
#         return await self.request('post', f'/user_accounts', js = jdata)
    
#     async def stream_accounts(self):
#         return await self.stream('get',f'/streams/accounts')
    
#     async def stream_permissions(self):
#         return await self.stream('get',f'/streams/permissions')
    
#     async def stream_positions(self):
#         return await self.stream('get',f'/streams/positions')
    
#     async def stream_metrics(self):
#         return await self.stream('get',f'/streams/metrics')
    
#     async def get_symbols(self, **kwargs):
#         return await self.request('get',f'/symbols', params = kwargs)
    
#     async def get_symbol(self, symbol_id, **kwargs):
#         return await self.request('get',f'/symbols/info', params = {
#             **kwargs, 'symbolId':symbol_id
#         })

#     async def account_limits_get(self, account, **kwargs):
#         return await self.request('get',f'/accounts/{account}/limits', params = kwargs)

#     async def default_commissions_get(self):
#         return await self.request('get',f'/commissions')
    
#     async def account_commissions_get(self, account_id):
#         return await self.request('get',f'/accounts/{account_id}/commissions')
    
#     async def account_overnights_get(self, account_id):
#         return await self.request('get',f'/accounts/{account_id}/rates/overnights')
    
#     async def commission_group_overrides_get(self, commission_group_id):
#         return await self.request('get',f'/commissions/groups/{commission_group_id}/overrides')
    
#     async def commission_group_overrides_post(self, commission_group_id, jdata):
#         return await self.request('post',f'/commissions/groups/{commission_group_id}/overrides', js=jdata)
    
#     async def commission_groups_get(self):
#         return await self.request('get',f'/commissions/groups')
    
#     async def get_rebate_accounts(self, acc, **kwargs):
#         return await self.request('get',f'/accounts/{acc}/rebate_accounts', params = kwargs)
    
#     async def post_rebate_accounts(self, acc, jdata):
#         """
#         [
#             {
#                 "id": "FBB1234.002",
#                 "percent": 0.05
#             }
#         ]
#         """
#         return await self.request('post',f'/accounts/{acc}/rebate_accounts', js = jdata)
    
#     async def delete_rebate_account(self, acc, rebate_acc):
#         return await self.request('delete', f'/accounts/{acc}/rebate_accounts/{rebate_acc}')
    
#     async def default_overnights_get(self):
#         return await self.request('get', f'/rates/overnights')
    
#     async def default_overnights_post(self, jdata):
#         return await self.request('post', f'/rates/overnights', js=jdata)
    
#     async def transactions_get(self, **kwargs):
#         default_fields = "exanteCounterparty,id,uuid,parentUuid,accountId,timestamp,operationType,asset,sum,rawSum,convertedSum,who,comment,internalComment,symbolId,isin,symbolType,valueDate,orderId,orderPos,price,clientType,executionCounterparty,category,baseCurrency,transferId,internalCounterparty,legalEntity,chainId"
#         return await self.request('get', f'/transactions', params = {"fields":default_fields, **kwargs})

#     async def account_summary_get(self, account_id, currency='EUR', **kwargs):
#         return await self.request('get', f'/accounts/{account_id}/summary/{currency}', params = kwargs)
    
#     async def accounts_get(self, **kwargs):
#         return await self.request('get', f'/accounts', params = kwargs)
    
#     async def global_metrics_get(self, currency='EUR', **kwargs):
#         return await self.request('get', f'/metrics/{currency}', params = kwargs or {})
    
#     async def global_metrics_post(self, jdata, currency='EUR'):
#         return await self.request('post', f'/metrics/{currency}', js=jdata)
    
#     async def client_metrics_get(self, currency='EUR', **kwargs):
#         return await self.request('get', f'/reports/clients_metrics/{currency}', params = kwargs)
    
#     async def transactions_post(self, jdata):
#         """
#         [
#             {
#                 "accountId": "string",
#                 "amount": "1000.0",
#                 "asset": "EUR",
#                 "clientCounterparty": "string",
#                 "clientCountry": "string",
#                 "comment": "string",
#                 "commission": {
#                     "currency": "EUR",
#                     "type": "BANK CHARGE",
#                     "value": 0
#                 },
#                 "exanteCounterparty": "string",
#                 "extraData": {
#                     "clientCustody": "string",
#                     "clientCustodyAccount": "string",
#                     "clientName": "string",
#                     "custody": "string",
#                     "custodyAccount": "string",
#                     "ignoreMirroring": true,
#                     "marketPrice": "",
#                     "requestId": "string",
#                     "san": "123456789",
#                     "tags": {}
#                 },
#                 "internalComment": "string",
#                 "internalCounterparty": "string",
#                 "operationType": "FUNDING/WITHDRAWAL",
#                 "price": "",
#                 "settlementCounterparty": "",
#                 "symbolId": "EUR",
#                 "transferId": "12345678",
#                 "useAutoCashConversion": "false",
#                 "valueDate": "2021-06-02"
#             }]
#         """
#         return await self.request('post', f'/transactions', js=jdata)
    
#     async def transaction_rollback(self, trid):
#         return await self.request('post',f'/transactions/rollback/{trid}')


