import logging
import requests
import json
from retrying import retry
import time

def conerror(exc):
    exception = [requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout]
    return any([isinstance(exc, x) for x in exception])

class Cbonds:
    '''Class to work with Cbonds API'''
    default_cred_file = '/etc/support/auth/cbonds.json'

    def __init__(self,login=None,password=None,cred_file=None,lang='rus',cache_all_revalidate=0,nocache_all=0):
        self.main_url = 'https://ws.cbonds.info/services/json/'
        self.props = '?lang={}&cache_all_revalidate={}&nocache_all={}'.format(lang,cache_all_revalidate,nocache_all)

        if not login or not password:

            if not cred_file:
                cred_file = self.default_cred_file
            with open(cred_file, "r") as cf:
                creds = json.load(cf)
                login = creds['login']
                password = creds['password']

        self.data = {
            "auth":{
                "login":login,
                "password":password
            }
        }

        self.session = requests.Session()
        self.session.mount(self.main_url, requests.adapters.HTTPAdapter(max_retries=3))
        self._last_request_time = None
        self._schema = None
        self._cc = None

    ### generic methods
    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=conerror)
    def _get(self, url, payload=None):
        """
        internal generic method to retrieve data from cbonds api
        :param url: url of request
        :param payload: additional data
        :return: response dict
        :raises: RuntimeError in case of not OK server answer
        """
        if (self._last_request_time is not None
        and (time.time()-self._last_request_time) < 2):
            time.sleep(2)
        self._last_request_time = time.time()
                
        if payload:
            res = self.session.post(url, data=json.dumps(payload), timeout=30)
        else:
            res = self.session.post(url, timeout=30)

        if res.ok:
            if (res.json().get('error') is not None
            and res.json()['error']['err_no'] == 900000):
                #Max requests per minute limit (30) exceeded
                time.sleep(60)
                raise requests.ConnectionError()
            else:
                return res.json()
        else:
            raise RuntimeError(
                '{} ({}): {} \n for request {} with parameters: {}'.format(
                    res.status_code,
                    res.reason,
                    res.text,
                    url,
                    payload
                )
            )
    @property
    def cc(self):
        """Country codes"""
        if not self._cc:
            fields = ['id','alpha_2_code','name_eng']
            self._cc = {
                cnt['id']: {
                    'alpha_2_code': cnt['alpha_2_code'],
                    'name_eng':cnt['name_eng']
                } for cnt
                in self.request('get_countries',fields=fields)
            }
        return self._cc

    @property
    def schema(self):
        if not self._schema:
            self._schema = self._get(
                '{}?login={}&password={}'.format(
                    self.main_url,
                    self.data['auth']['login'],
                    self.data['auth']['password']
                )
            )
        return self._schema
        

    def get_methods(self):
        return list(self.schema['service'].keys())

    def get_method(self, method_name):
        return self.schema['service'][method_name]

    def get_method_filters(self, method_name):
        return self.schema['service'][method_name]['arguments']['filters']

    def request(self, method, filters=None, quantity=None, sorting=None, fields=None):
        """
        main request method
        :param method:   method_name from cbonds service schema
        :param filters:  filters by fileds eligible for this method_name
        :param quantity: quantity as {"limit": 100, "offset": 0 }
        :param sorting:  sorting as [{'field': '', 'order': 'asc'}]
        :return: response dict
        """
        url='{}/{}/{}'.format(self.main_url, method, self.props)

        payload = self.data
        if filters:
            payload['filters'] = filters
        if filters:
            payload['sorting'] = sorting
        if quantity:
            payload['quantity'] = quantity
            offset = 0 if quantity.get('offset') is None else quantity['offset']
        else:
            payload['quantity']={}
            offset = 0

        res = []
        while True:
            payload['quantity']['offset'] = offset
            tmp = self._get(url, payload)
            total = tmp['total']
            items = tmp['items']
            limit = tmp['limit']
            if not fields:
                res.append(items)
            else:
                for item in items:
                    res.append({field: item[field] for field in fields})

            if limit * (offset + 1) > total:
                break
            else:
                offset+=1
        return res

    ### specialized methods
    def get_emissions(self, isin, full_data=False, fields=None):
        """
        get emission data by ISIN
        :param isin: ISIN
        :param fullData: return full data or only predefined fields
        :retrun: array of dicts
        """
        if isinstance(isin,list):
            isins = ';'.join(isin)
        else:
            isins = isin
        filters = [{
                    "field": "isin_code",
                    "operator":"in",
                    "value":isins
                }]
        if not full_data:
            if fields is None: 
                fields = [
                    'id',
                    'isin_code',
                    'sedol',
                    'bbgid',
                    'emitent_id',
                    'formal_emitent_id',
                    'emitent_country',
                    'formal_emitent_country',
                    'auction_type_id',
                    'bond_type',
                    'convertable',
                    'coupon_type_id',
                    'cupon_period',
                    'currency_id',
                    'currency_name',
                    'document_eng',
                    'early_redemption_date',
                    'emission_cupon_basis_title',
                    'floating_rate',
                    'kind_id',
                    'maturity_date',
                    'nominal_price',
                    'settlement_date',
                    'cupon_eng',
                    'status_name_eng',
                    'emitent_branch_id',
                    'emitent_country_name_eng',
                    'emitent_full_name_eng',
                    'emitent_name_eng',
                    'emitent_type',
                    'emitent_type_name_eng',
                    'offert_eng',
                    'private_offering_name_eng',
                    'qgc_rewrite_type',
                    'reference_rate_name_eng',
                    'status_issue_form'
                ]
        else:
            fields = None

        return self.request('get_emissions', filters=filters, fields=fields)
        
    def get_id_by_isin(self, isin):
        """
        get cbond id of emission by ISIN
        :param isin: ISIN
        :return: id of emission
        """
        try:
            return self.get_emissions(isin)[0]['id']
        except:
            logging.info('Can\'t find emission with isin {}'.format(isin))
            return None
