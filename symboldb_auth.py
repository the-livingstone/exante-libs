#!/usr/bin/env python3

import datetime as dt
import json
import logging
import re
import os
import requests
import uuid
import time
from retrying import retry
from copy import deepcopy

from libs.authdb import get_session, keyload, NotAuthorized

def conerror(exc):
    exception = [
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ReadTimeout]
    print(exc, type(exc))
    return any([isinstance(exc, x) for x in exception])


class SymbolDBError(Exception):
    """Common exception for problems with SDB"""
    pass


class SymbolDB:
    """Class for work with SymbolDB"""

    months = ('', 'F', 'G', 'H', 'J', 'K', 'M',
              'N', 'Q', 'U', 'V', 'X', 'Z')

    def __init__(self, env='prod', user=None, password=None,
                 credentials=('%s/credentials.json' % os.path.expanduser('~'))):
        """
        class init method
        :param env: environment
        """
        self.env = env
        if self.env == 'demo':
            self.env = 'prod'
        self.session_id = None

        if credentials:
            try:
                self.session_id = keyload(credentials, self.env, 'symboldb')
            except KeyError:
                self.logger.warning('Provided file has no symboldb_editor session for {}'.format(self.env))
        elif self.session_id is None:
            if user and password:
                self.session_id = get_session(user, password, 'symboldb', self.env)
        else:
            raise NotAuthorized('Either credentials file or user-pass must be provided')

        self.headers = {'Content-Type': 'application/json',
                        'X-Auth-SessionId': self.session_id}

        self.domain = 'zorg.sh'
        self.version = 'v1.0'
        self.url = f'http://symboldb.{self.env}.{self.domain}/symboldb-editor/api/{self.version}/'
        self.session = requests.Session()
        self.session.mount(self.url, requests.adapters.HTTPAdapter())


    def __repr__(self):
        return 'SymbolDB({})'.format(repr(self.env))

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")
    
    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=conerror)
    def __request(self, method, handle, params=None, jdata=None, data=None, headers=None):
        """
        wrapper method for requests
        :param method: requests method to be invoked
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :param jdata: json to pass with request
        :return: requests response object
        """
        if headers is None:
            headers = self.headers
        response = method(self.url + handle, params=params, json=jdata, data=data, headers=headers)
        self.logger.debug('headers: {}'.format(headers))
        self.logger.debug('full url: {}'.format(response.url))
        if params:
            self.logger.debug('passed params: {}'.format(params))
        if jdata:
            self.logger.debug('passed json: {}'.format(jdata))
        if data:
            self.logger.debug('passed string: {}'.format(data))
        if response.status_code > 209:
            self.logger.error("server returns error code {} while requesting\n{}\n{}"
                          .format(response.status_code, response.url, response.text))
        return response

    def _get(self, handle, params=None):
        """
        wrapper method for requests.get
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        return self.__request(method=self.session.get, handle=handle, params=params)

    def _delete(self, handle, params=None):
        """
        wrapper method for requests.delete
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: requests response object
        """
        return self.__request(method=self.session.delete, handle=handle, params=params)

    def _post(self, handle, jdata=None, data=None, params=None, headers=None):
        """
        wrapper method for requests.post
        :param params:
        :param handle: backoffice api handle
        :param jdata: json to pass with request
        :return: requests response object
        """
        if headers is None:
            headers = self.headers
        return self.__request(method=self.session.post, handle=handle, jdata=jdata, data=data, params=params, headers=headers)

    def _put(self, handle, jdata, params=None):
        """
        wrapper method for requests.put
        :param params:
        :param handle: backoffice api handle
        :param jdata: json to pass with request
        :return: requests response object
        """
        return self.__request(method=self.session.put, handle=handle, jdata=jdata, params=params)

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=lambda x: isinstance(x, requests.exceptions.Timeout))
    def __recursive_copy(self, data, path, uuid, top=False, sleep=5):
        """
        hidden method which implements copying
        :param data: dictionary with ids as keys and content of source symbols
        as values
        :param path: path for new record
        :param uuid: if of old record
        :param top: flag for top of tree
        :param sleep: delay between insertions
        """
        new_record = deepcopy(data[uuid])

        # Disable top of new tree to avoid a dozen of problems
        if top:
            self.logger.debug('Disabling top of new tree')
            new_record['isTrading'] = False
        # Disable branch if it's enabled
        if new_record.get('isTrading'):
            self.logger.info('"isTrading" removed for id {} ({})'.format(
                uuid, data[uuid]['name']))
            del (new_record['isTrading'])

        # Some preparement for creating new record
        del (new_record['_id'])
        del (new_record['_rev'])
        new_record['path'] = path

        result = self.create(new_record)
        if result.get('_id'):
            self.logger.debug(result)

            # Get list of right descendants of just copied record
            depth = len(data[uuid]['path']) + 1
            children = [i for i in data.keys() if
                        len(data[i]['path']) == depth and uuid in data[i]['path']]
            for i in children:
                self.__recursive_copy(
                    data=data,
                    path=path + [result['_id']],
                    uuid=i,
                    sleep=sleep)
                time.sleep(sleep)
        else:
            self.logger.error(
                f"{result['message']} ({result['code']}) caused by adding:\n{data[uuid]}"
            )

    def get(self, data: str, fields: list = None) -> dict:
        """
        get instrument by id or EXANTEId
        :param data: instrument id
        :return: instrument
        """
        if not fields:
            fields = []
        self.logger.debug(data)
        params = {}
        if isinstance(data, str) and self.is_uuid(data):
            if fields:
                fields_str = ','.join(fields)
                params.update({
                    'fields': fields_str
                })
            response = self._get(f'instruments/{data}', params=params)
            return response.json()
        elif isinstance(data, str):
            response = self.get_v2(f'^{data}$', fields=fields)
            if response:
                return response[0]
            else:
                return dict()
        elif isinstance(data, list):
            items = []
            for item in data:
                instrument = self.get(item)
                items.append(instrument)
            return items
        else:
            raise RuntimeError('Only strings or lists of string supported for this method')

    def get_compiled(self, data) -> dict:
        """
        get compiled instrument by id
        :param data: instrument id
        :return: compiled instrument
        """
        if isinstance(data, str) and self.is_uuid(data):
            response = self._get(f'compiled_instruments/{data}')
            try:
                return response.json()[0]
            except KeyError:
                return None
        else:
            return None

    def update(self, data) -> dict:
        """
        updates single instrument
        :param data: data to be posted
        :return: id, rev of updated symbol
        """
        _id = data['_id']
        response = self._post(f'instruments/{_id}', jdata=data)
        return json.loads(response.text)

    def batch_update(
            self,
            input_data: list[dict],
            data_type='instrument',
            action='update',
            purify=True,
            preserve_id=False,
            withDependents=False
        ):
        """
        updates list of objects
        :param input_data: list of dicts
        :param data_type: type of objects in dicts
        :param action: update, create or delete
        :param purify: remove unneded fields
        :param preserve_id: preserve id during creation
        :param withDependents: delete with all dependents (for instruments only)
        """
        if action not in ['update', 'create', 'delete']:
            raise Exception('Unknown action!')
        if data_type not in ['brokerProvider',
                             'currency',
                             'exchange',
                             'executionScheme',
                             'feedProvider',
                             'instrument',
                             'schedule']:
            raise Exception('Unknown data_type!')

        unneeded = ['_creationTime','_lastUpdateTime','_rev']
        if action == 'create' and not preserve_id:
            unneeded.append('_id')
        
        data_block = 'query' if action=='delete' else 'data'

        data = str()
        for el in input_data:
            if 'content' in el:
                el = el['content']
            if action=='delete':
                el['withDependents'] = withDependents
            if purify:
                el = {key: val for key, val in el.items() if key not in unneeded}
            update = dict()
            update.setdefault(data_block, el)
            update.setdefault('type', data_type)
            update.setdefault('action', action)
            data += json.dumps(update)
        headers = self.headers
        headers.update(
            {'Content-Type': 'application/x-ld-json'}
        )
        response = self._post('batch', data=data, headers=headers)
        return response.text

    def create(self, data) -> dict:
        """
        creates single instrument
        :param data: data to be posted
        :return: id, rev of created symbol
        """
        response = self._post('instruments', jdata=data)
        return response.json()

    def batch_create(self, input_data) -> dict:
        """
        merely just a wrapper for batch_update
        :param instruments: list of dict
        """
        return self.batch_update(input_data, action='create')

    def update_schedule(self, data: dict, _id = None) -> dict:
        """
        updates schedule
        :param data: data to be posted
        :return: id, rev of updated schedule
        """
        if 'content' in data.keys():
            data = data['content']
        if _id is None:
            _id = data.get('_id')
        if _id:
            response = self._post(f'schedules/{_id}', jdata=data)
            return response.json()
        else:
            self.logger.error('schedule id is not specified!')
            return None

    def create_schedule(self, data) -> dict:
        """
        creates new schedule
        :param data: data to be posted
        :return: id, rev of created schedule
        """
        data = self.strip_id(data)
        response = self._post(f'schedules', jdata=data)
        return response.json()

    def get_tree(self, fields: list = None) -> list[dict]:
        """
        returns tree of instrument
        :param fields: list of fields to return
        :return: list
        """
        if not fields:
            fields = []
        default_fields=['path', 'name', '_id', 'isTrading', 'isAbstract']
        params = None
        if not len(fields) or fields[0] != 'all':
            fields.extend(default_fields)
            params = {
                'fields': ','.join(fields)
            }
        response = self._get(f'instruments', params=params)
        return response.json()

    def get_brokers(self, _id: str = None) -> list[dict]:
        """
        returns list of broker providers
        :param bid: broker id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        response = self._get(f'broker_providers{_id}')
        return response.json()

    def post_broker(self, data: dict, _id: str = None) -> dict:
        """
        post broker provider to sdb or update existing one if broker id is given
        :param data: data to be posted
        :param _id: broker id to update
        """
        if 'content' in data.keys():
            data = data['content']
        if _id is None:
            _id = data.get('_id', '')
        if _id:
            _id = f"/{_id}"
        data = self.strip_id(data)
        return self._post(f'broker_providers/{_id}', jdata=data)

    def get_broker_accounts(self, _id: str = None) -> list[dict]:
        """
        returns list of broker accounts
        :param _id: broker id to get only this particular id
        :return: list
        """
        
        _id = f'/{_id}' if _id else ''
        response = self._get(f'broker_accounts{_id}')
        return response.json()

    def get_exchanges(self) -> list[dict]:
        """
        returns list of exchanges
        :return: list
        """
        response = self._get(f'exchanges')
        return response.json()

    def get_schedule(self, _id: str = None) -> list[dict]:
        """
        returns schedule
        :param _id: if specified, returns single schedule with specified id
        :return: self.get_list()
        """
        _id = f'/{_id}' if _id and self.is_uuid(_id) else ''
        response = self._get(f'schedules{_id}')
        return response.json()

    def get_feed_gateways(self, _id: str = None) -> list[dict]:
        """
        returns list of feed gateways
        :param _id: feed id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        response = self._get(f'feed_gateways{_id}')
        return response.json()

    def get_feeds(self, _id: str = None) -> list[dict]:
        """
        returns list of feed providers
        :param _id: feed id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        response = self._get(f'feed_providers{_id}')
        return response.json()

    def post_feed(self, data: dict, _id: str = None) -> dict:
        """
        post feed provider to sdb or update existing one if feed id is given
        :param data: data to be posted
        :param _id: broker id to update
        """
        if 'content' in data.keys():
            data = data['content']
        if not _id:
            _id = data.get('_id', '')
        if _id:
            _id = f"/{_id}"
        data = self.strip_id(data)
        return self._post(f'feed_providers{_id}', jdata=data).json()

    def get_feed_providers(self, search_string='.*') -> list[dict]:
        """
        method which returns feed_providers by regexp
        :param search_string: regexp for search
        :return: list of providers which match to regexp
        """
        res = []
        for feed in self.get_feeds():
            feed_gateways = ''
            for key in feed.get('gateways', list()):
                if self.env.upper() in feed['gateways'][key]['environment'].upper():
                    feed_gateways += '{} ({})'.format(feed['gateways'][key]['name'],
                                                      feed['gateways'][key]['feedAddress'])
            if re.match(search_string, feed['name']):
                res.append({
                    'name': feed['name'],
                    'providerId': feed['_id'],
                    'gateways': feed_gateways
                })
        return res

    def get_execution_schemes(self, _id: str = None) -> list[dict]:
        _id = f'/{_id}' if _id else ''
        return self._get(f'execution_schemes{_id}').json()

    def get_heirs(
            self,
            _id,
            full: bool = False,
            recursive: bool = False,
            fields: list = None,
            name=None,
            tree=None
        ) -> list[dict]:
        """
        method to get child tree
        :param _id: parent id, if not set returns all instruments
        :param full: return full document instead of just id
        :param folders: if False it returns only first layer, without included folders
        :param fields: Only request specified fields in response. isAbstract and _id will be added to this argument
        if not present
        :type name: symbol name e.g. 'AAPL'
        :param tree: instead of requesting from symboldb, work with given list of instrumentrs
        :param without_abstract: returns without abstract files (folder names)
        :return: list of abstract child objects id by default, or abstract child objects full content if full == True
        also return folders if folders == True
        """
        default_fields = ['_id', 'name', 'isAbstract']
        if not fields:
            fields = []
        fields = set(fields)
        fields.update(default_fields)
        fields = list(fields)
        if tree and all([x in tree[0].keys() for x in fields]):
            if recursive:
                return [
                    x for x
                    in tree
                    if _id in x['path'][:-1]
                ]
            else:
                return [
                    x for x
                    in tree
                    if len(x['path']) > 1
                    and x['path'][-2] == _id
                ]
        heirs = []
        fields_str = ''
        if not full:
            default_fields += [x for x in fields if x not in ['_id', 'name', 'isAbstract']]
            fields_str = ','.join(default_fields)
        params = {
            'parentId': _id,
            'fields': fields_str,
            'symbolId_regexp': name
        }
        params = {k: v for k, v in params.items() if v}
        response = self._get(f'instruments', params=params)
        if not response.ok:
            raise RuntimeError(
                f'{response.status_code} ({response.reason}): {response.text}'
            )
        for r in response.json():
            heirs.append(r)
            if r['isAbstract'] and recursive:
                heirs += [
                    x for x in self.get_heirs(
                        r['_id'],
                        full=full,
                        recursive=recursive,
                        fields=fields,
                        name=None
                    )
                ]
        return heirs
    
    def get_parents(self, _id, fields=[]) -> list[dict]:
        """
        gets self dictionary plus all parents' dicts
        :param _id: uuid of the instrument
        :return: list of instruments' dicts
        """
        default_fields = ['_id', 'name', 'path']
        params = {
            'childId': _id
        }
        fields_str = str()
        if fields:
            default_fields += [x for x in fields if x not in ['_id', 'name', 'path']]
            fields_str = ','.join(default_fields)
            params.update({
                'fields': fields_str
            })
        response = self._get('instruments', params=params).json()
        return list(sorted(response, key=lambda instr: len(instr['path'])))

    def get_currencies(self, _id: str = None, full=False) -> list[dict]:
        """
        gets list of currencies registered in SymbolDB
        :return: list of currencies
        """
        _id = f'/{_id}' if _id else ''
        response = self._get(f'currencies{_id}')
        if full:
            return response.json()
        return [cur['_id'] for cur in response.json()]

    def post_currency(self, data, _id: str = None) -> dict:
        """
        post currency to sdb or update existing one if id
        :param data: data to be posted
        """
        _id = f'/{_id}' if _id else ''
        return self._post(f'currencies{_id}', jdata=data)

    def get_them_all(self) -> list[dict]:
        """
        gets ALL records from SymbolDB via erlang API
        :return: list
        """
        response = self._get(f'instruments')
        return response.json()

    def get_uuid_by_path(self, path: list, tree: list = None) -> str:
        """
        gets uuid by list of names sorted as path
        :param path: list of names
        :param tree: previously received tree
        :return: id of symbol or None if path absents
        :raises: RuntimeError if path is ambiguousself._get
        """
        if not tree:
            tree = self.get_tree()
        result = None
        parent = None
        for level, item in enumerate(path):
            nodes = [i for i in tree
                     if len(i['path']) == level + 1
                     and i['name'] == item
                     and (parent is None or i['path'][level - 1] == parent)]
            if len(nodes) > 1:
                raise RuntimeError('Ambiguous path {} (node {}, {} choices)'
                                   .format(path, item, len(nodes)))
            elif not nodes:
                return
            else:
                result = nodes[0]['_id']
                parent = result
        return result

    def is_uuid(self, string) -> bool:
        """
        check whether or not string is uuid
        :param string: input string
        :return: True if string is uuid
        """
        try:
            u = uuid.UUID(string)
            return True
        except (ValueError, TypeError):
            return False

    def strip_id(self, data: dict) -> dict:
        """
        method to remove id and times from source
        :param data: source data
        :return: stripped data
        """
        strip_fields = [
            '_id',
            '_rev',
            '_lastUpdateTime',
            '_creationTime'
        ]
        data = {key: val for key, val in data.items() if key not in strip_fields}
        return data

    def copy_tree(self, from_id, to_id, sleep=5):
        """
        method to copy tree from from_id to to_id
        to_id will be parent of new instance of from_id
        :param from_id: source ID
        :param to_id: destination ID
        :param sleep: delay between insertions in second, default 5
        """
        initial_data = {i['_id']: i for i in self.get(
            [j['_id'] for j in self.get_tree()
             if from_id in j['path']])}
        destination = self.get(to_id)['path']
        if initial_data and destination:
            self.__recursive_copy(
                data=initial_data,
                path=destination,
                uuid=from_id,
                top=True,
                sleep=sleep)
        else:
            self.logger.error('Source or destination is incorrect'
                          '(id is incorrect or doesn\'t exist)')


    def date_to_sdb(self, dto: dt.date, include_day: bool = True) -> dict:
        """
        method to convert date to valid symboldb format
        :param dto: datetime object
        :param maturity: build maturity date
        :param expiry: build date in format of expiry date
        :param name: build name
        :return: symboldb date
        """
        if include_day:
            return {'day': dto.day, 'month': dto.month, 'year': dto.year}
        else:
            return {'month': dto.month, 'year': dto.year}

    def sdb_to_date(self, date_dict) -> dt.date:
        """
        takes dictionary with Month, Year, Day keys and returns
        datetime object
        :param date_dict: date dictionary
        :return: datetime object
        """
        try:
            return dt.date(
                year=date_dict.get('year'),
                month=date_dict.get('month'),
                day=date_dict.get('day', 1)
            )
        except TypeError:
            return None
        except AttributeError:
            return None

    def find_in_tree(self, ticker) -> list[dict]:
        """
        method to find elements in symboldb tree by their name
        :param ticker: symbol tree name
        :return: symbol dict
        """
        return [x for x in self.get_tree() if x['name'] == ticker]

    def get_symbol_uuid(self, string) -> str:
        """
        method which returns symbol uuid
        :param string: request string
        :return: symbol uuid
        """
        if self.is_uuid(string):
            return string
        else:
            return self.get(string).get('_id')

    def delete_by_ids(self, _id) -> dict:
        """
        method to delete item by its id
        :param _id: symbol ID
        :return: deleted id and rev
        """
        response = self.session.delete('{}instruments/{}'.format(self.url, _id))
        return response.json()

    def get_ids_by_name(self, ticker) -> list[str]:
        """
        method which returns symbol ids
        :param ticker: symbol ticker
        :return: list of found ids
        """
        ids = []
        results = self.find_in_tree(ticker)
        if not results:
            return ids
        for item in results:
            ids.append({
                '_id': item['_id'],
                '_rev': self.get_latest_revision(item['_id'])
            })
        return ids

    def get_latest_revision(self, _id) -> str:
        """
        get latest revision
        :param _id: symbol ID
        :return: latest revision
        """
        _rev = ''
        try:
            _rev = self.get(_id)['_rev']
        except KeyError:
            pass
        return _rev

    def get_v2(
            self,
            exante_id,
            _id: str = '',
            fields: list = [],
            is_expired: bool = None,
            is_trading: bool = None
        ) -> list[dict]:
        """
        can retrieve json for expired symbol
        """
        params = {
            'symbolId_regexp': exante_id,
            'isExpired': is_expired,
            'isTrading': is_trading
        }
        if fields:
            fields_str = ','.join(fields)
            params.update({
                'fields': fields_str
            })
        params = {k: v for k, v in params.items() if v is not None}
        if _id:
            _id = f'/{_id}'
        return self._get(f'instruments{_id}', params=params).json()
    
    def get_by_shortname(
            self,
            description: str,
            fields: list = [],
            is_expired: bool = None,
            is_trading: bool = None
        ) -> list[dict]:
        params = {
            'shortName_regexp': description,
            'isExpired': is_expired,
            'isTrading': is_trading
        }
        if fields:
            fields_str = ','.join(fields)
            params.update({
                'fields': fields_str

            })
        params = {k: v for k, v in params.items() if v is not None}
        return self._get(f'instruments', params=params).json()

    def get_tasks(self) -> list[dict]:
        return self._get(f'tasks').json()

    def get_historical(
            self,
            _id,
            datefrom=None,
            dateuntil=None,
            limit: int = 1,
            order: str = 'desc'
        ) -> list[dict]:
        """
        get historical instruments
        :param _id: uuid of instrument
        :param from: from what datetime show the instrument settings
        :param until: until what datetime show the instrument settings
        :param limit: how many versions to show
        :param order: 'desc' - latest first, 'asc' - oldest first
        :return: list of historical versions of instrument
        """
        params = {
            'id': _id,
            'limit': limit,
            'order': order
        }
        if datefrom is not None:
            params.update({
                'fromDate': f'{dt.datetime.isoformat(datefrom)}Z'
            })
        if dateuntil is not None:
            params.update({
                'toDate': f'{dt.datetime.isoformat(dateuntil)}Z'
            })
        return self._get('historical/instruments', params=params).json()

    def post_currencies_holidays(self, currency, date, isHoliday=True):
        """
        update currencies holidays list
        :param currency: currency to be posted
        :param date: date to be posted
        :param isHoliday: boolean
        :return: response status code
        """
        data = [{'currency': currency, 'date': date, 'isHoliday': isHoliday}]
        response = self._post(f'currency_holidays', jdata=data)
        return response.status_code

    def batch_currencies_holidays(self, data: list):
        """
        update currencies holidays list
        :param currency: currency to be posted
        :param date: date to be posted
        :param isHoliday: boolean
        :return: response status code
        """
        verified = [x for x in data if x.get('currency') and x.get('date') and x.get('isHoliday') is not None]
        response = self._post(f'currency_holidays', jdata=verified)
        return response.text

    def get_update_size(self, _id) -> int:
        """
        get number of potentially affected
        instruments if _id gets updated
        :param _id: instrument (or most likely folder) uuid
        :return: number of affected children
        """
        response = self._get(f'instruments/{_id}/deps')
        if response.ok:
            return response.json()['affectedSymbols'] if response.json().get('affectedSymbols') else 1