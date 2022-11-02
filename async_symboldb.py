#!/usr/bin/env python3

import asyncio
from enum import Enum
from aiohttp import ClientResponseError, ClientSession, TCPConnector
from aiohttp.client_exceptions import (
    ClientConnectionError,
    ServerDisconnectedError,
    ClientPayloadError
)

import datetime as dt
import json
import logging
import re
import os
import uuid
import time
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from copy import deepcopy

from libs.authdb import get_session, keyload, NotAuthorized


class SymbolDBError(Exception):
    """Common exception for problems with SDB"""
    pass

class Months(Enum):
    F = 1 # Jan
    G = 2 # Feb
    H = 3 # Mar
    J = 4 # Apr
    K = 5 # May
    M = 6 # Jun
    N = 7 # Jul
    Q = 8 # Aug
    U = 9 # Sep
    V = 10 # Oct
    X = 11 # Nov
    Z = 12 # Dec

class SymbolDB:
    """Class for work with SymbolDB"""

    months = ('', 'F', 'G', 'H', 'J', 'K', 'M',
              'N', 'Q', 'U', 'V', 'X', 'Z')

    def __init__(
            self,
            env='prod',
            user=None,
            password=None,
            credentials=('%s/credentials.json' % os.path.expanduser('~')),
        ):
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
                self.logger.warning(
                    f'Provided file has no symboldb_editor session for {self.env}'
                )
        elif self.session_id is None:
            if user and password:
                self.session_id = get_session(user, password, 'symboldb', self.env)
        else:
            raise NotAuthorized('Either credentials file or user-pass must be provided')

        self.headers = {
            'Content-Type': 'application/json',
            'X-Auth-SessionId': self.session_id,
            'X-Use-Historical-Limits': 'false',
            'Accept-Encoding': 'gzip'
        }

        self.domain = 'zorg.sh'
        self.version = 'v1.0'
        self.url = f'http://symboldb.{self.env}.{self.domain}/symboldb-editor/api/{self.version}/'
        self.url_v2 = f'http://symboldb.{self.env}.{self.domain}/symboldb/api/v2.0/'


    def __repr__(self):
        return f'SymbolDB({self.env!r})'

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @retry(
        retry=retry_if_exception_type((
            ClientConnectionError,
            ServerDisconnectedError,
            ClientPayloadError,
            ClientResponseError
        )),
        stop=stop_after_attempt(10),
        wait=wait_fixed(10)
    )
    async def __request(
            self,
            method: str,
            handle: str,
            params: dict = None,
            jdata: dict = None,
            data=None,
            headers=None
        ):
        if not headers:
            headers = self.headers
        params = {} if not params else params
        # jdata = {} if not jdata else jdata
        
        async with ClientSession(connector=TCPConnector(limit=50, limit_per_host=30), headers=headers) as session:
            try:
                if method == 'get':
                    params = {
                        k: str(v) if not isinstance(v,(list,tuple)) else ','.join(v) for k, v
                        in params.items()
                    }
                self.logger.debug(f"{method.upper()} {self.url+handle}, {params=}, {jdata=}")
                async with session.request(
                    method,
                    self.url+handle,
                    params=params,
                    json=jdata,
                    data=data
                ) as response:
                    if response.status in [404, 400] or response.ok:
                        return await response.json(content_type=None)
                    else:
                        response.raise_for_status()
            except Exception as e:
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise e

    async def __request_no_retry(
            self,
            method: str,
            handle: str,
            params: dict = None,
            jdata: dict = None,
            data=None,
            headers=None
        ):
        if not headers:
            headers = self.headers
        params = {} if not params else params
        # jdata = {} if not jdata else jdata
        
        async with ClientSession(headers=headers) as session:
            try:
                if method == 'get':
                    params = {
                        k: str(v) if not isinstance(v,(list,tuple)) else ','.join(v) for k, v
                        in params.items()
                    }
                async with session.request(
                    method,
                    self.url+handle,
                    params=params,
                    json=jdata,
                    data=data
                ) as response:
                    if response.status in [404, 400] or response.ok:
                        return await response.json(content_type=None)
                    else:
                        response.raise_for_status()
            except asyncio.exceptions.TimeoutError:
                self.logger.warning(f'Response has been timeouted')
                return None
            except Exception as e:
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise e

    @retry(
        retry=retry_if_exception_type((
            ClientConnectionError,
            ServerDisconnectedError,
            ClientPayloadError,
            ClientResponseError
        )),
        stop=stop_after_attempt(10),
        wait=wait_fixed(10)
    )
    async def __request_v2(
            self,
            method: str,
            handle: str,
            params: dict = None,
            jdata: dict = None,
            data=None,
            headers=None
        ):
        if not headers:
            headers = self.headers
        params = {} if not params else params
        # jdata = {} if not jdata else jdata
        
        async with ClientSession(connector=TCPConnector(limit=50, limit_per_host=30), headers=headers) as session:
            try:
                if method == 'get':
                    params = {
                        k: str(v) if not isinstance(v,(list,tuple)) else ','.join(v) for k, v
                        in params.items()
                    }
                self.logger.debug(f"{method.upper()} {self.url+handle}, {params=}, {jdata=}")
                async with session.request(
                    method,
                    self.url_v2+handle,
                    params=params,
                    json=jdata,
                    data=data
                ) as response:
                    if response.status in [404, 400] or response.ok:
                        return await response.json(content_type=None)
                    else:
                        response.raise_for_status()
            except Exception as e:
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise e

    async def _get(self, handle, params=None):
        """
        wrapper method for requests.get
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        return await self.__request(method='get', handle=handle, params=params)

    async def _delete(self, handle, params=None):
        """
        wrapper method for requests.delete
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: requests response object
        """
        return await self.__request(method='delete', handle=handle, params=params)

    async def _post(self, handle, jdata=None, data=None, params=None, headers=None):
        """
        wrapper method for requests.post
        :param params:
        :param handle: backoffice api handle
        :param jdata: json to pass with request
        :return: requests response object
        """
        if headers is None:
            headers = self.headers
        return await self.__request(method='post', handle=handle, jdata=jdata, data=data, params=params, headers=headers)

    async def _put(self, handle, jdata, params=None):
        """
        wrapper method for requests.put
        :param params:
        :param handle: backoffice api handle
        :param jdata: json to pass with request
        :return: requests response object
        """
        return await self.__request(method='put', handle=handle, jdata=jdata, params=params)

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(1))
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

        result = asyncio.run(self.create(new_record))
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


# public methods

    # some handies
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

    @staticmethod
    def date_to_sdb(dto: dt.date, include_day: bool = True) -> dict:
        """
        method to convert date to valid symboldb format
        :param dto: datetime object
        :param maturity: build maturity date
        :param expiry: build date in format of expiry date
        :param name: build name
        :return: symboldb date
        """
        if include_day:
            r = {'day': dto.day, 'month': dto.month, 'year': dto.year}
        else:
            r = {'month': dto.month, 'year': dto.year}
        return r

    @staticmethod
    def sdb_to_date(date_dict: dict) -> dt.date:
        """
        takes dictionary with Month, Year, Day keys and returns
        datetime object
        :param date_dict: date dictionary
        :return: datetime object
        """
        try:
            return dt.date(year=date_dict.get('year'), month=date_dict.get('month'),
                                 day=date_dict.get('day', 1))
        except TypeError:
            return None

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

    async def get_tasks(self) -> list[dict]:
        return await self._get(f'tasks')

    async def get_update_size(self, _id) -> int:
        """
        get number of potentially affected
        instruments if _id gets updated
        :param _id: instrument (or most likely folder) uuid
        :return: number of affected children
        """
        response = await self._get(f'instruments/{_id}/deps')
        return response['affectedSymbols'] if response.get('affectedSymbols') else 1

    # single instrument methods
    async def get(self, data, fields: list = None) -> dict:
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
            return await self._get(f'instruments/{data}', params=params)

        elif isinstance(data, str):
            response = await self.get_v2(f'^{data}$', fields=fields)
            if response:
                return response[0]
            else:
                return {}
        else:
            raise RuntimeError('Only strings or lists of string supported for this method')

    async def get_compiled(self, data) -> dict:
        """
        get compiled instrument by id
        :param data: instrument id
        :return: compiled instrument
        """
        if isinstance(data, str) and self.is_uuid(data):
            response = await self._get(f'compiled_instruments/{data}')
            try:
                return response[0]
            except KeyError:
                return None
        else:
            return None

    async def update(self, data) -> dict:
        """
        updates single instrument
        :param data: data to be posted
        :return: id, rev of updated symbol
        """
        _id = data['_id']
        return await self._post(f'instruments/{_id}', jdata=data)

    async def create(self, data) -> dict:
        """
        creates single instrument
        :param data: data to be posted
        :return: id, rev of created symbol
        """
        return await self._post('instruments', jdata=data)

    async def delete_by_ids(self, _id) -> dict:
        """
        method to delete item by its id
        :param _id: symbol ID
        :return: deleted id and rev
        """
        return await self._delete(f'instruments/{_id}')

    async def get_uuid_by_path(self, path: list, tree: list = None) -> str:
        """
        gets uuid by list of names sorted as path
        :param path: list of names
        :param tree: previously received tree
        :return: id of symbol or None if path absents
        :raises: RuntimeError if path is ambiguous
        """
        if not tree:
            tree = await self.get_tree()
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

    async def find_in_tree(self, ticker) -> list[dict]:
        """
        method to find elements in symboldb tree by their name
        :param ticker: symbol tree name
        :return: symbol dict
        """
        return [x for x in await self.get_tree() if x['name'] == ticker]

    async def get_symbol_uuid(self, string) -> str:
        """
        method which returns symbol uuid
        :param string: request string
        :return: symbol uuid
        """
        if self.is_uuid(string):
            return string
        else:
            return await self.get(string).get('_id')

    async def get_ids_by_name(self, ticker) -> list[dict]:
        """
        method which returns symbol ids
        :param ticker: symbol ticker
        :return: list of found ids
        """
        ids = []
        results = await self.find_in_tree(ticker)
        if not results:
            return ids
        for item in results:
            ids.append({
                '_id': item['_id'],
                '_rev': self.get_latest_revision(item['_id'])
            })
        return ids

    async def get_latest_revision(self, _id) -> str:
        """
        get latest revision
        :param _id: symbol ID
        :return: latest revision
        """
        _rev = ''
        try:
            _rev = await self.get(_id)['_rev']
        except KeyError:
            pass
        return _rev

    async def get_historical(
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
        return await self._get('historical/instruments', params=params)

    # multiple instruments methods
    async def get_tree(self, fields: list = None) -> list[dict]:
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
        return await self._get(f'instruments', params=params)

    async def get_them_all(self) -> list[dict]:
        """
        gets ALL records from SymbolDB via erlang API
        :return: list
        """
        return await self._get(f'instruments')

    async def get_snapshot(
            self,
            symbol_id_regex: str = None,
            types: list = None,
            status: str = None,
            fields: list = None,
            last_event_id: str = None,
            broker_names: list= None
            ):
        
        params = {}
        if symbol_id_regex:
            params.update({
                'id_regexp': symbol_id_regex
            })
        if types:
            params.update({
                'symbolTypes': ','.join(types)
            })
        if status:
            params.update({
                'symbolStatus': status
            })
        if fields:
            params.update({
                'with': ','.join(fields)
            })
        if last_event_id:
            params.update({
                'lastEventId': last_event_id
            })
        if broker_names:
            params.update({
                'brokerProviders': ','.join(broker_names)
            })
        return await self.__request_v2(method='get', handle='snapshot', params=params)

    async def get_heirs(
            self,
            _id,
            full: bool = False,
            recursive: bool = False,
            fields: list = None,
            name: str = None,
            tree: list = None
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
        if not fields:
            fields = []
        default_fields = ['_id', 'name', 'isAbstract', 'path']
        fields.extend(default_fields)
        if tree:
            if recursive:
                heirs = [
                    x for x
                    in tree
                    if _id in x['path'][:-1]
                ]
            else:
                heirs = [
                    x for x
                    in tree
                    if len(x['path']) > 1
                    and x['path'][-2] == _id
                ]
            if full:
                return heirs
            else:
                return [
                    {
                        key: x.get(key) for key
                        in x
                        if key in fields
                    } for x
                    in heirs
                ]

        heirs = []
        fields = list(set(fields))
        fields_str = ','.join(fields) if not full else ''
        params = {
            'parentId': _id,
            'fields': fields_str,
            'symbolId_regexp': name
        }
        params = {k: v for k, v in params.items() if v}
        response = await self._get(f'instruments', params=params)
        heirs.extend(response)
        if recursive:
            recursive_heirs = await asyncio.gather(
                *[
                    self.get_heirs(
                        x['_id'],
                        full=full,
                        recursive=recursive,
                        fields=fields,
                        name=None
                    ) for x
                    in response
                    if x.get('isAbstract')
                ]
            )
            for rh in recursive_heirs:
                heirs.extend(rh)
        return heirs
    
    async def get_parents(self, _id, fields: list = None) -> list[dict]:
        """
        gets self dictionary plus all parents' dicts
        :param _id: uuid of the instrument
        :return: list of instruments' dicts
        """
        if not fields:
            fields = []
        default_fields = ['_id', 'name', 'path']
        params = {
            'childId': _id
        }
        fields_str = str()
        if fields:
            fields_str = ','.join(list(set(fields + default_fields)))
            params.update({
                'fields': fields_str
            })
        return await self._get('instruments', params=params)

    async def get_v2(
            self,
            exante_id,
            _id: str = None,
            fields: list = None,
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
        _id = f'/{_id}' if _id else ''
        return await self._get(f'instruments{_id}', params=params)
    
    async def get_by_shortname(
            self,
            description: str,
            fields: list = None,
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
        return await self._get(f'instruments', params=params)

    async def batch_update(
            self,
            input_data,
            data_type='instrument',
            action='update',
            purify=True,
            preserve_id=False,
            withDependents=False
        ) -> dict:
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
        
        data_block = 'query' if action == 'delete' else 'data'

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
        return await self.__request_no_retry(
            method='post',
            handle='batch',
            data=data,
            headers=headers
        )

    async def batch_create(self, input_data) -> dict:
        """
        merely just a wrapper for batch_update
        :param instruments: list of dict
        """
        return await self.batch_update(input_data, action='create')

    # schedules
    async def get_schedules(self, uuid: str = None, **kwargs) -> list[dict]:
        """
        returns schedule
        :param uuid: if specified, returns single schedule with specified id
        :return: list
        """
        uuid = f'/{uuid}' if uuid and self.is_uuid(uuid) else ''
        return await self._get(f'schedules{uuid}', params=kwargs)

    async def post_schedule(self, data: dict, _id = None) -> dict:
        """
        updates schedule
        :param data: data to be posted
        :return: id, rev of updated schedule
        """
        if 'content' in data.keys():
            data = data['content']
        if _id is None:
            _id = data.get('_id', '')
        if _id:
            _id = f'/{_id}'
        data = self.strip_id(data)
        return await self._post(f'schedules{_id}', jdata=data)

    # brokers
    async def get_brokers(self, _id: str = None) -> list[dict]:
        """
        returns list of broker providers
        :param _id: broker id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'broker_providers{_id}')

    async def post_broker(self, data: dict, _id: str = None) -> dict:
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
            _id = f'/{_id}'
        data = self.strip_id(data)
        return await self._post(f'broker_providers{_id}', jdata=data)

    async def get_broker_accounts(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of broker accounts
        :param _id: broker id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'broker_accounts{_id}', params=kwargs)

    # exchanges
    async def get_exchanges(self, **kwargs) -> list[dict]:
        """
        returns list of exchanges
        :return: list
        """
        return await self._get(f'exchanges', params=kwargs)

    # feeds
    async def get_feeds(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of feed providers
        :param _id: feed id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'feed_providers{_id}', params=kwargs)

    async def post_feed(self, data, _id: str = None) -> dict:
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
            data = self.strip_id(data)
            return await self._post(f'feed_providers{_id}', jdata=data)
        else:
            self.logger.error('feed provider id is not specified!')
            return None

    async def get_feed_gateways(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of feed gateways
        :param _id: feed id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'feed_gateways{_id}', params=kwargs)

    async def get_feed_providers(self, search_string='.*', **kwargs) -> list[dict]:
        """
        method which returns feed_providers by regexp
        :param search_string: regexp for search
        :return: list of providers which match to regexp
        """
        res = []
        for feed in await self.get_feeds(**kwargs):
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

    # execution schemes
    async def get_execution_schemes(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of execution schemes
        :param _id: execution scheme id to get only this particular id
        :return: list
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'execution_schemes{_id}', params=kwargs)

    # currencies
    async def get_currencies(self, _id: str = None, **kwargs) -> list[dict]:
        """
        gets list of currencies registered in SymbolDB
        :return: list of currencies
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'currencies{_id}', params=kwargs)

    async def post_currency(self, data, _id: str = None) -> dict:
        """
        post currency to sdb or update existing one if id
        :param data: data to be posted
        """
        _id = f'/{_id}' if _id else ''
        return await self._post(f'currencies{_id}', jdata=data)

    async def post_currencies_holidays(self, currency, date, is_holiday=True):
        """
        update currencies holidays list
        :param currency: currency to be posted
        :param date: date to be posted
        :param isHoliday: boolean
        :return: response status code
        """
        data = [{'currency': currency, 'date': date, 'isHoliday': is_holiday}]
        return await self._post(f'currency_holidays', jdata=data)

    async def batch_currencies_holidays(self, data: list):
        """
        update currencies holidays list
        :param currency: currency to be posted
        :param date: date to be posted
        :param isHoliday: boolean
        :return: response status code
        """
        verified = [
            x for x
            in data
            if x.get('currency')
            and x.get('date')
            and x.get('isHoliday') is not None
        ]
        return await self._post(f'currency_holidays', jdata=verified)

    # sections
    async def get_sections(self, _id: str = None, **kwargs) -> list[dict]:
        """
        gets list of exchange/schedule sections registered in SymbolDB
        :return: list of sections
        """
        _id = f'/{_id}' if _id else ''
        return await self._get(f'sections{_id}', params=kwargs)

    async def post_section(self, data, _id: str = None) -> dict:
        """
        post section to sdb or update existing one if id
        :param data: data to be posted
        """
        _id = f'/{_id}' if _id else ''
        return await self._post(f'sections{_id}', jdata=data)
