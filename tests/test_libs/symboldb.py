import asyncio
from copy import deepcopy
import datetime as dt
from enum import Enum
import json
import logging
import os
import re
import uuid

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
    def __init__(
            self,
            tree_path = f'{os.getcwd()}/libs/test_libs/selected_tree.json',
            sdb_lists_path = f'{os.getcwd()}/libs/test_libs/sdb_lists'
        ) -> None:
        self.tree_path = tree_path
        self.sdb_lists_path = sdb_lists_path
        with open(tree_path) as f:
            self.tree = json.load(f)

    def __repr__(self):
        return f'SymbolDB_test'

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

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
                return next((
                    {
                        key: x.get(key) for key
                        in x
                        if key in fields
                    } for x
                    in self.tree
                    if x['_id'] == data
                ), None)
            else:
                return next((
                    x for x
                    in self.tree
                    if x['_id'] == data
                ), None)

        elif isinstance(data, str):
            response = await self.get_v2(f'^{data}$', fields=fields)
            if response:
                return response[0]
            else:
                return {}
        else:
            raise RuntimeError(
                'Only strings supported for this method'
            )

    async def update(self, data) -> dict:
        uid = data['_id']
        instr_num = next((
            num for num, x
            in enumerate(self.tree)
            if x['_id'] == uid
        ), None)
        if instr_num is None:
            return {'_id': uid, 'message': 'instrument does not exist'}
        else:
            self.tree[instr_num] = data
            with open(self.tree_path, 'w') as f:
                json.dump(self.tree, f, indent=4)
            return {'_id': uid, '_rev': 'some_rev'}

    async def create(self, data) -> dict:
        # ! create symboldb-like id
        uid = uuid.uuid4().hex
        instr = deepcopy(data)
        instr['_id'] = uid
        self.tree.append(instr)
        with open(self.tree_path, 'w') as f:
            json.dump(self.tree, f, indent=4)
        return {'_id': uid, '_rev': 'some_rev'}

    async def get_tree(self, fields: list = None) -> list[dict]:
        if not fields:
            fields = []
        if fields and fields[0] == 'all':
            return self.get_them_all()
        default_fields=['path', 'name', '_id', 'isTrading', 'isAbstract']
        fields.extend(default_fields)
        tree = [
            {
                key: x.get(key) for key
                in fields
                if key not in ['symbolId', 'expiryTime']
            } for x
            in self.tree
        ]
        return tree

    async def get_them_all(self) -> list[dict]:
        """
        gets ALL records from SymbolDB via erlang API
        :return: list
        """
        return [
            {
                key: val for key, val
                in x.items()
                if key not in ['symbolId', 'expiryTime']
            } for x in self.tree
        ]

    async def get_heirs(
            self,
            _id,
            full: bool = False,
            recursive: bool = False,
            fields: list = None,
            tree: list[dict] = None
        ):
        if not fields:
            fields = []
        default_fields = ['_id', 'name', 'isAbstract']
        fields.extend(default_fields)
        if recursive:
            return [
                {
                    key: x.get(key) for key
                    in x
                    if key in fields
                } for x
                in self.tree
                if _id in x['path'][:-1]
            ]
        else:
            return [
                {
                    key: x.get(key) for key
                    in x
                    if key in fields
                } for x
                in self.tree
                if len(x['path']) > 1
                and x['path'][-2] == _id
            ]

    async def get_parents(self, _id, fields: list = None) -> list[dict]:
        if not fields:
            fields = []
        instrument = next((x for x in self.tree if x['_id'] == _id), None)
        if not instrument:
            return None
        parents = [x for x in self.tree if x['_id'] in instrument['path']]
        if fields:
            parents = [
                {
                    key: x.get(key) for key
                    in x
                    if key in fields
                } for x
                in parents
            ]
        return parents

    async def get_v2(
            self,
            exante_id,
            _id: str = None,
            fields: list = None,
            is_expired: bool = None,
            is_trading: bool = None
        ) -> list[dict]:
        # could not implement is_trading here :(
        if _id:
            return next((x for x in self.test_tree), None)
        if is_expired is False:
            instruments = [
                x for x
                in self.test_tree
                if x['expiryTime']
                and dt.datetime.fromisoformat(x['expiryTime'][:-1]) > dt.datetime.utcnow()
            ]
        elif is_expired is True:
            instruments = [
                x for x
                in self.test_tree
                if x['expiryTime']
                and dt.datetime.fromisoformat(x['expiryTime'][:-1]) < dt.datetime.utcnow()
            ]
        if fields:
            instruments = [
                {key: x.get('key') for key in fields} for x
                in self.test_tree
                if x['symbolId']
                and re.search(exante_id, x['symbolId'])
            ]
        else:
            instruments = [
                {
                    key: val for key, val
                    in x.items()
                    if key not in ['symbolId', 'expiryTime']
                } for x
                in self.test_tree
                if x['symbolId']
                and re.search(exante_id, x['symbolId'])
            ]
        return instruments

    async def batch_update(
            self,
            input_data,
            data_type='instrument',
            action='update',
            purify=True,
            preserve_id=False,
            withDependents=False
        ) -> dict:
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

        if action == 'create':
            for el in input_data:
                if el.get('_id') and el['_id'] in [x['_id'] for x in self.tree]:
                    return {'error': 'error', 'message': 'already exists'}
                el.update({
                    '_rev': 'some_rev',
                    '_creationTime': dt.datetime.utcnow() + 'Z',
                    '_lastUpdateTime': dt.datetime.utcnow() + 'Z'
                })
                if not preserve_id or not el.get('_id'):
                    el.update({
                        '_id': uuid.uuid4().hex
                    })
                self.tree.append(el)
            with open(self.tree_path, 'w') as f:
                json.dump(self.tree, f, indent=4)
            return ''
        elif action == 'update':
            for el in input_data:
                el_num = next((num for num, x in enumerate(self.tree) if x['_id'] == el.get('id')), None)
                if el_num is None:
                    return {'error': 'error', 'message': 'instrument does not exist'}
            for el in input_data:
                el_num = next((num for num, x in enumerate(self.tree) if x['_id'] == el.get('id')), None)
                self.tree[el_num] = el
                el.update({
                    '_rev': 'some_rev',
                    '_lastUpdateTime': dt.datetime.utcnow() + 'Z'
                })
            with open(self.tree_path, 'w') as f:
                json.dump(self.tree, f, indent=4)
            return ''
        elif action == 'delete':
            to_del = []
            for el in input_data:
                el_num = next((num for num, x in enumerate(self.tree) if x['_id'] == el.get('id')), None)
                if el_num is None:
                    return {'error': 'error', 'message': 'instrument does not exist'}
            for el in input_data:
                el_num = next((num for num, x in enumerate(self.tree) if x['_id'] == el.get('id')), None)
                to_del.append(el_num)
                if withDependents:
                    to_del.extend([num for num, x in enumerate(self.tree) if el['_id'] in x['path']])
            self.tree = [x for num, x in enumerate(self.tree) if num not in to_del]
            with open(self.tree_path, 'w') as f:
                json.dump(self.tree, f, indent=4)
            return ''

    async def batch_create(self, input_data) -> dict:
        """
        merely just a wrapper for batch_update
        :param instruments: list of dict
        """
        return await self.batch_update(input_data, action='create')

    async def get_schedules(self, uuid: str = None, **kwargs) -> list[dict]:
        """
        returns schedule
        :param uuid: if specified, returns single schedule with specified id
        :return: list
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'schedules.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        return cached

    async def get_broker_accounts(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of broker accounts
        :param _id: broker id to get only this particular id
        :return: list
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'accounts.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        if _id is None: 
            return cached
        else:
            return next((x for x in cached if x['_id'] == _id), None)

    async def get_exchanges(self, **kwargs) -> list[dict]:
        """
        returns list of exchanges
        :return: list
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'exchanges.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        return cached

    async def get_feed_gateways(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of feed gateways
        :param _id: feed id to get only this particular id
        :return: list
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'gateways.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        if _id is None: 
            return cached
        else:
            return next((x for x in cached if x['_id'] == _id), None)

    async def get_execution_schemes(self, _id: str = None, **kwargs) -> list[dict]:
        """
        returns list of execution schemes
        :param _id: execution scheme id to get only this particular id
        :return: list
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'execution_schemes.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        if _id is None: 
            return cached
        else:
            return next((x for x in cached if x['_id'] == _id), None)

    async def get_currencies(self, _id: str = None, **kwargs) -> list[dict]:
        """
        gets list of currencies registered in SymbolDB
        :return: list of currencies
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'currencies.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        if _id is None: 
            return cached
        else:
            return next((x for x in cached if x['_id'] == _id), None)

    async def get_sections(self, _id: str = None, **kwargs) -> list[dict]:
        """
        gets list of exchange/schedule sections registered in SymbolDB
        :return: list of sections
        """
        cached = []
        with open(f"{'/'.join([self.sdb_lists_path, 'sections.jsonl'])}", 'r') as f:
            for line in f:
                cached.append(json.loads(line))
        if _id is None: 
            return cached
        else:
            return next((x for x in cached if x['_id'] == _id), None)
