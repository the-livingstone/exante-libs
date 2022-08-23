#!/usr/bin/env python3
import ast
import asyncio
from copy import copy, deepcopy
import datetime as dt
from enum import Enum
import json
import os
import logging
import lupa
import pandas as pd
import numpy as np
import pytz
import re
from libs.backoffice import BackOffice
from libs.async_symboldb import SymbolDB
from libs.terminal_tools import (
    pick_from_list,
    pick_from_list_tm,
    colorize,
    sorting_expirations,
    ColorMode,
    StatusColor
)
from typing import Any, Union

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

class Environments(Enum):
    PROD = 'prod'
    CPROD = 'cprod'
    DEMO = 'demo'
    STAGE = 'stage'

class SdbLists(Enum):
    EXCHANGES = 'exchanges'
    EXECSCHEMES = 'execution_schemes'
    ACCOUNTS = 'accounts'
    GATEWAYS = 'gateways'
    SCHEDULES = 'schedules'
    CURRENCIES = 'currencies'
    SECTIONS = 'sections'

    EXECUTION_TO_ROUTE = 'execution_to_route'
    FEED_PERMISSIONS = 'feed_permissions'

    FEED_PROVIDERS = 'feed_providers'
    BROKER_PROVIDERS = 'broker_providers'

    STOCK_RICS = 'stock_rics'
    USED_SYMBOLS = 'used_symbols'
    USED_SYMBOLS_DEMO = 'used_symbols_demo'
    TREE = 'tree'

ROOT_FOLDERS = {
    'prod': '0509b7989c5a565c815c6ef657454f2d',
    'demo': '0509b7989c5a565c815c6ef657454f2d',
    'cprod': '2d1040e2962c4bab9fcf9d66af4bfb49',
    'stage': '0509b7989c5a565c815c6ef657454f2d'
}

class SDBAdditional:
    # paths to cache files and lua stdlib
    cache_conf = {
        'exchanges': {
            'expiry': dt.timedelta(days=3)
        },
        'execution_schemes': {
            'expiry': dt.timedelta(days=3)
        },
        'accounts': {
            'expiry': dt.timedelta(days=3)
        },
        'gateways': {
            'expiry': dt.timedelta(days=3)
        },
        'schedules': {
            'expiry': dt.timedelta(days=3)
        },
        'currencies': {
            'expiry': dt.timedelta(days=3)
        },
        'sections': {
            'expiry': dt.timedelta(minutes=120)
        },
        'tree': {
            'expiry': dt.timedelta(minutes=120)
        },
        'used_symbols': {
            'expiry': dt.timedelta(minutes=120)
        },
        'used_symbols_demo': {
            'expiry': dt.timedelta(minutes=120)
        },
        'stock_rics': {
            'expiry': dt.timedelta(days=30)
        },
        'execution_to_route': {
            'expiry': dt.timedelta(days=30)
        },
        'feed_permissions': {
            'expiry': dt.timedelta(days=30)
        }
    }

    tree = []
    used_symbols = []
    used_symbols_demo =[]
    stock_rics = []
    execution_to_route = []
    tree_df = pd.DataFrame()

    def __init__(
            self,
            env: str = 'prod',
            sdb: SymbolDB = None,
            bo: BackOffice = None,
            nocache: bool = False,
            test: bool = False
        ) -> None:
        self.env = env
        self.sdb = sdb if sdb else SymbolDB(env)
        self.bo = bo if bo else BackOffice(env)
        self.test = test
        self.nocache = nocache
        self.current_dir = os.getcwd()
        self.instrument_cache = []
        if self.current_dir == '/':
            self.current_dir = '/home/instsupport/airflow/dags'
        self.current_dir = self.current_dir if self.current_dir[-1] != '/' else self.current_dir[:-1]
        self.lua_lib = f'{self.current_dir}/libs/stdlib.lua'
        self.lua = lupa.LuaRuntime(unpack_returned_tuples=True)
        # load lua
        try:
            with open(self.lua_lib, 'r') as f:
                self.lua.execute(f.read())
        except FileNotFoundError:
            self.logger.warning(f'{self.lua_lib} not found! Lua templates compilation may not work')
        except lupa._lupa.LuaError as e:
            self.logger.warning(f'invalid {self.lua_lib} file: {e}')
            self.logger.warning('Lua templates compilation may not work')
        asyncio.run(self.__cached_lists())
        pass


    async def __cached_lists(self):
        # smaller lists we download wtihout asking
        # bigger ones (like USED_SYMBOLS or TREE) we download on demand
        collect_tasks = []
        for l in list(SdbLists.__members__)[:9]:
            collect_tasks.append(
                self.__load_cache(SdbLists[l], silent=True)
            )
        (
            self.sdb_exchs,
            self.sdb_execs,
            self.sdb_accs,
            self.sdb_gws,
            self.sdb_scheds,
            self.sdb_currencies,
            self.sdb_sections,
            self.execution_to_route,
            self.feed_perms
        ) = await asyncio.gather(*collect_tasks)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __check_file_path(self, file_path: list):
        for i in range(2, 4):
            if not os.path.exists('/'.join(file_path[:i])):
                os.mkdir('/'.join(file_path[:i]))



    async def __load_cache(self, list_name: SdbLists, env=None, silent=False, df=False):
        if df:
            cached = pd.DataFrame()
        else:
            cached = []

        if list_name not in SdbLists:
            raise RuntimeError(f'{list_name} is not in cache config')
        if env is None:
            env = self.env
        if env == 'demo' and list_name not in [SdbLists.USED_SYMBOLS, SdbLists.FEED_PERMISSIONS]:
            env = 'prod'
        file_path = [
            self.current_dir,
            'cache',
            env,
            f"{list_name.value}.jsonl"
        ]
        self.__check_file_path(file_path)
        if not os.path.exists('/'.join(file_path)):
            if not silent:
                self.logger.warning(
                    f"{'/'.join(file_path)} cache file is not found, "
                    "cache will be refreshed in runtime"
                )
            return cached
        last_update = dt.datetime.fromtimestamp(
            os.path.getctime(
                f"{'/'.join(file_path)}"
            )
        )
        if (dt.datetime.now() - last_update) > self.cache_conf[list_name.value]['expiry']:
            if not silent:
                self.logger.info(f"{list_name} cache file is out of date ({last_update.isoformat()=}), loading from sdb (or BO)")
            return cached
        if df:
            try:
            # for i in 'a':
                cached_df = pd.read_json(
                    f"{'/'.join(file_path)}",
                    lines=True
                ).replace({np.nan: None})
                return cached_df
            except Exception as e:
                self.logger.warning(f"{e.__class__.__name__}: {list_name.value} cache is not loaded")
                return pd.DataFrame()
        else:
            try:
                with open(f"{'/'.join(file_path)}", 'r') as f:
                    for line in f:
                        cached.append(json.loads(line))
                return cached
            except json.decoder.JSONDecodeError:
                if not silent:
                    self.logger.warning(
                        f"{self.cache_conf[list_name.value]['path']} cache file is malformed, cache will be refreshed in runtime"
                    )
                return cached

    async def __write_cache(self, list_name: SdbLists, payload: Union[list[dict], pd.DataFrame], env: str = None):
        if env is None:
            env = self.env
        if env == 'demo' and list_name not in [SdbLists.USED_SYMBOLS, SdbLists.FEED_PERMISSIONS]:
            env = 'prod'
        file_path = [
            self.current_dir,
            'cache',
            env,
            f"{list_name.value}.jsonl"
        ]
        self.__check_file_path(file_path)
        try:
            if isinstance(payload, list):
                with open(f"{'/'.join(file_path)}", 'w') as f:
                    for p in payload:
                        f.write(json.dumps(p))
                        f.write('\n')
            elif isinstance(payload, pd.DataFrame):
                payload.to_json('/'.join(file_path), 'records', lines=True)
        except Exception as e:
            self.logger.warning(f"{e.__class__.__name__}: {list_name.value} cache is not updated")

    def __update_instrument_cache(self, instruments: list[dict]):
        for instr in instruments:
            if not all(
                (key in instr) for key
                in ['_id', '_rev', '_creationTime', '_lastUpdateTime']
            ): # not a strict verification that instrument is fully loaded (not selected fields only)
                continue
            existing_num = next((
                num for num, x
                in enumerate(self.instrument_cache)
                if x['_id'] == instr['_id']
            ), None)
            # new
            if existing_num is None:
                self.instrument_cache.append(deepcopy(instr))
            # existing
            else:
                self.instrument_cache[existing_num] = deepcopy(instr)


    def browse_folders(self, input_path: list = None, message: str = None, only_folders: bool = False, allowed: list = None) -> str:
        """
        method to navigate through the symboldb tree in interactive fashion
        :param input_path: the point where to start browsing, path of an instrument or '.'
        :param message: some text to display in the the header of menu
        :param only_folders: don't show non-abstract instruments
        :return: final destination uuid
        """
        # some visual decorations

        def decorate(parents: list, heirs: list):
            indent = ''
            indent_symbols = {'trunk': '│', 'leaf': '├', 'last': '└'}
            state_symbols = {'not_trading': '░','expired': '▒','active': '▓'}

            if len(parents) > 1:
                for d in range(len(parents) - 2):
                    indent += indent_symbols['trunk']
                indent += indent_symbols['leaf']
            for num, h in enumerate(heirs):
                if indent and num == len(heirs) - 1:
                    indent = indent[:-1] + indent_symbols['last']
                if h['isTrading'] is False:
                    state = state_symbols['not_trading']
                elif self.isexpired(h):
                    state = state_symbols['expired']
                else:
                    state = state_symbols['active']
                if h.get('isAbstract'):
                    heirs[num]['prefix'] = f"{state} {indent}»"
                else:
                    heirs[num]['prefix'] = f"{state} {indent}·"
            if only_folders:
                count_instruments = len([x for x in all_heirs if not x.get('isAbstract')])
                heirs.append({
                    'prefix': f"  {indent}",
                    'name': f"({count_instruments} instruments are here)",
                    '_id': parents[-1]['_id']
                })
            for p in reversed(parents):
                heirs.insert(0, p)
            heirs.append({'name': '..', '_id': '..', 'prefix': ''})

        parents = []
        if not allowed:
            allowed = []
        if not input_path:
            input_path = []
        tree = asyncio.run(self.load_tree(fields=['expiryTime', 'expiry']))
        parents.append(deepcopy(next(x for x in tree if len(x['path']) == 1)))
        parents[0].update({
            'prefix': ''
        })
        selected = None
        if input_path and input_path[0] in ['.', 'Root']:
            input_path.pop(0)
        initial_path = deepcopy(input_path)
        while True:
            is_trading = parents[-1].get('isTrading')
            expiry_time = parents[-1].get('expiryTime')
            # go through the given path: pass the items one by one to the pick_from_list_tm
            # as "specify" argument
            pick = input_path.pop(0) if input_path else None

            # if we are on the last item of initial_path let's show only items from allowed list (instead of all available)
            if (
                allowed
                and len(parents) - 1 == len(initial_path)
                and not next((
                    x for num, x
                    in initial_path
                    if x not in parents[num+1]['name']
                    ), False)
                ):
                all_heirs = sorted(
                    [
                        deepcopy(x) for x
                        in tree
                        if len(x['path']) > 1
                        and x['path'][-2] == parents[-1]['_id']
                        and x['name'] in allowed
                    ],
                    key=lambda e: e['name']
                )
            else:
                all_heirs = sorted(
                    [
                        deepcopy(x) for x
                        in tree
                        if len(x['path']) > 1
                        and x['path'][-2] == parents[-1]['_id']
                    ],
                    key=lambda e: e['name']
                )
            # let's inherit isTrading and expiryTime
            for h in all_heirs:
                if h.get('isTrading') is None:
                    h['isTrading'] = is_trading
                if h.get('expiryTime') is None:
                    h['expiryTime'] = expiry_time
            # show only folders if requested
            if only_folders:
                heirs = [x for x in all_heirs if x.get('isAbstract')]
            else:
                heirs = all_heirs
            
            # add some computor graphics
            decorate(parents, heirs)
            if not message:
                message = self.show_path([x['_id'] for x in parents])
            
            # show menu
            selected = pick_from_list_tm(
                [
                    (f"{x['prefix']} {x['name']}", x['_id'], x['name']) for x
                    in heirs
                ],
                option_name='instruments',
                message=message,
                specify=pick,
                cursor_index=len(parents)
            )

            # what to do with selection
            if selected is None:
                return None, None
            # go one level up
            elif heirs[selected]['_id'] == '..':
                if len(parents) > 1:
                    parents = parents[:-1]
                else:
                    return None, None
            # if one of the parents (except for the last one) is selected go to its heirs
            elif selected < len(parents) - 1:
                parents = parents[:selected + 1]
            # if selected is not a folder or it is the lowest level parent return it
            elif heirs[selected].get('isAbstract') == False or heirs[selected]['_id'] == parents[-1]['_id']:
                sym_id = (heirs[selected]['name'], heirs[selected]['_id'])
                return sym_id
            else:
                parents.append(heirs[selected])
    
    async def build_inheritance(self, payload, include_self=False, cache: list[dict] = None) -> dict:
        """
        build the full dict of instrument with all inherited properties
        :param payload: uuid or symbolId or dict of instrument or full list of instruments to be compiled
        :param include_self: include the given instrument properties or build parent folder properties
        :param cache: already downloaded list of instruments to avoid unnecessary http_requests 
        :return: full dict of inherited properties
        """

        def go_deeper(child, compiled):
            # just check fields one by one
            # · if the field is not present in compiled copy it from child 
            # · if the field is simple type (str, bool or int) override parent value with child's one
            # · if the field is complex type (dict or list) look inside
            if isinstance(child, dict):
                for ckey, cval in child.items():
                    # if field is not present in parent copy it from child, no matter what type is it
                    if not ckey in compiled:
                        compiled[ckey] = cval
                    # sometimes happens that parent has some str type value
                    # but child has $template dict in the same place
                    # we should always make preference for child's value 
                    elif isinstance(cval, dict) and cval.get('$template') and isinstance(compiled[ckey], str):
                        compiled[ckey] = cval
                    # here we don't bother what type the field is
                    else:
                        compiled[ckey] = go_deeper(cval, compiled[ckey])
                return compiled
            # · sometimes members order doesn't matter (like in forbiddenTags list)
            #   and the child list overrides inherited completely (if specified)
            # · in other cases the items order is imporant (like feed gateways or broker acccounts),
            #   so it needs special attention:
            #   - not mentioned members are inherited from parents
            #   - specified members in child stand before inherited members,
            #     whether they have new settings or same as parent's
            #   - members in list do not duplicate. That means, if there is inherited member X
            #     in the middle of the list and it also specified in child, it doesn't appear twice
            #     in list, it just moves up according to child list position
            elif isinstance(child, list) and compiled:
                simple_list = False
                for ch in reversed(child):
                    if not isinstance(ch, dict):
                        simple_list = True
                        break
                    child_id_type = next((x for x in ['account', 'gateway'] if child[0].get(x)), None)
                    if not child_id_type:
                        simple_list = True
                        break
                    child_id_type += 'Id'
                    child_id = ch[child_id_type]
                    num = next((n for n, x in enumerate(compiled) if x[child_id_type] == child_id), None)
                    if num is not None:
                        compiled[num] = go_deeper(ch, compiled[num])
                        compiled.insert(0, compiled.pop(num))
                    else:
                        compiled.insert(0, ch)
                if simple_list:
                    return deepcopy(child)
                else:
                    return compiled
            # if there is nothing to inherit take child settings
            else:
                return copy(child)

        if cache is None:
            cache = []
        cache.extend([
            x for x
            in self.instrument_cache
            if x['_id'] not in [
                y['_id'] for y in cache
            ]
        ])
        compilation = {}
        exclude = [
            '_rev',
            '_lastUpdateTime',
            '_id',
            '_creationTime',
            'name'
        ]
        parents = []
        if isinstance(payload, str):
            if cache and payload in [x['_id'] for x in cache if x.get('_id')]:
                instrument = deepcopy(
                    next(x for x in cache if x['_id'] == payload)
                )
            else:
                instrument = await self.sdb.get(payload)
                if not instrument.get('_id'):
                    return {}
                self.__update_instrument_cache([instrument])
        elif isinstance(payload, dict): # build inheritance for a given dict
            instrument = deepcopy(payload)
        elif isinstance(payload, list) and len(payload) and all(isinstance(x, dict) for x in payload):
            with_ids = sorted([x for x in payload if x.get('_id')], key=lambda p: len(p['path']))
            without_ids = sorted([x for x in payload if not x.get('_id')], key=lambda p: len(p['path']))
            parents = deepcopy(with_ids + without_ids)
            instrument = parents[-1]
        else:
            return {}
        if not parents and not instrument.get('_id'):
            gather = []
            for p in instrument['path']:
                if cache and p in [x['_id'] for x in cache if x.get('_id')]:
                    parents.append(
                        next(deepcopy(x) for x in cache if x['_id'] == p)
                    )
                else:
                    gather.append(self.sdb.get(p))
            if gather:
                loaded = await asyncio.gather(*gather)
                self.__update_instrument_cache(loaded)
                parents.extend(loaded)
            parents = sorted(parents, key=lambda p: len(p['path']))
        elif not parents:
            if cache and all(
                p in [x['_id'] for x in cache] for p
                in instrument['path'][:-1]
            ):
                parents = [x for x in cache if x['_id'] in instrument['path'][:-1]]
                parents.append(instrument)
            else:
                parents = await self.sdb.get_parents(instrument['_id'])
                self.__update_instrument_cache(parents)
            parents = sorted(parents, key=lambda p: len(p['path']))[:-1]
        if include_self:
            exclude = []
            if not isinstance(payload, list):
                parents.append(instrument)
        for p in parents:
            compilation.update(go_deeper(p, compilation))
        compilation = {key: val for key, val in compilation.items() if key not in exclude}
        return compilation

    def compile_expiry_time(self, instrument, compiled=False, cache=None):
        if not compiled:
            compiled_instrument = asyncio.run(self.build_inheritance(instrument, include_self=True, cache=cache))
        else:
            compiled_instrument = instrument
        schedule_tz = next((
            x[2] for x
            in asyncio.run(
                self.get_list_from_sdb(
                    SdbLists.SCHEDULES.value,
                    additional_fields=['timezone']
                )
            )
            if x[1] == compiled_instrument.get('scheduleId')
        ), None)
        expiration_date = self.sdb.sdb_to_date(compiled_instrument.get('expiry', {}))
        if not schedule_tz or not expiration_date:
            return None
        localized_exp = pytz.timezone(schedule_tz).localize(
            dt.datetime.combine(
                expiration_date,
                dt.time.fromisoformat(
                    compiled_instrument['expiry'].get('time', '00:00:00')
                )
            )
        )
        expiry_time = (localized_exp - localized_exp.utcoffset()).strftime('%Y-%m-%dT%X.000Z')
        return expiry_time

    def compile_symbol_id(self, instrument, compiled=False, strike: str = 'B*', cache=None):
        if isinstance(instrument, dict) and instrument.get('isAbstract'):
            return None
        if not compiled:
            compiled_instrument = asyncio.run(self.build_inheritance(instrument, include_self=True, cache=cache))
        else:
            compiled_instrument = instrument
        if compiled_instrument.get('isAbstract'):
            return None
        exchange_name = next((
            x[0] for x
            in asyncio.run(self.get_list_from_sdb(SdbLists.EXCHANGES.value))
            if x[1] == compiled_instrument.get('exchangeId')
        ), None)
        instrument_type = compiled_instrument.get('type')
        if not exchange_name or not instrument_type:
            return None
        if instrument_type in ['FX_SPOT', 'FOREX']:
            ticker = f"{compiled_instrument.get('baseCurrency')}/{compiled_instrument.get('currency')}"
        else:
            ticker = compiled_instrument.get('ticker')
        symbol_id = f"{ticker}.{exchange_name}"
        if instrument_type in ['FX_SPOT', 'FOREX', 'BOND', 'CFD', 'FUND', 'STOCK']:
            pass
        elif instrument_type == 'CALENDAR_SPREAD':
            try:
                near_symbolic = (
                    f"{Months(compiled_instrument['nearMaturityDate']['month']).name}"
                    f"{compiled_instrument['nearMaturityDate']['year']}"
                )
                far_symbolic = (
                    f"{Months(compiled_instrument['farMaturityDate']['month']).name}"
                    f"{compiled_instrument['farMaturityDate']['year']}"
                )
            except KeyError:
                return None
            if compiled_instrument.get('spreadType') == 'FORWARD':
                symbol_id += ".CS/"
            elif compiled_instrument.get('spreadType') == 'REVERSE':
                symbol_id += f".RS/"
            else:
                return None
            symbol_id += f"{near_symbolic}-{far_symbolic}"
        else:
            try:
                symbolic = (
                    f"{compiled_instrument['maturityDate'].get('day', '')}"
                    f"{Months(compiled_instrument['maturityDate']['month']).name}"
                    f"{compiled_instrument['maturityDate']['year']}"
                )
            except KeyError:
                if compiled_instrument.get('maturityName'):
                    symbol_id += f".{compiled_instrument['maturityName']}"
                    return symbol_id
                else:
                    return None
            symbol_id += f".{symbolic}"
            if instrument_type == 'OPTION':
                symbol_id += f".{strike}"
        if compiled_instrument.get('maturityName'):
            symbol_id += f".{compiled_instrument['maturityName']}"
        return symbol_id

    def find_target_instrument(self, search_string: str, is_shortname: bool = False):
        """
        returns dict
        {
            '_id': uuid,
            'symbol_type': Optional[str],
            'is_expired': Optional[]
            'columns': Optional[list]
        }
        """
        sym_uuid = None
        input_path = search_string.split('/')
        tree = asyncio.run(self.load_tree(
            fields=[
                'expiryTime',
                'description',
                'type'
            ]
        ))
        first_level = [
            x['name'] for x in tree if len(x['path']) == 2
        ]
        first_level.append('.')
        # there is an option to browse the instrument through the instrument tree
        # dunno if it's really useful
        if input_path[0] in first_level:
            browsed = self.browse_folders(input_path)
            if browsed[1] is None:
                print()
                print('(×_×)')
                return None

            sym_uuid = {
                'columns': [
                    browsed[0]
                ],
                '_id': browsed[1]
            }
            return sym_uuid
        # another option is to directly specify the instrument uuid
        if self.sdb.is_uuid(search_string):
            instrument = asyncio.run(self.sdb.get(search_string))
            if instrument:
                self.__update_instrument_cache([instrument])
                symbol_id = self.compile_symbol_id(instrument)
                sym_uuid = {
                    'columns': [
                        symbol_id if symbol_id else instrument['name']
                    ],
                    '_id': search_string
                }
                return sym_uuid
        # oh wow search by shortname works
        if is_shortname:
            search_sdb = asyncio.run(
                self.sdb.get_by_shortname(
                    search_string,
                    fields=[
                        'expiryTime',
                        'exchangeId',
                        'ticker',
                        'symbolId',
                        'description',
                        'type',
                        '_id',
                        'isAbstract',
                        'path'
                    ]
                )
            )
        else:
            search_sdb = asyncio.run(self.sdb.get_v2(
                input_path[0],
                fields=[
                    'expiryTime',
                    'exchangeId',
                    'ticker',
                    'symbolId',
                    'description',
                    'type',
                    '_id',
                    'isAbstract',
                    'path'
                ]
            ))
        for sym in search_sdb:
            if sym['isAbstract']:
                sym.update({
                    'symbolId': sym['name']
                })
            # if we request the exact name of instrument we won't bother with suggestions list
        if len(search_sdb) == 1:
            sym_uuid = {
                'columns': [
                    search_sdb[0]['symbolId']
                ],
                '_id': search_sdb[0]['_id']
            }
            return sym_uuid
        srch = list()
        for entry in search_sdb:
            entry_type = asyncio.run(self.get_instrument_type(entry['_id']))
            is_expired = True if entry.get('expiryTime')\
                and dt.datetime.fromisoformat(entry['expiryTime'][:-1]) < dt.datetime.now()\
                    else False
            if entry['_id'] in [x['_id'] for x in srch]:
                continue
            # we indicate non-abstract instruments with '·' sign:
            # · INSTRUMENT
            if entry['isAbstract']:
                srch.append({
                    'columns': [
                        f"» {self.show_path(entry['path'])}",
                        ''
                    ],
                    '_id': entry['_id'],
                    'symbol_type': entry_type,
                    'is_expired': is_expired
                })
            else:
                entry_payload = {
                    'columns': [
                        f"· {entry['symbolId']}",
                    ],
                    '_id': entry['_id'],
                    'symbol_type': entry_type,
                    'is_expired': is_expired
                }
                if entry.get('description') is not None:
                    entry_payload['columns'].append(entry['description'])
                    if is_shortname:
                        match = re.search(
                            rf'{input_path[0].lower()}',
                            entry['description'].lower()
                        )
                        entry_payload.update({'highlight': match.span()})
                else:
                    entry_payload['columns'].append('')
                srch.append(entry_payload)
            if entry['path'][-2] not in [x['_id'] for x in srch]:
                # we indicate folders with '»' sign and also write the path:
                # » PATH → TO → THE → FOLDER
                srch.append({
                    'columns': [
                        f"» {self.show_path(entry['path'][:-1])}",
                        ''
                    ],
                    '_id': entry['path'][-2],
                    'symbol_type': entry_type,
                    'is_expired': is_expired
                })
        if not srch:
            self.logger.error(f'nothing was found for {input_path[0]}')
            return None
        srch = sorted(srch, key=lambda e: sorting_expirations(e['columns'][0]))
        if len(input_path) > 1:
            specify = input_path[1]
        else:
            specify = None
        selected = pick_from_list(srch, 'instruments', specify=specify, color=True)
        if selected is None:
            return None
        sym_uuid = srch[selected]
        return sym_uuid

    def generate_lambda_futures(self, instrument, second_sequence: bool):
        '''
        Function generates sequences of future contracts
        :param instrument: ticker and exchange on underlying contract
        (i.e. RTS.FORTS)
        :param second: True if you need second sequence shifted for one contract
        :return: part of dict from root of instrument, containing sequence in lambda provider overrides
        '''

        def assign_to_path(dictionary: dict, path: list, value=dict()):
            for item in path[:-1]:
                dictionary = dictionary.setdefault(item, dict())
            dictionary[path[-1]] = value

        lambda_provider_id = next(
            (
                x[1] for x
                in asyncio.run(self.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value))
                if x[0] == 'LAMBDA'
            ), None)
        if not lambda_provider_id:
            self.logger.error(f'Environment {self.env} has no lambda gateway')
            return {}

        # Regexp '{}[^CPS]+' used to filter out options and VIX.CBOE.STRADDLE
        # instruments. All off there three letters are not used as month codes.
        if self.tree_df.empty:
            asyncio.run(self.load_tree())
        
        future_named = self.tree_df.loc[self.tree_df['name'] == 'FUTURE']
        future_folder_id = future_named[future_named.apply(lambda x: len(x['path']) == 2, axis=1)].iloc[0]['_id']
        futures = [
            x for x
            in asyncio.run(self.sdb.get_v2(
                rf'{instrument}[^CPS]+$',
                fields=['symbolId', 'expiryTime', 'path'],
                is_expired=False
            ))
            if x['path'][1] == future_folder_id
        ]
        if not futures:
            self.logger.warning(f'No active futures found for {instrument}')
            return {}

        first = []
        for future in futures:
            first.append({
                'instrumentId': future['symbolId'],
                'validUntil': future['expiryTime']
            })
        first.sort(key=lambda x: x['validUntil'])

        if second_sequence:
            second = []
            for left, right in zip(first, first[1:]):
                second.append({
                    'instrumentId': right['instrumentId'],
                    'validUntil': left['validUntil']
                })
            second.sort(key=lambda x: x['validUntil'])
        else:
            second = None

        source_path = [
            'feeds',
            'providerOverrides',
            lambda_provider_id,
            'lambdaSettings',
            'sources'
        ]
        dict_part = {}
        assign_to_path(dict_part, source_path + ['first', 'sequence'], first)
        if second:
            assign_to_path(dict_part, source_path + ['second', 'sequence'], second)
        
        return dict_part

    async def get_instrument_type(self, symbol, cache: list[dict] = None) -> str:
        """
        Method to get the inherited instrument type
        :param uuid: uuid or symbolId of instrument
        :return: instrument type
        """

        if cache is None:
            cache = []
        cache.extend([
            x for x
            in self.instrument_cache
            if x['_id'] not in [
                y['_id'] for y in cache
            ]
        ])
        if isinstance(symbol, str):
            instrument = next(
                (
                    x for x
                    in cache
                    if x['_id'] == symbol
                ),
                await self.sdb.get(symbol)
            )
            if not instrument:
                return None
            self.__update_instrument_cache([instrument])
        elif isinstance(symbol, dict) and symbol.get('path'):
            instrument = symbol
        else:
            return None

        if instrument.get('type'):
            return instrument['type']
        if instrument.get('_id'):
            if cache and all(
                p in [x['_id'] for x in cache] for p
                in instrument['path'][:-1]
            ):
                parents = [x for x in cache if x['_id'] in instrument['path'][:-1]]
            else:
                parents = await self.sdb.get_parents(instrument['_id'])
                self.__update_instrument_cache(parents)
        else:
            gather = []
            for p in instrument['path']:
                if cache and p in [x['_id'] for x in cache if x.get('_id')]:
                    parents.append(
                        next(deepcopy(x) for x in cache if x['_id'] == p)
                    )
                else:
                    gather.append(self.sdb.get(p))
            if gather:
                loaded = await asyncio.gather(*gather)
                self.__update_instrument_cache(loaded)
                parents.extend(loaded)
        sym_type = next((
            x['type'] for x
            in reversed(sorted(parents, key=lambda x: len(x['path'])))            
            if x.get('type')
        ), None)
        return sym_type

    async def get_list_from_sdb(self, list_name, id_only=True, additional_fields=[]) -> list:
        """
        Method to get list from sdb entities as pairs of their names and ids
        :param list_name: what list to get, possible values:
            accounts
            currencies
            exchanges
            execution_schemes
            gateways
            schedules
            feed_providers
            broker_providers
        :param id_only: if False return full dicts of gateways or accounts instead of just ids
        :param additional_fields: additional info to get with default values
        :return: list of tuples (name, id or dict) of requested entity

        lists are always made this way:
        · 0th value is a human readable form of item
        · 1st value is an id or dict for use in instrument dict
        · the rest values are from additional_fields
        (that's why the currencies have the same field twice)
        """
        # how it's intended to work:
        # · at the init we try to load cache file
        #   and check if there is a timestamp for a given environment
        # · if the timestamp (or even cache) does not exist or old enough
        #   we wipe all sdb_lists for given env, load requested list from sdb
        #   and update timestamp with current datetime
        # · otherwise check if we have requested list in cache, then get it from cache
        #   or load from sdb if not found in cache; in both cases timestamp remains unchanged
        #   as it holds the datetime of the last full wipe
        # · when the requested list is loaded we store it in self variable
        # 
        # Note, lists returned by this method are not the same as we get by symboldb methods

        async def check_cache():
            sdb_lists = {
                SdbLists.EXCHANGES: self.sdb_exchs,
                SdbLists.EXECSCHEMES: self.sdb_execs,
                SdbLists.ACCOUNTS: self.sdb_accs,
                SdbLists.GATEWAYS: self.sdb_gws,
                SdbLists.SCHEDULES: self.sdb_scheds,
                SdbLists.CURRENCIES: self.sdb_currencies,
                SdbLists.SECTIONS: self.sdb_sections
            }
            async def dummy(list_name: list):
                return list_name
            
            update_cache = []
            for l in sdb_lists:
                if not sdb_lists[l]:
                    update_cache.append(l)
            gather = [
                self.sdb.get_exchanges() if not self.sdb_exchs else dummy(self.sdb_exchs),
                self.sdb.get_execution_schemes() if not self.sdb_execs else dummy(self.sdb_execs),
                self.sdb.get_broker_accounts() if not self.sdb_accs else dummy(self.sdb_accs),
                self.sdb.get_feed_gateways() if not self.sdb_gws else dummy(self.sdb_gws),
                self.sdb.get_schedules() if not self.sdb_scheds else dummy(self.sdb_scheds),
                self.sdb.get_currencies() if not self.sdb_currencies else dummy(self.sdb_currencies),
                self.sdb.get_sections() if not self.sdb_sections else dummy(self.sdb_sections)
            ]
            (
                self.sdb_exchs,
                self.sdb_execs,
                self.sdb_accs,
                self.sdb_gws,
                self.sdb_scheds,
                self.sdb_currencies,
                self.sdb_sections
            ) = await asyncio.gather(*gather)
            sdb_lists = {
                SdbLists.EXCHANGES: self.sdb_exchs,
                SdbLists.EXECSCHEMES: self.sdb_execs,
                SdbLists.ACCOUNTS: self.sdb_accs,
                SdbLists.GATEWAYS: self.sdb_gws,
                SdbLists.SCHEDULES: self.sdb_scheds,
                SdbLists.CURRENCIES: self.sdb_currencies,
                SdbLists.SECTIONS: self.sdb_sections
            }
            if not self.test:
                for uc in update_cache:
                    await self.__write_cache(uc, sdb_lists[uc])
        
        await check_cache()
        if list_name == SdbLists.CURRENCIES.value:
            # check_cache('currencies')
            fields = ['_id', '_id'] + additional_fields
            currencies = [tuple([x.get(field) for field in fields]) for x in self.sdb_currencies]
            return currencies
        elif list_name == SdbLists.EXCHANGES.value:
            # check_cache('exchanges')
            if id_only:
                fields = ['exchangeName', '_id'] + additional_fields
            else:
                fields = ['name', '_id'] + additional_fields
            exchanges = [tuple([x.get(field) for field in fields]) for x in self.sdb_exchs]
            return exchanges
        elif list_name == SdbLists.ACCOUNTS.value:
            # check_cache('accounts')
            accounts = []
            for account in self.sdb_accs:
                br_name = account['providerName']
                gw_name = account['gatewayName']
                acc_name = account['name']
                human_name = f'{br_name}: {gw_name}: {acc_name}'
                base = {
                    'accountId': account['_id'],
                    'account': {
                        'providerId': account['providerId'],
                        'gatewayId': account['gatewayId']
                    }
                }
                if id_only:
                    fields = [human_name, account['_id']] + [account[f] for f in additional_fields]
                else:
                    fields = [human_name, base] + [account[f] for f in additional_fields]
                accounts.append(tuple(fields))
            return accounts
        elif list_name == SdbLists.GATEWAYS.value:
            # check_cache('gateways')
            gateways = []
            for gateway in self.sdb_gws:
                pr_name = gateway['providerName']
                gw_name = gateway['name']
                human_name = f'{pr_name}: {gw_name}'
                base = {
                    'gatewayId': gateway['_id'],
                    'gateway': {
                        'providerId': gateway['providerId'],
                    }
                }
                if id_only:
                    fields = [human_name, gateway['_id']] + [gateway[f] for f in additional_fields]
                else:
                    fields = [human_name, base] + [gateway[f] for f in additional_fields]
                gateways.append(tuple(fields))
            return gateways
        elif list_name == SdbLists.EXECSCHEMES.value:
            # check_cache(SdbLists.EXECSCHEMES.value)
            fields = ['name', '_id'] + additional_fields
            exec_schemes = [tuple([x.get(field) for field in fields]) for x in self.sdb_execs]
            return exec_schemes
        elif list_name == SdbLists.SCHEDULES.value:
            # check_cache('schedules')
            fields = ['name', '_id'] + additional_fields
            schedules = [tuple([x.get(field) for field in fields]) for x in self.sdb_scheds]
            return schedules
        elif list_name == SdbLists.FEED_PROVIDERS.value:
            # check_cache('gateways')
            providers = []
            for gw in self.sdb_gws:
                if gw['providerId'] not in [x[1] for x in providers]:
                    providers.append((gw['providerName'], gw['providerId']))
            return providers
        elif list_name == SdbLists.BROKER_PROVIDERS.value:
            # check_cache('accounts')
            providers = []
            for br in self.sdb_accs:
                if br['providerId'] not in [x[1] for x in providers]:
                    providers.append((br['providerName'], br['providerId']))
            return providers
        elif list_name == SdbLists.SECTIONS.value:
            if id_only:
                fields = ['name', '_id'] + additional_fields
            else:
                fields = ['name', '_id', 'exchangeId', 'scheduleId'] + additional_fields
            sections = [tuple([x.get(field) for field in fields]) for x in self.sdb_sections]
            return sections

        else:
            return None

    def isexpired(self, symbol) -> bool:
        """
        Simple mthod that returns True if given symbol is expired.
        Hint: passing symbolId as argument works precisely, passing uuid may return false-negative result
        :param symbol: symbolId or uuid of instrument or the instrument dict itself
        :return: bool True if symbol is expired otherwise False
        """
        if isinstance(symbol, str):
            instr = asyncio.run(self.sdb.get(symbol, fields=['expiryTime']))
        elif isinstance(symbol, dict):
            instr = symbol
        else:
            return False
        if instr.get('expiryTime') \
            and dt.datetime.fromisoformat(instr['expiryTime'][:-1]) < dt.datetime.now():
            return True
        elif (instr.get('expiry')
            and instr['expiry'].get('day')
            and self.sdb.sdb_to_date(instr['expiry']) < dt.date.today()):
            return True
        else:
            return False

    async def load_execution_to_route(self):
        def all_conditions(list_in_question):
            # check if:
            # · is a list
            # · large enough to contain at least 10 items
            if not isinstance(list_in_question, list) \
                or len(list_in_question) < 10:
                return False
            else:
                return True

        if not self.execution_to_route:
            self.execution_to_route: list = await self.__load_cache(SdbLists.EXECUTION_TO_ROUTE)
        if all_conditions(self.execution_to_route):
            return self.execution_to_route
        tree = await self.load_tree(fields=['brokers'])
        for symbol in tree:
            if not symbol.get('brokers') or not symbol['brokers'].get('accounts', []):
                continue
            for acc in symbol['brokers']['accounts']:
                if not acc['account'].get('executionSchemeId'):
                    continue
                if acc['account']['executionSchemeId'] not in [
                    x['_id'] for x in self.execution_to_route
                ]:
                    item = {
                        '_id': acc['account']['executionSchemeId'],
                        'name': next(
                            x[0] for x
                            in await self.get_list_from_sdb(SdbLists.EXECSCHEMES.value)
                            if x[1] == acc['account']['executionSchemeId']
                        ),
                        'routes': [
                            {
                                '_id': acc['accountId'],
                                'name': next(
                                    x[0] for x
                                    in await self.get_list_from_sdb(SdbLists.ACCOUNTS.value)
                                    if x[1] == acc['accountId']
                                )
                            }
                        ]
                    }
                    self.execution_to_route.append(item)
                else:
                    routes = next(
                        y['routes'] for y
                        in self.execution_to_route
                        if y['_id'] == acc['account']['executionSchemeId']
                    )
                    if acc['accountId'] not in [x['_id'] for x in routes]:
                        routes.append({
                            '_id': acc['accountId'],
                            'name': next(
                                x[0] for x
                                in await self.get_list_from_sdb(SdbLists.ACCOUNTS.value)
                                if x[1] == acc['accountId']
                            )
                        })
        if not self.test:
            await self.__write_cache(SdbLists.EXECUTION_TO_ROUTE, self.execution_to_route)
        return self.execution_to_route

    async def load_feed_permissions(self):

        def all_conditions(list_in_question):
            # check if:
            # · is a list
            # · large enough to contain at least 10 items
            if not isinstance(list_in_question, list) \
                or len(list_in_question) < 5:

                return False
            else:
                return True

        if not self.feed_perms:
            self.feed_perms: list = await self.__load_cache(SdbLists.FEED_PERMISSIONS)
        if all_conditions(self.feed_perms):
            return self.feed_perms
        self.feed_perms = self.bo.feed_permissions_get()
        if not self.test:
            await self.__write_cache(SdbLists.FEED_PERMISSIONS, self.feed_perms)
        return self.feed_perms

    async def load_stock_rics(self):

        def all_conditions(list_in_question, last_update):
            # check if:
            # · is a list
            # · large enough to contain at least 10 items
            if not isinstance(list_in_question, list) \
                or len(list_in_question) < 10:
                return False
            else:
                return True

        if not self.stock_rics:
            self.stock_rics: list = await self.__load_cache(SdbLists.STOCK_RICS)
        if all_conditions(self.stock_rics):
            return self.stock_rics
        tree = await self.load_tree(fields=['exchangeId', 'expiry', 'identifiers', 'ticker'])
        STOCK = next(x for x in tree if x['name'] == 'STOCK' and len(x['path']) == 2)
        stock_tree = [x for x in tree if STOCK['_id'] in x['path'] and len(x['path']) == 3]
        exchanges = sorted(
            [
                {
                    'exchange': next(
                        (
                            y[0] for y
                            in await self.get_list_from_sdb(SdbLists.EXCHANGES.value)
                            if x.get('exchangeId') == y[1]
                        ), ''
                    ),
                    '_id': x['_id']
                } for x in stock_tree
            ], key = lambda e: e['exchange']
        )

        for ex in exchanges:
            heirs = [
                x for x in tree
                if ex['_id'] in x['path']
                and x.get('isTrading', True) != False
                and not x.get('expiry')
                and not x['isAbstract']
            ]
            ric_suffices = {
                x['identifiers']['RIC'].split('.')[-1] for x
                in heirs
                if x['identifiers']
                and x['identifiers'].get('RIC')
            }
            # all this monstrousity is rather self-explaining:
            # for every exchange we make a list of all ric suffices
            # along with number of stocks where these are present
            # and sort them from most frequent to least frequent  
            suffix_count = list(reversed(sorted(
                [(
                    suffix,
                    len([
                        x for x in heirs
                        if x['identifiers']
                        and x['identifiers'].get('RIC', '').split('.')[-1] == suffix
                    ])
                ) for suffix in ric_suffices],
                key=lambda r: r[1]
            )))
            if not len(suffix_count):
                ex['common_rics'] = []
                continue
            # let's say, that ric suffices that appear in folder 3 times less
            # frequently that most popular one are negligible
            ex['common_rics'] = [
                x[0] for x
                in suffix_count
                if x[1] > suffix_count[0][1] / 3
            ]
        self.stock_rics = exchanges
        if not self.test:
            await self.__write_cache(SdbLists.STOCK_RICS, self.stock_rics, env='prod')
        return self.stock_rics

    async def load_tree(self, fields: list = [], reload_cache: bool = False, return_dict=True) -> list:
        """
        method to load info for all instruments. Default minimum: name, path in sdb tree and uuid
        :param fields: load default fields plus given here 
        :param reload_cache: force load from sdb
        :return: list 
        """
        # same intention as in load_used_symbols: load tree is quiet a long request,
        # so we store it in global var and write into cache


        def all_conditions(tree_in_question, fields) -> bool:
            if not isinstance(tree_in_question, pd.DataFrame) \
                or tree_in_question.shape[0] < 1000 \
                or next((x for x in fields if x not in tree_in_question.columns), None):

                return False
            else:
                return True

        # try to get from class
        if all_conditions(self.tree_df, fields) and not reload_cache:
            if return_dict:
                self.tree = self.tree_df.to_dict('records')
                return self.tree
            else:
                return None
        # try to load from cache
        tree_df: pd.DataFrame = await self.__load_cache(SdbLists.TREE, df=True)
        if not tree_df.empty:
            tree_df.set_index(
                pd.Index(
                    tree_df['_id'],
                    name='uuid'
                ),
                drop=False,
                inplace=True
            )
        if all_conditions(tree_df, fields) and not reload_cache:
            self.tree_df = tree_df
            if return_dict:
                self.tree = tree_df.to_dict('records')
                return self.tree
            else:
                return None
        # if cache is not good, load from sdb
        old_cache_tree_df = tree_df
        # slow, use only if necessary
        if 'symbolId' in fields:
            v2_fields = [fields.pop(fields.index('symbolId')), '_id']
            loaded_tree_raw, all_syms = await asyncio.gather(
                self.sdb.get_tree(fields=fields),
                self.sdb.get_v2('.*', fields=v2_fields)
            )
        else:
            loaded_tree_raw = await self.sdb.get_tree(fields=fields)
            all_syms = []
        loaded_tree_df = pd.DataFrame(loaded_tree_raw)
        get_v2_df = pd.DataFrame(all_syms)
        if not get_v2_df.empty:
            loaded_tree_df = loaded_tree_df.merge(
                get_v2_df,
                how='outer',
                on='_id',
                suffixes=(None, '_drop')
            )
            loaded_tree_df.drop(
                columns=[
                    x for x
                    in loaded_tree_df.columns
                    if '_drop' in x
                ],
                inplace=True
            )
        # that means we have cached tree but not all fields we are interested in are present
        # let's update existing tree with new fields
        if loaded_tree_df.shape[0] == old_cache_tree_df.shape[0]:
            loaded_tree_df = loaded_tree_df.merge(
                old_cache_tree_df,
                how='outer',
                on='_id',
                suffixes=(None, '_drop')
            )
            loaded_tree_df.drop(
                columns=[
                    x for x
                    in loaded_tree_df.columns
                    if '_drop' in x
                ],
                inplace=True
            )
        loaded_tree_df = loaded_tree_df.replace({np.nan: None})
        loaded_tree_df.set_index(
            pd.Index(
                loaded_tree_df['_id'],
                name='uuid'
            ),
            drop=False,
            inplace=True
        )
        self.tree_df = loaded_tree_df
        if not self.test:
            await self.__write_cache(SdbLists.TREE, self.tree_df)
        if return_dict:
            self.tree = loaded_tree_df.to_dict('records')
            return self.tree
        else:
            return None

    async def load_used_symbols(self, reload_cache: bool = False, consider_demo = True) -> list:
        """
        Method to return the used symbols, get them from cache if not yet
        or get from backoffice if previous options did not succeed
        :param reload_cache: force the update from backoffice
        :param consider_demo: load symbols also used in demo environment
        :return: used symbols list
        """
        # loading used symbols takes a lot of time, so we make an effort to minimize requests to BO:
        # · we store the list in a class variable to make it accessible from everywhere in modules
        # · we write the list into a cache file to make it accessible for more than one run of script
        # cache expires rather soon (2 hours) to ensure that the data is not out of date
        # but not too soon in case if we want to run some scripts several times one after another
        
        def all_conditions(list_in_question):
            # check if:
            # · used_symbols is a list
            # · large enough to contain at least 1000 items
            # · has last_update timestamp
            # · last_update is not too old
            if not isinstance(list_in_question, list) \
                or len(list_in_question) < 1000:

                return False
            else:
                return True
        
        lookup_envs = [self.env]
        if consider_demo:
            lookup_envs.append('demo')
        result = set()
        # it's easier to iterate one or two envs rather than trying to mention demo everywhere
        # for env in lookup_envs:
            # try to get from class
        if all_conditions(self.used_symbols) and not reload_cache:
            result.update(self.used_symbols)
        else:
        # try to get from cache
            self.used_symbols: list = await self.__load_cache(SdbLists.USED_SYMBOLS)
            if all_conditions(self.used_symbols) and not reload_cache:
                result.update(self.used_symbols)
            else:
                # finally, request from backoffice
                self.used_symbols = BackOffice().used_symbols()
                result.update(self.used_symbols)
                if not self.used_symbols:
                    raise RuntimeError(f'Cannot get used symbols for {self.env}!')
        if not self.test:
            await self.__write_cache(SdbLists.USED_SYMBOLS, self.used_symbols)
        if consider_demo:
            if all_conditions(self.used_symbols_demo) and not reload_cache:
                result.update(self.used_symbols_demo)
            else:
            # try to get from cache
                self.used_symbols_demo: list = await self.__load_cache(SdbLists.USED_SYMBOLS_DEMO)
                if all_conditions(self.used_symbols_demo) and not reload_cache:
                    result.update(self.used_symbols_demo)
                else:
                    # finally, request from backoffice
                    self.used_symbols_demo = BackOffice('demo').used_symbols()
                    result.update(self.used_symbols_demo)
                    if not self.used_symbols_demo:
                        raise RuntimeError(f'Cannot get used symbols for demo!')
            if not self.test:
                await self.__write_cache(SdbLists.USED_SYMBOLS_DEMO, self.used_symbols_demo)

        return tuple(result)
    
    def lua_compile(self, instrument: dict, template: str, compiled=False, cache=None):
        if not compiled:
            compiled_instrument = asyncio.run(self.build_inheritance(instrument, include_self=True, cache=cache))
        else:
            compiled_instrument = deepcopy(instrument)
        if isinstance(instrument, str) and not self.sdb.is_uuid(instrument):
            compiled_instrument.update(self.sdb.get(instrument, fields=['symbolId', 'expiryTime']))
        if compiled_instrument.get('symbolId') is None:
            if compiled_instrument['isAbstract'] is False:
                compiled_instrument.update({
                    'symbolId': self.compile_symbol_id(compiled_instrument, compiled=True),
                    'expiryTime': self.compile_expiry_time(compiled_instrument, compiled=True)
                })
            else:
                compiled_instrument.update({
                    'symbolId': None,
                    'expiryTime': None
                })
        compiled_instrument.update({
            'EXANTEId': compiled_instrument['symbolId']
        })
        while True:
            try:
                template_func = self.lua.eval(f'function(instrument) {template} end')
                compiled_template = template_func(compiled_instrument)
                return compiled_template
            except KeyError as kerr:
                match = re.search(rf'instrument[\w\.]*{kerr}', template)
                if match:
                    missing_key_path = template_func[match.span()[0]:match.span()[1]].split('.')[1:]
                    helping_dict = compiled_instrument
                    for field in missing_key_path[:-1]:
                        if helping_dict.get(field) is None:
                            helping_dict[field] = dict()
                        helping_dict = helping_dict[field]
                    helping_dict[missing_key_path[-1]] = None
                    continue
                else:
                    self.logger.error(f'Cannot compile lua template: {template}')
                    self.logger.error(f'{kerr.__class__.__name__}: {kerr}')
                    return None
            except Exception as e:
                self.logger.error(f'Invalid lua string: {template}')
                self.logger.error(f'{e.__class__.__name__}: {e}')
                return None

    def show_path(self, path) -> str:
        """
        translate path as list of uuid into string of readable names
        :param path: list of uuids or string of instrument uuid or symbolId or dict that contains path
        :return: string of names connected with arrows
        """
        if isinstance(path, list):
            gather = [self.uuid_to_name(x) for x in path]
        elif isinstance(path, str):
            get_instr = asyncio.run(self.sdb.get(path))
            if not get_instr:
                return None
            get_path = get_instr['path']
            gather = [self.uuid_to_name(x) for x in get_path]
        elif isinstance(path, dict):
            get_path = path.get('path', [])
            if not get_path:
                return None
            gather = [self.uuid_to_name(x) for x in get_path]
        return " → ".join([x[0] for x in gather])

    def uuid_to_name(self, uuid) -> tuple:
        """
        get the name for given uuid
        :param uuid: id of instrument
        :return: tuple of (name, uuid)
        """
        tree_reload = False
        while True:
            asyncio.run(self.load_tree(reload_cache=tree_reload, return_dict=False))
            try:
                get_name = self.tree_df.at[uuid, 'name']
                if not get_name:
                    get_name = uuid
                return get_name, uuid
            except KeyError:
                if not tree_reload:
                    tree_reload = True
                    continue
                return uuid, uuid


    # fancy_dict group
    def fancy_print(self, entry, **kwargs) -> None:
        """
        print fancy dict and/or fancy durations (seems obvious)
        :param entry: dict or list to be formatted
        :param kwargs: any arguments passed to fancy_dict method
        """
        data = self.fancy_dict(entry, **kwargs)
        for d in data:
            print(d[0] + d[1])

    def fancy_format(self, entry, prefix: bool = True, **kwargs) -> list:
        """
        get the payload ready to fancy print enriched with original key names and values
        :param entry: dict or list to be formatted
        :param prefix: include prefix into printable string
        :param kwargs: any arguments passed to fancy_dict method
        :return: list of tuples (printable string, original key name)
        """
        data = self.fancy_dict(entry, **kwargs)
        if prefix:
            return [(d[0] + d[1], d[2]) for d in data if d[2] is not None]
        else:
            return [(d[1], d[2]) for d in data if d[2] is not None]

    def fancy_dict(
        self,
        entity,
        depth: list = None,
        exclude: list = None,
        fields: list = None,
        parent=None,
        recursive: int = 100,
        sort: bool = True,
        colors: dict = None,
        **kwargs):
        """
        Formats sdb entities into something good looking:
        all ids replaced with readable names, pronounced structure of nested objects
        :param entity: part to format list or dict
        :param exclude: don't include these fields into output
        :param fields: show only these fields, empty for all
        :param parent: helping parameter to decide where we are
        :param recursive: level of how deep we go into nested objects
        :param sort: should we sort our fields?
        :return: list of elements [prefix, readable key name, original key name, original value]
        """
        def prefix(prefix_symbols: list):
            return ''.join(prefix_symbols)

        if not depth:
            depth = []
        if not fields:
            fields = []
        if not exclude:
            exclude = []
        if not colors:
            colors = {}
        
        def child_colorize_dict(colors, key):
            colorize_dict = {
                '/'.join(x.split('/')[1:]): val for x, val
                in colors.items()
                if len(x.split('/')) > 1
                and x.split('/')[0] == str(key)
            }
            if colors.get('None'):
                colorize_dict.update({
                    'None': colors['None']
                })
            return colorize_dict
            
        
        current_fields = [x.split('/')[0] for x in fields if isinstance(x, str)] # fields on current layer of dict
        current_colorize = {x: val for x, val in colors.items() if len(x.split('/')) == 1}
        for f in range(len(current_fields)): # if the field is a number (meaning we try to access item in list), let's count from 1 
            if current_fields[f].isdecimal():
                current_fields[f] = int(current_fields[f]) - 1
        child_fields = [
            '/'.join(x.split('/')[1:]) for x
            in fields if isinstance(x, str)
            and len(x.split('/')) > 1
        ] # the fields should be passed on child level

        always_transform = {
            'gatewayId': asyncio.run(self.get_list_from_sdb(SdbLists.GATEWAYS.value)),
            'providerId': asyncio.run(self.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)) \
                + asyncio.run(self.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)),
            'executionSchemeId': asyncio.run(self.get_list_from_sdb(SdbLists.EXECSCHEMES.value)),
            'accountId': asyncio.run(self.get_list_from_sdb(SdbLists.ACCOUNTS.value)),
            'scheduleId': asyncio.run(self.get_list_from_sdb(SdbLists.SCHEDULES.value))
        }
                
        always_exclude = ['gatewayId', 'accountId']
        exclude += always_exclude
        tree_figs = {
            'leaf':  ['├ '],
            'trunk': ['│ '],
            'last':  ['└ '],
            'space': ['  ']
        }
        elements = []
        if recursive < 0:
            recursive = 0

        if isinstance(entity, list):

            depth += ['·']
            for i in range(len(depth[:-1])):
                if depth[i] == '·':
                    depth[i] = ' '
            if parent in ['gateways', 'accounts']:
                # depth = depth[1:]
                for num, list_item in enumerate(entity):
                    item_type = parent[:-1] + 'Id'
                    item_name = next((
                        x[0] for x
                        in always_transform[item_type]
                        if x[1] == list_item[item_type]
                    ), str())
                    if current_fields and not (
                        num in current_fields or next((
                            x for x in current_fields
                            if isinstance(x, str)
                            and x in item_name
                        ), None)
                    ):
                        continue
                    elements.append([
                        prefix(depth[:-2] + ['·']),
                        item_name,
                        num,
                        list_item[item_type]
                    ])
                    if recursive:
                        child_colors = child_colorize_dict(colors, num)
                        elements += self.fancy_dict(
                            list_item,
                            depth=depth[:-2].copy(),
                            fields=child_fields,
                            parent=parent,
                            place_in_list=num,
                            recursive=recursive-1,
                            sort=sort,
                            colors=child_colors
                        )
                        elements.append([
                            prefix(depth[:-1]),
                            '',
                            None,
                            None
                        ])
            else:
                for num, list_item in enumerate(entity):
                    if not isinstance(list_item, (list, dict)):
                        elements.append([
                            prefix(depth),
                            str(list_item),
                            num,
                            str(list_item)
                        ])
                    elif not recursive:
                        elements.append([
                            prefix(depth),
                            f'{num+1}: <...>',
                            num,
                            type(list_item)
                        ])
                    else:
                        elements.append([
                            prefix(depth), f'{num+1}:', num, type(list_item)
                        ])
                        child_colors = child_colorize_dict(colors, num)
                        elements += self.fancy_dict(
                            list_item,
                            depth=depth.copy(),
                            fields=child_fields,
                            parent=parent,
                            place_in_list=num,
                            recursive=recursive-1,
                            sort=sort,
                            colors=child_colors
                        )

        elif isinstance(entity, dict):
            # display filtering and ordering
            if fields:
                to_display = [
                    x for x
                    in entity
                    if x in current_fields
                    or x in [
                        y[1] for y
                        in always_transform['providerId']
                        if y[0] in current_fields
                    ]
                    and x not in exclude
                ]
            else:
                to_display = [
                    x for x in entity if x not in exclude
                ]
            if sort:
                to_display = sorted(to_display)
                for to_end in ['constraints', 'scheduleId', 'availableOrderDurationTypes']:
                    if to_end in to_display:
                        to_display.remove(to_end)
                        to_display.append(to_end)
                for to_top in ['brokers', 'feeds', 'providerOverrides', 'path', 'symbolId']:
                    if to_top in to_display:
                        to_display.remove(to_top)
                        to_display.insert(0, to_top)
            # some display decorations
            depth += tree_figs['trunk']
            for i in range(len(depth)):
                if depth[i] == '·':
                    depth.pop(i)
                    depth.insert(i, ' ')
            for num, key in enumerate(to_display):
                suffix = tree_figs['leaf']
                if depth[-1] == tree_figs['trunk'][0] and num == len(to_display) - 1:
                    depth = depth[:-1] + tree_figs['last']
                    suffix = tree_figs['last']

                if depth[-1] == tree_figs['last'][0]:
                    depth = depth[:-1] + tree_figs['space']

                if not recursive\
                    and isinstance(entity[key], (list, dict))\
                    and key not in ['account', 'gateway']:

                    if key == 'providerOverrides' and parent:
                        readable_name = f'{parent[:-1].capitalize()} overrides'
                    
                    elif parent == 'providerOverrides':
                        readable_name = next((
                            x[0] for x in always_transform['providerId'] if key == x[1]
                        ), '<<no name>>')
                    else:
                        readable_name = key
                    elements.append([
                            prefix(depth[:-1] + suffix),
                            f'{readable_name}  <···>',
                            key,
                            type(entity[key])
                        ])
                    continue

                elif isinstance(entity[key], (list, dict)):

                    if key == 'availableOrderDurationTypes':
                        elements += self.fancy_durations(entity[key], depth=depth.copy())
                    elif key == 'path': 
                        elements.append([
                            prefix(depth[:-1] + suffix),
                            self.show_path(entity[key]),
                            key,
                            type(entity[key])
                        ])

                    elif key in [x[1] for x in always_transform['providerId']]:
                        pr_name = next(
                            x[0] for x in always_transform['providerId'] if key == x[1]
                        )
                        elements.append([
                            prefix(depth[:-2]),
                            f'·{pr_name}:',
                            key,
                            type(entity[key])
                        ])
                        child_colors = child_colorize_dict(colors, key)
                        elements += self.fancy_dict(
                            entity[key],
                            depth=depth[:-2].copy(),
                            fields=child_fields,
                            parent=key,
                            place_in_list=None,
                            recursive=recursive-1,
                            sort=sort,
                            colors=child_colors
                        )
                        elements.append([
                            prefix(depth[:-2]),
                            '',
                            None,
                            type(entity[key])
                        ])

                    elif key == 'providerOverrides' and parent:
                        elements.append([
                            prefix(depth[:-2] + suffix),
                            f'{parent[:-1].capitalize()} overrides:',
                            key,
                            type(entity[key])
                        ])
                        child_colors = child_colorize_dict(colors, key)
                        elements += self.fancy_dict(
                            entity[key],
                            depth=depth.copy(),
                            fields=child_fields,
                            parent=key,
                            place_in_list=None,
                            recursive=recursive-1,
                            sort=sort,
                            colors=child_colors
                        )

                    elif key in ['gateway', 'account']:
                        exclude_ids = [
                            x for x in always_transform if x != 'executionSchemeId'
                        ]
                        child_colors = child_colorize_dict(colors, key)
                        elements += self.fancy_dict(
                            entity[key],
                            depth=depth[:-1].copy(),    # hide one level
                            exclude=exclude_ids,        # don't show ids
                            fields=child_fields,
                            parent=key,
                            place_in_list=None,
                            recursive=recursive-1,
                            sort=True,                   # always sort
                            colors=child_colors
                        )

                    elif key in ['gateways', 'accounts']:
                        title = 'Feeds:' if key == 'gateways' else 'Brokers:'
                        elements.append([
                            prefix(depth[:-2] + tree_figs['leaf']),
                            title,
                            parent,
                            type(entity[key])
                        ])
                        child_colors = child_colorize_dict(colors, key)
                        elements += self.fancy_dict(
                            entity[key],
                            recursive=recursive,        # preserve recurrency level
                            fields=child_fields,
                            parent=key,
                            place_in_list=None,
                            depth=depth.copy(),
                            sort=sort,
                            colors=child_colors
                        )
                    else:
                        if key not in ['feeds', 'brokers']:
                            elements.append([
                                prefix(depth[:-1] + suffix),
                                f'{key}: ',
                                key,
                                type(entity[key])
                            ])
                        child_colors = child_colorize_dict(colors, key)
                        elements += self.fancy_dict(
                            entity[key],
                            depth=depth.copy(),
                            fields=child_fields,
                            parent=key,
                            place_in_list=None,
                            recursive=recursive-1,
                            sort=sort,
                            colors=child_colors
                        )

                elif key in always_transform:
                    item_name, item_id = next((
                        x for x
                        in always_transform[key]
                        if x[1] == entity[key]
                    ), ('', ''))

                    elements.append([
                        prefix(depth[:-1] + suffix),
                        f'{key[:-2]}: {item_name}',
                        key,
                        item_id
                    ])
                elif isinstance(entity[key], bool):
                    if entity[key]:
                        elements.append([
                            prefix(depth[:-1] + suffix),
                            f'▮ {key}',
                            key,
                            True
                        ])
                    else:
                        elements.append([
                            prefix(depth[:-1] + suffix),
                            f'_ {key}',
                            key,
                            False
                        ])
                elif entity[key] is None:
                    elements.append([
                        prefix(depth[:-1] + suffix),
                        f'? {key}',
                        key,
                        None
                    ])
                else:
                    elements.append([
                        prefix(depth[:-1] + suffix),
                        f'{key}: {str(entity[key])}',
                        key,
                        entity[key]
                    ])
        for e in elements:
            if e[2] in current_colorize:
                e[0] = colorize(e[0], ColorMode.STATUS, state=current_colorize[e[2]])
                e[1] = colorize(e[1], ColorMode.STATUS, state=current_colorize[e[2]])
            elif (
                'None' in current_colorize
                and e[3] is None
                and e[2]
                and e[2] != 'availableOrderDurationTypes'
            ):
                e[0] = colorize(e[0], ColorMode.STATUS, state=current_colorize['None'])
                e[1] = colorize(e[1], ColorMode.STATUS, state=current_colorize['None'])
        for missing in [
            x for x
            in current_colorize
            if x not in [
                y[2] for y in elements
            ]
            and x != 'None'
        ]:
            elements.insert(0,
                [
                    colorize(prefix(depth), ColorMode.STATUS, state=StatusColor.MISSING),
                    colorize(f"{missing}: missing", ColorMode.STATUS, state=StatusColor.MISSING),
                    missing,
                    None
                ]
            )

        return elements

    def fancy_durations(self, durations, depth: list = None, elements: list = None) -> list:
        """
        method to format the sdb dict of available order durations/types into fancy table
        :param durations: the dict availableOrderDurationTypes
        :param depth: list of symbols that preceed every string
        :return
        """
        def prefix(prefix_symbols: list):
            return ''.join(prefix_symbols)

        if not elements:
            elements = []
        if not depth:
            depth = []
        if not durations:
            return []
        order_durs = {
            'day': 'DAY',
            'gtc': 'GOOD_TILL_CANCEL',
            'gtt': 'GOOD_TILL_TIME',
            'ioc': 'IMMEDIATE_OR_CANCEL',
            'fok': 'FILL_OR_KILL',
            'ato': 'AT_THE_OPENING',
            'atc': 'AT_THE_CLOSE'
        }
        order_types = {
            'Limit  ': 'LIMIT',
            'Market ': 'MARKET',
            'Stop   ': 'STOP',
            'S_Limit': 'STOP_LIMIT',
            'Twap   ': 'TWAP',
            'Iceberg': 'ICEBERG',
            'T_Stop ': 'TRAILING_STOP'
        }
        elements = []
        head = f"AODT:  {' '.join([x for x in order_durs])}"
        elements.append([
            prefix(depth[:-1] + ['└ ']),
            head,
            'availableOrderDurationTypes',
            None
        ])
        for type_key, type_val in order_types.items():
            aodt_str = type_key
            if type_val not in durations or 'DISABLED' in durations.get(type_val):
                aodt_str += ' _   _   _   _   _   _   _  '
                continue
            for duration in order_durs.values():
                if duration in durations.get(type_val):
                    aodt_str += ' ▮  '
                else:
                    aodt_str += ' _  '
            elements.append([
                prefix(depth), 
                aodt_str, 
                None, 
                None
            ])
        return elements

if __name__ == '__main__':
    print('SymbolDB additionals by @alser')
    pass