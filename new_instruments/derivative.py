import asyncio
from copy import deepcopy
import datetime as dt
import pandas as pd
import numpy as np
import json
import re
import logging

from pandas import DataFrame
from libs.backoffice import BackOffice
from libs.monitor import Monitor
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional, Months, SdbLists
from pprint import pformat, pp
from libs.new_instruments import (
    Instrument,
    InitThemAll,
    InstrumentTypes,
    get_uuid_by_path,
    NoInstrumentError
)

class Balancer:
    def __init__(self, feed_type: str, blacklist = None, env: str = 'prod'):
        self.mon = Monitor(env)
        self.demo_mon = Monitor('demo')
        self.sdbadds = SDBAdditional(env)
        self.raw_feeds_info = dict()
        if isinstance(blacklist, str):
            try:
                with open(blacklist) as bl:
                    self.blacklist = json.load(bl)
            except json.decoder.JSONDecodeError:
                with open(blacklist) as bl:
                    self.blacklist = bl.read().splitlines()
            except FileNotFoundError:
                self.logger.warning(f'Cannot read blacklist from file: {blacklist}')
                self.blacklist = []
        elif isinstance(blacklist, list):
            self.blacklist = blacklist
        else:
            self.blacklist = [
                'DXFEED: CBOE-TEST'
            ]

        self.feed_gateways = self.get_feed_gateways(feed_type)
        self.demo_gateways = self.get_feed_gateways(feed_type, demo=True)
        self.match_feeds_to_modules()
    
    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def match_feeds_to_modules(self):
        feeds = asyncio.run(
            self.sdbadds.get_list_from_sdb(
                SdbLists.GATEWAYS.value,
                id_only=False,
                additional_fields=['feedSource']
            )
        )
        self.feeds = sorted(
            [
                {
                    'gw_name': x[0],
                    'sdb_section': x[1],
                    'monitor_module': self.feed_gateways.get(f"gw-feed-{x[2].split('.')[0]}")
                } for x
                in feeds
                if self.feed_gateways.get(f"gw-feed-{x[2].split('.')[0]}")
                and x[0] not in self.blacklist
                and x[2].split('.')[0] not in self.blacklist
            ],
            key=lambda f: f['gw_name']
        )
        self.demo_feeds = [
            {
                'gw_name': x[0],
                'sdb_section': x[1],
                'monitor_module': self.demo_gateways.get(f"gw-feed-{x[2].split('.')[0]}")
            } for x
            in feeds
            if self.demo_gateways.get(f"gw-feed-{x[2].split('.')[0]}")
            and x[0] not in self.blacklist
            and x[2].split('.')[0] not in self.blacklist
        ]
    
    def get_feed_gateways(self, feed_type: str, demo=False):
        if feed_type == 'CBOE':
            provider = 'dxfeed' if not demo else 'delay'
            regexp = re.compile(rf'gw-feed-{provider}-cboe')
        elif feed_type == 'DXFEED':
            if not demo:
                regexp = re.compile(r'gw-feed-dxfeed\d+')
            else:
                regexp = re.compile(r'gw-feed-delay-(?!cboe)')
        else:
            regexp = re.compile(r'gw-feed')
        if not demo:
            result = {
                x['name'].split('@')[0].replace('-proc1', ''): x['name'] for x
                in self.mon.all_modules()
                if re.match(regexp, x['name'])
            }
        else:
            result = {
                x['name'].split('@')[0].replace('-proc1', ''): x['name'] for x
                in self.demo_mon.all_modules()
                if re.match(regexp, x['name'])
            }

        if not result:
            raise RuntimeError("Cannot parse feed gateways")

        return result

    def least_busy_feed(self, demo=False):
        # path = ['connections', 'symboldb (feed gateway)', 'symbols']
        path = ['connections', 'symboldb (feed gateway instrument)', 'objects']
        # for feedname in feeds:
        #     module_name = mapper[self.feed_info(feedname)[-1]]
        #     symbols = self.mon.indicator_status(module_name, path)['state']['description'].split()[0]
        #     symbols = int(symbols)
        #     result[feedname] = symbols
        gateway_load = {}
        for module in self.feeds:
            try:
                load = int(
                    self.mon.indicator_status(
                        module['monitor_module'],
                        path
                    )['state']['description'].split()[0]
                )
                module.update({
                    'load': load
                })
            except TypeError:
                continue
        for module in self.demo_feeds:
            try:
                load = int(
                    self.demo_mon.indicator_status(
                        module['monitor_module'],
                        path
                    )['state']['description'].split()[0]
                )
                module.update({
                    'load': load
                })
            except TypeError:
                continue
        main_gateway = min(
            [
                x for x in self.feeds
                if not 'backup' in x['monitor_module']
                and x.get('load')
            ],
            key=lambda x: x['load']
        )
        backup = None
        i = len(main_gateway['monitor_module'])
        while i:
            backup = next((
                x for x
                in self.feeds
                if main_gateway['monitor_module'][:i] in x['monitor_module']
                and 'backup' in x['monitor_module']
            ), None)
            i -= 1
            if backup:
                break
        demo = min(
            [
                x for x in self.demo_feeds if x.get('load')
            ],
            key=lambda x: x['load']
        )
        return {
            'main': main_gateway,
            'backup': backup,
            'demo': demo
        }

class Derivative(Instrument):
    def __init__(
            self,
            ticker: str,
            exchange: str,
            instrument_type: str,
            instrument: dict,
            env: str = 'prod',
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            # init parameters
            reload_cache: bool = True,
            **kwargs
        ):
        self.env = env
        self.bo, self.sdb, self.sdbadds, self.tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env,
            reload_cache=reload_cache
        ).get_instances
        self.instrument_type = instrument_type
        self.instrument = instrument
        self.ticker = ticker
        self.exchange = exchange
        self.set_la = False
        self.set_lt = False
        super().__init__(
            instrument=self.instrument,
            instrument_type=self.instrument_type,
            env=self.env,

            sdb=self.sdb,
            sdbadds=self.sdbadds,
            tree_df=self.tree_df,
            reload_cache=reload_cache,
            option_type=kwargs.get('option_type'),
            calendar_type=kwargs.get('calendar_type')
        )

    @staticmethod
    def _set_parent_folder(
            parent_folder,
            sdb: SymbolDB = None,
            env = 'prod'
        ):
        if not sdb:
            sdb = SymbolDB(env)
        if isinstance(parent_folder, str):
            parent_folder = asyncio.run(sdb.get(parent_folder))

            if parent_folder and parent_folder.get('isAbstract'):
                parent_folder_id = parent_folder['_id']
            else:
                raise RuntimeError(
                    f"Wrong parent folder id is given: {parent_folder}. "
                    "Leave it empty to search parent folder automatically"
                )
        elif isinstance(parent_folder, dict):
            parent_folder_id = parent_folder.get('_id')
        else:
            raise RuntimeError(
                    f"Given parent_folder is wrong_type: {type(parent_folder)}. "
                    "Leave it empty to search parent folder automatically"
                )
        return parent_folder_id, parent_folder

    @staticmethod
    def _find_option_series(
            ticker: str,
            parent_folder_id: str,
            parent_tree: list[dict] = None,
            sdb: SymbolDB = None,
            tree_df: DataFrame = None,
            reload_cache: bool = True,
            env: str = 'prod'
        ):
        if not sdb:
            sdb = SymbolDB(env)
        if tree_df is None or tree_df.empty:
            sdbadds = SDBAdditional(env)
            asyncio.run(
                sdbadds.load_tree(
                    fields=['expiryTime'],
                    reload_cache=reload_cache,
                    return_dict=False
                )
            )
            tree_df = sdbadds.tree_df
        if parent_tree and parent_folder_id:
            instrument = next((
                x for x
                in parent_tree
                if parent_folder_id in x['path']
                and x['name'] == ticker
                and x['isAbstract']
            ), None)
            series_tree = [
                x for x
                in parent_tree
                if x['path'][:len(instrument['path'])] == instrument['path']
            ] if instrument else []
        elif parent_tree:
            instrument = next((
                x for x
                in parent_tree
                if x['name'] == ticker
                and x['isAbstract']
            ), None)
            series_tree = [
                x for x
                in parent_tree
                if x['path'][:len(instrument['path'])] == instrument['path']
            ] if instrument else []

        else:
            instrument, series_tree = Derivative._find_series(
                ticker,
                parent_folder_id,
                sdb=sdb,
                tree_df=tree_df,
                parent_tree=parent_tree,
                env=env
            )
        return instrument, series_tree
    
    @staticmethod
    def _find_series(
            ticker: str,
            parent_folder_id: str,
            sdb: SymbolDB = None,
            tree_df: DataFrame = None,
            reload_cache: bool = True,
            parent_tree: list[dict] = None,
            env: str = 'prod'
        ):
        if not sdb:
            sdb = SymbolDB(env)
        if tree_df is None or tree_df.empty:
            sdbadds = SDBAdditional(env)
            asyncio.run(
                sdbadds.load_tree(
                    fields=['expiryTime'],
                    reload_cache=reload_cache,
                    return_dict=False
                )
            )
            tree_df = sdbadds.tree_df
        same_name = tree_df.loc[
            (tree_df['name'] == ticker)
            & (tree_df['isAbstract'] == True)
        ]
        if not same_name.empty:
            same_parent = same_name[
                same_name.apply(
                    lambda x: parent_folder_id in x['path'],
                    axis=1
                )
            ]
            instr_id = same_parent.iloc[0]['_id'] if not same_parent.empty else None
        else:
            instr_id = None
        instrument = asyncio.run(sdb.get(instr_id)) if instr_id else {}
        if instrument.get('message') and instrument.get('description'):
            raise RuntimeError('sdb_tree is out of date')
        # works for options to reduce sdb requests
        if parent_tree:
            series_tree = [
                x for x
                in parent_tree
                if instrument['_id'] in x['path']
            ]
        elif instrument:
            series_tree = asyncio.run(
                sdb.get_heirs(
                    instrument['_id'],
                    full=True,
                    recursive=True
                )
            )
            series_tree.append(instrument)
        else:
            return {}, []
        return instrument, series_tree

    @staticmethod
    def create_series_dict(
            ticker: str,
            exchange: str,
            shortname: str,
            parent_folder: dict,
            **kwargs
        ) -> dict:
        record = {
            'isAbstract': True,
            'name': ticker,
            'shortName': shortname,
            'ticker': ticker,
            'path': parent_folder['path']
        }
        [
            record.update({
                key: val
            }) for key, val in kwargs.items()
        ]
        return record

    @staticmethod
    def less_loaded_feeds(
            feed_type: str,
            env: str = 'prod'
        ) -> list[dict]:
        b = Balancer(feed_type=feed_type, env=env)
        feed_set = b.least_busy_feed()
        if not (feed_set.get('main') and feed_set.get('backup') and feed_set.get('demo')):
            logging.warning('Cannot set feed gateways!')
            return []
        main_gw = feed_set['main']['sdb_section']
        backup_gw = feed_set['backup']['sdb_section']
        demo_gw = feed_set['demo']['sdb_section']
        logging.info(
            f"Following gateways have been set:"
        )
        logging.info(
            f"{feed_set['main']['gw_name']}, "
            f"{feed_set['backup']['gw_name']}, "
            f"{feed_set['demo']['gw_name']}"
        )
        main_gw['gateway'].update({
            'allowFallback': True,
            'enabled': True
        })
        backup_gw['gateway'].update({
            'allowFallback': True,
            'enabled': True
        })
        demo_gw['gateway'].update({
            'enabled': True
        })

        return [
            main_gw,
            backup_gw,
            demo_gw
        ]

    def _align_expiry_la_lt(self, contracts):
        compiled = asyncio.run(
            self.sdbadds.build_inheritance(
                [self.compiled_parent, self.instrument],
                include_self=True
            )
        )
        if compiled.get('lastAvailable', {}).get('time') \
            and compiled.get('lastTrading', {}).get('time'):
            
            self.set_la = True
            self.set_lt = True
            return None
        if compiled.get('expiry', {}).get('time'):
            if [x for x in contracts if x.instrument.get('lastTrading')] \
                and not compiled.get('lastTrading', {}).get('time'):

                self.set_field_value(compiled['expiry']['time'], ['lastTrading', 'time'])
                self.set_lt = compiled['expiry']['time']
                
            if [x for x in contracts if x.instrument.get('lastAvailable')] \
                and not compiled.get('lastAvailable', {}).get('time'):

                self.set_field_value(compiled['expiry']['time'], ['lastAvailable', 'time'])
                self.set_la = compiled['expiry']['time']

            for ch in contracts:
                if ch.instrument.get('isTrading') is False:
                    continue
                if ch.instrument.get('lastTrading') and ch.instrument.get('lastAvailable'):
                    continue
                if not ch.instrument.get('expiry'):
                    continue
                if not self.set_lt and not self.set_la:
                    continue
                if self.set_lt:
                    ch.set_field_value(
                        self.sdb.date_to_sdb(ch.expiration),
                        ['lastTrading']
                    )
                    ch.set_field_value(self.set_lt, ['lastTrading', 'time'])
                if self.set_la:
                    ch.set_field_value(
                        self.sdb.date_to_sdb(
                            ch.expiration + dt.timedelta(days=3)
                        ),
                        ['lastAvailable']
                    )
                    ch.set_field_value(self.set_la, ['lastAvailable', 'time'])
                self.logger.info(
                    f"{ch.contract_name}: "
                    'lastAvailable and lastTrading have been updated'
                )

    def create(self, dry_run: bool = False):
        if dry_run:
            print(f"Dry run. New folder {self.instrument['name']} to create:")
            pp(self.instrument)
            self.instrument['path'].append(f"<<new {self.ticker}.{self.exchange} folder id>>")
            self.reference = deepcopy(self.instrument)
            return None
        create = asyncio.run(self.sdb.create(self.instrument))
        if not create.get('_id'):
            self.logger.error(pformat(create))
            raise RuntimeError(
                f"Can not create instrument {self.ticker}: {create['message']}"
            )
        self.logger.debug(f'Result: {pformat(create)}')
        self.instrument['_id'] = create['_id']
        self.instrument['_rev'] = create['_rev']
        self.instrument['path'].append(self.instrument['_id'])
        new_record = pd.DataFrame([{
            key: val for key, val
            in self.instrument.items()
            if key in self.tree_df.columns
        }], index=[self.instrument['_id']])
        pd.concat([self.tree_df, new_record])
        self.tree_df.replace({np.nan: None})
        # self.tree.append(new_record)

        self.reference = deepcopy(self.instrument)

    def update(self, diff: dict = None, dry_run: bool = False):
        self.logger.info(f'{self.ticker}.{self.exchange}: following changes have been made:')
        self.logger.info(pformat(diff))
        if dry_run:
            print(f"Dry run. The folder {self.instrument['name']} to update:")
            pp(diff)
            return {}
        response = asyncio.run(self.sdb.update(self.instrument))
        if response.get('message'):
            print(f'Instrument {self.ticker} is not updated, we\'ll try again after expirations are done')
            self.logger.info(pformat(response))

    def clean_up_times(self):
        contracts = asyncio.run(self.sdb.get_heirs(self.instrument['_id'], full=True))
        to_upd = []
        for c in contracts:
            updated = False
            if c.get('lastAvailable', {}).get('time'):
                c['lastAvailable'].pop('time')
                updated = True
            if c.get('lastTrading', {}).get('time'):
                c['lastTrading'].pop('time')
                updated = True
            if updated:
                to_upd.append(c)
        
        asyncio.run(self.sdb.batch_update(to_upd))

