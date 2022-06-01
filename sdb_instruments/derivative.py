from abc import ABC, abstractmethod

import asyncio
from copy import deepcopy
import datetime as dt
from enum import Enum
import json
import re
import logging
from libs.backoffice import BackOffice
from libs.monitor import Monitor
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional, Months, SdbLists
from pprint import pformat, pp
from libs.sdb_instruments import (
    Instrument,
    InstrumentTypes,
    ExpirationError,
    NoInstrumentError,
    NoExchangeError
)

EXPIRY_BEFORE_MATURITY = ['VIX']

def format_maturity(input_data):
    """
    Make well-formed maturity string (YYYY-MM-DD or YYYY-MM)
    from literally every possible input
    """
    if isinstance(input_data, dict):
        maturity = f"{input_data['year']}-0{input_data['month']}" if input_data['month'] < 10\
            else f"{input_data['year']}-{input_data['month']}"
        if input_data.get('day'):
            maturity += f"-0{input_data['day']}" if input_data['day'] < 10\
                else f"-{input_data['day']}"
        return maturity
    elif isinstance(input_data, str):
        # 2021-08-01, 20210801, 2021-8-1, 2021-8, 2021-08 
        match = re.match(r"(?P<year>\d{4})(-)?(?P<month>(0|1)?\d)(-)?(?P<day>\d{0,2})", input_data)
        if match:
            month = match.group('month') if len(match.group('month')) == 2 else f"0{match.group('month')}"
            maturity = f"{match.group('year')}-{month}"
            if len(match.group('day')) == 2:
                return f"{maturity}-{match.group('day')}"
            elif len(match.group('day')) == 1:
                return f"{maturity}-0{match.group('day')}"
            else:
                return maturity
        # Q21, Q2021, 8-2021, 08-21, 082021
        match = re.match(r"(?P<month>(0|1)?\d|[FGHJKMNQUVXZ])(-)?(?P<year>(20)?\d{2})$", input_data)
        if match:
            if match.group('month').isdecimal():
                month = match.group('month') if len(match.group('month')) == 2 else f"0{match.group('month')}"
            else:
                month_num = Months[match.group('month')].value
                month = str(month_num) if month_num > 9 else f"0{month_num}"
            year = match.group('year') if len(match.group('year')) == 4 else f"20{match.group('year')}"
            return f"{year}-{month}"
        # Q1
        match = re.match(r"(?P<month>[FGHJKMNQUVXZ])(-)?(?P<year>\d)$", input_data)
        if match:
            month_num = Months[match.group('month')].value
            month = str(month_num) if month_num > 9 else f"0{month_num}"
            year = int(f"202{match.group('year')}")
            while year < dt.datetime.now().year:
                year += 10
            return f"{year}-{month}"
        # 1Q2021, 01Q2021, 1Q21
        match = re.match(r"(?P<day>\d{1,2})(?P<month>[FGHJKMNQUVXZ])(?P<year>(20)?\d{2})$", input_data)
        if match:
            day = match.group('day') if len(match.group('day')) == 2 else f"0{match.group('day')}"
            literal = Months[match.group('month')].value
            month = str(literal) if literal > 9 else f"0{literal}"
            year = match.group('year') if len(match.group('year')) == 4 else f"20{match.group('year')}"
            return f"{year}-{month}-{day}"
        # 01-08-2021
        match = re.match(r"(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})$", input_data)
        if match:
            return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
        else:
            return None

class Balancer:
    def __init__(self, feed_type: str, blacklist = None, env: str = 'prod'):
        self.mon = Monitor(env)
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
        result = {
            x['name'].split('@')[0].replace('-proc1', ''): x['name'] for x
            in self.mon.all_modules()
            if re.match(regexp, x['name'])
        }
        return result

    def least_busy_feed(self, demo=False):
        path = ['connections', 'symboldb (feed gateway)', 'symbols']
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
            series
        ):
        self.env = series.env
        self.sdb = series.sdb if series.sdb else SymbolDB(self.env)
        self.bo = series.bo if series.bo else BackOffice(env=self.env)
        self.sdbadds = series.sdbadds if series.sdbadds else SDBAdditional(self.env)

        self.set_la = False
        self.set_lt = False
        self.parent_folder_id = None
        
        self.ticker = series.ticker
        self.exchange = series.exchange
        self.shortname = series.shortname
        self.parent_folder = series.parent_folder
        self.instrument_type = InstrumentTypes[series.instrument_type]
        self.spread_type = None

        self.reload_cache = series.reload_cache
        self.recreate = series.recreate
        self.silent = series.silent
        if 'week_number' in series.__dir__():
            self.week_number = series.week_number
        if 'option_type' in series.__dir__():
            self.option_type = series.option_type
        if 'parent_tree' in series.__dir__():
            self.parent_tree = series.parent_tree
        if 'spread_type' in series.__dir__():
            self.spread_type = series.spread_type
        if 'calendar_type' in series.__dir__():
            self.calendar_type = series.calendar_type
        
        self.tree = asyncio.run(
            self.sdbadds.load_tree(
                fields=['expiryTime'],
                reload_cache=self.reload_cache
            )
        )

        if isinstance(self.parent_folder, str):
            self.parent_folder = asyncio.run(self.sdb.get(self.parent_folder))
            if self.parent_folder and self.parent_folder.get('isAbstract'):
                self.parent_folder_id = self.parent_folder['_id']
            else:
                raise RuntimeError(
                    f"Wrong parent folder id is given: {self.parent_folder}. "
                    "Leave it empty to search parent folder automatically"
                )
        elif isinstance(self.parent_folder, dict):
            self.parent_folder_id = self.parent_folder.get('_id')
            pass
            # ?????????????????????????????
        elif not self.parent_folder:
            self.parent_folder_id = self._find_parent_folder()
            self.parent_folder = asyncio.run(self.sdb.get(self.parent_folder_id))
        super().__init__(
            instrument_type=self.instrument_type,
            env=self.env,

            sdb=self.sdb,
            sdbadds=self.sdbadds,
            tree=self.tree,
            reload_cache=self.reload_cache,

            silent=self.silent
        )
        self.instrument, self.reference = self._find_series()

    def _find_parent_folder(self) -> str:
        """
        sets parent_folder_id if none
        """
        if self.instrument_type is InstrumentTypes.OPTION:
            for o_type in [self.option_type, 'OPTION', 'OPTION ON FUTURE']:
                parent_folder_id = asyncio.run(
                    self.sdb.get_uuid_by_path(
                        ['Root', o_type, self.exchange],
                        self.tree
                    )
                )
                if parent_folder_id:
                    self.option_type = o_type
                    break
        elif self.instrument_type in [InstrumentTypes.SPREAD, InstrumentTypes.FUTURE]:
            parent_folder_id = asyncio.run(
                self.sdb.get_uuid_by_path(
                    ['Root', self.instrument_type.name, self.exchange],
                    self.tree
                )
            )
        else:
            raise NotImplementedError(
                    f'Instrument type {self.instrument_type.name} is unknown'
                )


        if not parent_folder_id:
            # In my ideal world folders OPTION and OPTION ON FUTURE
            # contain only folders with exchange names
            # as they appear in exante id
            # but... here's slow and dirty hack for real world
            possible_exchanges = [
                x[1] for x
                in asyncio.run(
                    self.sdbadds.get_list_from_sdb(SdbLists.EXCHANGES.value)
                )
                if x[0] == self.exchange
            ]
            if self.instrument_type is InstrumentTypes.OPTION:
                opt_id = asyncio.run(self.sdb.get_uuid_by_path(
                    ['Root', 'OPTION'], self.tree
                ))
                oof_id = asyncio.run(self.sdb.get_uuid_by_path(
                    ['Root', 'OPTION ON FUTURE'], self.tree
                ))
                exchange_folders = asyncio.run(self.sdb.get_heirs(
                    opt_id,
                    fields=['name', 'exchangeId']))
                exchange_folders += asyncio.run(self.sdb.get_heirs(
                    oof_id,
                    fields=['name', 'exchangeId']))
            else:
                fld_id = asyncio.run(self.sdb.get_uuid_by_path(
                    ['Root', self.instrument_type.name], self.tree
                ))
                exchange_folders = asyncio.run(self.sdb.get_heirs(
                    fld_id,
                    fields=['name', 'exchangeId']
                ))

            possible_exchange_folders = [
                x for x in exchange_folders 
                if x['exchangeId'] in possible_exchanges
            ]
            if len(possible_exchange_folders) < 1:
                raise RuntimeError(
                    f'Exchange {self.exchange} does not exist in SymbolDB {self.env}'
                )
            elif len(possible_exchange_folders) == 1:
                parent_folder_id = possible_exchange_folders[0]['_id']
            else:
                ticker_folders = [
                    x for pef in possible_exchange_folders for x
                    in asyncio.run(
                        self.sdb.get_heirs(
                            pef['_id'],
                            fields=['path'],
                            recursive=True
                        )
                    )
                    if x['name'] == self.ticker
                ]
                if len(ticker_folders) == 1:
                    parent_folder_id = ticker_folders[0]['path'][2]
                    self.option_type = next(
                        x['name'] for x
                        in self.tree
                        if x['_id'] == ticker_folders[0]['path'][1]
                    )
                else:
                    raise RuntimeError(
                        f'{self.ticker}.{self.exchange}: cannot select exchange folder'
                    )
        return parent_folder_id

    def _find_series(self):
        """
        sets instrument as full series document
        creates self.reference to compare with before posting to sdb
        sets contracts as list of OptionExpiration objects of all non-abstract heirs of series
        sets weekly_commons as list of WeeklyCommon objects
        """
        # part with self.parent tree is made to avoid multiple tree searches
        # and sdb requests in weekly instances
        if self.instrument_type is InstrumentTypes.OPTION \
            and self.parent_tree \
            and self.parent_folder_id:

            instrument = next((
                x for x
                in self.parent_tree
                if self.parent_folder_id in x['path']
                and x['name'] == self.ticker
            ), None)
        elif self.instrument_type is InstrumentTypes.OPTION \
            and self.parent_tree:

            instrument = next((
                x for x
                in self.parent_tree
                if x['name'] == self.ticker
            ), None)
        else:
            instr_id = next((
                x['_id'] for x
                in self.tree
                if self.parent_folder_id in x['path']
                and x['name'] == self.ticker
            ), None)
            instrument = asyncio.run(self.sdb.get(instr_id)) if instr_id else {}
            if instrument.get('message') and instrument.get('description'):
                raise RuntimeError('sdb_tree is out of date')

        # reference to compare if any changes have been provided before posting to sdb
        reference = deepcopy(instrument)

        # recreate mode is this:
        # · keep _rev, _id, _creationTime and _lastUpdateTime, discard the rest
        # · create new series document from scratch (shortName is required), add saved underline fields to the document
        # · post document to sdb using sdb.update() method
        if self.recreate and reference.get('_id'):
            if not self.shortname:
                raise NoInstrumentError(
                    f'Shortname for {self.ticker} should be specified in order to recreate folder'
                )
            instrument: dict = self.create_series_dict()
            instrument.update({
                key: val for key, val
                in reference.items()
                if key[0] == '_'
            })
            instrument['path'].append(reference['_id'])

        # series exists
        if instrument:
            # in most cases we don't need the whole tree
            if self.instrument_type is InstrumentTypes.OPTION and self.parent_tree:
                self.series_tree = [
                    x for x
                    in self.parent_tree
                    if instrument['_id'] in x['path']
                ]
            else:
                self.series_tree = asyncio.run(
                    self.sdb.get_heirs(
                        instrument['_id'],
                        full=True,
                        recursive=True
                    )
                )
                self.series_tree.append(instrument)
            # only 1st level non-abstract heirs
            # meaning monthly expirations for monthly instrument, weeklies for weekly instrument
            self.set_instrument(instrument)
            
        else:
            if not self.shortname:
                raise NoInstrumentError(
                    f'{self.ticker}.{self.exchange} series does not exist in SymbolDB. '
                    'Shortname should be specified'
                )
            instrument = self.create_series_dict()
            self.set_instrument(instrument)
        return instrument, reference

    def _align_expiry_la_lt(self, contracts, update_expirations):
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
                update_expirations.append(ch)
                if not self.silent:
                    self.logger.info(
                        f"{ch.ticker}.{ch.exchange} {ch.maturity}: "
                        'lastAvailable and lastTrading have been updated'
                    )

    def create_series_dict(self, **kwargs) -> dict:
        record = {
            'isAbstract': True,
            'name': self.ticker,
            'shortName': self.shortname,
            'ticker': self.ticker,
            'path': self.parent_folder['path']
        }
        if self.instrument_type is InstrumentTypes.OPTION:
            record.update({
                'description': f'Options on {self.shortname}'
            })
            if not self.week_number:
                record.update({
                    'underlying': self.ticker
                })
                if self.exchange == 'CBOE':
                    record.update({
                        'feeds': {
                            'gateways': self.less_loaded_feeds('CBOE')
                        }
                    })
                    try:
                        underlying_stock = asyncio.run(self.sdb.get_v2(
                            rf'{self.ticker}\.(NASDAQ|NYSE|AMEX|ARCA|BATS)',
                            is_expired=False,
                            fields=['symbolId']
                        ))[0]['symbolId']
                        self.underlying_dict = {
                            'id': underlying_stock,
                            'type': 'symbolId'
                        }
                        record.update({'underlyingId': self.underlying_dict})
                    except (IndexError, KeyError):
                        self.logger.warning(f'Can not find underlyingId for {self.ticker}')
                elif self.option_type is InstrumentTypes.OPTION and self.underlying_dict:
                    record['underlyingId'] = self.underlying_dict
                elif self.option_type is InstrumentTypes.OPTION:
                    self.logger.warning(
                        f"Underlying for {self.ticker}.{self.exchange} is not set!"
                    )
        elif self.instrument_type is InstrumentTypes.FUTURE:
            record.update({
                'description': f'{self.shortname} Futures'
            })
        elif self.instrument_type is InstrumentTypes.SPREAD:
            record.update({
                'description': f'{self.shortname} Spreads'
            })
            if self.spread_type == 'CALENDAR_SPREAD' and self.calendar_type == 'REVERSE':
                record.update({
                    'spreadType': 'REVERSE'
                })
            elif self.spread_type == 'SPREAD':
                record.update({
                    'type': 'FUTURE'
                })


        [
            record.update({
                key: val
            }) for key, val in kwargs.items()
        ]
        return record

    def create(self, dry_run: bool = False):
        if dry_run:
            print(f"Dry run. New folder {self.instrument['name']} to create:")
            pp(self.instrument)
            self.instrument['path'].append(f"<<new {self.ticker}.{self.exchange} folder id>>")
        else:
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
            self.tree.append(self.instrument)
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

    def force_tree_reload(self, fields: list = None):
        if not fields:
            fields = []
        fields_list = [
                'expiryTime'
            ]
        fields_list.extend([x for x in fields if x not in fields_list])
        self.tree = asyncio.run(self.sdbadds.load_tree(
            fields=fields_list,
            reload_cache=True
        ))

    def _date_to_symbolic(self, date_str: str) -> str:
        if len(date_str.split('-')) == 3:
            try:
                year, month, day = date_str.split('-')
                formatted = f"{int(day.split(' ')[0])}{Months(int(month)).name}{year}"
                return formatted
            # in case of old shitty named expirations
            except Exception:
                self.logger.warning(f"cannot convert date: {date_str}")
                return None
        elif len(date_str.split('-')) == 2:
            year, month = date_str.split('-')
            try:
                return f"{Months(int(month)).name}{year}"
            # in case of old shitty named expirations
            except ValueError:
                self.logger.warning(f"cannot convert date: {date_str}")
                return None
        elif len(date_str) > 4 and date_str[-5] in Months.__members__:
            month_num = Months[date_str[-5]].value
            month = f"{month_num}" if month_num >= 10 else f"0{month_num}"
            if len(date_str) == 5:
                return f"{date_str[-4:]}-{month}"
            elif len(date_str) == 6:
                return f"{date_str[-4:]}-{month}-0{date_str[:1]}"
            elif len(date_str) == 7:
                return f"{date_str[-4:]}-{month}-{date_str[:2]}"

    def less_loaded_feeds(self, feed_type: str) -> list[dict]:
        b = Balancer(feed_type=feed_type, env=self.env)
        feed_set = b.least_busy_feed()
        if not (feed_set.get('main') and feed_set.get('backup') and feed_set.get('demo')):
            self.logger.warning('Cannot set feed gateways!')
            return []
        main_gw = feed_set['main']['sdb_section']
        backup_gw = feed_set['backup']['sdb_section']
        demo_gw = feed_set['demo']['sdb_section']
        if not self.silent:
            self.logger.info(
                f"Following gateways have been set:"
            )
            self.logger.info(
                f"{feed_set['main']['gw_name']}, {feed_set['backup']['gw_name']}, {feed_set['demo']['gw_name']}"
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
