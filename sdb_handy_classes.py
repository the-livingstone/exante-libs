import asyncio
from copy import deepcopy
import datetime as dt
from enum import Enum
import json
import operator
import re
import logging
from typing import Union
from deepdiff import DeepDiff
from functools import reduce
from libs.backoffice import BackOffice
from libs.monitor import Monitor
from libs import sdb_schemas_cprod as cdb_schemas
from libs import sdb_schemas as sdb_schemas
from libs.sdb_schemas import type_mapping
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional, Months, SdbLists
from pprint import pformat, pp
from pydantic import BaseModel
from pydantic.error_wrappers import ValidationError

class InstrumentTypes(Enum):
    BOND = 'BOND'
    SPREAD = 'SPREAD'
    CFD = 'CFD'
    FOREX = 'FOREX'
    FUND = 'FUND'
    FUTURE = 'FUTURE'
    FX_SPOT = 'FX_SPOT'
    OPTION = 'OPTION'
    STOCK = 'STOCK'

EXPIRY_BEFORE_MATURITY = ['VIX']

BOND_REGIONS = {
    'Malta Bonds': 'MT',
    'Argentina': 'AR',
    'Brazil': 'BR',
    'Canadian': 'CA',
    'Asia': {
        'AF',
        'AM',
        'AZ',
        'BH',
        'BD',
        'BT',
        'BN',
        'KH',
        'CN',
        'GE',
        'HK',
        'IN',
        'ID',
        'IR',
        'IQ',
        'IL',
        'JP',
        'JO',
        'KZ',
        'KW',
        'KG',
        'LA',
        'LB',
        'MO',
        'MY',
        'MV',
        'MN',
        'MM',
        'NP',
        'KP',
        'OM',
        'PK',
        'PH',
        'QA',
        'SA',
        'SG',
        'KR',
        'LK',
        'SY',
        'TW',
        'TJ',
        'TH',
        'TR',
        'TM',
        'AE',
        'UZ',
        'VN',
        'YE'
    },
    'European': {
        'AL',
        'AD',
        'AT',
        'BY',
        'BE',
        'BA',
        'BG',
        'HR',
        'CY',
        'CZ',
        'DK',
        'EE',
        'FO',
        'FI',
        'FR',
        'DE',
        'GI',
        'GR',
        'HU',
        'IS',
        'IE',
        'IM',
        'IT',
        'XK',
        'LV',
        'LI',
        'LT',
        'LU',
        'MK',
        'MD',
        'MC',
        'ME',
        'NL',
        'NO',
        'PL',
        'PT',
        'RO',
        'SM',
        'RS',
        'SK',
        'SI',
        'ES',
        'SE',
        'CH',
        'UA',
        'VA'
    },
    'Latin American': {
        'AI',
        'AW',
        'BS',
        'BB',
        'BZ',
        'BM',
        'BO',
        'VG',
        'KY',
        'CL',
        'CO',
        'CR',
        'CU',
        'CW',
        'DM',
        'DO',
        'EC',
        'SV',
        'FK',
        'GL',
        'GP',
        'GT',
        'GY',
        'HT',
        'HN',
        'JM',
        'MX',
        'MS',
        'NI',
        'PA',
        'PY',
        'PE',
        'PR',
        'BL',
        'KN',
        'LC',
        'MF',
        'PM',
        'VC',
        'SR',
        'TT',
        'UY',
        'VE'
    },
    'US Corporate': 'US',
    'US Sovereign': 'US',
    'UK Corporate': 'GB',
    'UK Sovereign': 'GB'
}

# dict to eliminate sdb_schemas import everywhere

set_schema = {
    'prod': {
        'BOND': sdb_schemas.BondSchema,
        'SPREAD': {
            'SPREAD': sdb_schemas.SpreadSchema,
            'CALENDAR_SPREAD': sdb_schemas.CalendarSpreadSchema,
        },
        'CALENDAR': sdb_schemas.CalendarSpreadSchema,
        'PRODUCT': sdb_schemas.SpreadSchema,
        'CFD': sdb_schemas.CfdSchema,
        'FOREX': sdb_schemas.ForexSchema,
        'FUND': sdb_schemas.FundSchema,
        'FUTURE': sdb_schemas.FutureSchema,
        'FX_SPOT': sdb_schemas.FxSpotSchema,
        'OPTION': sdb_schemas.OptionSchema,
        'OPTION ON FUTURE': sdb_schemas.OptionSchema,
        'STOCK': sdb_schemas.StockSchema,
        'navigation': sdb_schemas.SchemaNavigation
    },
    'cprod': {
        'BOND': cdb_schemas.BondSchema,
        'SPREAD': {
            'SPREAD': cdb_schemas.SpreadSchema,
            'CALENDAR_SPREAD': cdb_schemas.CalendarSpreadSchema,
        },
        'CALENDAR': sdb_schemas.CalendarSpreadSchema,
        'PRODUCT': sdb_schemas.SpreadSchema,
        'CFD': cdb_schemas.CfdSchema,
        'FOREX': cdb_schemas.ForexSchema,
        'FUND': cdb_schemas.FundSchema,
        'FUTURE': cdb_schemas.FutureSchema,
        'FX_SPOT': cdb_schemas.FxSpotSchema,
        'OPTION': cdb_schemas.OptionSchema,
        'STOCK': cdb_schemas.StockSchema,
        'navigation': cdb_schemas.SchemaNavigation

    }
}

stock_exchange_mapping = {
    'ARCA': 'NYSE ARCA',
    'AMEX': 'NYSE AMEX',
    'SA': 'Tadawul',
    'LSEAIM': 'LSE AIM',
    'LSEIOB': 'LSE IOB',
    'BZ': 'BM&F BoveSpa'
}


def set_instrument_type(
        instrument_type: str = None,
        schema: BaseModel = None,
        target_dict: dict = None
    ):
    if target_dict is None:
        target_dict = set_schema
    if schema:
        instrument_type = None
        for key, val in target_dict.items():
            if val == schema:
                instrument_type = key
            elif isinstance(val, dict):
                try:
                    instrument_type = set_instrument_type(
                        schema=schema,
                        target_dict=val
                    )
                except RuntimeError:
                    pass
            elif isinstance(val, list):
                for i in val:
                    if i == schema:
                        instrument_type = key
                    elif isinstance(i, dict):
                        instrument_type = set_instrument_type(schema=schema, target_dict=i)
    if isinstance(instrument_type, InstrumentTypes):
        return instrument_type
    elif isinstance(instrument_type, str):
        if instrument_type in ['SPREAD', 'CALENDAR_SPREAD', 'CALENDAR', 'PRODUCT']:
            return InstrumentTypes.SPREAD
        elif instrument_type == 'OPTION ON FUTURE':
            return InstrumentTypes.OPTION
        if instrument_type not in InstrumentTypes.__members__:
            raise RuntimeError(
                f'Instrument type {instrument_type} is unknown'
            )
        return InstrumentTypes[instrument_type]
    else:
        raise RuntimeError(
            f'Instrument type {instrument_type} is unknown'
        )


def get_part(instr, path=[]):
    try:
        return reduce(operator.getitem, path, instr)
    except KeyError:
        return None

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


class ExpirationError(Exception):
    pass

class NoInstrumentError(Exception):
    """Common exception for problems with Series"""
    pass

class NoExchangeError(Exception):
    """Common exception for problems with Series"""
    pass

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
        feeds = self.sdbadds.get_list_from_sdb(SdbLists.GATEWAYS.value, id_only=False, additional_fields=['feedSource'])
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

class Instrument:
    def __init__(
            self,
            # general
            schema: BaseModel = None,
            ticker: str = None,
            exchange: str = None,
            instrument: dict = None,
            instrument_type: str = None,
            shortname: str = None,
            parent_folder: Union[str, dict] = None,
            env: str = 'prod',
            # init parameters
            reload_cache: bool = False,
            recreate: bool = False,
            tree: list[dict] = None,

            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,

            silent: bool = False,
            **kwargs
            # option specific:
                # week_number: int = 0,
                # option_type: str = None,
                # parent_tree: list[dict] = None,
            # spread specific
                # spread_type: str = None,
        ):
        self.used_symbols = []
        self.set_la = False
        self.set_lt = False
        self.parent_folder_id = None
        if schema:
            self.schema = schema
            if not instrument_type:
                self.instrument_type = set_instrument_type(schema=schema)
            else:
                self.instrument_type = set_instrument_type(instrument_type=instrument_type)
        elif instrument_type and env:
            self.instrument_type = set_instrument_type(instrument_type=instrument_type)
            if self.instrument_type is InstrumentTypes.SPREAD:
                if kwargs.get('spread_type') in ['SPREAD', 'CALENDAR_SPREAD']:
                    self.schema: BaseModel = set_schema[env][instrument_type][kwargs['spread_type']]
                else:
                    raise RuntimeError(
                        f"Spread type {kwargs.get('spread_type')} is unknown, cannot set Instrument schema"
                    )
            else:
                self.schema: BaseModel = set_schema[env][self.instrument_type.name]
        else:
            raise RuntimeError(
                f"{instrument_type=} or environment {env=} is unknown, "
                "cannot set Instrument schema"
            )
        self.navi: sdb_schemas.SchemaNavigation = set_schema[env]['navigation'](self.schema)

        self.ticker = ticker
        self.exchange = exchange
        self.shortname = shortname
        self.parent_folder = parent_folder
        self.env = env
        if kwargs.get('week_number'):
            self.week_number = kwargs['week_number']
        if kwargs.get('option_type'):
            self.option_type = kwargs['option_type']
        if kwargs.get('parent_tree'):
            self.parent_tree = kwargs['parent_tree']
        if kwargs.get('spread_type'):
            self.spread_type = kwargs['spread_type']
        if kwargs.get('calendar_type'):
            self.calendar_type = kwargs['calendar_type']
        
        self.recreate = recreate
        self.silent = silent
        if sdb:
            self.sdb = sdb
        else:
            self.sdb = SymbolDB(env)
        if bo:
            self.bo = bo
        else:
            self.bo = BackOffice(env=env)
        if sdbadds:
            self.sdbadds = sdbadds
        else:
            self.sdbadds = SDBAdditional(env)
        self.tree = tree if tree else asyncio.run(
            self.sdbadds.load_tree(
                fields=['expiryTime'],
                reload_cache=reload_cache
            )
        )

        if self.ticker and self.exchange:
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
            (
                self.instrument,
                self.reference,
                self.contracts,
                self.weekly_commons,
                self.leg_futures,
                self.gap_folders
            ) = self._find_series()
        else:
            self.set_instrument(instrument)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

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
                in self.sdbadds.get_list_from_sdb(SdbLists.EXCHANGES.value)
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
        gap_folders = []
        leg_futures = []
        weekly_commons = []
        contracts = []
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
                raise RuntimeError(
                    'sdb_tree is out of date, '
                    'please restart with addditional arg --drop-cache')

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
            if self.instrument_type is InstrumentTypes.OPTION:
                contracts = [
                    OptionExpiration(self, payload=x) for x
                    in self.series_tree
                    if x['path'][:-1] == instrument['path']
                    and not x['isAbstract']
                ]
                # common weekly folders where single week folders are stored
                # in most cases only one is needed but there are cases like EW.CME
                if not self.week_number:
                    weekly_folders = [
                        x for x in self.series_tree
                        if x['path'][:-1] == instrument['path']
                        and 'weekly' in x['name'].lower()
                        and x['isAbstract']
                    ]
                    weekly_commons = [
                        WeeklyCommon(self, uuid=x['_id']) for x in weekly_folders
                    ]
            elif self.instrument_type is InstrumentTypes.FUTURE:
                contracts = [
                    FutureExpiration(self, payload=x) for x
                    in self.series_tree
                    if x['path'][:-1] == instrument['path']
                    and not x['isAbstract']
                ]
            elif self.instrument_type is InstrumentTypes.SPREAD:
                contracts = [
                    SpreadExpiration(self, payload=x) for x
                    in self.series_tree
                    if x['path'][:-1] == instrument['path']
                    and not x['isAbstract']
                ]
                if self.spread_type in ['CALENDAR', 'CALENDAR_SPREAD']:
                    gap_folders = [
                        x for x
                        in self.series_tree
                        if x['path'][:-1] == instrument['path']
                        and x['isAbstract']
                        and re.match(r'\d{1,2} month', x['name'])
                    ]
                    for gf in gap_folders:
                        contracts.extend([
                            SpreadExpiration(self, payload=x) for x
                            in self.series_tree
                            if x['path'][:-1] == gf['path']
                            and not x['isAbstract']
                        ])

                    try:
                        future = Future(self.ticker, self.exchange, env=self.env, reload_cache=False)
                        leg_futures = future.contracts
                    except Exception as e:
                        self.logger.error(
                            f"{self.ticker}.{self.exchange}: {e.__class__.__name__}: {e}"
                        )
                        self.logger.error(
                            f'{self.ticker}.{self.exchange} '
                            'futures are not found in sdb! Create them in first place'
                        )
                elif len(self.ticker.split('-')) == 2:
                    for leg_ticker in self.ticker.split('-')[:2]:
                        try:
                            future = Future(leg_ticker, self.exchange, env=self.env)
                            leg_futures += future.contracts
                        except Exception as e:
                            self.logger.error(
                                f"{self.ticker}.{self.exchange}: {e.__class__.__name__}: {e}"
                            )
                            self.logger.error(
                                f'{leg_ticker}.{self.exchange} '
                                'futures are not found in sdb! Create them in first place'
                            )
        # series does not exist, shortName is required
        else:
            if not self.shortname:
                raise NoInstrumentError(
                    f'{self.ticker}.{self.exchange} series does not exist in SymbolDB. '
                    'Shortname should be specified'
                )
            instrument = self.create_series_dict()
            self.set_instrument(instrument)
            contracts = []
        return instrument, reference, contracts, weekly_commons, leg_futures, gap_folders

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

    def set_instrument(self, instrument: dict, parent = None):
        self.instrument = instrument
        if parent:
            parent: Instrument = parent
            self.compiled_parent = asyncio.run(self.sdbadds.build_inheritance(
                [
                    parent.compiled_parent,
                    parent.instrument,
                    self.instrument
                ], include_self=True
            ))
        elif self.instrument.get('_id'):
            self.compiled_parent = asyncio.run(self.sdbadds.build_inheritance(
                self.instrument['path'][-2], include_self=True
            ))
        elif self.instrument.get('path'):
            self.compiled_parent = asyncio.run(self.sdbadds.build_inheritance(
                self.instrument['path'][-1], include_self=True
            ))
        else:
            self.compiled_parent = {}

    def post_instrument(self, dry_run: bool = False):
        if self.instrument.get('_id'):
            diff = DeepDiff(self.instrument, asyncio.run(self.sdb.get(self.instrument['_id'])))
            if diff:
                self.logger.info(f"{self.instrument['name']}: following changes have been made:")
                self.logger.info(pformat(diff))
                validation = self.validate_instrument()
                if validation is True:
                    if not dry_run:
                        response = asyncio.run(self.sdb.update(self.instrument))
                    else:
                        print('Validation passed, updated instrument:')
                        self.sdbadds.fancy_print(self.instrument)
                        return {'_id': True}
                else:
                    return validation
        else:
            validation = self.validate_instrument()
            if validation is True:
                if not dry_run:
                    response = asyncio.run(self.sdb.create(self.instrument))
                else:
                    print('Validation passed, new instrument:')
                    self.sdbadds.fancy_print(self.instrument)
                    return {'_id': True}
            else:
                return validation
        return response

    def get_field_properties(self, path: list = [], **kwargs) -> dict:
        try:
            props = self.navi.schema_lookup(path)
            if len(props) == 1:
                return props[0]
            elif path[-1] in kwargs:
                props = next(x for x in props if x.get('type') == kwargs[path[-1]] or x.get('title') == kwargs[path[-1]])
                return props
            elif not props:
                self.logger.warning(f'Nothing is found for {path}')
                return {}
            else:
                self.logger.warning(f"More than one possible option for {path[-1]}:")
                self.logger.warning(pformat(props))
                self.logger.warning('Define one of them in args')
                return {}
        except KeyError as e:
            self.logger.warning(e)
            return {}
    
    def set_field_value(self, value, path: list = [], **kwargs):
        # path should include the field name
        # validate value
        if not self.instrument or not path:
            return False
        field_props = self.get_field_properties(path, **kwargs)
        if not field_props:
            return False
        # prepare the place in dict
        self.__check_n_create(path, **kwargs)
        try:
            value = type_mapping[field_props['type']](value)
        except ValueError:
            self.logger.warning(f"{value} is wrong type (should be {type_mapping[field_props['type']]}")
            return False
        except TypeError:
            self.logger.warning(f"{'/'.join(path)} is set to None")
        if field_props.get('opts_list'):
            if isinstance(field_props['opts_list'][0], str) and value not in field_props['opts_list']:
                self.logger.warning(f'{value} is not in list of possible values for {path[-1]}, not updated')
                return False
            if isinstance(field_props['opts_list'][0], tuple) \
                and value not in [x[1] for x in field_props['opts_list']]:

                self.logger.warning(f'{value} is not in list of possible values for {path[-1]}, not updated')
                return False
        get_part(self.instrument, path[:-1])[path[-1]] = value
        # should not be anything beyond that so terminate
        return True

    def set_provider_overrides(self, provider, **kwargs):
        '''
        · provider is UPPER CASE human readable name e.g. REUTERS or LEK
        · properties are passed through kwargs as a dict: {'property_name': value}
          e.g. {'symbolName': 'AAPL'}
        · properties are validated through the validation schemes, so if you pass
          (provider='REUTERS', {'ric': 'LSE.L'}) it will find its right place as:
            {
                'providerOverrides': {
                    '04a47f56b3d29913fdaea70beb9da503':{
                        'reutersProperties': {
                            'ric': 'LSE.L'
                        }
                    }
                }
            }
          but you always could help pointing out the path divided by '/':
          (provider='REUTERS', {'reutersProperties/quoteRic/base': 'JGL'})
        · if it happens that feed provider has the same name as broker provider
          (e.g. LAMBDA or HTTP) you could pass additional kwarg broker=True or feed=True

        '''
        feed_provider_id = next((x[1] for x in self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value) if x[0] == provider), None)
        broker_provider_id = next((x[1] for x in self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value) if x[0] == provider), None)
        if feed_provider_id and kwargs.get('broker') != True:
            additional = ['feeds', 'providerOverrides', feed_provider_id]
        elif broker_provider_id and kwargs.get('feed') != True:
            additional = ['brokers', 'providerOverrides', broker_provider_id]
        else:
            self.logger.warning(f'{provider} is not found in available feed or broker providers')
            return False
        success = dict()
        for prop, val in kwargs.items():
            if prop in ['feed', 'broker']:
                success.update({prop: val})                
                continue
            field_type = next(x for x, y in type_mapping.items() if y == type(val))
            path = self.navi.find_path(prop, field_type, *additional)
            if not path:
                self.logger.warning(f'Cannot find a path to {prop}')
                return False
            # replace dummy
            path[2] = additional[-1]
            prop_type = {path[-1]: mapped for mapped, x in type_mapping.items() if isinstance(val, x)}
            if self.set_field_value(val, path, **prop_type):
                success.update({prop: val})
            else:
                self.logger.warning(f'{prop}: {val} is not written to the instrument')
        return True if success == kwargs else False
    
    def get_provider_overrides(
            self,
            provider: str,
            *args,
            compiled: bool = False,
            silent: bool = False
        ) -> list:
        '''
        provider properties live inside the dict which name is a provider id. It makes overrides access
        quite uncomfortable, so this method is intended to help dig out fields of interest
        :param provider: human readable provider name UPPER CASE, e.g DXFEED or BLOOMBERG
        :param silent: don't show warning messages on not found paths
        :param args: fields of interest, could be provided with some last items of path divided by '/', e.g. 'ric/suffix'
        :return: list of field values same length and order as args
        '''
        feed_provider_id = next((x[1] for x in self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value) if x[0] == provider), None)
        broker_provider_id = next((x[1] for x in self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value) if x[0] == provider), None)
        if feed_provider_id and 'broker' not in args:
            additional = ['feeds', 'providerOverrides', feed_provider_id]
        elif broker_provider_id and 'feed' not in args:
            additional = ['brokers', 'providerOverrides', broker_provider_id]
        else:
            self.logger.warning(f'{provider} is not found in available feed or broker providers')
            return False
        payload = dict()
        for arg in args:
            if arg in ['broker', 'feed']:
                continue
            path = self.navi.find_path(arg, *additional)
            if not path:
                self.logger.warning(f'Cannot find a path to {arg}')
                continue
            # replace dummy
            path[2] = additional[-1]
            try:
                if not compiled:
                    payload.update({
                        arg: reduce(operator.getitem, path, self.instrument)
                    })
                else:
                    payload.update({
                        arg: reduce(operator.getitem, path, self.compiled_parent)
                    })
            except KeyError:
                if not silent:
                    self.logger.warning(f"{'/'.join(path)} is not a valid path")
            except TypeError:
                if not silent:
                    self.logger.warning(f"{'/'.join(path)} is not a valid path")
        return payload

    def __check_n_create(self, path: list, **kwargs):
        '''
        checks if the field with given path exists in the instrument,
        creates it if not. All creations are verified with schema.
        The value of field could be given as last item in path,
        e.g.: path=['brokers', 'accounts', 1, 'account', 'constraints', 'forbiddenSide', 'BUY']
        that is: if there is no 'constraints' field in 1st account the following dict will be added
        into the account:
        {
            'constraints': {
                'forbiddenSide': 'BUY'
            }
        }
        '''
        part = self.instrument
        for num, p in enumerate(path):
            if num < len(path) - 1:
                kwargs.update({p: 'object'})
            lookup = self.get_field_properties(path[:num+1], **kwargs)
            if isinstance(part, dict) and not part.get(p):
                if not lookup:
                    self.logger.warning(f"cannot find {p} in {self.schema.schema()['title']}")
                    return None
                p_type: type = type_mapping[lookup['type']]
                part[p] = p_type()
            elif isinstance(part, list):
                # need to step back for list
                lookup = self.get_field_properties(path[:num], **kwargs)
                if lookup['items'].get('type') and num == len(path) - 1:
                    # entity in schema is a list of strings or numbers and p is the last in given list
                    item_type = type_mapping[lookup['items']['type']]
                    if isinstance(p, list):
                        good_ones = list()
                        for item in p:
                            if lookup.get('opts_list') and item not in lookup['opts_list']:
                                self.logger.warning(f'{item} is not in list of possible values for {path[num-1]}, not added')
                                continue
                            if type(item) != item_type:
                                self.logger.warning(f'{item} is wrong type for {path[num-1]} (should be {item_type}), not added')
                                continue
                            good_ones.append(item)
                        get_part(self.instrument, path[:num-1])[path[num-1]] = good_ones
                    else:
                        if lookup.get('opts_list') and p not in lookup['opts_list']:
                            self.logger.warning(f'{p} is not in list of possible values for {path[num-1]}, not added')
                            return None
                        if type(p) != item_type:
                            self.logger.warning(f'{p} is wrong type for {path[num-1]} (should be {item_type}), not added')
                            return None
                        get_part(self.instrument, path[:num-1])[path[num-1]].append(p)

                elif not isinstance(p, int):
                    self.logger.warning(f"{path[num-1]} is a list, {p} should be an integer")
                    return None
                elif p >= len(part):
                    self.logger.warning(f"{p} is out of the list range")
                    return None
            elif isinstance(part, (bool, int, float, str)):
                try:
                    value = type_mapping[lookup['type']](p)
                except ValueError:
                    self.logger.warning(f"{p} is wrong type (should be {type_mapping[lookup['type']]}")
                    return None
                if lookup.get('opts_list') and value not in lookup['opts_list']:
                    self.logger.warning(f'{value} is not in list of possible values for {path[num-1]}, not updated')
                    return None
                get_part(self.instrument, path[:num-1])[path[num-1]] = value
                # should not be anything beyond that so terminate
                return None
            part = get_part(self.instrument, path[:num+1])

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

    def get_routes(self, compiled=False, default=False) -> list:
        if default:
            routes = self.compiled_parent.get('brokers', {}).get('accounts', [])
        elif compiled:
            routes = asyncio.run(self.sdbadds.build_inheritance(self.instrument, include_self=True)).get('brokers', {}).get('accounts', [])
        else:
            routes = self.instrument.get('brokers', {}).get('accounts', [])
        result = []
        for r in routes:
            route_name = next((
                x[0] for x in self.sdbadds.get_list_from_sdb(SdbLists.ACCOUNTS.value) if x[1] == r['accountId']
            ), None)
            route_payload = {
                key: val for key, val in r['account'].items()
                if key not in ['providerId', 'gatewayId']
            }
            if not route_name:
                self.logger.error(f'Smth is wrong, cannot get name for route: {r}')
                return None
            result.append((route_name, route_payload))
        return result

    def validate_instrument(self):
        if self.compiled_parent:
            compiled_instrument = asyncio.run(self.sdbadds.build_inheritance(
                [self.compiled_parent, self.instrument], include_self=True
            ))
        else:
            compiled_instrument = asyncio.run(self.sdbadds.build_inheritance(
                self.instrument, include_self=True
            ))
        try:
            self.schema(**compiled_instrument)
            return True
        except ValidationError as valerr:
            if self.instrument.get('isAbstract') is False:
                self.logger.error(valerr)
            else:
                self.logger.info(valerr)
                return True
            return {
                'validation_errors': valerr.errors()
            }

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


from libs.sdb_instruments.future import Future, FutureExpiration
from libs.sdb_instruments.option import Option, OptionExpiration, WeeklyCommon
from libs.sdb_instruments.spread import Spread, SpreadExpiration
from libs.sdb_instruments.stock import Stock
from libs.sdb_instruments.bond import Bond

class Future(Future):
    pass

class FutureExpiration(FutureExpiration):
    pass

class Option(Option):
    pass

class OptionExpiration(OptionExpiration):
    pass

class WeeklyCommon(WeeklyCommon):
    pass

class Spread(Spread):
    pass

class SpreadExpiration(SpreadExpiration):
    pass

class Stock(Stock):
    pass

class Bond(Bond):
    pass
