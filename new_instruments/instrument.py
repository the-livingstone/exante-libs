import asyncio
from copy import deepcopy
import datetime as dt
from time import sleep
from deepdiff import DeepDiff
from enum import Enum
from functools import reduce
import operator
import logging
import pandas as pd
from pandas import DataFrame
from pprint import pformat
from pydantic import BaseModel, Field, root_validator, validator
from pydantic.error_wrappers import ValidationError
import re
from typing import Union

from libs.async_symboldb import SymbolDB
from libs.replica_sdb_additional import SDBAdditional, Months, SdbLists
from libs.backoffice import BackOffice
from libs import sdb_schemas_cprod as cdb_schemas
from libs import sdb_schemas as sdb_schemas
from libs.sdb_schemas import ValidationLists, type_mapping

class InstrumentTypes(Enum):
    BOND = 'BOND'
    CALENDAR_SPREAD = 'CALENDAR_SPREAD'
    CFD = 'CFD'
    FOREX = 'FOREX'
    FUND = 'FUND'
    FUTURE = 'FUTURE'
    FX_SPOT = 'FX_SPOT'
    OPTION = 'OPTION'
    STOCK = 'STOCK'

class InitThemAll:
    def __init__(
            self,
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env: str = 'prod',
            test: bool = False
        ):
        self.env = env
        self.sdb = sdb if sdb else SymbolDB(self.env)
        self.bo = bo if bo else BackOffice(env=self.env)
        self.sdbadds = sdbadds if sdbadds else SDBAdditional(self.env, sdb=sdb, test=test)
    
    @property
    def get_instances(self):
        return self.bo, self.sdb, self.sdbadds

class SetSectionId(BaseModel):
    exchange_id: str = Field(
        alias='exchangeId'
    )
    schedule_id: str = Field(
        alias='scheduleId'
    )
    section_id: str = Field(
        alias='sectionId'
    )

    @validator('schedule_id')
    def check_schedule_id(cls, item):
        if item not in [x[1] for x in ValidationLists.schedules]:
            raise ValueError(f'{item} is invalid schedule id')
        return item

    @validator('exchange_id')
    def check_exchange_id(cls, item):
        if item not in [x[1] for x in ValidationLists.exchanges]:
            raise ValueError(f'{item} is invalid exchange id')
        return item

    @root_validator(pre=True)
    def set_section_id(cls, values: dict):
        if not values.get('scheduleId'):
            raise ValueError("scheduleId is not set")
        if not values.get('exchangeId'):
            raise ValueError("exchangeId is not set")
        section = next((
            x for x
            in ValidationLists.sections
            if x[2] == values['exchangeId']
            and x[3] == values['scheduleId']
        ), None)
        if not section:
            raise ValueError(
                f"section with exchangeId {values['exchangeId']}, scheduleId {values['scheduleId']} "
                "is not found in SymbolDB"
            )
        values['section_id'] = section[1]
        return values


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
        'CALENDAR_SPREAD': sdb_schemas.CalendarSpreadSchema,
        'SPREAD': sdb_schemas.SpreadSchema,
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

def get_uuid_by_path(input_path: list, engine) -> str:
    ids = []
    for num, p in enumerate(input_path):
        conditions = [f"path[{id_num+1}] = '{str(x)}'" for id_num, x in enumerate(ids)]
        conditions.append(f'cardinality(path) = {num+1}')
        records_df = pd.read_sql(
            'SELECT id as _id, "extraData" as extra FROM instruments '
            f'WHERE {" AND ".join(conditions)}',
            engine
        )
        records_df['name'] = records_df.apply(
            lambda row: row['extra'].get('name'),
            axis=1
        )
        found_df = records_df[records_df['name'] == p]
        if found_df.empty:
            logging.error(f"Path {' → '.join(map(str, input_path))} does not exist in sdb")
            return None
        ids.append(found_df.iloc[0]['_id'])
    return ids[-1] if ids else None


class Instrument:
    def __init__(
            self,
            # general
            schema: BaseModel = None,
            instrument: dict = None,
            reference: dict = None,
            instrument_type: str = None,
            parent: 'Instrument' = None,
            env: str = 'prod',

            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            **kwargs
        ):
        self.env = env
        self.bo, self.sdb, self.sdbadds = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env,
        ).get_instances
        
        # set schema
        if schema:
            self.schema = schema
            self._instrument_type = self.__set_instrument_type_by_schema(
                schema
            )
        elif instrument_type:
            self._instrument_type = self.__set_instrument_type(
                instrument_type
            )
            if instrument_type == 'SPREAD':
                self.schema = set_schema[env]['SPREAD']
            else:
                self.schema = set_schema[env][self._instrument_type]
        elif instrument:
            self._instrument_type = self.__set_instrument_type_by_payload(
                instrument,
                self.sdbadds
            )
            self.schema = set_schema[env][self._instrument_type]
        else:
            raise RuntimeError(
            f'Instrument type could not be defined'
        )
        self.navi: sdb_schemas.SchemaNavigation = set_schema[env]['navigation'](self.schema)

        # set instrument
        self.set_instrument(instrument, parent, reference)

    def __repr__(self):
        return f"Instrument({self.instrument_type}, {self.schema.__name__})"

    @property
    def instrument_type(self) -> str:
        return self._instrument_type

    @staticmethod
    def __set_instrument_type_by_schema(
            schema: BaseModel
        ) -> str:
        if isinstance(schema, sdb_schemas.SpreadSchema):
            return InstrumentTypes.FUTURE
        instrument_type = next((
            x for env 
            in set_schema.values() for x, val
            in env.items()
            if val == schema
        ), None)
        if instrument_type == 'OPTION ON FUTURE':
            return InstrumentTypes.OPTION.value
        elif instrument_type == 'CALENDAR_SPREAD':
            return InstrumentTypes.CALENDAR_SPREAD.value
        elif instrument_type == 'SPREAD':
            return InstrumentTypes.FUTURE.value
        elif instrument_type in InstrumentTypes.__members__:
            return InstrumentTypes[instrument_type].value
        else:
            raise RuntimeError(
                f'Instrument type {instrument_type} is unknown'
            )

    @staticmethod
    def __set_instrument_type_by_payload(
            payload: dict,
            sdbadds: SDBAdditional
        ) -> str:
        instrument_type = asyncio.run(sdbadds.get_instrument_type(payload))
        if instrument_type in InstrumentTypes.__members__:
            return InstrumentTypes[instrument_type].value
        else:
            raise RuntimeError(
                f'Instrument type {instrument_type} is unknown'
            )

    @staticmethod
    def __set_instrument_type(
            instrument_type: str
        ) -> str:
        if instrument_type == 'OPTION ON FUTURE':
            return InstrumentTypes.OPTION.value
        elif instrument_type == 'CALENDAR_SPREAD':
            return InstrumentTypes.CALENDAR_SPREAD.value
        elif instrument_type == 'SPREAD':
            return InstrumentTypes.FUTURE.value
        elif instrument_type in InstrumentTypes.__members__:
            return InstrumentTypes[instrument_type].value
        else:
            raise RuntimeError(
                f'Instrument type {instrument_type} is unknown'
            )

    @staticmethod
    def get_part(instr, path: list):
        def safe_getitem(part, key):
            if isinstance(part, dict) and key in part:
                return operator.getitem(part, key)
            elif isinstance(part, (list, str)) and key in range(len(part)):
                return operator.getitem(part, key)
            else:
                return None
        
        return reduce(safe_getitem, path, instr)

    @staticmethod
    def format_maturity(input_data) -> str:
        """
        Make well-formed maturity string (YYYY-MM-DD or YYYY-MM)
        from literally every possible input
        """
        if not input_data:
            return None
        if isinstance(input_data, dict):
            maturity = f"{input_data['year']}-{input_data['month']:0>2}"
            if input_data.get('day'):
                maturity += f"-{input_data['day']:0>2}"
            return maturity
        elif isinstance(input_data, str):
            # 2021-08-01, 20210801, 2021-8-1, 2021-8, 2021-08 
            match = re.match(
                r"(?P<year>\d{4})(-)?(?P<month>(0|1)?\d)(-)?(?P<day>\d{0,2})",
                input_data
            )
            if match:
                maturity = f"{match.group('year')}-{match.group('month'):0>2}"
                if match.group('day'):
                    return f"{maturity}-{match.group('day'):0>2}"
                return maturity
            # Q21, Q2021, 8-2021, 08-21, 082021
            match = re.match(
                r"(?P<month>(0|1)?\d|[FGHJKMNQUVXZ])(-)?(?P<year>(20)?\d{2})$",
                input_data
            )
            if match:
                if match.group('month').isdecimal():
                    month = f"{match.group('month'):0>2}"
                else:
                    month = f"{Months[match.group('month')].value:0>2}"
                return f"20{match.group('year')[-2:]}-{month}"
            # Q1
            match = re.match(
                r"(?P<month>[FGHJKMNQUVXZ])(-)?(?P<year>\d)$",
                input_data
            )
            if match:
                month = f"{Months[match.group('month')].value:0>2}"
                year = int(f"202{match.group('year')}")
                while year < dt.datetime.now().year:
                    year += 10
                return f"{year}-{month}"
            # 1Q2021, 01Q2021, 1Q21
            match = re.match(
                r"(?P<day>\d{1,2})(?P<month>[FGHJKMNQUVXZ])(?P<year>(20)?\d{2})$",
                input_data
            )
            if match:
                day = f"{match.group('day'):0>2}"
                month = f"{Months[match.group('month')].value:0>2}"
                return f"20{match.group('year')[-2:]}-{month}-{day}"
            # 01-08-2021
            match = re.match(
                r"(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})$",
                input_data
            )
            if match:
                return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
            else:
                return None
    
    @staticmethod
    def normalize_date(input_date: Union[str, dict, dt.date, dt.datetime]) -> dt.date:
        if isinstance(input_date, dict):
            return SymbolDB.sdb_to_date(input_date)
        if isinstance(input_date, dt.date):
            return input_date
        if isinstance(input_date, dt.datetime):
            return input_date.date()
        if isinstance(input_date, str):
            input_date = input_date[:-1] if input_date[-1] == 'Z' else input_date
            try:
                return dt.date.fromisoformat(input_date.split('T')[0])
            except ValueError:
                return None
            except AttributeError:
                return None

    @staticmethod
    def _maturity_to_symbolic(maturity: str) -> str:
        if maturity is None:
            return None
        # YYYY-MM
        match = re.match(r'(?P<year>\d{4})-(?P<month>\d{2})$', maturity)
        if match:
            return f"{Months(int(match.group('month'))).name}{match.group('year')}"
        # YYYY-MM-DD
        match = re.match(r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})$', maturity)
        if match:
            return f"{int(match.group('day'))}{Months(int(match.group('month'))).name}{match.group('year')}"
        # Chicago with day
        match = re.match(r'(?P<day>\d{1,2})?(?P<month>[FGHJKMNQUVXZ])(?P<year>\d{4})$', maturity)
        if match:
            month = f"{Months[match.group('month')].value:0>2}"
            if match.group('day'):
                day = f"{match.group('day'):0>2}"
                return f"{match.group('year')}-{month}-{day}"
            return f"{match.group('year')}-{month}"

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @property
    def instrument(self):
        return deepcopy(self._instrument)

    @property
    def reference(self):
        return deepcopy(self._reference)

    @property
    def _id(self) -> str:
        return self._instrument.get('_id', '')

    @property
    def compiled_parent(self):
        if self._parent:
            return asyncio.run(self.sdbadds.build_inheritance(
                [
                    self._parent.compiled_parent,
                    self._parent.instrument
                ], include_self=True
            ))
        if 'path' in self.__dir__():
            use_path = self.path
        else:
            use_path = self._instrument.get('path', [])
        
        if self._id:
            return asyncio.run(self.sdbadds.build_inheritance(
                use_path[-2], include_self=True
            ))
        elif use_path:
            return asyncio.run(self.sdbadds.build_inheritance(
                use_path[-1], include_self=True
            ))
        else:
            return {}


    def set_instrument(self, instrument: dict, parent = None, reference: dict = None):
        if isinstance(instrument, str):
            instrument = asyncio.run(self.sdb.get(instrument))
        self._instrument = instrument if instrument else {}
        self._parent = parent
        self._reference = reference if reference else asyncio.run(self.sdb.get(self._id))

    def force_tree_reload(self, fields: list = None):
        if not fields:
            fields = []
        fields_list = [
                'expiryTime'
            ]
        fields_list.extend([x for x in fields if x not in fields_list])
        asyncio.run(self.sdbadds.load_tree(
            fields=fields_list,
            reload_cache=True,
            return_dict=False
        ))
        self.tree_df = self.sdbadds.tree_df

    def reduce_instrument(self):
        preserve = [
            'gatewayId',
            'accountId',
            'providerId',
            'path',
            'executionSchemeId',
            'isAbstract'
        ]

        def process_dict(child: dict, sibling: dict):
            difference = {}
            if not sibling:
                # omit empty (unset) values, but take care of zero, as it's not an empty value!
                difference.update({
                    key: val for key, val in child.items()
                    if key in preserve
                    or child.get(key) is not None
                })
                return difference
            for key, val in child.items():
                if key in preserve:
                    difference.update({key: val})
                elif key not in sibling:
                    if val is not None: # key not in sibling and val is not empty
                        difference.update({key: val})
                elif val == sibling[key]:
                    if key in ['account', 'gateway']:
                        payload = go_deeper(val, sibling[key])
                        if payload: # should not ever fall out of here, but anyway
                            difference.update({key: payload})
                elif isinstance(val, (dict, list)): # val is dict or list
                    payload = go_deeper(val, sibling[key])
                    if payload:
                        difference.update({key: payload})
                else: # child[key] != sibling[key]
                    # compare if template corresponds to compiled value
                    if isinstance(val, str) and isinstance(sibling[key], dict) and sibling[key].get('base'):
                        sibling[key] = sibling[key]['base'] # ??????
                    if isinstance(val, str) and isinstance(sibling[key], dict) and sibling[key].get('$template'):
                        sibling_val = self.sdbadds.lua_compile(self.instrument, sibling[key].get('$template'))
                        if val != sibling_val:
                            difference.update({key: val})
                    elif val is not None: # eliminate empty values
                        difference.update({key: val})
            return difference

        def process_list_of_dicts(child: list[dict], sibling: list[dict]):
            # assume that all entries in list are the same type
            # firsly let's flatten the dicts
            # we will use these artificial items to catch differencies in order or/and
            # content and then call the real items to be compared and reduced
            if not (child[0].get('account') or child[0].get('gateway')):
                if len(child) != len(sibling):
                    return child
                for num, i in enumerate(child):
                    if i != sibling[num]:
                        return child
                return None
            reduced_list = []
            flatten_child = []
            flatten_sibling = []
            for chi in child:
                flatten_chi = {}
                for chi_v in chi.values():
                    if isinstance(chi_v, str):
                        flatten_chi['route_id'] = chi_v
                    elif isinstance(chi_v, dict):
                        flatten_chi.update(chi_v)
                flatten_child.append(flatten_chi)
            for sib in sibling:
                flatten_sib = {}
                for sib_v in sib.values():
                    if isinstance(sib_v, str):
                        flatten_sib['route_id'] = sib_v
                    elif isinstance(sib_v, dict):
                        flatten_sib.update(sib_v)
                flatten_sibling.append(flatten_sib)
            # now let's compare dicts next to each other with following considerations:
            # · if both lists all the same, we write nothing (easy)
            # · if there's some difference we write down all items from first
            #   to the last that have changes (to preserve the order)
            # · if child n-th route doesn't match with sibling n-th route
            #   we seek this route in sibling and if found pop (x) it out of list:
            #   c:  s: →    c:  s: →    c:  s:
            #   C   A       C   A       C  (d)
            #   A   B       A   B       A   A
            #   B   C       B  (x)      B   B
            #   D   D       D   D       D   D
            #   
            #   and insert the dummy (d) to the n-th place in sibling
            # · we stop to write on the last route with difference

            # firstly let's align lists and place the dummies
            while True: # the cycle breaks when order of flatten_sibling is the same
                moved = None
                for i in range(len(flatten_child)):
                    if len(flatten_sibling) < i + 1 \
                        or flatten_child[i]['route_id'] != flatten_sibling[i]['route_id']:
                        moved = i
                        break
                if moved is None:
                    break
                flatten_child[moved].update({'moved': True})
                # try to find a match if any and pop it out
                match = next((num for num, x
                        in enumerate(flatten_sibling)
                        if x['route_id'] == flatten_child[moved]['route_id']), None)
                if match: # move sibling to meet the child order
                    flatten_sibling.insert(moved, flatten_sibling.pop(match))
                else: # place the dummy if child member is new
                    flatten_sibling.insert(moved, {'route_id': flatten_child[moved]['route_id']})
            stop_write = None
            # now let's catch the differencies
            if len(flatten_child) > len(flatten_sibling):
                stop_write = len(flatten_child)-1
                # should not happen, but anyway
            else:
                # if the only difference is order, we will catch it
                # thanks to new item in child {'moved': True}
                # after the cycle is finished stop_write
                # gets the index of the last child item that has to be written
                for i in range(len(flatten_child)):
                    for key in flatten_child[i]:
                        if flatten_child[i][key] != flatten_sibling[i].get(key):
                            # here we avoid to catch when child's key is False and no such key in sibling
                            if flatten_child[i][key] == False and not flatten_sibling[i].get(key):
                                continue
                            else:
                                stop_write = i
                                break
            if stop_write is not None:
                for j in range(stop_write + 1):
                    sibling_to_compare = next((x for x in sibling
                        if flatten_child[j]['route_id'] in x.values()), None)
                    reduced_member = go_deeper(child[j], sibling_to_compare)
                    if reduced_member:
                        reduced_list.append(reduced_member)
            return reduced_list if reduced_list else None

        def go_deeper(child, sibling):
            if isinstance(child, dict):
                return process_dict(child, sibling)
            elif isinstance(child, list) and len(child) > 0:
                if not sibling: # nothing to inherit
                    return child
                if all(isinstance(x, list) for x in child):
                    list_of_lists = []
                    for num, i in enumerate(child):
                        list_of_lists.append(go_deeper(i, sibling[num]))
                    return list_of_lists
                elif all(isinstance(x, dict) for x in child):
                    return process_list_of_dicts(child, sibling)
                elif set(child) != set(sibling):
                    return child
            elif not sibling or child != sibling:
                return child
                    
        reduced_instrument = go_deeper(self.instrument, self.compiled_parent)
        self._instrument = reduced_instrument

    def validate_instrument(self, contract: bool = False):
        instrument_dict = self.get_instrument if contract else self.instrument
        if self.compiled_parent:
            compiled_instrument = asyncio.run(self.sdbadds.build_inheritance(
                [self.compiled_parent, instrument_dict], include_self=True
            ))
        else:
            compiled_instrument = asyncio.run(self.sdbadds.build_inheritance(
                instrument_dict, include_self=True
            ))
        try:
            self.schema(**compiled_instrument)
            return True
        except ValidationError as valerr:
            if instrument_dict.get('isAbstract') is False:
                self.logger.error(valerr)
            else:
                self.logger.info(valerr)
                return True
            return {
                'validation_errors': valerr.errors()
            }

    def post_instrument(self, dry_run: bool = False):
        if self._id:
            diff = DeepDiff(self.instrument, self.reference)
            if diff:
                self.logger.info(f"{self.instrument['name']}: following changes have been made:")
                self.logger.info(pformat(diff))
                set_sec = self.set_section_id(dry_run)
                validation = self.validate_instrument()
                if validation is True:
                    if not dry_run:
                        response = asyncio.run(self.sdb.update(self.instrument))
                    else:
                        print('Validation passed, updated instrument:')
                        self.sdbadds.fancy_print(self.instrument)
                        return {'_id': self._id, '_rev': True}
                else:
                    return validation
        else:
            set_sec = self.set_section_id(dry_run)
            validation = self.validate_instrument()
            if validation is True:
                if not dry_run:
                    response = asyncio.run(self.sdb.create(self.instrument))
                else:
                    print('Validation passed, new instrument:')
                    self.sdbadds.fancy_print(self.instrument)
                    return {'_id': True, '_rev': True}
            else:
                return validation
        return response

    def wait_for_sdb(self, wait_time: int = 10):
        while True:
            tasks = asyncio.run(self.sdb.get_tasks())
            if not tasks:
                break
            task_queue = [x for x in tasks if x['state'] == 'queued']
            if len(task_queue) < 5:
                break
            self.logger.info('Waiting for sdb...')
            sleep(wait_time)

    def create_new_section(
            self,
            exchange_id: str,
            schedule_id: str,
            dry_run: bool = False
        ):
        exchange_name = next((
            x[0] for x
            in ValidationLists.exchanges
            if x[1] == exchange_id
        ), None)
        schedule_name = next((
            x[0] for x
            in ValidationLists.schedules
            if x[1] == schedule_id
        ), None)
        if exchange_name and schedule_name:
            new_section = {
                "exchangeId": exchange_id,
                "name": f"[{exchange_name}] {schedule_name}",
                "scheduleId": schedule_id,
                'description': ' '
            }
            if dry_run:
                return {'_id': '<<new_sectionId>>'}
            response = asyncio.run(self.sdb.post_section(new_section))
            if response.get('_id'):
                ValidationLists.sections = asyncio.run(
                    self.sdbadds.get_list_from_sdb(
                        SdbLists.SECTIONS.value,
                        id_only=False,
                        force_reload=True
                    )
                )
            return response
        return {}

    def set_section_id(self, dry_run: bool = False):
        compiled = asyncio.run(self.sdbadds.build_inheritance(
            [
                self.compiled_parent,
                self.instrument
            ],
            include_self=True
        ))
        try:
            validated = SetSectionId(**compiled)
            if compiled.get('sectionId') != validated.section_id:
                self._instrument['sectionId'] = validated.section_id
            return True
        except ValidationError as valerr:
            errors = [v['msg'] for v in valerr.errors()]
            no_section = next((
                x for x
                in errors
                if "is not found in SymbolDB" in x
            ), None)
            if not no_section:
                self.logger.error(pformat(valerr.errors()))
                return False
            self.logger.warning(no_section)
            result = self.create_new_section(
                compiled['exchangeId'],
                compiled['scheduleId'],
                dry_run
            )
            if result.get('_id'):
                self._instrument['sectionId'] = result['_id']
                return True
            return False

    def get_field_properties(self, path: list = [], **kwargs) -> dict:
        try:
            props = self.navi.schema_lookup(path)
            if len(props) == 1:
                return props[0]
            elif path[-1] in kwargs:
                props = next(
                    x for x
                    in props
                    if x.get('type') == kwargs[path[-1]]
                    or x.get('title') == kwargs[path[-1]]
                )
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

    def set_fields(self, payload: dict, **kwargs):
        def update_list(part, path, opts, **kwargs):
            for item in part:
                if not isinstance(item, (dict, list)):
                    if item not in self.get_part(self.instrument, path):                
                        if opts and item not in opts:
                            self.logger.warning(
                                f'{item} is not in list of possible values for {path[-1]}, not updated'
                            )
                            continue
                        self.get_part(self._instrument, path).append(item)
                elif isinstance(item, list):
                    self.logger.error(
                            f'Updating list of lists is not implemented, sorry'
                        )
                else:
                    id_key = next((x for x in item.keys() if str(x).endswith('Id')), None)
                    if not id_key:
                        self.logger.error(
                            f'Cannot update list of dicts {path[-1]}'
                        )
                        continue
                    present = next((
                        num for num, x
                        in enumerate(self.get_part(self._instrument, path))
                        if x.get(id_key) == item[id_key]
                    ), None)
                    if present is not None:
                        result = go_deeper(item, path + [present], **kwargs)
                    elif kwargs.get(path[-1]) in ['first', 'head', 'beginning']:

                        self.get_part(self._instrument, path).insert(0, {id_key: item.pop(id_key)})
                        go_deeper(item, path + [0], **kwargs)
                    else:
                        self.get_part(self._instrument, path).append({id_key: item.pop(id_key)})
                        eol = len(self.get_part(self._instrument, path)) - 1
                        go_deeper(item, path + [eol], **kwargs)
                    

        def go_deeper(part, path: list = None, **kwargs):
            field_props = self.get_field_properties(path, **kwargs)
            if not field_props:
                return False
            # prepare the place in dict
            if isinstance(part, bool) and type_mapping[field_props['type']] != bool:
                self.logger.warning(
                    f"{part} is wrong type (should be {type_mapping[field_props['type']]})"
                )
                return False
            try:
                part = type_mapping[field_props['type']](part)
            except ValueError:
                self.logger.warning(
                    f"{part} is wrong type (should be {type_mapping[field_props['type']]})"
                )
                return False
            except TypeError:
                self.logger.warning(f"{'/'.join(path)} is set to None")
            self.__check_n_create(path, **kwargs)
            opts = []
            if field_props.get('opts_list'):
                if all(isinstance(x, str) for x in field_props['opts_list']):
                    opts = field_props['opts_list']
                elif all(isinstance(x, tuple) for x in field_props['opts_list']):
                    opts = [x[1] for x in field_props['opts_list']]
            if isinstance(part, dict):
                for key, val in part.items():
                    result = go_deeper(val, path + [key], **kwargs)
            elif isinstance(part, list):
                result = update_list(part, path, opts, **kwargs)
            else:
                self.get_part(self._instrument, path[:-1])[path[-1]] = part
        for key, val in payload.items():
            go_deeper(val, [key], **kwargs)


    
    def set_field_value(self, value, path: list = [], **kwargs):
        # path should include the field name
        # validate value
        if not self.instrument or not path:
            return False
        field_props = self.get_field_properties(path, **kwargs)
        if not field_props:
            return False
        # prepare the place in dict
        if isinstance(value, bool) and type_mapping[field_props['type']] != bool:
            self.logger.warning(
                f"{value} is wrong type (should be {type_mapping[field_props['type']]})"
            )
            return False
        try:
            value = type_mapping[field_props['type']](value)
        except ValueError:
            self.logger.warning(
                f"{value} is wrong type (should be {type_mapping[field_props['type']]})"
            )
            return False
        except TypeError:
            self.logger.warning(f"{'/'.join(path)} is set to None")
        self.__check_n_create(path, **kwargs)
        if field_props.get('opts_list'):
            if isinstance(field_props['opts_list'][0], str) \
                and value not in field_props['opts_list']:
                
                self.logger.warning(
                    f'{value} is not in list of possible values for {path[-1]}, not updated'
                )
                return False
            if isinstance(field_props['opts_list'][0], tuple) \
                and value not in [x[1] for x in field_props['opts_list']]:

                self.logger.warning(
                    f'{value} is not in list of possible values for {path[-1]}, not updated'
                )
                return False
        self.get_part(self._instrument, path[:-1])[path[-1]] = value
        # should not be anything beyond that so terminate
        return True

    def set_provider_overrides(self, provider, **kwargs):
        '''
        · provider is UPPER CASE human readable name e.g. REUTERS or LEK
        · properties are passed through kwargs as a dict: {'property_name': value}
          e.g. {'symbolName': 'AAPL'}
        · properties are validated through the validation schemes, so if you pass
          (provider='REUTERS', **{'ric': 'LSE.L'}) it will find its right place as:
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
          (provider='REUTERS', **{'reutersProperties/quoteRic/base': 'JGL'})
        · if it happens that feed provider has the same name as broker provider
          (e.g. LAMBDA or HTTP) you could pass additional kwarg broker=True or feed=True

        '''
        feed_provider_id = next((
            x[1] for x in asyncio.run(
                self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
            ) if x[0] == provider
        ), None)
        broker_provider_id = next((
            x[1] for x
            in asyncio.run(
                self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
            ) if x[0] == provider
        ), None)
        if feed_provider_id and kwargs.get('broker') != True:
            additional = ['feeds', 'providerOverrides', feed_provider_id]
        elif broker_provider_id and kwargs.get('feed') != True:
            additional = ['brokers', 'providerOverrides', broker_provider_id]
        else:
            self.logger.warning(
                f'{provider} is not found in available feed or broker providers'
            )
            return False
        success = dict()
        for prop, val in kwargs.items():
            if prop in ['feed', 'broker']:
                success.update({prop: val})                
                continue
            field_type = next(
                x for x, y
                in type_mapping.items()
                if y == type(val)
            )

            path = self.navi.find_path(
                '/'.join(additional + [prop]),
                field_type,
                *additional
            )
            if not path:
                self.logger.warning(f'Cannot find a path to {prop}')
                return False
            # replace dummy
            path[2] = additional[-1]
            prop_type = {
                path[-1]: mapped for mapped, x
                in type_mapping.items()
                if isinstance(val, x)
            }
            if self.set_field_value(val, path, **prop_type):
                success.update({prop: val})
            else:
                self.logger.warning(
                    f'{prop}: {val} is not written to the instrument'
                )
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
        :param args: fields of interest, could be provided with some last items of path divided by '/',
            e.g. 'ric/suffix'
        :return: list of field values same length and order as args
        '''
        feed_provider_id = next((
            x[1] for x
            in asyncio.run(
                self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
            ) if x[0] == provider
        ), None)
        broker_provider_id = next((
            x[1] for x
            in asyncio.run(
                self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
            ) if x[0] == provider
        ), None)
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
            path = self.navi.find_path(
                '/'.join(additional + [arg]),
                *additional
            )
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

    def get_routes(self, compiled=False, default=False) -> list:
        if default:
            routes = self.compiled_parent.get('brokers', {}).get('accounts', [])
        elif compiled:
            routes = asyncio.run(
                self.sdbadds.build_inheritance(
                    self._instrument,
                    include_self=True
                )
            ).get('brokers', {}).get('accounts', [])
        else:
            routes = self._instrument.get('brokers', {}).get('accounts', [])
        result = []
        for r in routes:
            route_name = next((
                x[0] for x
                in asyncio.run(
                    self.sdbadds.get_list_from_sdb(SdbLists.ACCOUNTS.value)
                )
                if x[1] == r['accountId']
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
        part = self._instrument
        for num, p in enumerate(path):
            if num < len(path) - 1 and isinstance(p, str):
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
                                self.logger.warning(
                                    f'{item} is not in list of possible values for {path[num-1]}, not added'
                                )
                                continue
                            if type(item) != item_type:
                                self.logger.warning(
                                    f'{item} is wrong type for {path[num-1]} (should be {item_type}), not added'
                                )
                                continue
                            good_ones.append(item)
                        self.get_part(self._instrument, path[:num-1])[path[num-1]] = good_ones
                    else:
                        if lookup.get('opts_list') and p not in lookup['opts_list']:
                            self.logger.warning(
                                f'{p} is not in list of possible values for {path[num-1]}, not added'
                            )
                            return None
                        if type(p) != item_type:
                            self.logger.warning(
                                f'{p} is wrong type for {path[num-1]} (should be {item_type}), not added'
                            )
                            return None
                        self.get_part(self._instrument, path[:num-1])[path[num-1]].append(p)

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
                    self.logger.warning(f"{p} is wrong type (should be {type_mapping[lookup['type']]})")
                    return None
                if lookup.get('opts_list') and value not in lookup['opts_list']:
                    self.logger.warning(f'{value} is not in list of possible values for {path[num-1]}, not updated')
                    return None
                self.get_part(self._instrument, path[:num-1])[path[num-1]] = value
                # should not be anything beyond that so terminate
                return None
            part = self.get_part(self._instrument, path[:num+1])
