from abc import ABC, abstractmethod
import datetime as dt
import logging
import re
from enum import Enum
from pydantic import BaseModel, Field, validator, root_validator, ValidationError
from typing import Optional, Union, Dict, Tuple, List, Any

# do not import schemas with aliases!
from libs.sdb_schemas import (
    SdbDate,
    ValidationLists,
    Identifiers
)


class Months(Enum):
    F = 1
    G = 2
    H = 3
    J = 4
    K = 5
    M = 6
    N = 7
    Q = 8
    U = 9
    V = 10
    X = 11
    Z = 12

class CallMonths(Enum):
    A = 1
    B = 2
    C = 3
    D = 4
    E = 5
    F = 6
    G = 7
    H = 8
    I = 9
    J = 10
    K = 11
    L = 12

class PutMonths(Enum):
    M = 1
    N = 2
    O = 3
    P = 4
    Q = 5
    R = 6
    S = 7
    T = 8
    U = 9
    V = 10
    W = 11
    X = 12

class FractionCurrencies(Enum):
    AUc = 'AUD' # Australian Cent
    BWt = 'BWP' # Botswanan Thebe
    EUc = 'EUR' # Euro Cent
    GBp = 'GBP' # Great Britain Pence
    ILa = 'ILS' # Israeli Agora
    NAc = 'NAD' # Namibian Cent
    NZc = 'NZD' # New Zealand Cent
    SGc = 'SGD' # Singapore Cent
    ZAc = 'ZAR' # South African Cent
    USc = 'USD' # US Cent



def convert_maturity(
        maturity_symbolic: Union[str, dt.date, dt.datetime],
        day: bool = False,
        option_months: bool = False
    ):
    """
    :param maturity_symbolic: could be any representation of date as well as Chicago notation (like Z2021)
    :return: sdb-compliant string based on maturity date (like 2021-12)
    """


    if isinstance(maturity_symbolic, str) and re.match(r'\d{4}-\d{2}(-\d{2})?', maturity_symbolic):
        if not day:
            return maturity_symbolic[:7]
        else:
            return maturity_symbolic
    elif isinstance(maturity_symbolic, (dt.date, dt.datetime)):
        day = f"0{maturity_symbolic.day}" if maturity_symbolic.day < 10 else str(maturity_symbolic.day)
        month = f"0{maturity_symbolic.month}" if maturity_symbolic.month < 10 else str(maturity_symbolic.month)
        if day:
            return f"{maturity_symbolic.year}-{month}-{day}"
        else:
            return f"{maturity_symbolic.year}-{month}"
    elif isinstance(maturity_symbolic, str):
        if maturity_symbolic.isdecimal() and len(maturity_symbolic) == 6:
            return f"{maturity_symbolic[:4]}-{maturity_symbolic[4:]}"
        elif maturity_symbolic.isdecimal() and len(maturity_symbolic) == 8:
            if not day:
                return f"{maturity_symbolic[:4]}-{maturity_symbolic[4:6]}"
            return f"{maturity_symbolic[:4]}-{maturity_symbolic[4:6]}-{maturity_symbolic[6:]}"
        elif not option_months and maturity_symbolic[0] in Months.__members__:
            mmy_month = Months[maturity_symbolic[0]].value
        elif maturity_symbolic[0] in CallMonths.__members__:
            mmy_month = CallMonths[maturity_symbolic[0]].value
        elif maturity_symbolic[0] in PutMonths.__members__:
            mmy_month = PutMonths[maturity_symbolic[0]].value
        mmy_year = int(f"202{maturity_symbolic[-1]}")
        while dt.date(mmy_year, mmy_month, 1).year < dt.date.today().year:
            mmy_year += 10
        return f"{mmy_year}-0{mmy_month}" if mmy_month < 10 else f"{mmy_year}-{mmy_month}"

# Some helping pieces
class UnderlyingId(BaseModel):
    type: str = Field(
        'symbolId',
        const=True
    )
    id: str

class StrikePrice(BaseModel):
    strikePrice: float
    isAvailable: Optional[bool] = Field(
        True
    )
    identifiers: Optional[Identifiers]

    @root_validator(pre=True, allow_reuse=True)
    def mk_identifiers(cls, values):
        identifiers = {
            fut_id: values[fut_id] for fut_id
            in ['ISIN', 'RIC', 'SEDOL', 'FIGI', 'CUSIP']
            if values.get(fut_id)
        }
        if identifiers:
            values['identifiers'] = identifiers
        return values

class StrikePrices(BaseModel):
    CALL: List[StrikePrice]
    PUT: List[StrikePrice]

    @validator('CALL', 'PUT', pre=True, allow_reuse=True)
    def sort_strikes(cls, item):
        item = sorted(item, key=lambda s: s['strikePrice'])
        return item

class Leg(BaseModel):
    quantity: int
    exanteId: str

# Here we define models to pass to instrument adders

class FutureContract(BaseModel):
    name: str
    expiry: SdbDate
    maturityDate: SdbDate
    maturity: str
    identifiers: Optional[Identifiers]
    isAbstract: bool = Field(
        False,
        const=True
    )

    @root_validator(pre=True, allow_reuse=True)
    def format_future_name_and_maturity(cls, values):
        values['maturity'] = convert_maturity(values.get('maturity'))
        values['name'] = values['maturity']
        if isinstance(values.get('expiry'), str) and values['expiry']: # not empty str
            if values['expiry'][-1] == 'Z':
                values['expiry'] = values['expiry'][:-1]
            if 'T' in values['expiry']:
                # it's ok to raise ValueError here
                values['expiry'] = dt.datetime.fromisoformat(values['expiry'])
            else:
                values['expiry'] = dt.date.fromisoformat(values['expiry'])
        exp = {
            'day': values['expiry'].day,
            'month': values['expiry'].month,
            'year': values['expiry'].year
        }
        if isinstance(values['expiry'], dt.datetime):
            exp.update({'time': values['expiry'].strftime('%H:%M:%S')})
        values['expiry'] = exp
        values['maturityDate'] = {
            'year': values['maturity'].split('-')[0],
            'month': values['maturity'].split('-')[1]
        }
        if len(values['maturity'].split('-')) == 3:
            values['maturityDate'].update({
                'day': values['maturity'].split('-')[2]
            })
        return values

    @root_validator(pre=True, allow_reuse=True)
    def mk_identifiers(cls, values):
        identifiers = {
            fut_id: values[fut_id] for fut_id
            in ['ISIN', 'RIC', 'SEDOL', 'FIGI', 'CUSIP']
            if values.get(fut_id)
        }
        if identifiers:
            values['identifiers'] = identifiers
        return values

class ParsedFutureSchema(BaseModel):
    ticker: str
    exchange: str
    shortName: str
    description: Optional[str]
    expiry: Optional[dict]
    feedMinPriceIncrement: Optional[float]
    orderMinPriceIncrement: Optional[float]
    contractMultiplier: Optional[float]
    currency: Optional[str]
    country: Optional[str]
    MIC: Optional[str]
    isPhysicalDelivery: Optional[bool]
    underlyingId: Optional[UnderlyingId]
    base_ric_: Optional[str]
    isAbstract: bool = Field(
        True,
        const=True
    )


    @root_validator(pre=True, allow_reuse=True)
    def mk_fut_description(cls, values):
        if not values.get('description') and values.get('shortName'):
            values['description'] = values.get('shortName')
        elif not values.get('shortName'):
            values['shortName'] = values.get('description')
        return values

    @validator('country', allow_reuse=True)
    def check_country(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item
        
    @validator('currency', allow_reuse=True)
    def check_currency(cls, item):
        if item not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{item} is invalid currency')
        return item

    @validator('isPhysicalDelivery', allow_reuse=True, pre=True)
    def check_settlement(cls, item):
        if isinstance(item, bool):
            return item
        elif isinstance(item, str) and item.lower() in ['physical', 'deliverable']:
            return True
        elif isinstance(item, str) and item.lower() in ['financial', 'cash']:
            return False
        elif item is None:
            return None
        else:
            raise ValueError(f'Settlement "{item}" is unknown type')

    @root_validator(pre=True, allow_reuse=True)
    def mk_expiry_time(cls, values):
        try:
            if values.get('expiry_time_') and dt.time.fromisoformat(values.get('expiry_time_')):
                values['expiry'] = {
                    'year': 2100,
                    'month': 1,
                    'day': 1,
                    'time': values['expiry_time_']}
        except Exception:
            pass
        return values


class CalendarSpreadContract(BaseModel):
    name: str
    expiry: SdbDate
    nearMaturityDate: SdbDate
    farMaturityDate: SdbDate
    maturity: str
    far_maturity_: str
    identifiers: Optional[Identifiers]
    isAbstract: bool = Field(
        False,
        const=True
    )
    legs: List[Leg]
    leg_gap: int

    @root_validator(pre=True)
    def format_calendar_spread_name_and_maturity(cls, values):
        values['name'] = f"{values['maturity']} {values['far_maturity_']}"
        if isinstance(values.get('expiry'), str) and values['expiry']: # not empty str
            if values['expiry'][-1] == 'Z':
                values['expiry'] = values['expiry'][:-1]
            if 'T' in values['expiry']:
                # it's ok to raise ValueError here
                values['expiry'] = dt.datetime.fromisoformat(values['expiry'])
            else:
                values['expiry'] = dt.date.fromisoformat(values['expiry'])
        exp = {
            'day': values['expiry'].day,
            'month': values['expiry'].month,
            'year': values['expiry'].year
        }
        if isinstance(values['expiry'], dt.datetime):
            exp.update({'time': values['expiry'].strftime('%H:%M:%S')})
        values['expiry'] = exp
        values['nearMaturityDate'] = {
            'year': values['maturity'].split('-')[0],
            'month': values['maturity'].split('-')[1]
        }
        if len(values['maturity'].split('-')) == 3:
            values['nearMaturityDate'].update({
                'day': values['maturity'].split('-')[2]
            })
        values['farMaturityDate'] = {
            'year': values['far_maturity_'].split('-')[0],
            'month': values['far_maturity_'].split('-')[1]
        }
        if len(values['far_maturity_'].split('-')) == 3:
            values['farMaturityDate'].update({
                'day': values['far_maturity_'].split('-')[2]
            })
        
        return values

class ProductSpreadContract(BaseModel):
    name: str
    expiry: SdbDate
    maturityDate: SdbDate
    maturity: str
    identifiers: Optional[Identifiers]
    isAbstract: bool = Field(
        False,
        const=True
    )
    legs: List[Leg]

    @root_validator(pre=True)
    def format_product_spread_name_and_maturity(cls, values):
        values['name'] = values['maturity']
        if isinstance(values.get('expiry'), str) and values['expiry']: # not empty str
            if values['expiry'][-1] == 'Z':
                values['expiry'] = values['expiry'][:-1]
            if 'T' in values['expiry']:
                # it's ok to raise ValueError here
                values['expiry'] = dt.datetime.fromisoformat(values['expiry'])
            else:
                values['expiry'] = dt.date.fromisoformat(values['expiry'])
        exp = {
            'day': values['expiry'].day,
            'month': values['expiry'].month,
            'year': values['expiry'].year
        }
        if isinstance(values['expiry'], dt.datetime):
            exp.update({'time': values['expiry'].strftime('%H:%M:%S')})
        values['expiry'] = exp
        values['maturityDate'] = {
            'year': values['maturity'].split('-')[0],
            'month': values['maturity'].split('-')[1]
        }
        if len(values['maturity'].split('-')) == 3:
            values['maturityDate'].update({
                'day': values['maturity'].split('-')[2]
            })
        return values

class ParsedSpreadSchema(BaseModel):
    ticker: str
    second_ticker_: Optional[str]
    exchange: str
    shortName: str
    description: Optional[str]
    feedMinPriceIncrement: Optional[float]
    orderMinPriceIncrement: Optional[float]
    contractMultiplier: Optional[float]
    currency: Optional[str]
    country: Optional[str]
    MIC: Optional[str]
    isPhysicalDelivery: bool = Field(
        False,
        const=True
    )
    base_ric_: Optional[str]
    isAbstract: bool = Field(
        True,
        const=True
    )

    @root_validator(pre=True, allow_reuse=True)
    def mk_spread_description(cls, values):
        values['description'] = values['shortName']
        return values

    @validator('country', allow_reuse=True)
    def check_country(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item

    @validator('currency', allow_reuse=True)
    def check_currency(cls, item):
        if item not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{item} is invalid currency')
        return item


class OptionContract(BaseModel):
    name: str
    exchange: str
    is_weekly_: bool
    expiry: SdbDate
    maturityDate: SdbDate
    maturity: str
    strikePrices: StrikePrices
    underlyingId: Optional[UnderlyingId]
    isAbstract: bool = Field(
        False,
        const=True
    )

    @root_validator(pre=True)
    def format_option_name_and_maturity(cls, values):
        if values['exchange'] == 'CBOE' or (values['exchange'] == 'FORTS' and values['weekly'] is True):
            values['maturity'] = convert_maturity(values.get('maturity'), day=True)
        else:
            values['maturity'] = convert_maturity(values.get('maturity'))
        values['name'] = values['maturity']
        if isinstance(values.get('expiry'), str) and values['expiry']: # not empty str
            if values['expiry'][-1] == 'Z':
                values['expiry'] = values['expiry'][:-1]
            if 'T' in values['expiry']:
                # it's ok to raise ValueError here
                values['expiry'] = dt.datetime.fromisoformat(values['expiry'])
            else:
                values['expiry'] = dt.date.fromisoformat(values['expiry'])
        exp = {
            'day': values['expiry'].day,
            'month': values['expiry'].month,
            'year': values['expiry'].year
        }
        if isinstance(values['expiry'], dt.datetime):
            exp.update({'time': values['expiry'].strftime('%H:%M:%S')})
        if values.get('_expiry_time'):
            exp.update({'time': values['_expiry_time']})
        values['expiry'] = exp
        values['maturityDate'] = {
            'year': values['maturity'].split('-')[0],
            'month': values['maturity'].split('-')[1]
        }
        if len(values['maturity'].split('-')) == 3:
            values['maturityDate'].update({
                'day': values['maturity'].split('-')[2]
            })
        return values

class ParsedOptionSchema(BaseModel):
    ticker: str
    exchange: str
    shortName: str
    description: Optional[str]
    feedMinPriceIncrement: Optional[float]
    orderMinPriceIncrement: Optional[float]
    contractMultiplier: Optional[float]
    currency: Optional[str]
    country: Optional[str]
    MIC: Optional[str]
    isPhysicalDelivery: Optional[bool]
    exerciseStyle: Optional[str]
    underlyingId: Optional[UnderlyingId]
    base_ric_: Optional[str]
    strike_price_multiplier_: Optional[float]
    expiry_time_: Optional[str]
    expiry: Optional[SdbDate]
    isAbstract: bool = Field(
        True,
        const=True
    )

    @root_validator(pre=True, allow_reuse=True)
    def mk__opt_description(cls, values):
        if not values.get('description') and values.get('shortName'):
            values['description'] = values.get('shortName', '')
        elif not values.get('shortName'):
            values['shortName'] = values.get('description', None)
        return values

    @validator('country', allow_reuse=True)
    def check_country(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item
        
    @validator('currency', allow_reuse=True)
    def check_currency(cls, item):
        if item not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{item} is invalid currency')
        return item

    @validator('isPhysicalDelivery', allow_reuse=True, pre=True)
    def check_settlement(cls, item):
        if isinstance(item, bool):
            return item
        elif isinstance(item, str) and item.lower() in ['physical', 'deliverable', 'close']:
            return True
        elif isinstance(item, str) and item.lower() in ['financial', 'cash']:
            return False
        elif item is None:
            return None
        else:
            raise ValueError(f'Settlement "{item}" is unknown type')

    @validator('exerciseStyle', allow_reuse=True)
    def check_exercise_style(cls, item):
        if item not in ValidationLists.exercise_styles:
            raise ValueError(f'{item} is invalid exercise style')
        return item

    @root_validator(pre=True, allow_reuse=True)
    def mk_expiry_time(cls, values):
        try:
            if values.get('expiry_time_'):
                values['expiry'] = {
                    'year': 2100,
                    'month': 1,
                    'day': 1,
                    'time': values['expiry_time_']}
        except Exception:
            pass
        return values


class ParsedStockSchema(BaseModel):
    ticker: str
    exchange: str
    identifiers: Identifiers
    shortname: str = Field(
        alias='shortName'
    )
    description: str
    feed_mpi: Optional[float] = Field(
        alias='feedMinPriceIncrement'
    )
    order_mpi: Optional[float] = Field(
        alias='orderMinPriceIncrement'
    )
    contract_multiplier: float = Field(
        alias='contractMutliplier'
    )
    currency: Optional[str]
    country: Optional[str]

    @root_validator(pre=True, allow_reuse=True)
    def mk_mpi(cls, values):
        if values.get('mpi'):
            values['feed_mpi'] = values['mpi']
            values['order_mpi'] = values['mpi']
        return values

    @root_validator(pre=True, allow_reuse=True)
    def mk_description(cls, values):
        if not values.get('description'):
            values['descripton'] = values['shortName']
        elif not values.get('shortName'):
            values['shortName'] = values['descripton']

    @validator('country', allow_reuse=True)
    def check_country(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item
        
    @validator('currency', allow_reuse=True)
    def check_currency(cls, item):
        if item not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{item} is invalid currency')
        return item

class ExchangeParser(ABC):
    to_sdb_schema = {
        'FUTURE': {
            'series': ParsedFutureSchema,
            'contract': FutureContract
        },
        'OPTION': {
            'series': ParsedOptionSchema,
            'contract': OptionContract
        },
        'OPTION ON FUTURE': {
            'series': ParsedOptionSchema,
            'contract': OptionContract
        },
        'SPREAD': {
            'series': ParsedSpreadSchema,
            'contract': ProductSpreadContract
        },
        'CALENDAR_SPREAD': {
            'series': ParsedSpreadSchema,
            'contract': CalendarSpreadContract
        }
    }


    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def transform_to_sdb(
            self,
            series_data: dict,
            contracts: list = None,
            product: str = 'FUTURE',
            spread_type: str = None
        ):
        if product in [
            'FUTURE',
            'OPTION',
            'OPTION ON FUTURE',
            'SPREAD',
            'CALENDAR_SPREAD'
        ]:
            series_folder = {}
            if series_data:
                try:
                    if spread_type:
                        series = self.to_sdb_schema[spread_type]['series'](**series_data)
                    else:
                        series = self.to_sdb_schema[product]['series'](**series_data)
                except ValidationError as valerr:
                    self.logger.error(
                        f"{series_data.get('ticker')}.{series_data.get('exchange')}: "
                        "folder validation has failed"
                    )
                    self.logger.error(valerr.errors())
                    return {
                        'validation_errors': valerr.errors()
                    }
                series_exclude = {
                    x for x
                    in series.__dir__()
                    if x[-1] == '_'
                    and x[0] != '_'
                }
                series_exclude.update({'strikePrice'})
                series_folder = series.dict(
                    exclude_none=True,
                    exclude=series_exclude
                )
            ready_to_add = []
            if contracts:
                contracts = sorted(contracts, key=lambda d: d['expiry'])
                for c in contracts:
                    try:
                        if spread_type:
                            expiration = self.to_sdb_schema[spread_type]['contract'](**c)
                        else:
                            expiration = self.to_sdb_schema[product]['contract'](**c)
                        expiration_exclude = {
                            x for x
                            in expiration.__dir__()
                            if x[-1] == '_'
                            and x[0] != '_'
                        }
                        expiration_exclude.update({'maturity', 'exchange'})
                        ready_to_add.append(
                            expiration.dict(
                                exclude_none=True,
                                exclude=expiration_exclude
                            )
                        )
                    except ValidationError as valerr:
                        self.logger.warning(
                            f"{c.get('ticker')}.{c.get('exchange')} {c.get('maturity')}: "
                            "contract validation has failed"
                        )
                        self.logger.warning(valerr.errors())
            return {
                'series': series_folder,
                'contracts': ready_to_add
            }

    @abstractmethod
    def futures(
            self,
            series: str,
            overrides: dict = None,
            **kwargs
        ):
        """
        method to get futures data from CP
        """
        raise NotImplementedError

    @abstractmethod
    def options(
            self,
            series: str,
            overrides: dict = None,
            product: str = 'OPTION ON FUTURE',
            **kwargs
        ):
        """
        method to get options data from CP
        """
        raise NotImplementedError

    @abstractmethod
    def spreads(
            self,
            series: str,
            overrides: dict = None,
            spread_type: str = 'CALENDAR SPREAD',
            **kwargs
        ):
        """
        method to get spreads data from CP
        """
        raise NotImplementedError
    
    # @abstractmethod
    # def stocks(self, series: str, overrides: dict = None, **kwargs):
    #     """
    #     method to get options data from CP
    #     """
    #     raise NotImplementedError

    # @staticmethod
    # def mk_ticker(ticker: str, delimiters: Tuple[str] = (".", "p"), new: str = "/") -> str:
    #     """
    #     According to MO current identification rules are"
    #     - replace all preffered shares delimeters with `/`
    #     """
    #     for c in delimiters:
    #         if c in ticker:
    #             ticker = ticker.replace(c, new)
    #     return ticker
