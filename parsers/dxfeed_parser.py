#!/usr/bin/env python3

'''
https://downloads.dxfeed.com/specifications/dxFeed_Instrument_Profile_Format.pdf
https://downloads.dxfeed.com/specifications/dxFeed-Symbol-Guide.pdf
'''

import asyncio
from copy import deepcopy
import datetime as dt
import logging
from pprint import pformat
import pandas as pd
import re
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional
from libs.parsers import (
    DxFeed,
    Months,
    convert_maturity,
    ExchangeParser
)
from libs.sdb_schemas import Identifiers
from pydantic import (
    BaseModel,
    Field,
    validator,
    root_validator,
    ValidationError
)
from typing import Optional, List, Union

exchange_set = {
    'CBOE': {
        'suffix': [''],
        'maturity': '??????',
        'underlying': [
            'stock',
            'index'
        ]
    },
    'CBOT': {
        'suffix': ['XCBT'],
        'maturity': '???',
        'underlying': ['future']
    },
    'CME': {
        'suffix': ['XCME'],
        'maturity': '???',
        'underlying': ['future']
    },
    'COMEX': {
        'suffix': ['XCEC'],
        'maturity': '???',
        'underlying': ['future']
    },
    'EUREX': {
        'suffix': ['XEUR'],
        'maturity': '??????',
        'underlying': ['future', 'index']
    },
    'NYMEX': {
        'suffix': ['XNYM'],
        'maturity': '???',
        'underlying': ['future']
    },
    'ICE': {
        'suffix': [
            'IFEU', # Europe futures
            'IFUS', # US futures
            'ICEU', # ICE Europe
            'IFLO', # LIFFE options
            'IFLL', # LIFFE futures
            'IFLX', # LIFFE commodities
            'ICUS'
        ],
        'maturity': '???',
        'underlying': ['future']
    },
    'LIFFE': {
        'suffix': [
            'IFEU', # Europe futures
            'IFUS', # US futures
            'ICEU', # ICE Europe
            'IFLO', # LIFFE options
            'IFLL', # LIFFE futures
            'IFLX', # LIFFE commodities
            'ICUS'
        ],
        'maturity': '???',
        'underlying': ['future']
    }
}

def dxfeed_maturity_to_sdb(dxfeed: str): # w/o day
    short = re.compile(r'(?P<ticker>\w+)(?P<maturity>[FGHJKMNQUVXZ]\d{2})([CP]\d+\.?\d+)?(?P<suffix>\:\w{4})$')
    mid = re.compile(r'(?P<ticker>\w+)(?P<maturity>[FGHJKMNQUVXZ]\d{4})(?P<suffix>\:\w{4})$')
    long = re.compile(r'(?P<ticker>\w+)(?P<maturity>\d{6})([CP]\d+\.?\d+)?(?P<suffix>\:\w{4})?$') # there is no suffix on CBOE options

    for regex in short, mid, long:
        match = re.search(regex, dxfeed)
        if not match:
            continue
        if regex == long:
            month_num = int(match.group('maturity')[2:4])
            if not match.group('suffix') and int(match.group('maturity')[-2:]) < 10:
                return (
                    match.group('ticker'),
                    f"{match.group('maturity')[-1]}{Months(month_num).name}20{match.group('maturity')[:2]}"
                )
            elif not match.group('suffix'):
                return (
                    match.group('ticker'),
                    f"{match.group('maturity')[-2:]}{Months(month_num).name}20{match.group('maturity')[:2]}"
                )
            else:
                return (
                    match.group('ticker'),
                    f"{Months(month_num).name}20{match.group('maturity')[:2]}"
                )
        elif regex == mid:
            return match.group('ticker'), f"{match.group('maturity')[0]}20{match.group('maturity')[3:5]}" # need to check if valid
        elif regex == short:
            return match.group('ticker'), f"{match.group('maturity')[0]}20{match.group('maturity')[1:3]}"
    return dxfeed, None

def date_dict_to_str(sdb_dict: dict) -> str:
    date_str = f"{sdb_dict['year']}-{sdb_dict['month']}" if sdb_dict['month'] > 9 \
        else f"{sdb_dict['year']}-0{sdb_dict['month']}"
    if sdb_dict.get('day'):
        date_str += f"-{sdb_dict['day']}" if sdb_dict['day'] > 9 \
            else f"-0{sdb_dict['day']}"
    return date_str


class DerivativeSchema(BaseModel):
    ticker: str
    exchange: str
    symbol_override_: Optional[str]
    identifier_: str = Field(
        alias='SYMBOL'
    )
    MIC: str = Field(
        alias='OPOL'
    )
    currency: Optional[str] = Field(
        alias='CURRENCY'
    )
    expiry: Union[dt.datetime, dt.date] = Field(
        alias='EXPIRATION'
    )
    expiry_time_: Optional[str]
    contractMultiplier: Optional[float] = Field(
        alias='MULTIPLIER'
    )
    maturity: str = Field(
        alias='MMY'
    )
    feedMinPriceIncrement: Optional[float] = Field(
        alias='PRICE_INCREMENTS'
    )
    orderMinPriceIncrement: Optional[float] = Field(
        alias='PRICE_INCREMENTS'
    )
    CFI: Optional[str] = Field(
        alias='CFI'
    )
    ISIN: Optional[str] = Field(
        alias='ISIN'
    )
    is_weekly_: Optional[bool] = Field(
        alias='EXPIRATION_STYLE'
    )
    isPhysicalDelivery: Optional[bool] = Field(
        alias='SETTLEMENT_STYLE'
    )

    @root_validator(pre=True)
    def normalize_derivative(cls, values):
        # if derivative has First Notice Day and it earlier than expiration we should use it as expiration date
        try:
            if values.get('FND'):
                values['EXPIRATION'] = min(
                    dt.date.fromisoformat(values['FND']),
                    dt.date.fromisoformat(values['EXPIRATION'])
                )
            else:
                values['EXPIRATION'] = dt.date.fromisoformat(values['EXPIRATION'])
        except Exception:
            raise ValueError(f"Wrong expiration date format: {values['EXPIRATION']}")
        # maturity should be formatted as it appears in sdb: YYYY-MM
        if values['exchange'] == 'CBOE': 
            values['MMY'] = convert_maturity(values['MMY'], day=True)
        elif len(values['MMY']) == 8: # like 20220930. If exchange is not CBOE, highly likely the month is wrong and we should look to symbol
            values['MMY'] = convert_maturity(dxfeed_maturity_to_sdb(values['SYMBOL'])[1], day=False)
        else:
            values['MMY'] = convert_maturity(values['MMY'], day=False)
        if not values.get('MULTIPLIER'):
            values['MULTIPLIER'] = 1
        if values.get('EXPIRATION_STYLE') == 'Weeklys':
            values['EXPIRATION_STYLE'] = True
        else:
            values['EXPIRATION_STYLE'] = False
        if 'SETTLEMENT_STYLE' in values:
            if not values.get('SETTLEMENT_STYLE'):
                values.pop('SETTLEMENT_STYLE')
            elif isinstance(values['SETTLEMENT_STYLE'], bool):
                pass
            elif isinstance(values['SETTLEMENT_STYLE'], str):
                if values['SETTLEMENT_STYLE'].lower() in ['physical', 'deliverable', 'close']:
                    values['SETTLEMENT_STYLE'] = True
                elif values['SETTLEMENT_STYLE'].lower() in ['financial', 'cash', 'open']:
                    values['SETTLEMENT_STYLE'] = False
                else:
                    raise ValueError(f"Settlement {values['SETTLEMENT_STYLE']} is unknown type")
        if values['ticker'] == 'BTC':
            values['PRICE_INCREMENTS'] = 5

        return values

    @validator('feedMinPriceIncrement', 'orderMinPriceIncrement', pre=True)
    # yup, it's super strange but sometimes it appears as 'PRICE_INCREMENTS': true
    def mk_mpi(cls, item):
        if isinstance(item, bool):
            item = 1
        elif isinstance(item, str):
            item = item.split(' ')[0]
        return item

class StrikeSchema(DerivativeSchema):
    strike_side_: str
    strikePrice: float = Field(
        alias='STRIKE'
    )
    shortName: Optional[str] = Field(
        alias='DESCRIPTION'
    )
    underlying: str = Field(
        alias='UNDERLYING'
    )
    strike_price_multiplier_: Optional[float]

    @root_validator(pre=True)
    def mk_strike_side(cls, values):
        dx_ticker = values['symbol_override_'] if values.get('symbol_override_') else values['ticker']
        match = re.match(
            rf"(\.|\.\/)(?P<ticker>{dx_ticker})"
            r"(?P<maturity>\d{6}|[FGHJKMNQUVXZ]\d{2}|[FGHJKMNQUVXZ]\d{4})"
            r"(?P<strike_side>[CP])(?P<strike_price>\d*\.?\d*)",
            values['SYMBOL']
        )
        if match and match.group('strike_side') == 'P':
            values['strike_side_'] = 'PUT'
        elif match and match.group('strike_side') == 'C':
            values['strike_side_'] = 'CALL'
        else:
            raise ValueError(f"Cannot get strike side for {values['SYMBOL']}")
        return values

    @root_validator(pre=True)
    def get_underlying(cls, values):
        if values.get('EUREX_UNDERLYING'):
            values['UNDERLYING'] = values['EUREX_UNDERLYING']
        return values

    @root_validator(pre=True)
    def check_description(cls, values):
        if not values.get('DESCRIPTION') \
            or re.match(rf"^{values['ticker']}", values['DESCRIPTION']):
            
            values['DESCRIPTION'] = None
        else:
            values['DESCRIPTION'] = values['DESCRIPTION'].replace(' Options', '')
        return values

    @root_validator(pre=True)
    def mk_spm(cls, values):
        if values.get('EXCHANGE_DATA', '').split(';')[-1].isdecimal():
            values['strike_price_multiplier_'] = 1 / float(values['EXCHANGE_DATA'].split(';')[-1])
        return values

class FutureSchema(DerivativeSchema):
    shortName: Optional[str] = Field(
        alias='DESCRIPTION'
    )

    @root_validator(pre=True)
    def check_fnd(cls, values):
        if values['exchange'] in ['ICE', 'LIFFE'] and not values.get('FND'):
            raise ValueError(f"{values['SYMBOL']} FND is invalid, cannot set expiration date")
        return values

    @validator('shortName', pre=True)
    def format_shortname(cls, item):
        if item:
            item = item[:-8]
            item.capitalize()
        return item

class SpreadLeg(BaseModel):
    quantity: int
    exanteId: str

class SpreadSchema(DerivativeSchema):
    second_ticker_: Optional[str]
    far_maturity_: Optional[str]
    shortName: str
    legs: List[SpreadLeg]
    leg_gap: Optional[int]
    isPhysicalDelivery: Optional[bool]
    spreadType: Optional[str]

    @root_validator(pre=True)
    def check_fnd(cls, values):
        if values['exchange'] in ['ICE', 'LIFFE'] and not values.get('FND'):
            raise ValueError(f"{values['SYMBOL']} FND is invalid, cannot set expiration date")
        return values

    @root_validator(pre=True)
    def mk_ticker(cls, values):
        if values.get('second_ticker_'):
            values['ticker'] = f"{values['ticker']}-{values['second_ticker_']}"
        return values
    
    @root_validator(pre=True)
    def mk_spread_type(cls, values):
        return values


class Parser(DxFeed, ExchangeParser):
    provider = 'DXFEED'
    

    def __init__(self, scheme='US', env='prod', engine=None):
        self.sdb = SymbolDB(env)
        self.sdbadds = SDBAdditional(env='prod')
        super().__init__(scheme=scheme, engine=engine)
        self.provider_id = next((
            x['providerId'] for x in asyncio.run(self.sdb.get_feed_providers()) if x['name'] == 'DXFEED'
        ), None)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __search_underlying(self, series_data: dict, contracts: list, underlying_types: list):
        future_giveup = False
        stock_giveup = False
        index_giveup = False
        description_giveup = False
        if 'stock' in underlying_types \
            and not series_data.get('underlyingId') \
            and not stock_giveup:

            if series_data['ticker'][-1].isdecimal():
                ticker = series_data['ticker'][:-1] # in case of splits or other weird actions when option series start to get additional number to the ticker
            else:
                ticker = series_data['ticker']
            underlying_search = [
                x for x in
                asyncio.run(self.sdb.get_v2(
                    rf"^{ticker}\.(NASDAQ|NYSE|AMEX|ARCA|BATS)$",
                    is_expired=False,
                    fields=['symbolId']
                ))
                if 'test' not in x['symbolId'].lower()
            ]
            if underlying_search:
                series_data['underlyingId'] = {
                    'type': 'symbolId',
                    'id': underlying_search[0]['symbolId']}
                if not series_data.get('shortName') and not description_giveup:
                    search_description = next(
                        (
                            x['DESCRIPTION'] for x in self.search_stock([ticker])
                            if x['COUNTRY'] == 'US' and x['OPOL'] in ['XNAS', 'XNYS', 'ARCX', 'BATS', 'XASE']
                        ),
                        next(
                        (
                            x['DESCRIPTION'] for x in self.search_etf([ticker])
                            if x['COUNTRY'] == 'US' and x['OPOL'] in ['XNAS', 'XNYS', 'ARCX', 'BATS', 'XASE']
                        ), '')                    
                    )
                    if not search_description:
                        description_giveup = True
                    # trim useless part
                    match = re.search(
                        r'(,)?( LLC| Ltd| LTD| Inc| INC| plc| PLC| Corp| NV| N\.V\.| -| L\.P\.| Common Stock| Incorporated| Co\.| \(The\))( |\.|$)',
                        search_description
                    )
                    if match:
                        series_data['shortName'] = search_description[:match.start()]
                    else:
                        series_data['shortName'] = search_description
            else:
                self.logger.warning(
                    f"Cannot find underlying stock for "
                    f"{series_data['ticker']}.{series_data['exchange']}"
                )
                stock_giveup = True
        if 'index' in underlying_types \
            and not series_data.get('underlyingId') \
            and not index_giveup:

            some_contract = next(x for x in contracts)
            underlying = some_contract.get('underlying', '')
            search_index = asyncio.run(self.sdb.get_v2(f'{underlying}.INDEX', fields=['symbolId']))
            if search_index:
                series_data['underlying'] = search_index[0]['symbolId']
            else:
                self.logger.warning(
                    f"Cannot find underlying for "
                    f"{series_data['ticker']}.{series_data['exchange']} in sdb")
                index_giveup = True
        if 'future' in underlying_types \
            and not series_data.get('underlyingId') \
            and not future_giveup:

            time_giveup = False
            for c in contracts:
                underlying = c.get('underlying', '')
                udl_ticker, udl_maturity = dxfeed_maturity_to_sdb(underlying)
                if udl_maturity:
                    sdb_futures = asyncio.run(self.sdb.get_v2(
                        rf"^{udl_ticker}\.{series_data['exchange']}\.{udl_maturity}$",
                        fields=['symbolId', 'expiry', 'path']
                    ))
                    if sdb_futures:
                        udl_future = sdb_futures[0]
                        expiry_date = self.sdb.sdb_to_date(udl_future['expiry'])
                        if expiry_date \
                            and expiry_date == c['expiry']:
                            
                            expiry_time = udl_future['expiry'].get('time')
                            if not series_data.get('expiry_time_'):
                                series_data['expiry_time_'] = expiry_time
                            if not time_giveup:
                                for p in reversed(udl_future['path'][:-1]):
                                    time_giveup = True
                                    if series_data.get('expiry_time_'):
                                        break
                                    parent = asyncio.run(self.sdb.get(p))
                                    series_data['expiry_time_'] = parent.get('expiry', {}).get('time')

                        c['underlyingId'] = {
                            'type': 'symbolId',
                            'id': f"{udl_ticker}.{series_data['exchange']}.{udl_maturity}"
                        }
                    else:
                        self.logger.warning(
                            f"{series_data['ticker']}.{series_data['exchange']} {c['maturity']}: "
                            f"underlying future ({udl_ticker}.{series_data['exchange']}.{udl_maturity}) "
                            f"is not found in sdb"
                        )
                elif underlying:
                    sdb_futures = asyncio.run(self.sdb.get_v2(
                        rf"^{underlying}\.{series_data['exchange']}\.[FGHJKMNQUVXZ]\d{{4}}$",
                        fields=['symbolId', 'expiryTime']
                    ))
                    if sdb_futures:
                        selected = min(
                            [
                                x for x
                                in sdb_futures
                                if dt.date.fromisoformat(x['expiryTime'].split('T')[0]) >= c['expiry']
                            ],
                            key=lambda e: dt.date.fromisoformat(e['expiryTime'].split('T')[0]),
                            default=None
                        )
                        if selected:
                            c['underlyingId'] = {
                                'type': 'symbolId',
                                'id': f"{selected['symbolId']}"
                            }
                            series_data['expiry_time_'] = selected['expiryTime'].split('T')[1].replace('Z', '') # It's UTC time, should be adjusted with schedule timezone

                    pass
                    # try to find future with exact expiration date or nearest one

    def __mk_legs_and_descr(self, first_futs: list, second_futs: list, contracts: list[dict]):
        sdb_futures = []
        first_descr = re.sub(r'( )?Future(s)?( )?(On )?', '', first_futs[0]['shortName'])
        first_seq = [
            dxfeed_maturity_to_sdb(x['identifier_'])[1] for x
            in sorted(
                first_futs,
                key=lambda m: m['maturity']
            )
        ]
        first_ticker = first_futs[0]['ticker']
        exchange = first_futs[0]['exchange']
        sdb_futures.extend(
            asyncio.run(self.sdb.get_v2(
                rf"^{first_ticker}\.{exchange}\.[FGHJKMNQUVXZ]\d{{4}}$",
                fields=['symbolId', 'expiryTime', 'maturityDate', 'isTrading', '_id']
            ))
        )
        if second_futs:
            second_descr = re.sub(r'( )?Future(s)?( )?(On )?', '', second_futs[0]['shortName'])
            second_seq = [
                dxfeed_maturity_to_sdb(x['identifier_'])[1] for x
                in sorted(
                    second_futs,
                    key=lambda m: m['maturity']
                )
            ]
            spread_descr = first_descr + ' / ' + second_descr + ' Spread'
            second_ticker = second_futs[0]['ticker']
            sdb_futures.extend(
                asyncio.run(self.sdb.get_v2(
                    rf"^{second_ticker}\.{exchange}\.[FGHJKMNQUVXZ]\d{{4}}$",
                    fields=['symbolId', 'expiryTime', 'isTrading', '_id']
                ))
            )

        else:
            spread_descr = first_descr + ' Spread'
            second_ticker = None
        some_contracts = [
            max(
                [
                    x for x
                    in sdb_futures
                    if x['symbolId'] and first_ticker in x['symbolId']
                    and x.get('isTrading') is not False
                ],
                key=lambda e: dt.datetime.fromisoformat(e['expiryTime'][:-1])
            )
        ]
        if second_ticker:
            some_contracts.append(
                max(
                    [
                        x for x
                        in sdb_futures
                        if x['symbolId'] and second_ticker in x['symbolId']
                        and x.get('isTrading') is not False
                    ],
                    key=lambda e: dt.datetime.fromisoformat(e['expiryTime'][:-1])
                )
            )
        physical = False
        for sc in some_contracts:
            compiled = asyncio.run(self.sdbadds.build_inheritance(sc['_id'], include_self=True))
            if compiled.get('isPhysicalDelivery'):
                physical = True
                break

        for c in contracts:
            c.update({
                'shortName': spread_descr,
                'isPhysicalDelivery': physical
            })
            if not re.match(r'=\/\w+:\w{4}-\/\w+:\w{4}', c.get('SYMBOL', '')):
                self.logger.warning(f"unknown contract type: {c.get('SYMBOL', '')}")
                continue
            legs = c.get('SYMBOL', '').split('-')
            leg_gap = None
            post_legs = []
            for leg in legs:
                leg_ticker, leg_maturity = dxfeed_maturity_to_sdb(leg)
                if not leg_maturity:
                    continue
                sdb_leg = next((
                    num for num, x
                    in enumerate(sdb_futures)
                    if x['symbolId'] == f"{leg_ticker}.{exchange}.{leg_maturity}"
                ), None)
                if sdb_leg is not None:
                    fut_e_time = sdb_futures[sdb_leg]['expiryTime']
                    if fut_e_time \
                        and dt.date.fromisoformat(fut_e_time.split('T')[0]) == c['EXPIRATION']:

                        c['expiry_time_'] = fut_e_time.split('T')[1].replace('Z', '') # It's UTC time, should be adjusted with schedule timezone
                    post_legs.append({
                        'quantity': 0,
                        'exanteId': f"{leg_ticker}.{exchange}.{leg_maturity}"
                    })

                else:
                    if second_futs:
                        self.logger.warning(
                            f"{first_ticker}-{second_ticker}.{exchange}.{dxfeed_maturity_to_sdb(c['SYMBOL'].split('-')[0])[1]}: "
                            f"leg future ({leg_ticker}.{exchange}.{leg_maturity}) "
                            f"is not found in sdb"
                        )
                    else:
                        self.logger.warning(
                            f"{first_ticker}.{exchange}.*S/"
                            f"{dxfeed_maturity_to_sdb(c['SYMBOL'].split('-')[0])[1]}-{dxfeed_maturity_to_sdb(c['SYMBOL'].split('-')[1])[1]}: "
                            f"leg future ({leg_ticker}.{exchange}.{leg_maturity}) "
                            f"is not found in sdb"
                        )
            if len(post_legs) != 2:
                if second_futs:
                    self.logger.warning(
                        f"{first_ticker}-{second_ticker}.{exchange}{dxfeed_maturity_to_sdb(c['SYMBOL'].split('-')[0])[1]}: "
                        "legs are not set"
                    )
                else:
                    self.logger.warning(
                        f"{first_ticker}.{exchange}.*S/"
                        f"{dxfeed_maturity_to_sdb(c['SYMBOL'].split('-')[0])[1]}-{dxfeed_maturity_to_sdb(c['SYMBOL'].split('-')[1])[1]}: "
                        "legs are not set"
                    )
                continue
            post_legs[0]['quantity'] = 1
            post_legs[1]['quantity'] = -1
            c['legs'] = post_legs

            # set leg gap
            if not second_futs:
                far_maturity = max([
                    date_dict_to_str(x['maturityDate']) for x
                    in sdb_futures
                    if x['symbolId'] in [
                        y['exanteId'] for y in post_legs
                    ]
                ])
                c['far_maturity_'] = far_maturity
                try:
                    leg_gap = first_seq.index(post_legs[1]['exanteId'].split('.')[-1]) \
                        - first_seq.index(post_legs[0]['exanteId'].split('.')[-1])
                    if leg_gap < 0:
                        c['spreadType'] = 'REVERSE'                        
                    c['leg_gap'] = abs(leg_gap)
                except Exception as e:
                    self.logger.warning(
                        f"{e.__class__.__name__}: {e}"
                    )
                    self.logger.warning(
                        f"{first_ticker}.{exchange}.*S/ {c['MMY']}: "
                        f"leg gap is not set"
                    )

    def __filtering_regexp(
            self,
            search_list: dict,
            product: str,
        ):
        prefix_dict = {
            'FUTURE': '\/',
            'OPTION': '\.',
            'OPTION ON FUTURE': '\.\/',
            'CALENDAR': '=\/',
            'PRODUCT': '=\/'
        }
        ticker = search_list.get('ticker')
        exchange = search_list.get('exchange')
        maturity = search_list.get('maturity')
        search_str = search_list.get('search_str', '')
        second_ticker = search_list.get('second_ticker')
        second_maturity = search_list.get('second_maturity')
        suffix = search_list.get('suffix')

        if None in [ticker, product]:
            return None
        


        prefix = prefix_dict[product]
        col_suffix = f':{suffix}' if suffix else ''
        strikes=r'(?P<side>P|C)\d+(\.\d+)?' if product in ['OPTION', 'OPTION ON FUTURE'] else ''

        if product in ['CALENDAR', 'PRODUCT']:
            if second_ticker:
                if maturity is None:
                    maturity=r'[FGHJKMNQUVXZ]\d{2}'
                regexp = rf'{prefix}{ticker}{maturity}{col_suffix}-\/{second_ticker}{maturity}{col_suffix}$'
            elif second_maturity:
                if maturity is None:
                    maturity=r'[FGHJKMNQUVXZ]\d{2}'
                second_maturity=r'[FGHJKMNQUVXZ]\d{2}'
                regexp = rf'{prefix}{ticker}{maturity}{col_suffix}-\/{ticker}{second_maturity}{col_suffix}$'
            else:
                return None
        else:
            if exchange == 'EUREX' and 'OPTION' in product:
                prefix=prefix_dict['OPTION']
            if maturity is None:
                maturity=r'\d{6}' if re.search(r'\?\?\?\?\?\?', search_str) \
                    else r'[FGHJKMNQUVXZ]\d{4}' if re.search(r'\?\?\?\?\?', search_str) \
                    else r'[FGHJKMNQUVXZ]\d{2}' if re.search(r'\?\?\?', search_str) else ''
            regexp = rf'{prefix}{ticker}{maturity}{strikes}{col_suffix}$'
        return regexp

    def __create_search_list(self, series: str, product: str, overrides: dict = None) -> list:
        ticker = ''
        second_ticker = ''
        maturity_type = ''
        col_suffix = ''
        
        re_stock = r"(?P<ticker>\w+)\.(?P<exchange>[A-Z]*)"
        re_fut_opt = r"(?P<ticker>\w+)\.(?P<exchange>\w+)(\.(?P<mat>\d{0,2}[FGHJKMNQUVXZ]\d{4}))?"
        re_cal_spread = r"(?P<ticker>\w+)\.(?P<exchange>\w+)(\.[CR]S\/(?P<mat>[FGHJKMNQUVXZ]\d{4})-(?P<scnd_mat>[FGHJKMNQUVXZ]\d{4}))?"
        re_prod_spread = r"(?P<ticker>\w+)-(?P<second_ticker>\w+)\.(?P<exchange>\w+)(\.(?P<mat>[FGHJKMNQUVXZ]\d{4}))?"

        db_template = {
            'FUTURE': r'\/{ticker}{maturity_type}{col_suffix}',
            'OPTION': r'\.{ticker}{maturity_type}[CP]\d+(\.\d+)?{col_suffix}',
            'OPTION ON FUTURE': r'\.\/{ticker}{maturity_type}[CP]\d+(\.\d+)?{col_suffix}',
            'STOCK': r'{ticker}',
            'CALENDAR': r'\=\/{ticker}{maturity_type}{col_suffix}-\/{ticker}{maturity_type}{col_suffix}',
            'PRODUCT': r'\=\/{ticker}{maturity_type}{col_suffix}-\/{second_ticker}{maturity_type}{col_suffix}'
        }
        http_template = {
            'FUTURE': '/{ticker}{maturity_type}{col_suffix}',
            'OPTION': (
                ".{ticker}{maturity_type}P*{col_suffix},"
                ".{ticker}{maturity_type}C*{col_suffix}"
            ),
            'OPTION ON FUTURE': (
                "./{ticker}{maturity_type}P*{col_suffix},"
                "./{ticker}{maturity_type}C*{col_suffix}"
            ),
            'STOCK': '{ticker}',
            'CALENDAR': (
                "=/{ticker}{maturity_type}{col_suffix}"
                "-/{ticker}{maturity_type}{col_suffix}"
            ),
            'PRODUCT': (
                "=/{ticker}{maturity_type}{col_suffix}"
                "-/{second_ticker}{maturity_type}{col_suffix}"
            )
        }
        if product in ['FUTURE', 'OPTION', 'OPTION ON FUTURE']:
            payload = {
                'ticker': re.match(re_fut_opt, series).group('ticker'),
                'exchange': re.match(re_fut_opt, series).group('exchange'),
                'maturity': re.match(re_fut_opt, series).group('mat')
            }
            if overrides.get('symbolName'):
                payload['ticker'] = overrides['symbolName']
            elif overrides.get('symbolIdentifier/identifier'):
                payload['ticker'] = overrides['symbolIdentifier/identifier']
        elif product == 'CALENDAR':
            payload = {
                'ticker': re.match(re_cal_spread, series).group('ticker'),
                'exchange': re.match(re_cal_spread, series).group('exchange'),
                'maturity': re.match(re_cal_spread, series).group('mat'),
                'second_maturity': re.match(re_cal_spread, series).group('scnd_mat')
            }
        elif product == 'PRODUCT':
            payload = {
                'ticker': re.match(re_prod_spread, series).group('ticker'),
                'second_ticker': re.match(re_prod_spread, series).group('second_ticker'),
                'exchange': re.match(re_prod_spread, series).group('exchange'),
                'maturity': re.match(re_prod_spread, series).group('mat')
            }
        elif product == 'STOCK':
            payload = {
                'ticker': re.match(re_stock, series).group('ticker'),
                'exchange': re.match(re_stock, series).group('exchange')
            }
        else:
            self.logger.error("No exchange defined")
            payload = {'search_str': None}
            return payload
        
        ticker = payload.get('ticker', '')
        second_ticker = payload.get('second_ticker', '')


        if overrides is None:
            overrides = {}
        suffix = overrides['suffix'] if overrides.get('suffix') else \
            exchange_set[payload['exchange']].get('suffix', [''])[0]
        payload['suffix'] = suffix
        if self.engine:
            col_suffix = rf'\:{suffix}' if suffix else ''
        else:
            col_suffix = f':{suffix}' if suffix else ''

        if overrides.get('useLongMaturityFormat') is True:
            maturity_type = r'\d{6}' if self.engine else '??????'
        elif product == 'OPTION' and payload['exchange'] in ['CBOE', 'EUREX']:
            maturity_type = r'\d{6}' if self.engine else '??????'
        # elif product == 'FUTURE' and payload['exchange'] == 'EUREX':
        #     maturity_type = r'\d{6}' if self.engine else '??????'
        else:
            maturity_type = r'[FGHJKMNQUVXZ]\d{2}' if self.engine else '???'

        if product == 'STOCK':
            search_str = db_template[product].format(
                ticker=ticker
            ) if self.engine else http_template[product].format(
                ticker=ticker
            )
        elif product == 'PRODUCT':
            search_str = db_template[product].format(
                ticker=ticker,
                second_ticker=second_ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            ) if self.engine else http_template[product].format(
                ticker=ticker,
                second_ticker=second_ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            )
            first_fut = db_template['FUTURE'].format(
                ticker=ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            ) if self.engine else http_template['FUTURE'].format(
                ticker=ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            )
            second_fut = db_template['FUTURE'].format(
                ticker=second_ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            ) if self.engine else http_template['FUTURE'].format(
                ticker=second_ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            )
            payload.update({
                'first_fut': first_fut,
                'second_fut': second_fut
            })
        else:
            search_str = db_template[product].format(
                ticker=ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            ) if self.engine else http_template[product].format(
                ticker=ticker,
                maturity_type=maturity_type,
                col_suffix=col_suffix
            )
            if product == 'CALENDAR':
                first_fut = db_template['FUTURE'].format(
                    ticker=ticker,
                    maturity_type=maturity_type,
                    col_suffix=col_suffix
                ) if self.engine else http_template['FUTURE'].format(
                    ticker=ticker,
                    maturity_type=maturity_type,
                    col_suffix=col_suffix
                )
                payload.update({
                    'first_fut': first_fut
                })
        payload.update({
            'search_str': search_str
        })
        return payload
    
    def __get_and_filter_data(self, series: str, product: str, overrides: dict):
        ticker, exchange = series.split('.')[:2]
        if exchange in ['ICE', 'LIFFE']:
            self.set_region('ICE')
        else:
            self.set_region('US')
        if exchange in ['ICE', 'LIFFE'] and not overrides.get('suffix'):
            for s in exchange_set['ICE']['suffix']:
                overrides['suffix'] = s
                search_list = self.__create_search_list(series, product, overrides)
                self.logger.debug(f'Search list: {pformat(search_list)}')
                if product == 'FUTURE':
                    data = self.search_future([search_list['search_str']])
                elif product in ['OPTION', 'OPTION ON FUTURE']:
                    data = self.search_option([search_list['search_str']])
                elif product in ['PRODUCT', 'CALENDAR']:
                    data = self.search_spread([search_list['search_str']])
                else:
                    raise RuntimeError(f'{product} search is not implemented')
                if data:
                    break
        else:
            search_list = self.__create_search_list(series, product, overrides)
            self.logger.debug(f'Search list: {pformat(search_list)}')
            if product == 'FUTURE':
                data = self.search_future([search_list['search_str']])
            elif product in ['OPTION', 'OPTION ON FUTURE']:
                data = self.search_option([search_list['search_str']])
            elif product in ['PRODUCT', 'CALENDAR']:
                data = self.search_spread([search_list['search_str']])
            else:
                raise RuntimeError(f'{product} search is not implemented')

        filter_re = self.__filtering_regexp(search_list, product=product)
        if filter_re:
            data = [x for x in data if re.match(filter_re, x['SYMBOL'])]
        return data, search_list

    def futures(
            self,
            series: str,
            overrides: dict = None,
            data: list[dict] = None,
            **kwargs
        ):
        series_data = {}
        contracts = []
        ticker, exchange = series.split('.')[:2]
        if overrides is None:
            overrides = {}
        if not data:
            data, search_list = self.__get_and_filter_data(series, 'FUTURE', overrides)
        else:
            search_list = {
                'ticker': ticker,
                'exchange': exchange
            }
        if not data:
            self.logger.info(f"Nothing found for {search_list['ticker']}.{search_list['exchange']}")
            return series_data, contracts
        for d in data:
            try:
                contracts.append(
                    FutureSchema(
                        ticker=ticker,
                        exchange=exchange,
                        symbol_override_=search_list['ticker'],
                        **d
                    ).dict()
                )
            except ValidationError as valerr:
                self.logger.warning(
                    f"contract data {d.get('SYMBOL')} is invalid: {pformat(valerr.errors())}"
                )

        data_df = pd.DataFrame(contracts)
        series_data.update({
            key: next(x for x in data_df[key]) for key
            in data_df.columns
            if key not in [
                'identifier_',
                'identifiers',
                'expiry',
                'maturityDate',
                'ISIN'
            ]
        })
        self.logger.info(f"Folder settings:")
        self.logger.info(pformat(series_data))
        self.logger.info(f"Found contracts:")
        self.logger.info(pformat(contracts))
        return series_data, contracts

    def options(
            self,
            series: str,
            overrides: dict = None,
            product: str = 'OPTION',
            data: list[dict] = None,
            **kwargs
        ):
        series_data = {}
        contracts = []
        ticker, exchange = series.split('.')[:2]
        if overrides is None:
            overrides = {}
        if not data:
            data, search_list = self.__get_and_filter_data(series, product, overrides)
        else:
            search_list = {
                'ticker': ticker,
                'exchange': exchange
            }
        if not data:
            self.logger.info(f"Nothing found for {search_list['ticker']}.{search_list['exchange']}")
            return series_data, contracts
        formatted_data = []
        for d in data:
            try:
                formatted_data.append(
                    StrikeSchema(
                        ticker=ticker,
                        exchange=exchange,
                        symbol_override_=search_list['ticker'],
                        **d
                    ).dict()
                )
            except ValidationError as valerr:

                self.logger.warning(
                    f"contract data {d.get('SYMBOL')} is invalid: {pformat(valerr.errors())}"
                )
        data_df = pd.DataFrame(formatted_data)
        if data_df.empty:
            return series_data, contracts
        series_exclude = [
                'identifier_',
                'expiry',
                'expiry_time_',
                'maturity',
                'maturityDate',
                'ISIN',
                'is_weekly_',
                'strike_side_',
                'strike'
            ]
        contract_include = [
                    'exchange',
                    'is_weekly_',
                    'expiry',
                    'maturity'
        ]
        if product == 'OPTION ON FUTURE':
            series_exclude.append('underlying')
            contract_include.append('underlying')
        for key in data_df.columns:
            val = next((x for x in data_df[key]), None)
            if val is None or key in series_exclude:
                continue
            series_data.update({key: val})
        
        logging.info(f"Folder settings: {series_data}")

        for mat in {x for x in data_df['maturity']}:
            contract_df = data_df.loc[data_df['maturity'] == mat]
            contract = {
                'strikePrices': {
                    'CALL': [],
                    'PUT': []
                }
            }
            contract.update({
                key: next(x for x in contract_df[key]) for key
                in contract_df.columns
                if key in contract_include
            })
            for side in ['PUT', 'CALL']:
                side_df = contract_df.loc[contract_df['strike_side_'] == side][['strikePrice', 'ISIN']]
                contract['strikePrices'][side] = side_df.to_dict('records')
            contracts.append(contract)
            underlying_types = exchange_set.get(exchange, {}).get('underlying', [])
        if product == 'OPTION ON FUTURE':
            underlying_types = ['future']
        self.__search_underlying(series_data, contracts, underlying_types)
        self.logger.info(f"Folder settings:")
        self.logger.info(pformat(series_data))
        self.logger.info(f"Found contracts:")
        self.logger.info(pformat(contracts))
        return series_data, contracts

    def spreads(
            self,
            series: str,
            overrides: dict = None,
            data: list[dict] = None,
            **kwargs
        ):
        series_data = {}
        contracts = []
        futures = []
        tickers, exchange = series.split('.')[:2]
        if len(tickers.split('-')) == 2: 
            spread_type = 'PRODUCT'
            ticker, second_ticker = tickers.split('-')[:2]
        elif len(tickers.split('-')) == 1:
            ticker = tickers
            second_ticker = None
            spread_type = 'CALENDAR'
        else:
            self.logger.error(
            f'Wrong spread name: {series}. '
            'Should look like TICKER.EXCHANGE or TICKER1-TICKER2.EXCHANGE'
        )
            return series_data, contracts
        if overrides is None:
            overrides = {}
        if not data:
            data, search_list = self.__get_and_filter_data(series, spread_type, overrides)
        else:
            search_list = {
                'ticker': ticker,
                'exchange': exchange
            }
        if not data:
            self.logger.info(f"Nothing found for {search_list['ticker']}.{search_list['exchange']}")
            return series_data, contracts
        first_fut_series, first_fut_contracts = self.futures(f"{ticker}.{exchange}")
        if second_ticker:
            second_fut_series, second_fut_contracts = self.futures(f"{second_ticker}.{exchange}")
        else:
            second_fut_series = {}
            second_fut_contracts = []
        self.__mk_legs_and_descr(first_fut_contracts, second_fut_contracts, data)
        for d in data:
            try:
                contracts.append(
                    SpreadSchema(
                        ticker=ticker,
                        exchange=exchange,
                        second_ticker_=second_ticker,
                        symbol_override_=search_list['ticker'],
                        **d
                    ).dict()
                )
            except ValidationError as valerr:

                self.logger.warning(
                    f"contract data {d.get('SYMBOL')} is invalid: {pformat(valerr.errors())}"
                )
        data_df = pd.DataFrame(contracts)
        if data_df.empty:
            return series_data, contracts
        series_exclude = [
                'identifier_',
                'second_ticker_',
                'expiry',
                'expiry_time_',
                'maturity',
                'maturityDate',
                'ISIN',
                'legs'
            ]
        series_data.update({
            key: next(x for x in data_df[key]) for key
            in data_df.columns
            if key not in series_exclude
        })
        self.logger.info(f"Folder settings:")
        self.logger.info(pformat(series_data))
        self.logger.info(f"Found contracts:")
        self.logger.info(pformat(contracts))
        series_data.update({
            '_futures': futures
        })
        return series_data, contracts
 