import asyncio
import datetime as dt
import logging
import pandas as pd
import re
from typing import Dict, List, Optional, Union

from libs.parsers import (
    FractionCurrencies,
    ExchangeParser,
    convert_maturity,
    Datascope,
    Months
)
from libs.async_sdb_additional import SDBAdditional
from libs.sdb_instruments import Future, Instrument, Option, Stock
from libs.sdb_instruments.instrument import get_part
from pydantic import BaseModel, Field, root_validator, validator, ValidationError


class DerivativeSchema(BaseModel):
    ticker: str
    exchange: str
    base_ric_: str
    identifier_: str = Field(
        alias='Identifier'
    )
    currency: str = Field(
        alias='Currency Code'
    )
    currency_multiplier_: bool
    expiry: Union[dt.datetime, dt.date] = Field(
        alias='ExpirationDate'
    )
    expiry_time_: Optional[str]
    contractMultiplier: float = Field(
        alias='Lot Size'
    )
    maturity: str
    feedMinPriceIncrement: float
    orderMinPriceIncrement: float
    CFI: Optional[str] = Field(
        alias='CFI Code'
    )
    ISIN: Optional[str]

    RIC: Optional[str] = Field(
        alias='RIC'
    )

    is_weekly_: Optional[bool]
    isPhysicalDelivery: Optional[bool] = Field(
        alias='Method of Delivery'
    )

    @root_validator(pre=True)
    def normalize_derivative(cls, values):
        # expiry
        # if derivative has First Notice Day and it earlier than expiration we should use it as expiration date
        try:
            if values.get('First Notice Day'):
                values['ExpirationDate'] = min(
                    dt.date.fromisoformat(values['First Notice Day'].split('T')[0]),
                    dt.date.fromisoformat(values['ExpirationDate'].split('T')[0])
                )
            else:
                values['ExpirationDate'] = dt.date.fromisoformat(values['ExpirationDate'].split('T')[0])
        except Exception:
            raise ValueError(f"Wrong expiration date format: {values['ExpirationDate']}")

        # contract multiplier, currency, mpi
        if not values.get('Lot Size'):
            values['Lot Size'] = 1
        # MIND LESSER CURRENCIES!!
        # adjust currency to base one (e.g. GBp → GBP), adjust mpi accordingly, set indicator to adjust overrides
        if values.get('Currency Code') in FractionCurrencies.__members__:
            values['currency_multiplier_'] = True
            values['Currency Code'] = FractionCurrencies[values['Currency Code']].value
        else:
            values['currency_multiplier_'] = False
        if values.get('Tick Value'):
            values['feedMinPriceIncrement'] = values['Tick Value'] / values['Lot Size']
            if values['currency_multiplier_']:
                values['feedMinPriceIncrement'] /= 100
            values['orderMinPriceIncrement'] = values['feedMinPriceIncrement']
        
        # CFI
        if values.get('CFI Code'):
            values['CFI Code'] = values.get('CFI Code').upper()
        
        # isPhysicalDelivery
        if values.get('Method of Delivery'):
            if isinstance(values['Method of Delivery'], str):
                if values['Method of Delivery'].lower() in ['physical', 'deliverable', 'close']:
                    values['Method of Delivery'] = True
                elif values['Method of Delivery'].lower() in ['financial', 'cash']:
                    values['Method of Delivery'] = False
                else:
                    raise ValueError(f"Settlement {values['Method of Delivery']} is unknown type")
            else:
                raise ValueError(f"Settlement {values['Method of Delivery']} is unknown type")

        # ISIN
        if values.get('IdentifierType') == 'Isin':
            values['ISIN'] = values.get('Identifier')

        # is_weekly
        values['is_weekly_'] = bool(values.get('is_weekly_', False))
        return values

class StrikeSchema(DerivativeSchema):
    strike_side_: str = Field(
        alias='PutCallCode'
    )
    strikePrice: float = Field(
        alias='StrikePrice'
    )
    shortName: Optional[str] = Field(
        alias='Underlying Security Description'
    )
    underlying_ric: str = Field(
        alias='Underlying RIC'
    )
    exerciseStyle: str = Field(
        alias='Exercise Style'
    )

    @validator('strike_side_', pre=True)
    def mk_strike_side(cls, item):
        if isinstance(item, str):
            item = item.upper()
            if item in ['PUT', 'CALL']:
                return item
        raise ValueError(f'Cannot determine strike side: {item}')

    @validator('shortName', pre=True)
    def set_shortname(cls, item):
        if isinstance(item, str):
            item = ' '.join(item.split(' ')[:-1]).capitalize() # trim the expration
            return item
        raise ValueError(f'ShortName is invalid: {item}')

    @validator('exerciseStyle', pre=True)
    def set_exercise_style(cls, item):
        item = 'EUROPEAN' if item == 'E' else 'AMERICAN' if item == 'A' else ''
        return item

    @root_validator(pre=True)
    def set_option_maturity(cls, values):
        # maturity
        # maturity should be formatted as it appears in sdb: YYYY-MM
        match = re.match(
            rf"^{values['base_ric_']}(?P<strike>\d+)(?P<maturity>\w\d{{1,2}})(?P<sign>[PN])?(?P<suffix>.*)?$",
            values['RIC']
        )
        if match and match.group('maturity'):
            values['maturity'] = convert_maturity(
                match.group('maturity'),
                day=False,
                option_months=True
            )
        else:
            raise ValueError(f"Cannot determine maturity, invalid RIC: {values['RIC']}")
        return values

class FutureSchema(DerivativeSchema):

    @root_validator(pre=True)
    def set_future_maturity(cls, values):
        # maturity
        # maturity should be formatted as it appears in sdb: YYYY-MM
        match = re.match(
            rf"^{values['base_ric_']}(?P<maturity>\w\d{{1,2}})(?P<suffix>.*)?$",
            values['RIC']
        )
        if match and match.group('maturity'):
            values['maturity'] = convert_maturity(
                match.group('maturity'),
                day=False,
                option_months=False
            )
        else:
            raise ValueError(f"Cannot determine maturity, invalid RIC: {values['RIC']}")
        return values


class Parser(Datascope, ExchangeParser):
    """
    Class for using Datascope DSS API
    More information about all methods:
    https://developers.refinitiv.com/en/api-catalog/datascope-select/datascope-select-rest-api/tutorials

    or use web-app from Datascope [we have access to InstrumentSearch, EquitySearch, FuturesAndOptionsSearch]:
    https://select.datascope.refinitiv.com/DataScope/Home
    """
    provider = 'REUTERS'
    exchange_codes = {
        'CBOE': ['CBF'],
        'FORTS': ['RTF'],
        'LIFFE': ['IEU'],
        'EUREX': ['EUX'],
        'ICE': ['IFS', 'ICA', 'IEU', 'IUS'],
        'OE': ['OSA'],
        'EURONEXT': ['AEX', 'MAT', 'MNP', 'LIS', 'BFX'],
        'HKEX': ['HFE'],
        'ASX': ['SFE'],
        'IDEM': ['MIL'],
        'MEFF': ['MRV'],
        'SGX': ['SIM'],
        'DGCX': ['DGX'],
        'TOCOM': ['OSA', 'TCE'],
        'BIST': ['IST']
    }

    parse_isins = [
        'ICE',
        'SGX',
        'LME',
        'LIFFE'
    ]

    def __init__(self):
        super().__init__()
        self.sdbadds = SDBAdditional()
        self.provider_id = next((
            x[1] for x
            in asyncio.run(
                self.sdbadds.get_list_from_sdb('feed_providers')
            )
            if x[0] == 'REUTERS'
        ), None)
        if not self.provider_id:
            raise RuntimeError('Reuters is not found in the sdb providers list... Strange!')

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")
    
    def stock(self, **kwargs):
        pass

    def __search_underlying(self, series_data: dict, contracts: list[dict], underlying_type: str):
        if underlying_type == 'index':
            pass
        elif underlying_type == 'future':
            exchange_futures_uuid = asyncio.run(
                self.sdbadds.sdb.get_uuid_by_path(
                    ['Root', 'FUTURE', series_data['exchange']],
                    self.sdbadds.tree
                )
            )
            all_exchange_futures = asyncio.run(
                self.sdbadds.sdb.get_heirs(
                    exchange_futures_uuid, recursive=True, full=True
                )
            )

            for c in contracts:
                match = re.match(r'^(?P<ticker>\w+)(?P<maturity>[FGHJKMNQUVXZ]\d{1,2}).*', c['underlying_ric'])
                if match:
                    ric_base = match.group('ticker')
                    underlying_futures_folder = next((
                        x for x in all_exchange_futures
                        if x['isAbstract']
                        and get_part(
                            x, [
                                'feeds',
                                'providerOverrides',
                                self.provider_id,
                                'reutersProperties',
                                'tradeRic',
                                'base'
                            ]
                        ) == ric_base
                    ), None)
                    if underlying_futures_folder:
                        children = [
                            x for x
                            in all_exchange_futures
                            if x['path'][:len(underlying_futures_folder['path'])] == underlying_futures_folder['path']
                            and not x['isAbstract']
                        ]
                        underlying_future = next((
                            self.sdbadds.compile_symbol_id(x) for x
                            in children
                            if f"{ric_base}{Months(x['maturityDate']['month']).name}{str(x['maturityDate']['year'])[-1]}" == c['underlying_ric']
                            and self.sdbadds.sdb.sdb_to_date(x['expiry']) > dt.date.today()
                        ), None)
                    else:
                        underlying_future = next((
                            self.sdbadds.compile_symbol_id(x) for x
                            in all_exchange_futures
                            if x.get('identifiers', {}).get('RIC') == c['underlying_ric']
                        ), None)
                    if underlying_future:
                        c['underlyingId'] = {
                            'type': 'symbolId',
                            'id': underlying_future
                        }



    def __create_search_list(self, series: str, product: str, overrides: dict = None) -> dict:
        try:
            if product == 'Futures':
                instrument = Future(*series.split('.')[:2])
            elif product in ['Options', 'FuturesOnOptions']:
                instrument = Option(*series.split('.')[:2])
            else:
                instrument = Instrument(*series.split('.')[:2])
        except Exception:
            self.logger.error(f"{series} is not found in SDB. You should provide identifiers in overrides arg")
            return {}
        properties = [
            'symbolIdentifier/type',
            'symbolIdentifier/identifier',
            'exchangeName',
            'volumeMultiplier',
            'priceMultiplier',
            'strikePriceMultiplier',
            'reutersProperties/tradeRic/base',
            'reutersProperties/tradeRic/suffix',
            'reutersProperties/tradeRic/optionSeparator',
            'reutersProperties/ric/base',
            'reutersProperties/ric/suffix',
            'reutersProperties/ric/optionSeparator'
        ]
        payload = instrument.get_provider_overrides('REUTERS', *properties, compiled=False, silent=True)
        exchange_id = instrument.instrument.get('exchangeId', instrument.compiled_parent.get('exchangeId'))
        ric = instrument.instrument.get('identifiers', {}).get('RIC')
        payload.update({
            'ticker': series.split('.')[0],
            'exchange': series.split('.')[1],
            'exchange_id': exchange_id,
            'RIC': ric
        })
        cleared = {key.split('/')[-1]: val for key, val in payload.items()}
        if not cleared.get('base') and cleared.get('type') == 'RIC' and cleared.get('RIC'):
            cleared['base'] = cleared['RIC']
        return cleared


    def futures(self, series: str, overrides: dict = None, **kwargs) -> dict:
        series_data = {}
        contracts = []
        product = 'Futures'
        if not overrides:
            overrides = self.__create_search_list(series, product)
        else:
            overrides.update({
                'ticker': series.split('.')[0],
                'exchange': series.split('.')[1]
            })
        rt_codes = self.exchange_codes.get(overrides['exchange'])
        self.logger.info(f"""Search parameters:
        search string {overrides['base']}*{overrides.get('suffix', '')},
        exchange codes {rt_codes},
        only active,
        search type Isin""")
        raw_search = []
        duplicates = [
            x for x in self.futures_raw(
                f"{overrides['base']}*{overrides.get('suffix', '')}",
                exchange_codes=rt_codes,
                only_active=True,
                search_type='Isin'
            )
            if x['Identifier'] in [item['Identifier'] for item in raw_search]
            or raw_search.append(x)
        ] # surprisingly, there are some duplicating search results so we get rid of them this way

        if not raw_search:
            self.logger.warning(f"nothing is found for {series}")
            return series_data, contracts
        if isinstance(raw_search[0], KeyError):
            self.logger.error(f"smth is wrong with search parameters")
            return series_data, contracts
        raw_search_df = pd.DataFrame(raw_search)
        composite_ids = [(x['Identifier'], x['IdentifierType']) for x in raw_search]
        id_types = {x[1] for x in composite_ids}
        composite_data = []
        for itype in id_types:
            composite_data.extend(
                self.composite(
                    [x[0] for x in composite_ids if x[1] == itype],
                    fields=['all'],
                    base_type=itype
                )
            )
        composite_df = pd.DataFrame(composite_data)

        combined_df = pd.merge(raw_search_df, composite_df, on=['Identifier', 'IdentifierType'])
        combined = combined_df.to_dict('records')
        future_regex = re.compile(rf"{overrides['base']}[FGHJKMNQUVXZ]\d{{1,2}}{overrides.get('suffix', '')}$")
        filtered_combined = [x for x in combined if re.match(future_regex, x['RIC'])]
        self.logger.info(f"filtered rics: {[x['RIC'] for x in filtered_combined]}")
        for d in filtered_combined:
            try:
                contracts.append(
                    FutureSchema(
                        ticker=overrides['ticker'],
                        exchange=overrides['exchange'],
                        base_ric_=overrides['base'],
                        **d
                    ).dict()
                )
            except ValidationError as valerr:
                self.logger.warning(
                    f"contract data {d.get('Identifier')} is invalid: {valerr.errors()}"
                )
        series_data = {
            key: val for key, val
            in contracts[0].items()
            if key not in [
                'identifier_',
                'expiry',
                'maturity',
                'is_weekly_',
                'ISIN',
                'RIC'
            ]
        }
        self.logger.info(f"Folder settings: {series_data}")
        self.logger.info(f"Found contracts: {contracts}")
        return series_data, contracts

    # options are sloooow! Better if you already know expirations
    # you are interested in and ask them one by one
    def options(
            self,
            series: str,
            overrides: dict = None,
            product: str = 'FuturesOnOptions',
            underlying=None,
            currency=None,
            only_active='Active',
            **kwargs
        ) -> dict:
        series_data = {}
        contracts = []
        if product in ['Options', 'OPTION']:
            product = 'Options'
        elif product in ['OPTION ON FUTURE', 'FuturesOnOptions']:
            product = 'FuturesOnOptions'

        if not overrides:
            overrides = self.__create_search_list(series, product)
        overrides.update({
            'ticker': series.split('.')[0],
            'exchange': series.split('.')[1]
        })
        if overrides['exchange'] in ['OE', 'HKEX', 'CBOE']:
            product = 'Options'
        rt_codes = self.exchange_codes.get(overrides['exchange'])
        raw_search =[]
        duplicates = [
            x for x in self.options_raw(
                f"{overrides['base']}*{overrides.get('suffix', '')}",
                underlying=underlying,
                currency=currency,
                exchange_codes=rt_codes,
                only_active=only_active,
                option_type=product,
                search_type='Isin'
            )
            if x['Identifier'] in [item['Identifier'] for item in raw_search]
            or raw_search.append(x)
        ] # surprisingly, there are some duplicating search results so we get rid of them this way
        filtering_regex = re.compile(
            rf"^{overrides['base']}(?P<strike>\d+)(?P<maturity>\w\d{{1,2}})(?P<sign>[PN])?{overrides.get('suffix', '')}$"
        )
        raw_search = [
            x for x
            in raw_search
            if re.match(filtering_regex, x['Identifier'])
        ]
        if not raw_search:
            self.logger.warning(f"nothing is found for {series}")
            return series_data, contracts
        if isinstance(raw_search[0], KeyError):
            self.logger.error(f"smth is wrong with search parameters")
            return series_data, contracts
        raw_search_df = pd.DataFrame(raw_search)
        composite_ids = [(x['Identifier'], x['IdentifierType']) for x in raw_search]
        id_types = {x[1] for x in composite_ids}
        composite_data = []
        for itype in id_types:
            composite_data.extend(
                self.composite(
                    [x[0] for x in composite_ids if x[1] == itype],
                    fields=['all'],
                    base_type=itype
                )
            )
        composite_df = pd.DataFrame(composite_data)

        combined_df = pd.merge(raw_search_df, composite_df, on=['Identifier', 'IdentifierType'])
        combined = combined_df.to_dict('records')
        formatted_data = []
        for d in combined:
            try:
                formatted_data.append(
                    StrikeSchema(
                        ticker=overrides['ticker'],
                        exchange=overrides['exchange'],
                        base_ric_=overrides['base'],
                        **d
                    ).dict()
                )
            except ValidationError as valerr:
                self.logger.warning(
                    f"contract data {d.get('RIC')} is invalid: {valerr.errors()}"
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
        if product == 'FuturesOnOptions':
            series_exclude.append('underlying_ric')
            contract_include.append('underlying_ric')
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
        underlying_type = 'index' if product == 'Options' else 'future'
        if product == 'Options':
            self.__search_underlying(series_data, contracts, 'index')
        else:
            self.__search_underlying(series_data, contracts, 'future')
        return series_data, contracts

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

"""
        fut_series = None
        search_give_up = False
        for ticcode in found:
            additional_info = self.composite([x['ric'] for x in found[ticcode].values()],
                                                fields=['Underlying RIC'], base_type='Ric')
            for exp in found[ticcode]:
                found[ticcode][exp]['CALL'] = set(found[ticcode][exp]['CALL'])
                found[ticcode][exp]['PUT'] = set(found[ticcode][exp]['PUT'])
            if not result.get(overrides['exchange']):
                result.update({overrides['exchange']: dict()})
            result[overrides['exchange']].update({
                overrides['ticker']: found[ticcode]
            })
            for exp, payload in found[ticcode].items():
                underlying = str()
                underlying_ric = next((x['Underlying RIC'] for x in additional_info if x['Identifier'] == payload['ric']), None)
                if product == 'Options':
                    pass
                    # Ugh, I gave up:(
                    # instruments = self.sdb.get_heirs(self.cfd_folder, full=True, recursive=True)
                    # underlying = next((
                    #     f"{x['name']}.{next(y['exchangeName'] for y in self.sdb.get_exchanges() if y['_id'] == )}" for x in instruments
                    #     if x.get('feeds', {}).get('providerOverrides',{}).get(self.provider_id, {}).get('reutersProperties', {}).get('tradeRic') == underlying_ric
                    #     or x.get('feeds', {}).get('providerOverrides',{}).get(self.provider_id, {}).get('reutersProperties', {}).get('ric') == underlying_ric
                    #     or x.get('identifiers', {}).get('RIC') == underlying_ric
                    # ), None)
                else:
                    underlying_regex = re.compile(r'^(?P<ticker>\w+)(?P<expiration>[FGHJKMNQUVXZ]\d)(?P<suffix>.*)?$')
                    if not underlying_ric or not re.match(underlying_regex, underlying_ric):
                        continue
                    fut_ticker = re.match(underlying_regex, underlying_ric).group('ticker')
                    fut_expiration = re.match(underlying_regex, underlying_ric).group('expiration')
                    all_futures = [x for x in self.tree if len(x['path']) > 1 and x['path'][1] == self.FUTURE]
                    fut_exchange_folders = [
                        x for x in all_futures
                        if x['name'] == overrides['exchange']
                        or x.get('exchangeId') == overrides['exchange_id']
                    ]
                    if not fut_series and not search_give_up:
                        fut_existing_tickers = []
                        for fef in fut_exchange_folders:
                            fut_existing_tickers += [
                                x for x in self.tree
                                if x['path'][:len(fef['path'])] == fef['path']
                                and x.get('ticker')
                                and x.get('isAbstract')
                                and x.get('isTrading', True) != False
                                and (x.get('expiryTime') is None or dt.datetime(x['expiryTime']) > dt.datetime.now())
                            ]
                        find_ric = [
                            'reutersProperties/tradeRic/base',
                            'reutersProperties/ric/base'
                        ]
                        for fet in fut_existing_tickers:
                            try:
                                fut_series = Future(fet['ticker'], overrides['exchange'])
                                fut_ric_base = next((
                                    val for key, val
                                    in fut_series.get_provider_overrides('REUTERS', *find_ric, silent=True).items()
                                ), None)
                                if fut_ric_base == fut_ticker:
                                    break
                            except NoInstrumentError:
                                self.logger.warning(f"{fet['ticker']}.{overrides['exchange']} futures are not found in sdb... Strange!")
                        if not fut_ric_base:
                            self.logger.warning(f"Underlying futures are not found!")
                            search_give_up = True
                        if not search_give_up:
                            ul_expiration_name = format_maturity(fut_expiration)
                            underlying = f"{fut_series.ticker}.{fut_series.exchange}.{fut_series._date_to_symbolic(ul_expiration_name)}"
                            if ul_expiration_name not in [x['name'] for x in fut_series.children]:
                                self.logger.warning(f"Underlying future ({underlying}) is not found in sdb! You should create it first")
                if underlying:
                    result[overrides['exchange']][overrides['ticker']][exp].update({
                        'underlying': underlying
                    })
        return result"""