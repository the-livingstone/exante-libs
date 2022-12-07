import asyncio
import datetime as dt
import json
import pandas as pd
import logging
from copy import copy, deepcopy
from typing import Any, Union
from enum import Enum
from sqlalchemy.engine import Engine
from libs.editor_interactive import EditInstrument
from libs.async_symboldb import SymbolDB
from libs.parsers import DxfeedParser, FtxParser, DscopeParser, ExchangeParser
from libs.replica_sdb_additional import SDBAdditional, Months, SdbLists
from libs.new_instruments import (
    Option,
    OptionExpiration,
    WeeklyCommon,
    Future,
    FutureExpiration,
    Spread,
    SpreadExpiration,
    Instrument,
    InitThemAll,
    get_uuid_by_path
)
from libs.scrapers.cqg_symbols import CqgSymbols
from libs.terminal_tools import pick_from_list_tm
from pprint import pformat, pprint


allowed_automation = {
    'FUTURE': {
        'CBOE': [],
        'COMEX': [],
        'NYMEX': []
    },
    'OPTION': {
        'CBOE': ['Equity Options']
    },
    'OPTION ON FUTURE': {
        "CBOT": [],
        "COMEX": [],
        "NYMEX": []
    }
}

class DerivativeType(Enum):
    OPTION = Option
    OPTION_ON_FUTURE = Option
    FUTURE = Future
    SPREAD = Spread
    CALENDAR_SPREAD = Spread

class Parser(Enum):
    DXFEED = DxfeedParser
    REUTERS = DscopeParser
    FTX = FtxParser

class TypeUndefined(Exception):
    pass


class DerivativeAdder:

    def __init__(
            self,
            ticker: str,
            exchange: str,
            derivative_type: str,
            series: Union[Future, Option, Spread] = None,
            weekly: bool = False,
            allowed_expirations: list = None,
            max_timedelta: int = None,
            croned: bool = False,

            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env: str = 'prod'
        ):
        self.errormsg = ''
        self.comment = ''
        self.validation_errors = {}
        self.new_ticker_parsed = {}
        self.report = {}
        (
            self.bo,
            self.sdb,
            self.sdbadds
        ) = InitThemAll(
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        ).get_instances
        self.croned = croned
        self.weekly = weekly
        self.ticker = ticker
        self.exchange = exchange
        self.allowed_expirations = allowed_expirations
        self.max_timedelta = max_timedelta
        self.series = series
        if series:
            if isinstance(series, Option):
                self.derivative_type = series.option_type # OPTION, OPTION ON FUTURE
            elif isinstance(series, Spread):
                self.derivative_type = 'SPREAD'
            else:
                self.derivative_type = series.instrument_type # FUTURE
            self.existing_expirations = [
                x for x
                in self.series.contracts
                if x.instrument.get('isTrading') is not False
            ]
        else:
            if derivative_type.replace(' ', '_') not in DerivativeType.__members__:
                raise RuntimeError(
                    f"{derivative_type=} is invalid"
                )

    def __repr__(self):
        return f"DerivativeAdder({self.series=}, {self.allowed_expirations=}, {self.max_timedelta=}, {self.croned=})"

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

# constructors
    @classmethod
    def from_sdb(
            cls,
            ticker,
            exchange,
            derivative: str,

            parent_folder_id: str = None,
            weekly: bool = False,
            allowed_expirations: list = None,
            max_timedelta: int = None,
            croned: bool = False,

            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env='prod'
        ):
        (
            bo,
            sdb,
            sdbadds
        ) = InitThemAll(
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        ).get_instances
        if derivative.replace(' ', '_') not in DerivativeType.__members__:
            raise TypeUndefined(f'{derivative=} is unknown type')
        series_class: Union[Option, Future, Spread] = DerivativeType[derivative.replace(' ', '_')].value
        series = series_class.from_sdb(
            ticker,
            exchange,
            parent_folder_id=parent_folder_id,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env        
        ) # raises NoExchangeError (if exchange does not exist)
        # or NoInstrumentError (if ticker does not exist on particular exchange)
        if series.instrument.get('isTrading'):
            series.instrument.pop('isTrading')
        return cls(
            ticker,
            exchange,
            derivative,
            series,
            weekly=weekly,
            allowed_expirations=allowed_expirations,
            max_timedelta=max_timedelta,
            croned=croned,

            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )

    @classmethod
    def from_scratch(
            cls,
            ticker: str,
            exchange: str,
            derivative: str,

            shortname: str = None,
            parent_folder_id: str = None,
            option_type: str = None,
            calendar_type: str = None,
            recreate: bool = False,

            weekly: bool = False,
            allowed_expirations: list = None,
            max_timedelta: int = None,
            croned: bool = False,

            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env='prod'
        ):
        (
            bo,
            sdb,
            sdbadds
        ) = InitThemAll(
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        ).get_instances
        errormsg = ''
        if derivative.replace(' ', '_') not in DerivativeType.__members__:
            raise TypeUndefined(f'{derivative=} is unknown type')
        
        logger = logging.getLogger(
            f"DerivativeAdder.from_scratch({ticker}.{exchange})"
        )
        target, parsed = DerivativeAdder.setup_new_ticker(
            ticker,
            exchange,
            derivative,
            shortname=shortname,
            destination_id=parent_folder_id,
            recreate=recreate,
            croned=croned,

            sdb=sdb,
            sdbadds=sdbadds,
            logger=logger,
            errormsg=errormsg
        )
        drv = cls(
            ticker,
            exchange,
            derivative,
            series=target['target_folder'],
            weekly=weekly,
            allowed_expirations=allowed_expirations,
            max_timedelta=max_timedelta,
            croned=croned,

            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )
        drv.new_ticker_parsed = parsed
        return drv

    @classmethod
    def from_dict(
        cls,
        derivative: str,
        payload: dict,
        contracts_payload: list[dict] = None,

        weekly: bool = False,
        croned: bool = False,

        sdb: SymbolDB = None,
        sdbadds: SDBAdditional = None,
        env='prod'
    ):
        (
            bo,
            sdb,
            sdbadds
        ) = InitThemAll(
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        ).get_instances
        if derivative.replace(' ', '_') not in DerivativeType.__members__:
            raise TypeUndefined(f'{derivative=} is unknown type')
        series_class: Union[Option, Future, Spread] = DerivativeType[derivative.replace(' ', '_')].value
        series = series_class.from_dict(
            payload,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )
        if not contracts_payload:
            contracts_payload = []
        drv = cls(
            series.ticker,
            series.exchange,
            derivative,
            series=series,
            weekly=weekly,
            croned=croned,

            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )
        if contracts_payload and not weekly:
            drv.add_expirations(
                drv.series,
                contracts_payload
            )
        return drv


# common_methods
    @staticmethod
    def get_overrides(
            ticker: str,
            exchange: str,
            instrument: Instrument,
            parent: bool = False
        ):
        common = [
            'symbolIdentifier/type',
            'symbolIdentifier/identifier',
            'exchangeName',
            'volumeMultiplier',
            'priceMultiplier',
            'strikePriceMultiplier'
        ]
        provider_specific = {
            'REUTERS': [
                'reutersProperties/tradeRic/base',
                'reutersProperties/tradeRic/suffix',
                'reutersProperties/tradeRic/optionSeparator',
                'reutersProperties/ric/base',
                'reutersProperties/ric/suffix',
                'reutersProperties/ric/optionSeparator'
            ],
            'DXFEED': [
                'dxfeedProperties/useLongMaturityFormat',
                'dxfeedProperties/suffix'
            ]
        }

        http_feeds = [
            x[1] for x
            in asyncio.run(instrument.sdbadds.get_list_from_sdb(
                SdbLists.GATEWAYS.value
            ))
            if 'HTTP' in x[0]
        ]
        # as CBOE options are constantly balanced between gateways, CBOE folder doesn't have default routes,
        # so a little bit of help here by hardcoding DXFEED as main feed provider
        if exchange == 'CBOE':
            main_prov_name, main_feed_source = next(
                x for x
                in asyncio.run(
                    instrument.sdbadds.get_list_from_sdb(
                        SdbLists.FEED_PROVIDERS.value
                    )
                ) if x[0] == 'DXFEED'
            )
            feed_sources = [main_feed_source]
        else:
            if parent:
                compiled_parent = asyncio.run(
                    instrument.sdbadds.build_inheritance(
                        [
                            instrument.compiled_parent,
                            instrument.instrument
                        ]
                    )
                )
            else:
                compiled_parent = instrument.compiled_parent
            feed_sources = [
                x['gateway']['providerId'] for x
                in compiled_parent.get('feeds', {}).get('gateways', [])
                if x.get('gateway', {}).get('enabled')
                and x.get('gatewayId') not in http_feeds
            ]
            main_feed_source = feed_sources[0] if feed_sources else None
            main_prov_name = next((
                x[0] for x
                in asyncio.run(
                    instrument.sdbadds.get_list_from_sdb(
                        SdbLists.FEED_PROVIDERS.value
                    )
                )
                if x[1] == main_feed_source
            ), None)
        payload = {
            'provider': main_prov_name,
            'prov_id': main_feed_source,
            'derivative_type': instrument.sdbadds.uuid_to_name(
                instrument.instrument['path'][1]
            )[0],
            'exchange_id': instrument.instrument.get(
                'exchangeId',
                instrument.compiled_parent.get('exchangeId')
            )
        }
        if not parent:
            payload.update({
                'ticker': ticker,
                'exchange': exchange
            })
        for source in feed_sources:
            prov_name = next((
                x[0] for x
                in asyncio.run(
                    instrument.sdbadds.get_list_from_sdb(
                        SdbLists.FEED_PROVIDERS.value
                    )
                )
                if x[1] == source
            ), None)
            payload.setdefault(prov_name, {})
            overrides_to_get = common + provider_specific.get(prov_name, [])
            payload[prov_name] = instrument.get_provider_overrides(
                prov_name,
                *overrides_to_get,
                compiled=True,
                silent=True
            )
            if not parent:
                payload[prov_name].update(
                    instrument.get_provider_overrides(
                        prov_name,
                        *overrides_to_get,
                        compiled=False,
                        silent=True
                    )
                )
        return payload

    @staticmethod
    def check_cqg_symbolname(
            exchange: str,
            shortname: str,
            derivative_type: str
        ):
        cqg = CqgSymbols()
        suggestions_df = cqg.get_symbolname(
            instrument_type=derivative_type,
            description=shortname,
            exchange=exchange
        )
        if suggestions_df.shape[0] > 0:
            if suggestions_df.shape[0] > 1:
                logging.warning(
                    'CQG symbolName selection is ambiguous!'
                    'Please have a look at providerOverrides to check if it\'s ok'
                )
            return suggestions_df.iloc[0]['Symbol']
        return None



    def set_targets(
            self,
            weekly: bool = False,
            create_weeklies: str = 'Weekly'
        ) -> list[dict]:

        if not isinstance(self.series, Option):
            target = self.get_overrides(
                self.ticker,
                self.exchange,
                self.series
            )
            target['target_folder'] = self.series
            return [target]
        
        # options only
        elif not weekly or self.exchange == 'CBOE':
            # monthly options
            target = self.get_overrides(
                self.ticker,
                self.exchange,
                self.series
            )
            target['target_folder'] = self.series
            return [target]

        # weekly options
        weekly_targets = []
        weekly_commons_names = [
            x.common_name for x
            in self.series.weekly_commons
        ]
        # if we don't have common weekly folder
        # or we have one, but we need another with different name
        if self.series.weekly_commons and create_weeklies in weekly_commons_names:
            week_options = [
                x for y in self.series.weekly_commons for x in y.weekly_folders
            ]
            for wf in week_options:
                weekly_target = self.get_overrides(
                    wf.ticker,
                    wf.exchange,
                    wf
                )
                weekly_target['target_folder'] = wf
                weekly_targets.append(weekly_target)
            return weekly_targets
        if self.croned:
            raise RuntimeError(
                f'{self.ticker}.{self.exchange} '
               'weekly folders are not found in SDB and could not be set automatically'
            )
        overrides = self.get_overrides(
            self.ticker,
            self.exchange,
            self.series
        )
        weekly_templates = self.set_weekly_templates(overrides)
        weekly_common = self.series.create_weeklies(weekly_templates, common_name=create_weeklies)
        for wf in weekly_common.weekly_folders:
            weekly_cqg_symbolname = self.check_cqg_symbolname(
                exchange=self.exchange,
                shortname=wf.instrument['shortName'],
                derivative_type='OPTION'
            )
            if weekly_cqg_symbolname:
                cqg_providers = [
                    x[0] for x
                    in asyncio.run(self.sdbadds.get_list_from_sdb('broker_providers'))
                    if 'CQG' in x[0]
                ]
                for cqg in cqg_providers:
                    wf.set_provider_overrides(cqg, **{'symbolName': weekly_cqg_symbolname})
            
        weekly_targets = self.make_weekly_targets(weekly_common, overrides, weekly_templates)
        return weekly_targets

    @staticmethod
    def parse_available(
            target: dict = None,
            set_series: bool = False,
            croned: bool = False,
            db_engine: Engine = None,

            errormsg: str = None,
            logger: logging.Logger = None
        ) -> list[dict]:
        if not errormsg:
            errormsg = ''
        good_to_add = {
            'series': {},
            'contracts': [],
            'intermediate_series': {},
            'errormsg': errormsg
        }
        if target.get('target_folder'):
            ticker = target['target_folder'].ticker
            exchange = target['target_folder'].exchange
            spread_type=target['target_folder'].spread_type if target['derivative_type'] == 'SPREAD' else None

        else:
            ticker = target['ticker']
            exchange = target['exchange']
            if target['derivative_type'] == 'SPREAD':
                if len(ticker.split('-')) < 2:
                    spread_type = 'CALENDAR_SPREAD'
                else:
                    spread_type = 'SPREAD'
            else:
                spread_type = None
        if not logger:
            logger = logging.getLogger(
                f'DerivativeAdder.parse_available({ticker}.{exchange})'
            )

        feed_provider = target.get('provider')
        if not feed_provider:
            logger.error(f"Cannot get feed_provider for {target}")
            return good_to_add
        init_parser = {}
        if feed_provider == 'DXFEED':
            init_parser.update({
                'engine': db_engine
            })
        if feed_provider in Parser.__members__:
            parser: ExchangeParser = Parser[feed_provider].value(**init_parser)
        else:
            raise RuntimeError(f'Unknown {feed_provider=}')
        derivative_type = target.get('derivative_type')
        overrides_to_parse = {
            key.split('/')[-1]: val for key, val
            in target.get(feed_provider, {}).items()
        }
            
        logger.info(f"Parsing {ticker}.{exchange} on {feed_provider}...")
        if derivative_type == 'FUTURE':
            parsed_series, parsed_contracts = parser.futures(
                f"{ticker}.{exchange}",
                overrides=overrides_to_parse)
        elif derivative_type == 'SPREAD':
            parsed_series, parsed_contracts = parser.spreads(
                f"{ticker}.{exchange}",
                overrides=overrides_to_parse)
        elif derivative_type in ['OPTION', 'OPTION ON FUTURE']:
            parsed_series, parsed_contracts = parser.options(
                f"{ticker}.{exchange}",
                overrides=overrides_to_parse,
                product=derivative_type
            )
        else:
            return good_to_add
        if not parsed_contracts:
            logger.warning(
                f"Didn't find anything for {ticker}.{exchange} on {feed_provider}"
            )
            return good_to_add
        good_to_add['intermediate_series'] = parsed_series
        if set_series:
            while True:
                transformed = parser.transform_to_sdb(
                    parsed_series,
                    parsed_contracts,
                    product=derivative_type,
                    spread_type=spread_type
                )
                if transformed.get('series'):
                    break
                if not transformed.get('validation_errors'):
                    logger.error('smth wrong with transform_to_sdb method')
                    return None
                if croned:
                    errormsg += f"{ticker}.{exchange}: series validation has been failed:"
                    for err in transformed.get('validation_errors'):
                        errormsg += f"{'/'.join([str(x) for x in err['loc']])}: {err['msg']}" + '\n'
                    return None
                for err in transformed.get('validation_errors'):
                    err_field = '/'.join([x for x in err['loc'] if '__' not in x])
                    parsed_series.update({
                        err_field: input(f"{err['msg']}. Please, fill the correct value for {err_field}: ")
                    })
        else:
            transformed = parser.transform_to_sdb(
                {},
                parsed_contracts,
                product=derivative_type,
                spread_type=spread_type
            )
        good_to_add.update(transformed)
        if derivative_type in ['OPTION', 'OPTION ON FUTURE'] and feed_provider != 'REUTERS':
            spm = target[feed_provider].get('strikePriceMultiplier')
            if spm:
                for expiration in good_to_add['contracts']:
                    DerivativeAdder.multiply_strikes(expiration, spm)
                    logger.info(
                        f"parsed strikes for {expiration} have been multiplied by {1 / spm}"
                    )
        return good_to_add

    def set_allowed(
            self,
            target_folder: Union[Future, Option, Spread],
            contracts: list[dict]):
        if 'all' in self.allowed_expirations:
            allowed = None
        elif 'weekly' in self.allowed_expirations and target_folder != self.series:
            allowed = [
                self.sdb.sdb_to_date(x['expiry']).isoformat() for x
                in contracts
            ]
        elif len(self.allowed_expirations) == 1 and len(str(self.allowed_expirations[0])) < 3:
            try:
                forthcoming = int(self.allowed_expirations[0])
                allowed = sorted([
                        self.sdb.sdb_to_date(x['expiry']).isoformat() for x
                        in contracts
                        if self.sdb.sdb_to_date(x['expiry']) not in [
                            existing.expiration for existing
                            in self.existing_expirations
                        ]
                    ])[:forthcoming]
            except ValueError:
                self.logger.error('''Wrong -e argument value
                Should be a number of upcoming expirations or maturity
                Accepted formats: MMYY, MM-YY, MMYYYY, MM-YYYY
                Month could be a number or a letter (FGHJKMNQUVXZ)''')
                return None
        else:
            filter_expirations = list()
            for exp in self.allowed_expirations:
                translate_expiration = str()
                if exp[0] in Months.__members__:
                    translate_expiration = f"20{exp[-2:]}-{Months[exp[0]].value:0>2}"
                elif exp[0].isdecimal():
                    if int(exp[0]) > 1 or len(exp) == 3 or exp[1] == '-':
                        translate_expiration = f"20{exp[-2:]}-0{exp[0]}"
                    else:
                        translate_expiration = f"20{exp[-2:]}-{exp[0:2]}"
                else:
                    self.logger.error('''Wrong -e argument value
                    Should be a number of upcoming expirations or maturity
                    Accepted formats: MMYY, MM-YY, MMYYYY, MM-YYYY
                    Month could be a number or a letter (FGHJKMNQUVXZ)''')
                    continue
                filter_expirations.append(translate_expiration)
            allowed = [
                self.sdb.sdb_to_date(x['expiry']).isoformat() for x
                in contracts
                if x.get('name') in filter_expirations
            ]
        if self.max_timedelta is not None:
            approved = deepcopy(allowed)
            if contracts and contracts[0].get('farMaturityDate'):
                allowed = [
                    f"{Months(x['nearMaturityDate']['month']).name}{x['nearMaturityDate']['year']}-"
                    f"{Months(x['farMaturityDate']['month']).name}{x['farMaturityDate']['year']}" for x
                    in contracts
                    if (self.sdb.sdb_to_date(x['farMaturityDate']) - dt.date.today()).days < self.max_timedelta * 365
                    and (
                        approved is None
                        or self.sdb.sdb_to_date(x['expiry']).isoformat() in approved
                    )
                ]
            else:
                allowed = [
                    self.sdb.sdb_to_date(x['expiry']).isoformat() for x
                    in contracts
                    if (self.sdb.sdb_to_date(x['expiry']) - dt.date.today()).days < self.max_timedelta * 365
                    and (
                        approved is None
                        or self.sdb.sdb_to_date(x['expiry']).isoformat() in approved
                    )
                ]
        target_folder.allowed_expirations = allowed

    def add_expirations(
            self,
            target: Union[Future, Option, Spread],
            parsed_contracts: list[dict],
            skip_if_exists: bool = True
        ):
        additional = {
            'skip_if_exists': skip_if_exists
        }
        if isinstance(target, Option):
            additional.update({
                'week_num': target.week_number
            })
        for contract in parsed_contracts:
            try:
                target.add_payload(contract, **additional)
            except Exception as e:
                self.logger.warning(f"{e.__class__.__name__} {e}: expiration {contract['name']} is not added")

    def validate_series(
            self, 
            target: Union[
                Future,
                Option,
                Spread
            ]
        ):
        if target.new_expirations:
            some_contract = [x for x in target.new_expirations][0]
        elif target.contracts:
            some_contract = max(
                [x for x in target.contracts],
                key=lambda e: e.expiration
            )
        else:
            logging.warning('no existing or new expirations found, cannot validate')
            return None
        while True:
            highlighted = {}
            # we need to reload compiled parent instrument here
            some_contract.set_instrument(some_contract.instrument, target)
            validated = some_contract.validate_instrument()
            if isinstance(validated, dict) and validated.get('validation_errors'):
                for v in validated['validation_errors']:
                    highlighted.update({
                        f"{'/'.join([str(x) for x in v['loc']])}": v['msg']
                    })
                # if the only validation issue is underlyingId and opt type is oof, then folder is good
                expiration_issues = [
                    key for key, val
                    in highlighted.items()
                    if 'Expiry is less than last trading' in val
                    or 'Last available is less than expiry' in val
                ]
                if self.derivative_type == 'OPTION ON FUTURE':
                    expiration_issues.extend([
                        key for key, val
                        in highlighted.items()
                        if 'expires earlier than instrument' in val
                        or key == 'underlyingId'
                        or 'does not exist in sdb' in val
                        or 'UnderlyingId is not set' in val
                    ])
                for eiss in expiration_issues:
                    highlighted.pop(eiss)
                if not highlighted:
                    break
                self.validation_errors.setdefault(
                    f"{target.ticker}.{target.exchange}",
                    {}
                ).setdefault('series', {})
                self.validation_errors[f"{target.ticker}.{target.exchange}"]['series'] = highlighted
                if self.croned:
                    self.logger.error(
                        f"{self.ticker}.{self.exchange}: Series validation has been failed on following fields:"
                    )
                    self.logger.error(pformat(highlighted))
                    self.errormsg += f"{self.ticker}.{self.exchange}: Series validation has been failed on following fields:"
                    self.errormsg += '\n' + pformat(highlighted) + '\n'
                    return None

            elif validated is True:
                break
            self.series.instrument = EditInstrument(
                f'{self.ticker}.{self.exchange}',
                self.series.instrument,
                instrument_type = self.derivative_type.split(' ')[0],
                env=self.sdb.env
            ).edit_instrument(highlight=highlighted)
        return True

    def validate_expirations(
            self,
            expirations: list[
                Union[
                    FutureExpiration,
                    OptionExpiration,
                    SpreadExpiration
                ]
            ]
        ):
        drop_expirations = []
        if not expirations:
            return []
        for exp in expirations:
            attempts = 0
            dropped = False
            while True:
                symbolid = exp.get_expiration()[1]
                highlighted = {}
                exp.set_instrument(exp.instrument, self.series)
                validated = exp.validate_instrument()
                if isinstance(validated, dict) and validated.get('validation_errors'):
                    for v in validated['validation_errors']:
                        highlighted.update({
                            f"{'/'.join([str(x) for x in v['loc']])}": v['msg']
                        })
                    self.validation_errors.setdefault(
                        f"{exp.ticker}.{exp.exchange}",
                        {}
                    ).setdefault(exp.expiration.isoformat(), {})
                    self.validation_errors[f"{exp.ticker}.{exp.exchange}"][exp.expiration.isoformat()] = highlighted

                    if self.croned:
                        self.logger.error(
                            f"{symbolid}: Expiration validation has been failed on following fields:"
                        )
                        self.logger.error(pformat(highlighted))
                        self.errormsg += f"{symbolid}: Expiration validation has been failed on following fields:"
                        self.errormsg += '\n' + pformat(highlighted) + '\n'
                        drop_expirations.append(symbolid)
                        dropped = True
                        break
                elif validated is True:
                    break
                if attempts > 0 and not self.croned:
                    message = f"Validation of {symbolid} has failed. Do you want to edit it once more or drop it?"
                    edit_expiration = pick_from_list_tm(
                        ['Drop', 'Edit more'],
                        message=message,
                        clear_screen=False
                    )
                    if not edit_expiration:
                        drop_expirations.append(symbolid)
                        dropped = True
                        break
                if not dropped:
                    exp.instrument = EditInstrument(
                        symbolid,
                        exp.instrument,
                        instrument_type=self.derivative_type.split(' ')[0],
                        env=self.env,
                        sdb=self.sdb,
                        sdbadds=self.sdbadds
                    ).edit_instrument(highlight=highlighted)
                    attempts += 1
        for drop in drop_expirations:
            expirations.pop(
                next(
                    num for num, x
                    in enumerate(expirations)
                    if x.get_expiration()[1] == drop
                )
            )
        return expirations

    def post_to_sdb(self, dry_run: bool):
        self.report = self.series.post_to_sdb(dry_run)
        if self.report:
            for series, part_report in self.report.items():
                if part_report.get('created') or part_report.get('updated'):
                    self.comment += f'{series}' + '\n'
                else:
                    pass
                    # self.commented due to excessive output
                    # self.comment += f'No new data for {series}' + '\n'
                if part_report.get('updated'):
                    self.comment += 'Updated:\n'
                    for upd in part_report['updated']:
                        self.comment += f'* {upd}' + '\n'
                    self.comment += '\n'
                if part_report.get('created'):
                    self.comment += 'New Expirations:\n'
                    for new in part_report['created']:
                        self.comment += f'* {new}' + '\n'
                    self.comment += '\n'
                if part_report.get('to_create'):
                    self.comment += 'Dry run, expirations to create:\n'
                    for new in part_report['to_create']:
                        self.comment += f'* {new}' + '\n'
                    self.comment += '\n'
                if part_report.get('to_update'):
                    self.comment += 'Dry run, expirations to update:\n'
                    for upd in part_report['to_update']:
                        self.comment += f'* {upd}' + '\n'
                    self.comment += '\n'
                self.errormsg += f"{self.series.ticker}.{self.series.exchange} create error:" \
                    + pformat(part_report['create_error']) + '\n' if part_report.get('create_error') else ''
                self.errormsg += f"{self.series.ticker}.{self.series.exchange} update error:" \
                    + pformat(part_report['update_error']) + '\n' if part_report.get('update_error') else ''
        return self.comment, self.errormsg

# new series actions
    @staticmethod
    def __set_new_destination(
            ticker: str,
            exchange: str,
            derivative_type: str,
            destination_id: str = None,
            croned: bool = False,

            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            logger: logging.Logger = None
        ):

        def extend_path() -> list:
            MAPPING = {
                'FUTURE': {
                    'CME_group': 'cme_futs.json'
                },
                'OPTION': {
                    'CBOE': ['Equity Options']
                },
                'OPTION ON FUTURE': {
                    'CME_group': 'cme_opts.json'
                }
            }
            if exchange in ['CME', 'CBOT', 'NYMEX', 'COMEX']:
                try:
                    with open(f"{sdbadds.current_dir}/libs/mapping/{MAPPING[derivative_type]['CME_group']}", 'r') as f:
                        cme_map = json.load(f)
                except FileNotFoundError:
                    logger.warning(
                        f"{sdbadds.current_dir}/libs/mapping/{MAPPING[derivative_type]['CME_group']} "
                        "file is not found! Pls select destination folder manually"
                    )
                    return []
                except json.decoder.JSONDecodeError:
                    logger.warning(
                        f"{sdbadds.current_dir}/libs/mapping/{MAPPING[derivative_type]['CME_group']} "
                        "is malformed json file"
                    )
                    return []
                additional_path = cme_map.get(exchange, {}).get(ticker, {}).get('category')
                if additional_path:
                    return [additional_path]
                else:
                    return []
            elif isinstance(MAPPING[derivative_type].get(exchange), list):
                return MAPPING[derivative_type][exchange]
            else:
                return []

        if not logger:
            logger = logging.getLogger(
                f'DerivativeAdder.__set_new_destination({ticker}.{exchange})'
            )
        message = '''
        Ticker is not found in sdb, we are about to create new ticker folder.
        Select folder to go deeper into the tree, select the same folder again
        to set as destination for new ticker:
        '''
        # choose destination folder

        new_folder_destination = None
        parent_folder = asyncio.run(sdb.get(destination_id)) if destination_id else None
        if parent_folder and parent_folder.get('isAbstract'):
            suggested_path: list[str] = parent_folder['path']
        else:
            suggested_path = ['Root', derivative_type, exchange]
        additional = extend_path()
        suggested_path.extend(additional)
        if not croned:
            new_folder_destination = sdbadds.browse_folders(
                suggested_path, message=message, only_folders=True
            )
        else:
            new_folder_destination = (
                suggested_path[-1],
                get_uuid_by_path(
                    suggested_path,
                    sdbadds.engine
                )
            )
            # follback to main folder if category folder does not exist
            if new_folder_destination[1] is None and additional:
                new_folder_destination = (
                    suggested_path[-2],
                    get_uuid_by_path(
                        suggested_path,
                        sdbadds.engine
                    )
                )
        return new_folder_destination, suggested_path

    @staticmethod
    def __set_feed_provider(
            overrides: dict,
            sdbadds: SDBAdditional
        ):
        message = '''
        Feed provider is unknown. Please select one:
        '''
        feed_providers = asyncio.run(
            sdbadds.get_list_from_sdb(
                SdbLists.GATEWAYS.value,
                id_only=False
            )
        )
        selected = pick_from_list_tm(
            sorted([x[0] for x in feed_providers]), 'providers', message
        )
        if selected is not None:
            gateway = feed_providers[selected][1]
            gateway['gateway'].update({
                'enabled': True,
                'allowFallback': True
            })
            feed_provider = feed_providers[selected][0].split(':')[0]
            overrides.update({
                'provider': feed_provider,
                'prov_id': gateway['gateway']['providerId']
            })
            return gateway
            
    @staticmethod
    def setup_new_ticker(
            ticker: str,
            exchange: str,
            derivative_type: str,
            shortname: str = None,
            destination_id: str = None,
            recreate: bool = False,
            croned: bool = False,
            parsed_data: list[dict] = None,

            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            logger: logging.Logger = None,
            errormsg: str = None
        ):
        if not errormsg:
            errormsg = ''
        if not logger:
            logger = logging.getLogger(
            f"DerivativeAdder.setup_new_ticker({ticker}.{exchange})"
        )
        # try to set destination for new ticker
        new_folder_destination, suggested = DerivativeAdder.__set_new_destination(
            ticker,
            exchange,
            derivative_type,
            destination_id=destination_id,
            croned=croned,

            sdb=sdb,
            sdbadds=sdbadds,
            logger=logger
        )
        # check if it is ok and maybe correct it
        if not new_folder_destination:
            logger.error('New folder destination is not set')
            return None
        # We don't like to add smth out of category if categories are present
        heirs = asyncio.run(sdb.get_heirs(new_folder_destination[1], fields=['ticker']))
        if len([x for x in heirs if x.get('ticker')]) < len(heirs)/2:
            errormsg += (
                f"{ticker}.{exchange}: "
                f"Cannot set destination folder (suggested: {'→'.join(suggested)})" + "\n"
            )
            return None
        # you should choose the folder where series folder meant to be placed (generally the exchange folder)
        # but if you choose the old series folder I won't judge you, it's also ok:)
        if new_folder_destination[0] == ticker:
            old_folder = pd.read_sql(
                'SELECT id as _id, path '
                'FROM instruments '
                f"WHERE id = '{new_folder_destination[1]}'",
                sdbadds.engine
            ).iloc[0]
            destination_path = pd.read_sql(
                'SELECT id as _id, path '
                'FROM instruments '
                f"WHERE id = '{old_folder['path'][-2]}'",
                sdbadds.engine
            ).iloc[0]['path']
        else:
            destination_path = pd.read_sql(
                'SELECT id as _id, path '
                'FROM instruments '
                f"WHERE id = '{new_folder_destination[1]}'",
                sdbadds.engine
            ).iloc[0]['path']
        # check the derivative_type one more time
        inherited_type = pd.read_sql(
                'SELECT id as _id, "extraData" as extra '
                'FROM instruments '
                f"WHERE id = '{new_folder_destination[1]}'",
                sdbadds.engine
            ).iloc[0]['extra']['name']
        # inherited_type = next(x['name'] for x in tree if x['_id'] == destination_path[1])
        if inherited_type in ['OPTION', 'OPTION ON FUTURE'] and derivative_type in ['OPTION', 'OPTION ON FUTURE']:
            if derivative_type != inherited_type:
                logger.info(f'Derivative type is set to {inherited_type}')
                derivative_type = inherited_type
        elif derivative_type != inherited_type:
            logger.error(
                f"You should not place new {derivative_type.lower()}s here: "
                f"{sdbadds.show_path(destination_path)}"
            )
            return None

        if derivative_type.replace(' ', '_') not in DerivativeType.__members__:
            raise TypeUndefined(f'{derivative_type=} is unknown type')


        # try to get inherited overrides
        parent = Instrument(
            instrument=asyncio.run(sdb.get(destination_path[-1])),
            instrument_type=derivative_type,
            env=sdb.env,
            sdb=sdb,
            sdbadds=sdbadds
        )
        overrides = DerivativeAdder.get_overrides(
            ticker,
            exchange,
            parent,
            parent=False
        )
        feed_provider = overrides.get('provider')
        gateway = None

        # if no inherited feeds let's choose it
        if not feed_provider:
            if croned:
                logger.error(
                    'Feed provider is not defined, cannot create new folder'
                )
                return None
            gateway = DerivativeAdder.__set_feed_provider(overrides, sdbadds)
            if not gateway:
                logger.error(
                    'Feed provider is not set, cannot create new folder'
                )
                return None


        # reuters with its reutersProperties is somewhat special
        if feed_provider == 'REUTERS':
            overrides = DerivativeAdder.set_reuters_overrides(
                ticker,
                exchange,
                derivative_type=derivative_type,
                overrides=overrides)

        parsed = DerivativeAdder.parse_available(
            overrides,
            set_series=True,
            croned=croned,
            errormsg=errormsg,
            logger=logger
        )

        # make a valid series folder with all these overrides
        if gateway:
            parsed['series'].update({
                'feeds/gateways': [gateway]
            })
        if not shortname:
            shortname = parsed['series'].pop('shortName')
        else:
            parsed['series'].pop('shortName')
        series_class: Union[Option, Future, Spread] = DerivativeType[derivative_type.replace(' ', '_')].value
        series = series_class.from_scratch(
            ticker,
            exchange,
            shortname=shortname,
            parent_folder_id=destination_path[-1],
            recreate=recreate,

            sdb=sdb,
            sdbadds=sdbadds
        ) # raises RuntimeError if not recreate and series exists in sdb
        
        # set CQG overrides
        cqg_symbolname = DerivativeAdder.check_cqg_symbolname(
            exchange=exchange,
            shortname=shortname,
            derivative_type=derivative_type
        )
        if cqg_symbolname:
            cqg_providers = [
                x[0] for x
                in asyncio.run(sdbadds.get_list_from_sdb('broker_providers'))
                if 'CQG' in x[0]
            ]
            for cqg in cqg_providers:
                series.set_provider_overrides(cqg, {'symbolName': cqg_symbolname})

        for key, val in parsed['series'].items():
            series.set_field_value(val, key.split('/'))
        if overrides:
            series.set_provider_overrides(
                provider=feed_provider,
                **overrides[feed_provider]
            )

        if parsed['intermediate_series'].get('strike_price_multiplier_'):
            spm = parsed['intermediate_series']['strike_price_multiplier_']
            overrides.setdefault(feed_provider, {}).update({
                'strikePriceMultiplier': spm
            })
            for c in parsed['contracts']:
                DerivativeAdder.multiply_strikes(c, spm)

        # presence of currency_multiplier means that derivative is traded in fractional currency,
        # e.g. in US cents instead of US dollars, so we shoul adjust price multipliers both on feed and execution
        # as we always use only base currencies and never fractional ones
        if parsed['intermediate_series'].get('currency_multiplier_'):
            feed_currency_override = {
                'priceMultiplier': 0.01
            }
            broker_currency_override = {
                'priceMultiplier': 100
            }
            series_feed_providers = [
                next(
                    y[0] for y
                    in asyncio.run(sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value))
                    if y[1] == x
                ) for x
                in series.compiled_parent.get('feeds', {}).get('providerOverrides')
            ]
            series_broker_providers = [
                next(
                    y[0] for y
                    in asyncio.run(sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value))
                    if y[1] == x
                ) for x
                in series.compiled_parent.get('brokers', {}).get('providerOverridetes')
            ]
            for fp in series_feed_providers:
                series.set_provider_overrides(
                    provider=fp,
                    **feed_currency_override
                )
            for bp in series_broker_providers:
                series.set_provider_overrides(
                    provider=bp,
                    **broker_currency_override
                )

        overrides.update({
            'target_folder': series
        })
        return overrides, parsed

    @staticmethod
    def set_reuters_overrides(
            ticker: str,
            exchange: str,
            derivative_type: str,
            overrides: dict = None,
            weeklies=False
        ):
        dscope = DscopeParser()
        if overrides is None:
            overrides = {}
        if weeklies:
            ticker = input('Type weekly ticker template: ')
            if not ticker:
                return overrides
        overrides.update({'ticker': ticker})
        while True:
            base = input("ric base (the part preceeding expiration code): ")
            if not base:
                return overrides
            suffix = input(
                "ric suffix (the part after expiration code. Press enter if none): "
            )
            if derivative_type in ['OPTION', 'OPTION ON FUTURE']:
                separator = input(
                    "option separator (the part that separates expiration from strike. Press enter if none): "
                )
                print('Test search...')
                if '$' in base or '$' in separator or '$' in suffix:
                    sym = '$'
                    weeks = '12345'
                elif '@' in base or '@' in separator or '@' in suffix:
                    sym = '@'
                    weeks = 'ABCDE'
                else:
                    sym = '~~~' # smth that is never expected
                    weeks = ' '
                found = False
                for i in weeks:
                    srch_pld = {
                        'base': base.replace(sym, i),
                        'suffix': suffix.replace(sym, i),
                        'optionSeparator': separator.replace(sym, i),
                    }
                    search = dscope.options(
                        f"{ticker.replace(sym, i)}.{exchange}",
                        overrides=srch_pld
                    )
                    if search[1] and weeklies:
                        found = True
                        pprint(f"Week {i}: {search['contracts']}")
                    elif search[1]:
                        found = True
                        to_print = search['contracts'][:3]
                        if len(search['contracts']) > 3:
                            to_print.append(f"and {len(search['contracts']) - 3} more expirations")
                        pprint(f"Found contracts: {to_print}")
            elif derivative_type == 'FUTURE':
                separator = None
                print('Test search...')
                srch_pld = {
                    'base': base,
                    'suffix': suffix,
                }
                search = dscope.futures(
                    f"{ticker}.{exchange}",
                    overrides=srch_pld
                )
                if search[1]:
                    found = True
                    to_print = search['contracts'][:3]
                    if len(search['contracts']) > 3:
                        to_print.append(f"and {len(search['contracts']) - 3} more expirations")
                    pprint(f"Found contracts: {to_print}")

            if found:
                try_again = input('Looks good? Y/n: ')
                if try_again != 'n':
                    break
            else:
                try_again = input(
                    f"nothing is found... try again? Y/n: "
                )
                if try_again == 'n':
                    return overrides
        overrides['REUTERS'] = {
            'reutersProperties/quoteRic/base': base,
            'reutersProperties/tradeRic/base': base
        }
        if suffix:
            overrides['REUTERS'].update({
                'reutersProperties/quoteRic/suffix': suffix,
                'reutersProperties/tradeRic/suffix': suffix
            })
        if separator:
            overrides['REUTERS'].update({
                'reutersProperties/quoteRic/optionSeparator': separator,
                'reutersProperties/tradeRic/optionSeparator': separator
            })
        return overrides

# option methods
    @staticmethod
    def multiply_strikes(
            expiration: dict,
            multiplier: float = 1
        ):
        if multiplier == 1 or not multiplier:
            return None
        else:
            # logic was taken from cme_adder
            # previous = sorted(strikes['PUT'])[0] * multiplier
            # while (previous * multiplier < 1):
            #     multiplier = multiplier * 1000
            #     previous = previous * 1000
            for side in ['PUT', 'CALL']:
                for strike in expiration['strikePrices'][side]:
                    strike.update({
                        'strikePrice': round(strike['strikePrice'] / multiplier, 3)
                    })
 
    def make_weekly_targets(
            self,
            weekly_common: WeeklyCommon,
            overrides: dict,
            weekly_templates: dict
        ):
        parser_targets = []
        letters = ' ABCDE'
        for i in range(1, 6):
            weekly_folder = next((x for x in weekly_common.weekly_folders if x.week_number == i), None)
            if not weekly_folder:
                continue
            weekly_target = deepcopy(overrides)
            weekly_target['target_folder'] = weekly_folder
            weekly_target['ticker'] = copy(weekly_templates['ticker'])
            weekly_target['ticker'] = weekly_target['ticker'].replace('$', str(i))
            weekly_target['ticker'] = weekly_target['ticker'].replace('@', letters[i])
            for provider, ovrs in weekly_target.items():
                if isinstance(ovrs, dict):
                    weekly_target[provider] = {}
                    for key, override in ovrs.items():
                        if not isinstance(override, str):
                            weekly_target[provider].update({key: override})
                            continue
                        weekly_target[provider].update({
                            key: override.replace('$', str(i)).replace('@', letters[i])
                        })
            if weekly_target.get('REUTERS'):
                rics = {
                    key.split('/')[-1]: val for key, val
                    in weekly_templates['REUTERS'].items()
                }
                weekly_target['REUTERS'].update(rics)
            parser_targets.append(weekly_target)
        return parser_targets

    def set_weekly_templates(self, overrides: dict):
        ticker = ''
        feed_provider = overrides.get('provider')
        message = '''
        We are about to create new folders for weekly options.
        · If week identifier is a number replace it with "$"
        (e.g. for weeklies ZW1, ZW2, ..., ZW5 type ZW$
        and for weeklies R1E, R2E, ..., R5E type R$E)
        · If week identifier is a letter replace it with "@"
        (e.g for weeklies Si/A, Si/B, ..., Si/E type Si/@):
        '''
        print(message)
        if feed_provider == 'REUTERS':
            overrides = self.set_reuters_overrides(
                self.ticker,
                self.exchange,
                derivative_type=self.derivative_type,
                overrides=deepcopy(overrides),
                weeklies=True
            )
        else:
            while '$' not in ticker and '@' not in ticker:
                ticker = input('Type weekly ticker template: ')
            overrides.update({'ticker': ticker})
        return overrides


if __name__ == '__main__':
    print('Live fast. Die hard')
    print('Derivative adder by @alser')