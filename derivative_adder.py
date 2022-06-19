import asyncio
import datetime as dt
from pandas import DataFrame
import logging
from copy import copy, deepcopy
from typing import Union
from enum import Enum
from libs.editor_interactive import EditInstrument
from libs.async_symboldb import SymbolDB
from libs.parsers import DxfeedParser as DF
from libs.parsers import DscopeParser as DS
from libs.async_sdb_additional import SDBAdditional, Months, SdbLists
from libs.sdb_instruments import (
    NoInstrumentError,
    Option,
    OptionExpiration,
    WeeklyCommon,
    Future,
    FutureExpiration,
    Spread,
    SpreadExpiration,
    Instrument,
    set_schema
)
from libs.terminal_tools import pick_from_list_tm
from pprint import pformat, pprint


allowed_automation = {
    'OPTION': {
        'CBOE': ['Equity Options']
    }
}

class TypeUndefined(Exception):
    pass

class Parsers(Enum):
    REUTERS = DS
    DXFEED = DF


class DerivativeAdder:

    def __init__(
            self,
            ticker,
            exchange,
            shortname: str = None,
            derivative='FUTURE',
            weekly: bool = False,
            allowed_expirations: list = None,
            max_timedelta: int = None,
            recreate: bool = False,
            reload_cache: bool = True,
            croned: bool = False,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            tree_df: DataFrame = None,
            env='prod'
        ) -> None:
        self.errormsg = ''
        self.comment = ''
        self.env = env
        self.sdb = sdb if isinstance(sdb, SymbolDB) else SymbolDB(env)
        self.sdbadds = sdbadds if isinstance(sdbadds, SDBAdditional) else SDBAdditional(env)
        if tree_df is not None and not tree_df.empty:
            self.sdbadds.tree_df = tree_df
        self.croned = croned
        self.ticker = ticker
        self.weekly = weekly
        self.exchange = exchange
        self.shortname = shortname
        self.derivative_type = derivative
        self.allowed_expirations = allowed_expirations
        self.max_timedelta = max_timedelta
        self.series = self.set_series(
            recreate=recreate,
            reload_cache=reload_cache
        )
        if self.series:
            self.existing_expirations = [
            x for x in self.series.contracts
            if x.instrument.get('isTrading') is not False
        ]
            if isinstance(self.series, Option):
                self.derivative_type = self.series.option_type
        else:
            self.existing_expirations = []
        self.navi = set_schema[self.env]['navigation'](self.schema)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

# common_methods
    def set_series(
            self,
            shortname: str = None,
            destination: str = None,
            recreate: bool = False,
            reload_cache: bool = True
        ):
        if 'SPREAD' not in self.derivative_type:
            self.schema = set_schema[self.env][self.derivative_type]
        else:
            self.schema = set_schema[self.env]['SPREAD'][self.derivative_type]

        if self.derivative_type in ['OPTION', 'OPTION ON FUTURE']:
            try:
                option = Option(
                    self.ticker,
                    self.exchange,
                    shortname=shortname,
                    parent_folder=destination,

                    recreate=recreate,
                    reload_cache=reload_cache,

                    env=self.env,
                    sdb=self.sdb,
                    sdbadds=self.sdbadds
                )
                if option.instrument.get('isTrading') is not None:
                    option.instrument.pop('isTrading')
                return option
            except NoInstrumentError:
                return None

        elif self.derivative_type == 'FUTURE':
            try:
                future = Future(
                    self.ticker,
                    self.exchange,
                    shortname=shortname,
                    parent_folder=destination,

                    recreate=recreate,
                    reload_cache=reload_cache,

                    env=self.env,
                    sdb=self.sdb,
                    sdbadds=self.sdbadds
                )
                if future.instrument.get('isTrading') is not None:
                    future.instrument.pop('isTrading')
                return future
            except NoInstrumentError:
                return None

        elif self.derivative_type == 'SPREAD':
            try:
                spread = Spread(
                    self.ticker,
                    self.exchange,
                    shortname=shortname,
                    parent_folder=destination,

                    recreate=recreate,
                    reload_cache=reload_cache,

                    env=self.env,
                    sdb=self.sdb,
                    sdbadds=self.sdbadds
                )
                if spread.instrument.get('isTrading') is not None:
                    spread.instrument.pop('isTrading')
                return spread
            except NoInstrumentError:
                return None

        else:
            raise TypeUndefined(f'Derivative type ({self.derivative_type}) is unknown')

    def get_overrides(self, instrument: Instrument, parent: bool = False):
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
            ],
            'P2GATE': [
                'symbolName'
            ]
        }

        http_feeds = [
            x[1] for x
            in asyncio.run(self.sdbadds.get_list_from_sdb(SdbLists.GATEWAYS.value))
            if 'HTTP' in x[0]
        ]
        # as CBOE options are constantly balanced between gateways, CBOE folder doesn't have default routes,
        # so a little bit of help here by hardcoding DXFEED as main feed provider
        if self.exchange == 'CBOE':
            prov_name, main_feed_source = next(
                x for x
                in asyncio.run(
                    self.sdbadds.get_list_from_sdb(
                        SdbLists.FEED_PROVIDERS.value
                    )
                ) if x[0] == 'DXFEED'
            )
        else:
            if parent:
                compiled_parent = asyncio.run(
                    self.sdbadds.build_inheritance(
                        [
                            instrument.compiled_parent,
                            instrument.instrument
                        ]
                    )
                )
            else:
                compiled_parent = instrument.compiled_parent
            main_feed_source = next((
                x['gateway']['providerId'] for x
                in compiled_parent.get('feeds', {}).get('gateways', [])
                if x.get('gateway', {}).get('enabled')
                and x.get('gatewayId') not in http_feeds
            ), None)
            prov_name = next((
                x[0] for x
                in asyncio.run(
                    self.sdbadds.get_list_from_sdb(
                        SdbLists.FEED_PROVIDERS.value
                    )
                )
                if x[1] == main_feed_source
            ), None)
        payload = {
            'provider': prov_name,
            'prov_id': main_feed_source,
            'instrument_type': self.sdbadds.tree_df.loc[
                self.sdbadds.tree_df['_id'] == instrument.instrument['path'][1]
            ].iloc[0]['name'],
            # 'instrument_type': next(
            #     (
            #         x['name'] for x
            #         in instrument.tree
            #         if x['_id'] == instrument.instrument['path'][1]
            #     ),
            #     asyncio.run(self.sdb.get(instrument.instrument['path'][1]))['name']
            # ),
            'exchange_id': instrument.instrument.get(
                'exchangeId',
                instrument.compiled_parent.get('exchangeId')
            )
        }
        if not parent:
            payload.update({
                'ticker': instrument.instrument.get('ticker')
            })

        for provider, properties in provider_specific.items():
            overrides_to_get = properties + common
            payload[provider] = instrument.get_provider_overrides(
                provider,
                *overrides_to_get,
                compiled=True,
                silent=True
            )
            if not parent:
                payload[provider].update(
                    instrument.get_provider_overrides(
                        provider,
                        *overrides_to_get,
                        compiled=False,
                        silent=True
                    )
                )

        return payload

    def set_targets(
            self,
            weekly: bool = False,
            create_weeklies: str = 'Weekly'
        ) -> list[dict]:

        if not isinstance(self.series, Option):
            target = self.get_overrides(self.series)
            target['target_folder'] = self.series
            return [target]
        
        # options only
        elif not weekly or self.exchange == 'CBOE':
            # monthly options
            target = self.get_overrides(self.series)
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
                weekly_target = self.get_overrides(wf)
                weekly_target['target_folder'] = wf
                weekly_targets.append(weekly_target)
            return weekly_targets
        if self.croned:
            raise RuntimeError(
                f'{self.ticker}.{self.exchange} '
                'weekly folders are not found in SDB and could not be set automatically'
            )
        overrides = self.get_overrides(self.series)
        weekly_templates = self.set_weekly_templates(overrides)
        weekly_common = self.series.create_weeklies(weekly_templates, common_name=create_weeklies)
        weekly_targets = self.make_weekly_targets(weekly_common, overrides, weekly_templates)
        return weekly_targets

    def parse_available(self, target: dict = None) -> list[dict]:
        feed_provider = target.get('provider')
        if not feed_provider:
            self.logger.error(f"Cannot get feed_provider for {target}")
            return {'series': {}, 'contracts': []}
        if feed_provider == 'DXFEED':
            parser = DF()
        elif feed_provider == 'REUTERS':
            parser = DS()
        else:
            raise RuntimeError(f'Unknown feed provider: {feed_provider}')
        overrides_to_parse = {
            key.split('/')[-1]: val for key, val
            in target.get(feed_provider, {}).items()
        }
        ticker = target['target_folder'].ticker
        exchange = target['target_folder'].exchange
        self.logger.info(f"Parsing {self.ticker}.{self.exchange} on {feed_provider}...")
        if isinstance(target['target_folder'], Future):
            series, contracts = parser.futures(
                f"{ticker}.{exchange}",
                overrides=overrides_to_parse)
        elif isinstance(target['target_folder'], Spread):
            series, contracts = parser.spreads(
                f"{self.ticker}.{self.exchange}",
                overrides=overrides_to_parse)
        elif isinstance(target['target_folder'], Option):
            series, contracts = parser.options(
                f"{ticker}.{exchange}",
                overrides=overrides_to_parse,
                product=self.derivative_type
            )
        else:
            return {'series': {}, 'contracts': []}
        if contracts:
            good_to_add = parser.transform_to_sdb(
                {},
                contracts,
                product=self.derivative_type,
                spread_type=target['target_folder'].spread_type if self.derivative_type == 'SPREAD' else None
            )
        else:
            self.logger.warning(
                f"Didn't find anything for {ticker}.{exchange} on {feed_provider}"
            )
            return {'series': {}, 'contracts': []}
        if isinstance(target['target_folder'], Option) and feed_provider != 'REUTERS':
            spm = target[feed_provider].get('strikePriceMultiplier')
            if spm:
                for expiration in good_to_add['contracts']:
                    self.multiply_strikes(expiration, spm)
                    self.logger.info(
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
        elif len(self.allowed_expirations) == 1 and len(self.allowed_expirations[0]) < 3:
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
                    if Months[exp[0]].value < 10:
                        translate_expiration = f"20{exp[-2:]}-0{Months[exp[0]].value}"
                    else:
                        translate_expiration = f"20{exp[-2:]}-{Months[exp[0]].value}"
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
            target: Option,
            parsed_contracts: list[dict],
            skip_if_exists: bool = True
        ):
        for contract in parsed_contracts:
            target.add_payload(contract, skip_if_exists=skip_if_exists)

    def validate_series(
            self, 
            target: Union[
                Future,
                Option,
                Spread
            ]
        ):
        if target.update_expirations:
            some_contract = [x for x in target.update_expirations][0]
        elif target.new_expirations:
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
            some_contract.set_instrument(some_contract.instrument, self.series)
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
                instrument_type = self.derivative_type.split(' ')[0], # Mind 'CALENDAR SPREAD'!!
                env=self.env
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
        report = self.series.post_to_sdb(dry_run)
        if report:
            for series, part_report in report.items():
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
                self.errormsg += f"{self.series.ticker}.{self.series.exchange} create error:" \
                    + pformat(part_report['create_error']) + '\n' if part_report.get('create_error') else ''
                self.errormsg += f"{self.series.ticker}.{self.series.exchange} update error:" \
                    + pformat(part_report['update_error']) + '\n' if part_report.get('update_error') else ''
        return self.comment, self.errormsg


# new series actions
    def setup_new_ticker(self, recreate: bool = False, parsed_data: list[dict] = None):
        message = '''
        Ticker is not found in sdb, we are about to create new ticker folder.
        Select folder to go deeper into the tree, select the same folder again
        to set as destination for new ticker:
        '''
        # choose destination folder
        if not self.croned:
            if self.derivative_type == 'FUTURE':
                suggested_path = ['FUTURE', self.exchange]
            elif self.derivative_type == 'OPTION' and self.exchange == 'CBOE':
                suggested_path = ['OPTION', 'CBOE', 'Equity Options']
            elif self.derivative_type == 'OPTION':
                suggested_path = ['OPTION', self.exchange]
            elif self.derivative_type == 'OPTION ON FUTURE':
                suggested_path = ['OPTION ON FUTURE', self.exchange]
            new_folder_destination = self.sdbadds.browse_folders(
                suggested_path, message=message, only_folders=True
            )
        elif self.exchange in allowed_automation.get(self.derivative_type, []):
            new_folder_destination = (
                allowed_automation[self.derivative_type][self.exchange][-1],
                asyncio.run(
                    self.sdb.get_uuid_by_path(
                        ['Root', self.derivative_type, self.exchange].extend(
                            allowed_automation[self.derivative_type][self.exchange]
                        )
                    )
                )
            )
        if not new_folder_destination:
            self.logger.error('New folder destination is not set')
            return None

        # you should choose the folder where series folder meant to be placed (generally the exchange folder)
        # but if you choose the old series folder I won't judge you, it's also ok:)
        if new_folder_destination[0] == self.ticker:
            old_folder = self.sdbadds.tree_df.loc[
                self.sdbadds.tree_df['_id'] == new_folder_destination[1]
            ].iloc[0]
            # old_folder = next(x for x in tree if x['_id'] == new_folder_destination[1])
            destination_path = self.sdbadds.tree_df.loc[
                self.sdbadds.tree_df['_id'] ==  old_folder['path'][-2]
            ].iloc[0]['path']
            # destination_path = next(
            #     x['path'] for x
            #     in tree
            #     if x['_id'] == old_folder['path'][-2]
            # )
        else:
            destination_path = self.sdbadds.tree_df.loc[
                self.sdbadds.tree_df['_id'] == new_folder_destination[1]
            ].iloc[0]['path']

        # check the derivative_type one more time
        inherited_type = self.sdbadds.tree_df.loc[
            self.sdbadds.tree_df['_id'] == destination_path[1]
        ].iloc[0]['name']

        # inherited_type = next(x['name'] for x in tree if x['_id'] == destination_path[1])
        if self.derivative_type == 'FUTURE':
            if inherited_type != 'FUTURE':
                self.logger.error(f"You should not place new futures here: {self.sdbadds.show_path(destination_path)}")
                return None
        elif inherited_type in ['OPTION', 'OPTION ON FUTURE']:
            if self.derivative_type != inherited_type:
                self.logger.info(f'Derivative type is set to {inherited_type}')
                self.derivative_type = inherited_type
        else:
            self.logger.error(f"You should not place new options here: {self.sdbadds.show_path(destination_path)}")
            return None
        #try to get inherited overrides
        parent = Instrument(
            self.schema,
            instrument=asyncio.run(self.sdb.get(destination_path[-1])),
            env=self.env,
            sdb=self.sdb,
            sdbadds=self.sdbadds
        )
        overrides = self.get_overrides(parent, parent=True)
        overrides.update({'ticker': self.ticker})
        feed_provider = overrides.get('provider')
        gateway = None

        # if no inherited feeds let's choose it
        if not feed_provider:
            if self.croned:
                self.logger.error(
                    'Feed provider is not defined, cannot create new folder'
                )
                return None

            message = '''
            Feed provider is unknown. Please select one:
            '''
            feed_providers = asyncio.run(
                self.sdbadds.get_list_from_sdb(
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
                
            else:
                self.logger.error('Feed provider is not defined, cannot create new folder')
                return None
        
        # reuters with its reutersProperties is somewhat special
        if feed_provider == 'REUTERS':
            overrides = self.set_reuters_overrides(overrides)
            parser = DS()
        elif feed_provider == 'DXFEED':
            parser = DF()
        else:
            self.logger.error(f'Cannot set new ticker for {feed_provider} feed provider, sorry')
            return None
        if self.derivative_type == 'FUTURE':
            parsed_series, parsed_contracts = parser.futures(
                f'{self.ticker}.{self.exchange}',
                overrides=overrides.get(feed_provider),
                data=parsed_data
            )
        elif self.derivative_type in ['OPTION', 'OPTION ON FUTURE']:
            parsed_series, parsed_contracts = parser.options(
                f'{self.ticker}.{self.exchange}',
                overrides=overrides.get(feed_provider),
                product=self.derivative_type,
                data=parsed_data
            )
        if self.derivative_type == 'SPREAD':
            parsed_series, parsed_contracts = parser.spreads(
                f'{self.ticker}.{self.exchange}',
                overrides=overrides.get(feed_provider),
                data=parsed_data
            )
        overrides['DXFEED'] = {}
        if not parsed_series:
            self.logger.error(f'Nothing has been found for {self.ticker}.{self.exchange}')
            return None

        # if parsed data could not be validated we augment it with our own knowledge
        while True:
            transformed = parser.transform_to_sdb(
                parsed_series,
                parsed_contracts,
                product=self.derivative_type
            )
            if transformed.get('series'):
                break
            if not transformed.get('validation_errors'):
                self.logger.error('smth wrong with transform_to_sdb method')
                return None
            for err in transformed.get('validation_errors'):
                err_field = '/'.join([x for x in err['loc'] if '__' not in x])
                parsed_series.update({
                    err_field: input(f"{err['msg']}. Please, fill the correct value for {err_field}: ")
                })

        # make a valid series folder with all these overrides
        new_ticker_properties = transformed.get('series')
        if gateway:
            new_ticker_properties.update({
                'feeds/gateways': [gateway]
            })
        contracts = transformed.get('contracts')
        if parsed_series.get('strike_price_multiplier_'):
            spm = parsed_series['strike_price_multiplier_']
            overrides[feed_provider].update({
                'strikePriceMultiplier': spm
            })
            for expiration in transformed['contracts']:
                self.multiply_strikes(expiration, spm)
                self.logger.info(f"parsed strikes for {expiration} have been multiplied by {1 / spm}")

        shortname = new_ticker_properties.pop('shortName')
        self.series = self.set_series(
                shortname=shortname,
                destination=destination_path[-1],
                recreate=recreate,
                reload_cache=False
        )
        for key, val in new_ticker_properties.items():
            self.series.set_field_value(val, key.split('/'))
        if overrides:
            self.series.set_provider_overrides(
                provider=feed_provider,
                **overrides[feed_provider]
            )

        # presence of currency_multiplier means that derivative is traded in fractional currency,
        # e.g. in US cents instead of US dollars, so we shoul adjust price multipliers both on feed and execution
        # as we always use only base currencies and never fractional ones
        if parsed_series.get('currency_multiplier_'):
            feed_currency_override = {
                'priceMultiplier': 0.01
            }
            broker_currency_override = {
                'priceMultiplier': 100
            }
            series_feed_providers = [
                next(
                    y[0] for y
                    in asyncio.run(self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value))
                    if y[1] == x
                ) for x
                in self.series.compiled_parent.get('feeds', {}).get('providerOverrides')
            ]
            series_broker_providers = [
                next(
                    y[0] for y
                    in asyncio.run(self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value))
                    if y[1] == x
                ) for x
                in self.series.compiled_parent.get('brokers', {}).get('providerOverrides')
            ]
            for fp in series_feed_providers:
                self.series.set_provider_overrides(
                    provider=fp,
                    **feed_currency_override
                )
            for bp in series_broker_providers:
                self.series.set_provider_overrides(
                    provider=bp,
                    **broker_currency_override
                )
        overrides.update({
            'target_folder': self.series
        })
        parsed = {
            'contracts': contracts
        }
        return overrides, parsed

    def set_reuters_overrides(self, overrides: dict = None, weeklies=False):
        if overrides is None:
            overrides = {}
        if weeklies:
            ticker = input('Type weekly ticker template: ')
            if not ticker:
                return overrides
        else:
            ticker = self.ticker    
        overrides.update({'ticker': ticker})
        while True:
            base = input("ric base (the part preceeding expiration code): ")
            if not base:
                return overrides
            suffix = input(
                "ric suffix (the part after expiration code. Press enter if none): "
            )
            if self.derivative_type in ['OPTION', 'OPTION ON FUTURE']:
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
                    search = self.ds.options(
                        f"{ticker.replace(sym, i)}.{self.exchange}",
                        overrides=srch_pld
                    )
                    if search.get('contracts') and weeklies:
                        found = True
                        pprint(f"Week {i}: {search['contracts']}")
                    elif search.get('contracts'):
                        found = True
                        to_print = search['contracts'][:3]
                        if len(search['contracts']) > 3:
                            to_print.append(f"and {len(search['contracts']) - 3} more expirations")
                        pprint(f"Found contracts: {to_print}")
            elif self.derivative_type == 'FUTURE':
                separator = None
                print('Test search...')
                srch_pld = {
                    'base': base,
                    'suffix': suffix,
                }
                search = self.ds.futures(
                    f"{ticker}.{self.exchange}",
                    overrides=srch_pld
                )
                if search.get('contracts'):
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
    def multiply_strikes(
            self,
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
            overrides = self.set_reuters_overrides(deepcopy(overrides), weeklies=True)
        else:
            while '$' not in ticker and '@' not in ticker:
                ticker = input('Type weekly ticker template: ')
            overrides.update({'ticker': ticker})
        return overrides


if __name__ == '__main__':
    print('Live fast. Die hard')
    print('Derivative adder by @alser')