import asyncio
import datetime as dt
from time import time
import pandas as pd
import numpy as np
import logging
import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from deepdiff import DeepDiff
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import Months, SDBAdditional, SdbLists
from libs.backoffice import BackOffice
from pprint import pformat, pp
from typing import Dict, Optional, Union
from libs.sdb_instruments import (
    Instrument,
    Derivative,
    NoInstrumentError,
    ExpirationError,
    EXPIRY_BEFORE_MATURITY,
    format_maturity
)

@dataclass
class Option(Derivative):
    # series parameters
    ticker: str
    exchange: str
    shortname: Optional[str] = None
    underlying: Optional[str] = None
    parent_folder: Union[str, dict] = None
    week_number: int = 0
    env: str = 'prod'

    # init parameters
    reload_cache: bool = True
    recreate: bool = False
    silent: bool = False

    # class instances
    bo: BackOffice = None
    sdb: SymbolDB = None
    sdbadds: SDBAdditional = None

    # pass series tree to week instances
    parent_tree: list[dict] = None

    # non-init vars
    instrument_type: str = field(init=False, default='OPTION')
    option_type: str = field(init=False, default=None)
    instrument: dict = field(init=False, default_factory=dict)
    reference: dict = field(init=False, default_factory=dict)
    series_tree: list[dict] = field(init=False, default_factory=list)

    skipped: set = field(init=False, default_factory=set)
    allowed_expirations: list = field(init=False, default_factory=list)
    
    underlying_dict: dict = field(init=False, default_factory=dict)
    weekly_templates: dict = field(init=False, default_factory=dict)


    '''
    Class to operate on whole bunch of expirations on given ticker/exchange
    :param ticker: ticker of given option series
    :param exchange: exchange of given option series
    :param shortname: optional if series is already exists, required to create new series
    :param option_type: 'OPTION' or 'OPTION ON FUTURE'
    '''
    def __post_init__(self):
        self.contracts: list[OptionExpiration] = []
        self.new_expirations: list[OptionExpiration] = []
        self.update_expirations: list[OptionExpiration] = []

        # super sets up following self.vars:
            # self.schema,
            # self.instrument,
            # self.reference,
            # self.contracts,
            # self.weekly_commons
        super().__init__(self)
        self.__set_contracts()
        self.contracts: list[OptionExpiration] = self.contracts
        if self.option_type not in ['OPTION', 'OPTION ON FUTURE']:
            raise RuntimeError(f'unknown option type: {self.option_type}')
        if self.option_type == 'OPTION' and self.underlying:
            self.set_underlying(self.underlying)
        self._align_expiry_la_lt(self.contracts, self.update_expirations)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __repr__(self):
        week_indication = "Monthly" if not self.week_number else f"Week {self.week_number}"
        return f"Option({self.ticker}.{self.exchange}, {self.option_type=}, {week_indication} series)"

    def __set_contracts(self):
        if self.instrument:
            self.contracts = [
                OptionExpiration(self, payload=x) for x
                in self.series_tree
                if x['path'][:-1] == self.instrument['path']
                and not x['isAbstract']
            ]
            # common weekly folders where single week folders are stored
            # in most cases only one is needed but there are cases like EW.CME
            if not self.week_number:
                weekly_folders = [
                    x for x in self.series_tree
                    if x['path'][:-1] == self.instrument['path']
                    and 'weekly' in x['name'].lower()
                    and x['isAbstract']
                ]
                self.weekly_commons = [
                    WeeklyCommon(self, uuid=x['_id']) for x in weekly_folders
                ]
        else:
            self.contracts = []
            self.weekly_commons = []

    def find_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None,
            week_num: Union[int, bool] = None,
            ticker: str = None
        ):
        present_expirations: list[tuple[OptionExpiration, Option]] = []
        expiration_date = None

        # prepare expiration_date as dt.date
        if isinstance(expiration, OptionExpiration):
            lookup_folders = [self]
            if self.week_number == 0:
                lookup_folders.extend([
                    x for y in self.weekly_commons for x in y.weekly_folders
                    if x.week_number == week_num
                    or x.ticker == ticker
                ])
            for lf in lookup_folders:
                if lf.update_expirations:
                    expiration_nums = [
                        num for num, x
                        in enumerate(lf.update_expirations)
                        if x == expiration
                        and x.instrument.get('isTrading') is not False
                    ]
                    if len(expiration_nums) > 1:
                        self.logger.error(
                            'More than one expiration have been found, try to narrow search criteria'
                        )
                        return None, None
                    elif len(expiration_nums) == 1:
                        present_expirations.append(
                            (lf.update_expirations.pop(expiration_nums[0]), lf)
                        )
                        return present_expirations[0]
                present_expirations.extend([
                    (x, lf) for x
                    in lf.contracts
                    if x == expiration
                    and x.instrument.get('isTrading') is not False
                ])
            if len(present_expirations) == 1:
                present_expiration, lookup_folder = present_expirations[0]
                return present_expiration, lookup_folder
            elif len(present_expirations) > 1:
                self.logger.error('More than one expiration have been found, try to narrow search criteria')
                return None, None
            return None, self

        elif isinstance(expiration, str):
            try:
                expiration_date = dt.date.fromisoformat(expiration)
            except ValueError:
                pass
            except AttributeError:
                pass
            if expiration_date is None and maturity is None:
                maturity = expiration
        elif isinstance(expiration, dt.datetime):
            expiration_date = expiration.date()
        elif isinstance(expiration, dt.date):
            expiration_date = expiration
        # try to extract ticker from maturity
        if maturity and len(maturity.split('.')) > 1:
            ticker = maturity.split('.')[0]
            if ticker == self.ticker:
                ticker = None
            elif self.week_number:
                self.logger.info(
                    f'Cannot search contracts outside of week {self.week_number}, '
                    'use main series instance'
                )
                return None, None

            maturity = maturity.replace('.B*', '').replace('.P*', '').replace('.C*', '')
            maturity = maturity.split('.')[-1]
            
        # prepare maturity as YYYY-MM
        maturity = format_maturity(maturity)
        # symbolic_maturity as Z2021
        symbolic_maturity = self._date_to_symbolic(maturity)
        if isinstance(week_num, bool) and week_num is True and expiration_date:
            week_num = int((expiration_date.day - 1)/7) + 1
        if (week_num or isinstance(ticker, str)) and not self.week_number:
            lookup_folders = [
                x for y in self.weekly_commons for x in y.weekly_folders
                if x.week_number == week_num
                or x.ticker == ticker
            ]
        else:
            lookup_folders = [self]
        if not lookup_folders:
            self.logger.info(f'Week {week_num} folder is not found, hence neither expiration is')
            return None, self
        # firstly search in option.update_expirations
        for lf in lookup_folders:
            try_symbol_id = f"{lf.ticker}.{lf.exchange}.{symbolic_maturity}"
            if lf.update_expirations:
                expiration_nums = [
                    num for num, x
                    in enumerate(lf.update_expirations)
                    if (
                        x.expiration == expiration_date
                        or x.maturity == maturity
                        or x.get_expiration()[1] == try_symbol_id  # expiration: Z2021
                    ) and x.instrument.get('isTrading') is not False
                ]
                if len(expiration_nums) > 1:
                    self.logger.error(
                        'More than one expiration have been found, try to narrow search criteria'
                    )
                    return None, None
                elif len(expiration_nums) == 1:
                    present_expirations.append(
                        (lf.update_expirations.pop(expiration_nums[0]), lf)
                    )
        if len(present_expirations) == 1:
            present_expiration, lookup_folder = present_expirations[0]
            return present_expiration, lookup_folder
        elif len(present_expirations) > 1:
            self.logger.error('More than one expiration have been found, try to narrow search criteria')
            return None, None
        # if nothing is found, search in option.contracts

        for lf in lookup_folders:
            try_symbol_id = f"{lf.ticker}.{lf.exchange}.{expiration}"
            present_expirations.extend([
                (x, lf) for x
                in lf.contracts
                if (
                    x.expiration == expiration_date
                    or x.maturity == maturity
                    or x.get_expiration()[1] == try_symbol_id  # expiration: Z2021
                ) and x.instrument.get('isTrading') is not False
            ])
        if len(present_expirations) == 1:
            present_expiration, lookup_folder = present_expirations[0]
            return present_expiration, lookup_folder
        elif len(present_expirations) > 1:
            self.logger.error('More than one expiration have been found, try to narrow search criteria')
            return None, None
        return None, self

    def _set_target_folder(
            self,
            week_num: int = 0,
            weekly_ticker: Union[bool, str] = None
        ):
        # set target folder
        target_folder = None

        if weekly_ticker == self.ticker and self.week_number:
            # that means we call this method from some week Option instance and it matches
            target_folder = self
        elif isinstance(weekly_ticker, str) and not self.week_number:
            # self.week_number == 0 indicates main series
            weekly_folders = [
                x for y in self.weekly_commons for x in y.weekly_folders
                if x.ticker == weekly_ticker
            ]
            if len(weekly_folders) == 1:
                target_folder = weekly_folders[0]
            elif len(weekly_folders) > 1:
                target_folder = next((
                    x for x
                    in weekly_folders
                    if x.week_number == week_num
                ), None)
            if not target_folder:
                raise NoInstrumentError(
                    f"{self.ticker}.{self.exchange}: "
                    f"No weekly folder for ticker {weekly_ticker} have been found"
                )
        elif isinstance(weekly_ticker, bool) and weekly_ticker is True:
            if not self.weekly_commons:
                raise NoInstrumentError(
                    f"{self.ticker}.{self.exchange}: "
                    f"No weekly_common folder for ticker {weekly_ticker} have been found"
                )
            elif len(self.weekly_commons) == 1:
                target_folder = next((
                    x for x
                    in self.weekly_commons[0].weekly_folders
                    if x.week_number == week_num
                ), None)
            else: # more than one weekly_commons
                self.logger.error(
                    "More than one suitable folders have been found, cannot decide:"
                )
                self.logger.error(
                    pformat([
                        (
                            x.templates.get('ticker'),
                            x.payload.get('name')
                        ) for x
                        in self.weekly_commons
                    ])
                )
                raise ExpirationError(
                    f"{self.ticker}.{self.exchange}: "
                    "More than one weekly folder have been found, specify weekly_ticker"
                )
        else:
            target_folder = self
        return target_folder

    def add_payload(
            self,
            payload: dict,
            skip_if_exists: bool = True,
            overwrite_old: bool = False,
            weekly_ticker: Union[bool, str] = None
        ):
        # basic validation
        if not payload.get('expiry') \
            or not payload.get('maturityDate') \
            or not payload.get('strikePrices'):

            self.logger.error("bad data")
            return {}
        
        # get expiration date
        exp_date = self.sdb.sdb_to_date(payload['expiry'])
        maturity = format_maturity(payload['maturityDate'])
        expiration_day = payload['expiry']['day']
        week_num = int((expiration_day - 1)/7) + 1
        if weekly_ticker is None:
            if payload.get('ticker') and payload['ticker'] != self.ticker:
                weekly_ticker = payload.pop('ticker')
            elif payload.get('is_weekly_') is not None:
                weekly_ticker = payload.pop('is_weekly_')
            else:
                # It's monthly expiration
                target_folder = self._set_target_folder(0, weekly_ticker)
                pass
        if weekly_ticker is not None:
            target_folder = self._set_target_folder(week_num, weekly_ticker)

        

        # check if expiration already exists
        existing_exp, series = target_folder.find_expiration(exp_date, maturity)
        if existing_exp:
            if skip_if_exists:
                target_folder.skipped.add(exp_date.isoformat())
                return {}
            current_version = asyncio.run(self.sdb.get(existing_exp.instrument['_id']))
            reference = deepcopy(current_version)
            if overwrite_old:
                payload.update({key: val for key, val in current_version.items() if key[0] == '_'})
                update_contract = OptionExpiration(
                    target_folder,
                    payload=payload
                )
            else:
                current_version.update(payload)
                update_contract = OptionExpiration(
                    target_folder,
                    payload=current_version
                )
            update_contract.set_la_lt()
            diff: dict = DeepDiff(reference, update_contract.instrument)
            for change in diff.keys():
                if isinstance(diff[change], dict):
                    diff[change] = {key: val for key, val in diff[change].items() if 'strikePrices' not in key} 
            if update_contract.new_strikes.get('CALL') or update_contract.new_strikes.get('PUT'):
                diff.update({
                    'new_strikes': update_contract.new_strikes
                })
            if diff:
                self.logger.info(
                    f'{target_folder.ticker}.{target_folder.exchange} {maturity}: '
                    'following changes have been made:'
                )
                self.logger.info(pformat(diff))
                target_folder.update_expirations.append(update_contract)
            return {'updated': update_contract.get_expiration()[1], 'diff': diff}

        # expiration does not exist, add new one
        if self.allowed_expirations:
            if self.exchange == 'CBOE':
                symbolic = self._date_to_symbolic(exp_date.isoformat())
            else:
                symbolic = self._date_to_symbolic(exp_date.strftime('%Y-%m'))

            if not (
                    exp_date.isoformat() in self.allowed_expirations
                    or symbolic in self.allowed_expirations
                ):
                if not self.silent:
                    self.logger.info(
                        f"Allowed expirations are set and "
                        f"{exp_date.isoformat()} or {symbolic} is not in it"
                    )
                return {}
            
        new_contract = OptionExpiration(
            target_folder,
            payload=payload
        )
        new_contract.set_la_lt()
        target_folder.new_expirations.append(new_contract)
        return {'created': new_contract.get_expiration()[1]}

    def add(
            self,
            exp_date: str,
            strikes: dict,
            maturity: str = None,
            underlying: str = None,
            skip_if_exists: bool = True,
            weekly_ticker: Union[bool, str] = None,
            **kwargs
        ) -> dict:
        week_num = None
        if isinstance(weekly_ticker, bool) and weekly_ticker is True:
            week_num = int((int(exp_date.split('-')[-1]) - 1)/7) + 1
        
        target_folder = self._set_target_folder(week_num, weekly_ticker)
        existing_exp, series = self.find_expiration(
            exp_date,
            week_num=week_num,
            ticker=weekly_ticker
        )
        if existing_exp and skip_if_exists:
            self.skipped.add(exp_date)
            return {}
        elif existing_exp:
            if not maturity and existing_exp.instrument.get('maturityDate'):
                maturity = format_maturity(existing_exp.instrument['maturityDate'])
            elif not maturity and self.exchange != 'CBOE':
                self.logger.error(
                    f"{target_folder.ticker}.{target_folder.exchange} {exp_date}: "
                    "maturity is not set, not added"
                )
                return {}
            current_version = asyncio.run(self.sdb.get(existing_exp.instrument['_id']))
            reference = deepcopy(current_version)
            update_contract = OptionExpiration(
                target_folder,
                exp_date,
                strikes=strikes,
                maturity=maturity,
                payload=current_version,
                expiration_underlying=underlying
            )
            diff = update_contract.build_expiration_dict(strikes, reference=reference, **kwargs)
            for change in diff.keys():
                if isinstance(diff[change], dict):
                    diff[change] = {key: val for key, val in diff[change].items() if 'strikePrices' not in key} 
            if update_contract.new_strikes.get('CALL') or update_contract.new_strikes.get('PUT'):
                diff.update({
                    'new_strikes': update_contract.new_strikes
                })
            if diff:
                if not self.silent:
                    self.logger.info(
                        f'{target_folder.ticker}.{target_folder.exchange} {exp_date}: '
                        'following changes have been made:'
                    )
                    self.logger.info(pformat(diff))
                target_folder.update_expirations.append(update_contract)
                return {'updated': update_contract.get_expiration(), 'diff': diff}
            elif not self.silent:
                self.logger.info(f'No new data for existing expiration: {exp_date}')
                return {}
            else:
                return {}

        if self.allowed_expirations:
            if self.exchange == 'CBOE':
                symbolic = self._date_to_symbolic(exp_date)
            else:
                symbolic = self._date_to_symbolic(exp_date[:7])

            if not (
                    exp_date in self.allowed_expirations
                    or symbolic in self.allowed_expirations
                ):
                if not self.silent:
                    self.logger.info(
                        f"Allowed expirations are set and "
                        f"{exp_date} or {symbolic} is not in it"
                    )
                return {}
            
        new_contract = OptionExpiration(
            target_folder,
            exp_date,
            strikes=strikes,
            maturity=maturity,
            payload={},
            expiration_underlying=underlying
        )
        target_folder.new_expirations.append(new_contract)
        return {'created': new_contract.get_expiration()}

    def refresh_strikes(
            self,
            expiration: str,
            strikes: dict,
            maturity: str = None,
            week_num: int = None,
            ticker: str = None,
            safe: bool = True,
            disable: bool = False,
            reload_cache: bool = False,
            consider_demo: bool = True
        ) -> dict:
        refreshed = {}
        result = {'updated': {}, 'added': {}, 'removed': {}, 'preserved': {}}
        target_expiration, target_folder = self.find_expiration(expiration, maturity, week_num, ticker)
        if not target_expiration:
            self.logger.error(f"Expiration {expiration} is not found in existing expirations")
            return result
        if disable:
            added, removed = target_expiration.refresh_tradable_strikes(
                strikes=strikes,
                safe=safe
            )
            preserved = {}
        else:
            added, removed, preserved = target_expiration.refresh_strikes(
                strikes=strikes,
                safe=safe,
                reload_cache=reload_cache,
                consider_demo=consider_demo
            )
        # TEMPORARY FOR UPDATE ALL ISINS
        if next((
            x for x in [added, removed, preserved]
            if x and (x.get('PUT') or x.get('CALL'))
        ), None) and not removed.get('not_updated'):
            target_folder.update_expirations.append(target_expiration)
            result.update(
                {
                    'updated': target_expiration.get_expiration(),
                    'added': added,
                    'removed': removed,
                    'preserved': preserved
                }
            )
        else:
            result.update({
                'updated': target_expiration.get_expiration()
            })
            if not self.silent:
                self.logger.info(
                    f"No strikes have been updated for "
                    f"{target_folder.ticker}.{target_folder.exchange}: {expiration}"
                )
        return result
    
    def set_underlying(self, symbol_id) -> bool:
        # check if symbol exists in sdb
        if asyncio.run(self.sdb.get(symbol_id)):
            self.underlying_dict = {
                        'id': symbol_id,
                        'type': 'symbolId'
                    }
            return True
        self.logger.warning(
            f'{symbol_id} does not exist in sdb! Underlying id is not set'
        )
        return False

    def create_weeklies(
            self,
            templates: dict = None,
            common_name: str = 'Weekly',
            recreate: bool = False,
            week_number: int = 0
        ):
        if self.week_number:
            self.logger.error(f"{self.ticker}.{self.exchange}: Cannot create weeklies inside weekly folder")
            return None
        if not templates:
            templates = self.weekly_templates
        ticker_template = templates.get('ticker')
        # generally, one week differs from another by ticker, but it's not always the case
        if not ticker_template:
            self.logger.warning('No weekly folders have been created')
            return None
        # if ticker is the same for all weeks we try to find differencies in overrides
        if not [key for key, val in templates.items() if '$' in val or '@' in val]:
            self.logger.warning('No weekly folders have been created')
            return None
        weekly_common = next((
            x for x
            in self.weekly_commons
            if x.payload['name'] == common_name
            or x.templates.get('ticker') == ticker_template
        ), None)
        if not weekly_common:
            weekly_common = WeeklyCommon(self, common_name=common_name, templates=templates)
            self.weekly_commons.append(weekly_common)
        weekly_common.mk_weeklies(recreate, week_number)
        
        return weekly_common

    def recreate_folder(
            self,
            week_number: int = None,
            weekly_templates: dict = None,
            common_name: str = 'Weekly'
        ):
        '''
        use week_number = 0 to recreate all weeklies
        '''
        if week_number is None:
            self.instrument = self.create_series_dict()
            self.instrument.update({
                key: val for key, val in self.reference.items() if key[0] == '_'
            })
            return None
        elif week_number not in range(6):
            self.logger.error(
                'week number must be specified as a number between 0 and 5 (0 to recreate all week folders)'
            )
            return None
        if weekly_templates is None:
            self.logger.warning(
                'weekly template is not specified (use $ to identify week as a number, @ as a letter)'
            )
            return None
        self.create_weeklies(
            weekly_templates,
            common_name=common_name,
            recreate=True,
            week_number=week_number
        )
        return None

    def post_to_sdb(self, dry_run=True) -> dict:
        """Sends to SymbolDB absent expirations.
        If symbol doesn't exist, creates it."""

        # here and after the condition "if not {some_entry}.get('_id')"
        # tells if given entry exists in sdb, so it could be updated,
        # or we could take its _id to build a path for contracts,
        # or it doesn't exist yet and should be created as new

        # Create folder if need
        try_again_series = False
        if self.week_number:
            return {'error': 'Call post_to_sdb() method only from main series instance (not weeklies)'}
        if not self.instrument.get('_id'):
            self.create(dry_run)

        # Create common folder for weekly subfolders
        for wc in self.weekly_commons:
            if not wc.payload.get('_id'):
                wc.payload['path'] = deepcopy(self.instrument['path'])
                wc.create(dry_run)
            else:
                wc_diff = DeepDiff(wc.reference, wc.payload)
                if wc_diff:
                    wc.update(wc_diff, dry_run)
            # Create weekly subfolders
            for wf in wc.weekly_folders:
                if not wf.instrument.get('_id'):
                    wf.instrument['path'] = deepcopy(wc.payload['path'])
                    wf.create(dry_run)
                else:
                    wf_diff = DeepDiff(wf.reference, wf.instrument)
                    if wf_diff:
                        wf.update(wf_diff, dry_run)

        # Prepare new expirations: throw warning if there is no underlying future on "OPTION ON FUTURE" type,
        # replace week numbers in paths with real ids
        targets = [self]
        report = {}
        for wc in self.weekly_commons:
            targets.extend(wc.weekly_folders)
        for target in targets:
            report.update({f'{target.ticker}.{target.exchange}': {}})
            create_result = ''
            update_result = ''
            # Check if series folder has been changed
            diff = DeepDiff(target.reference, target.instrument)
            if diff:
                target.update(diff, dry_run)
            else:
                self.logger.info(f"No changes were made for {target.ticker}.{target.exchange}.*")
            for new in target.new_expirations:
                if target.option_type == 'OPTION ON FUTURE':
                    if not new.instrument.get('underlyingId', {}).get('id'):
                        self.logger.warning(f"Underlying for {new.get_expiration()[1]} is not set!")
                new.instrument['path'] = deepcopy(target.instrument['path'])
            if dry_run and target.new_expirations:
                print(f"Dry run, new expirations to create:")
                pp([x.get_expiration()[1] for x in target.new_expirations])
            if dry_run and target.update_expirations:
                print(f"Dry run, expirations to update:")
                pp([x.get_expiration()[1] for x in target.update_expirations])
            if dry_run:
                continue

            if target.new_expirations:
                create_result = asyncio.run(self.sdb.batch_create(
                    input_data=[x.instrument for x in target.new_expirations]
                ))
            else:
                create_result = 'Nothing to do'
            if target.update_expirations:
                update_result = asyncio.run(self.sdb.batch_update(
                    input_data=[x.instrument for x in target.update_expirations]
                ))
            else:
                update_result = 'Nothing to do'
            if create_result == 'Nothing to do':
                pass
            elif create_result:
                if isinstance(create_result, str):
                    create_result = json.loads(create_result)
                if create_result.get('symbolId', {}).get('message') == 'already exist':
                    for key, val in create_result['symbolId']['description'].items():
                        if isinstance(val, list):
                            create_result['symbolId']['description'][key] = [
                                '.'.join(val[0].split('.')[:-1]),
                                '...'
                            ]
                self.logger.error(
                    f'problems with creating new expirations: {pformat(create_result)}'
                )
                report[f'{target.ticker}.{target.exchange}'].update({
                    'create_error': create_result.get('description')
                })
            else:
                report[f'{target.ticker}.{target.exchange}'].update({
                    'created': [x.get_expiration()[1] for x in target.new_expirations]
                })
            if update_result == 'Nothing to do':
                pass
            elif update_result:
                if isinstance(update_result, str):
                    update_result = json.loads(update_result)
                self.logger.error(
                    f'problems with updating expirations: {pformat(update_result)}'
                )
                report[f'{target.ticker}.{target.exchange}'].update({
                    'update_error': update_result.get('description')
                })
            else:
                report[f'{target.ticker}.{target.exchange}'].update({
                    'updated': [x.get_expiration()[1] for x in target.update_expirations]
                })
        if report and try_again_series:
            response = asyncio.run(self.sdb.update(self.instrument))
            if response.get('message'):
                self.logger.error(f'instrument {self.ticker} is not updated:')
                self.logger.error(pformat(response))

        if not dry_run:
            self.clean_up_times()
        if [x for x, y in report.items() if y.get('created')] and self.reload_cache:
            self.logger.info('All good, reloading tree cache...')
            self.force_tree_reload()
        return report

class OptionExpiration(Instrument):
    def __init__(
            self,
            option: Option,
            expiration_date: Union[str, dt.date, dt.datetime] = '',
            strikes: dict = None,
            maturity: str = '',
            payload: dict = None,
            expiration_underlying: str = None
        ):
        self.env = option.env
        self.option = option
        super().__init__(
            instrument_type=option.instrument_type,
            env=option.env,
            sdb=option.sdb,
            sdbadds=option.sdbadds,
            silent=option.silent
        )
        self.ticker = option.ticker
        self.exchange = option.exchange
        self.option_type = option.option_type
        self.new_strikes = {
            'CALL': [],
            'PUT': []
        }
        if payload is None:
            payload = {}
        self.instrument: dict = payload
        if expiration_date:
            if isinstance(expiration_date, str):
                try:
                    self.expiration = dt.date.fromisoformat(expiration_date)
                except ValueError:
                    if not self.instrument:
                        raise ExpirationError(
                            f'Invalid expiration date format: {expiration_date}'
                        )
                    self.expiration = self.sdb.sdb_to_date(self.instrument['expiry'])
            elif isinstance(expiration_date, dt.date):
                self.expiration = expiration_date
            elif isinstance(expiration_date, dt.datetime):
                self.expiration = expiration_date.date()
        elif self.instrument.get('expiry'):
            self.expiration = self.sdb.sdb_to_date(self.instrument['expiry'])
        else:
            raise ExpirationError(f'Invalid expiration date format: {expiration_date}')
        if maturity:
            self.maturity = format_maturity(maturity)
        elif self.instrument.get('maturityDate'):
            self.maturity = format_maturity(self.instrument['maturityDate'])
        else:
            self.maturity = self.expiration.strftime('%Y-%m')
        if not self.maturity:
            raise ExpirationError(f'Cannot format maturity: {maturity}')
        self.expiration_underlying = {}
        self.compiled_parent = asyncio.run(self.sdbadds.build_inheritance(
            [self.option.compiled_parent, self.option.instrument],
            include_self=True
        ))
        if not self.instrument:
            if not strikes:
                raise ExpirationError('Strikes dict is required')
            self.build_expiration_dict(strikes)
        if not self.instrument.get('path'):
            self.instrument['path'] = deepcopy(self.option.instrument['path'])
            if not self.option.instrument.get('_id'):
                self.instrument['path'].append('<<new series id>>')
        

        self.instrument.setdefault('strikePrices', {})
        self.instrument['strikePrices'].setdefault('PUT', [])
        self.instrument['strikePrices'].setdefault('CALL', [])        
        if strikes:
            self.add_strikes(strikes)
        if self.option_type == 'OPTION ON FUTURE' and expiration_underlying:
            self.set_underlying_future(expiration_underlying)

    def __repr__(self):
        week_indication = "Monthly" if not self.option.week_number else f"Week {self.option.week_number}"
        return (
            f"OptionExpiration({self.ticker}.{self.exchange}.{self.maturity_to_str()}, "
            f"{self.expiration.isoformat()}, {week_indication})"
        )

    def __eq__(self, other):
        if self.expiration == other.expiration and self.ticker == other.ticker and self.exchange == other.exchange:
            return True
        else:
            return False

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def maturity_to_str(self):
        if self.instrument.get('maturityDate'):
            year = self.instrument['maturityDate']['year']
            month = self.instrument['maturityDate']['month']
            if 'day' in self.instrument['maturityDate']:
                day = self.instrument['maturityDate']['day']
                return f'{day}{Months(month).name}{year}'
            return f'{Months(month).name}{year}'
        elif self.maturity:
            if re.match(r'\d{4}-\d{2}$', self.maturity):
                return f"{Months(int(self.maturity[-2:])).name}{self.maturity[:4]}"
            if re.match(r'\d{4}-\d{2}-\d{2}$', self.maturity):
                return f"{int(self.maturity[-2:])}{Months(int(self.maturity[5:7])).name}{self.maturity[:4]}"

    def _generate_exante_id(self, strike: float, side: str):
        if re.search(r'\.0$', str(strike)):
            strike = int(strike)
        underline = str(strike).replace('.', '_')
        return f"{self.ticker}.{self.exchange}.{self.maturity_to_str()}.{side[0]}{underline}"

    def _generate_option_group_id(self):
        return f"{self.ticker}.{self.exchange}.{self.maturity_to_str()}"

    def add_strikes(self, strikes: dict):
        strike_prices = {
            'PUT': [],
            'CALL': []
        }
        update = {}
        if strikes['PUT'] and isinstance(list(strikes['PUT'])[0], (int, float, str)) \
        and strikes['CALL'] and isinstance(list(strikes['CALL'])[0], (int, float, str)):
            strikes = self.build_strikes(strikes)
        for side in ['PUT', 'CALL']:
            strike_prices[side] = {x['strikePrice'] for x in strikes[side]}
            update[side] = strike_prices[side] - {x['strikePrice'] for x in self.instrument['strikePrices'][side]}
            self.instrument['strikePrices'][side].extend([x for x in strikes[side] if x.get('strikePrice') in update[side]])
            self.instrument['strikePrices'][side] = sorted(self.instrument['strikePrices'][side], key=lambda sp: sp['strikePrice'])
        self.new_strikes.update({
            'PUT': list(update['PUT']),
            'CALL': list(update['CALL'])
        })
    
    def refresh_strikes(
            self,
            strikes: dict,
            safe: bool = True,
            reload_cache: bool = False,
            consider_demo: bool = True
        ):
        """
        strikes is a dict of active strikes that should stay and the rest should be removed
        with the exception of used symbols, thay also should stay

        updating strikes process is divided for two separate functions because adding new
        strikes is fast and could be done on demand. Removing non-tradable strikes requires
        to check their presence in used symbols and it is slow, so it may be done automatically on schedule
        """
        self.used_symbols = asyncio.run(self.sdbadds.load_used_symbols(reload_cache, consider_demo))
        MIN_STRIKES_ACCEPTABLE = 7
        MIN_INTERSECTION = (
            len(self.instrument['strikePrices']['CALL']) + 
            len(self.instrument['strikePrices']['PUT']) - 16
        )
        strike_prices = {
            'PUT': set(),
            'CALL': set()
        }
        preserved = {
            'PUT': set(),
            'CALL': set()
        }
        added = {
            'PUT': set(),
            'CALL': set()
        }
        removed = {
            'PUT': set(),
            'CALL': set()
        }
        cant_touch_this = {
            'PUT': set(),
            'CALL': set()
        }
        if 'PUT' not in strikes or 'CALL' not in strikes:
            self.logger.error("refresh strikes: Bad data")
            return None, None, None

        if strikes['PUT'] and (
            isinstance(strikes['PUT'], set) or isinstance(strikes['PUT'][0], (float, str))
        ) \
        and strikes['CALL'] and (
            isinstance(strikes['CALL'], set) or isinstance(strikes['CALL'][0], (float, str))
        ):
            strikes = self.build_strikes(strikes)

        if strikes['PUT'] \
            and isinstance(strikes['PUT'][0], dict) \
            and strikes['CALL'] \
            and isinstance(strikes['CALL'][0], dict):

            strike_prices['PUT'] = {x['strikePrice'] for x in strikes['PUT']}
            strike_prices['CALL'] = {x['strikePrice'] for x in strikes['CALL']}
        else:
            self.logger.error("refresh strikes: Bad data")
            return None, None, None

        
        if safe and (not strike_prices.get('PUT')
                or not strike_prices.get('CALL')
                or len(strike_prices['PUT']) + len(strike_prices['CALL']) < MIN_STRIKES_ACCEPTABLE
                or len(strike_prices['PUT'].intersection({x['strikePrice'] for x in self.instrument['strikePrices']['PUT']})) +
                len(strike_prices['CALL'].intersection({x['strikePrice'] for x in self.instrument['strikePrices']['CALL']})) < MIN_INTERSECTION):
            self.logger.warning(
                f"{self.ticker}.{self.exchange} {self.maturity}: "
                "Provided list and existing strikes do not look similar enough, no strikes removed"
            )
            return None, {'not_updated': True}, None
        cant_touch_this['PUT'] = [
            x for x 
            in self.instrument['strikePrices']['PUT']
            if self._generate_exante_id(x['strikePrice'], 'PUT') in self.used_symbols
        ]
        for side in ['PUT', 'CALL']:
            # can't_touch - new_strikes
            preserved[side] = {
                x['strikePrice'] for x in cant_touch_this[side]
            } - strike_prices[side]
            
            # self_strikes - new_strikes - can't_touch
            removed[side] = {
                x['strikePrice'] for x in self.instrument['strikePrices'][side]
            } - strike_prices[side] - {
                x['strikePrice'] for x in cant_touch_this[side]
            }

            # new_strikes - self_strikes
            added[side] = strike_prices[side] - {x['strikePrice'] for x in self.instrument['strikePrices'][side]}
            strikes[side].extend([
                x for x
                in cant_touch_this[side]
                if x['strikePrice'] not in [
                    y['strikePrice'] for y in strikes[side]
                ]
            ])
            self.instrument['strikePrices'][side] = sorted(strikes[side], key=lambda sp: sp['strikePrice'])

        if preserved.get('PUT') or preserved.get('CALL'):
            self.logger.info(
                f"{self.ticker}.{self.exchange} {self.expiration}: "
                f"cannot remove following strikes as they are present in used symbols {preserved}"
            )
        return added, removed, preserved
    
    def refresh_tradable_strikes(
            self,
            strikes: dict,
            safe: bool = True,
        ):
        """
        strikes is a dict of parsed strikes. Let's say we mark non-parsed already existed strikes as
        isAvailable == False, so we don't have to look in used symbols 
        """
        MIN_STRIKES_ACCEPTABLE = 7
        MIN_INTERSECTION = (
            len(self.instrument['strikePrices']['CALL']) + 
            len(self.instrument['strikePrices']['PUT']) - 16
        )
        strike_prices = {
            'PUT': set(),
            'CALL': set()
        }
        added = {
            'PUT': set(),
            'CALL': set()
        }
        disabled = {
            'PUT': set(),
            'CALL': set()
        }
        if 'PUT' not in strikes or 'CALL' not in strikes:
            self.logger.error("refresh strikes: Bad data")
            return None, None

        if strikes['PUT'] and (
            isinstance(strikes['PUT'], set) or isinstance(strikes['PUT'][0], (float, str))
        ) \
        and strikes['CALL'] and (
            isinstance(strikes['CALL'], set) or isinstance(strikes['CALL'][0], (float, str))
        ):
            strikes = self.build_strikes(strikes)

        if strikes['PUT'] \
            and isinstance(strikes['PUT'][0], dict) \
            and strikes['CALL'] \
            and isinstance(strikes['CALL'][0], dict):

            strike_prices['PUT'] = {x['strikePrice'] for x in strikes['PUT']}
            strike_prices['CALL'] = {x['strikePrice'] for x in strikes['CALL']}
        else:
            self.logger.error("refresh strikes: Bad data")
            return None, None

        
        if safe and (not strike_prices.get('PUT')
                or not strike_prices.get('CALL')
                or len(strike_prices['PUT']) + len(strike_prices['CALL']) < MIN_STRIKES_ACCEPTABLE
                or len(strike_prices['PUT'].intersection({x['strikePrice'] for x in self.instrument['strikePrices']['PUT']})) +
                len(strike_prices['CALL'].intersection({x['strikePrice'] for x in self.instrument['strikePrices']['CALL']})) < MIN_INTERSECTION):
            self.logger.warning(
                f"{self.ticker}.{self.exchange} {self.maturity}: "
                "Provided list and existing strikes do not look similar enough, no strikes removed"
            )
            return None, None
        for side in ['PUT', 'CALL']:
            added[side] = strike_prices[side] - {x['strikePrice'] for x in self.instrument['strikePrices'][side]}
            disabled[side] = {
                x['strikePrice'] for x
                in self.instrument['strikePrices'][side]
                if x.get('isAvailable') is not False # already disabled
            } - strike_prices[side]
        self.enable_strikes(disabled, enable=False)
        self.enable_strikes(added, enable=True)
        return added, disabled


    def build_strikes(self, strikes) -> Dict[str, list]:
        return {
            'CALL': [
                {
                    'strikePrice': call,
                    'isAvailable': True
                } for call in strikes.get('CALL')
            ],
            'PUT': [
                {
                    'strikePrice': put,
                    'isAvailable': True
                } for put in strikes.get('PUT')
            ]
        }
    
    def enable_strikes(self, strikes: dict, enable: bool = True):
        for side in ['PUT', 'CALL']:
            for strike in strikes.get(side):
                strike_num = next((
                    num for num, x in enumerate(
                        self.instrument.get('strikePrices',{}).get(side)
                    )
                    if x['strikePrice'] == strike
                ), None)
                if enable:
                    if strike_num is None:
                        if not self.instrument.get('strikePrices',{}).get(side):
                            self.instrument.update({
                                'strikePrices': {
                                    side: []
                                }
                            })
                        self.instrument['strikePrices'][side].append({
                            'strikePrice': strike,
                            'isAvailable': True
                        })
                    else:
                        self.instrument['strikePrices'][side][strike_num].update({
                            'isAvailable': True
                        })
                elif strike_num is not None: # do nothing if strike does not exist
                    self.instrument['strikePrices'][side][strike_num].update({
                        'isAvailable': False
                    })
            self.instrument['strikePrices'][side] = sorted(
                self.instrument['strikePrices'][side],
                key=lambda s: s['strikePrice']
            )

    def set_underlying_future(self, symbol_id: str) -> bool:
        # check if symbol exists in sdb
        underlying = asyncio.run(self.sdb.get(symbol_id, fields=['expiry', 'expiryTime', 'name', 'path', '_id']))
        if not underlying:
            self.logger.warning(f'{symbol_id} does not exist in sdb! Underlying id is not set')
            return False
        if dt.date.fromisoformat(underlying['expiryTime'].split('T')[0]) == self.expiration\
            and not self.option.instrument.get('expiry', {}).get('time'):
            for parent in reversed(underlying['path']):
                set_time = asyncio.run(self.sdb.get(parent)).get('expiry', {}).get('time')
                if set_time:
                    self.option.instrument['expiry'] = {'time': set_time}
                    self.logger.info(
                        f"{self.option.ticker}.{self.exchange} expiration time has been set same as underlying: {set_time}"
                    )
                    break
        elif dt.date.fromisoformat(underlying['expiryTime'].split('T')[0]) < self.expiration:
            self.logger.warning(
                f"Cannot set {symbol_id} as underlying for {self._generate_option_group_id()} as it expires earlier"
            )
            return False
        self.instrument['underlyingId'] = {
                    'id': symbol_id,
                    'type': 'symbolId'
                }
        return True
    
    def build_expiration_dict(
            self,
            strikes: dict,
            reference: dict = None,
            **kwargs
        ) -> dict:
        if not self.instrument:
            self.instrument = {
                'isAbstract': False,
                'name': None,
                'maturityDate': {},
                'expiry': {},
                'strikePrices': self.build_strikes(strikes)
            }
            reference = {}
        elif not reference:
            reference = deepcopy(self.instrument)
        # set expiration and maturity
        if self.exchange == 'CBOE':
            self.instrument.update({
                'maturityDate': {
                    'day': self.expiration.day,
                    'month': self.expiration.month,
                    'year': self.expiration.year
                }
            })
            if self.ticker in EXPIRY_BEFORE_MATURITY:
                actual_expiry = self.expiration - dt.timedelta(days=1)
                self.instrument['expiry'] = {
                    'year': actual_expiry.year,
                    'month': actual_expiry.month,
                    'day': actual_expiry.day
                }
            else:
                self.instrument['expiry'] = self.instrument['maturityDate']
        elif self.exchange == 'FORTS':
            self.instrument.update({
                'maturityDate': {
                    'year': self.expiration.year,
                    'month': self.expiration.month,
                    'day': self.expiration.day
                },
                'expiry': {
                    'year': self.expiration.year,
                    'month': self.expiration.month,
                    'day': self.expiration.day
                }
            })
        else:
            self.instrument.update({
                'maturityDate': {
                    'month': int(self.maturity.split('-')[1]),
                    'year': int(self.maturity.split('-')[0])
                },
                'expiry': {
                    'day': self.expiration.day,
                    'month': self.expiration.month,
                    'year': self.expiration.year
                }
            })
            if len(self.maturity.split('-')) == 3:
                self.instrument['maturityDate'].update({
                    'day': int(self.maturity.split('-')[2])
                })
        if not self.instrument.get('name'):
            self.instrument.update({
                'name': self.maturity
            })
        if self.instrument.get('isTrading') is not None:
            self.instrument.pop('isTrading')
        self.set_la_lt()
        if self.option_type == 'OPTION ON FUTURE' and self.expiration_underlying:
            self.instrument['underlyingId'] = self.expiration_underlying
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))
        # new strikes in DeepDiff output is a big mess, so we clean up and add them again in more acceptable way
        diff: dict = DeepDiff(reference, self.instrument)
        for change in diff.keys():
            if isinstance(diff[change], dict):
                diff[change] = {key: val for key, val in diff[change].items() if 'strikePrices' not in key} 
        if self.new_strikes.get('CALL') or self.new_strikes.get('PUT'):
            diff.update({
                'new_strikes': self.new_strikes
            })
        return diff

    def set_la_lt(self):
        if self.option.set_la:
            self.instrument['lastAvailable'] = self.sdb.date_to_sdb(
                self.sdb.sdb_to_date(self.instrument['expiry']) + dt.timedelta(days=3)
            )
            if isinstance(self.option.set_la, str):
                self.instrument['lastAvailable']['time'] = self.option.set_la
            

        if self.option.set_lt:
            self.instrument['lastTrading'] = deepcopy(self.instrument['expiry'])
            if isinstance(self.option.set_lt, str):
                self.instrument['lastTrading']['time'] = self.option.set_lt

    def get_expiration(self):
        exante_id = self._generate_option_group_id()
        return [self.instrument, exante_id]

class WeeklyCommon(Option):
    def __init__(
            self,
            option: Option,
            payload: dict = None,
            uuid: str = None,
            common_name: str = 'Weekly',
            templates: dict = None
        ):
        self.option = option
        self.bo = option.bo
        self.sdb = option.sdb
        self.sdbadds = option.sdbadds
        self.common_name = common_name
        self.weekly_folders = []
        if uuid:
            payload = asyncio.run(self.sdb.get(uuid))
            self.common_name = payload.get('name')
        if not payload:
            payload = {
                'isAbstract': True,
                'path': deepcopy(self.option.instrument['path']),
                'name': common_name
            }
        self.payload = payload
        self.reference = deepcopy(self.payload)
        self.templates = templates
        if not payload.get('_id'):
            if option.instrument.get('_id'):
                self.create()
            pass
        else:
            self.weekly_folders = self.__find_weekly_folders()
            if self.weekly_folders and not self.templates:
                self.templates = {
                    'ticker': re.sub(r'[12345]', '$', self.weekly_folders[0].ticker)
                }
            if len(self.weekly_folders) < 4 and self.templates:
                self.mk_weeklies()

    def __repr__(self):
        return f"WeeklyCommon({self.option.ticker}.{self.option.exchange}, {self.common_name=})"

    def __find_weekly_folders(self):
        weekly_folders: list[Option] = []
        existing_tickers = [
            x['ticker'] for x
            in self.option.series_tree
            if x.get('ticker')
            and x['path'][:-1] == self.payload['path']
            and x['isAbstract']
        ]
        for x in existing_tickers:
            if x and re.search(r'[12345]', x):
                try:
                    weekly_folder = Option(
                        ticker=x,
                        exchange=self.option.exchange,
                        parent_folder=self.payload,
                        reload_cache=False,
                        week_number=int(re.search(r'[12345]', x).group()),
                        parent_tree=self.option.series_tree,
                        bo=self.bo,
                        sdb=self.sdb,
                        sdbadds=self.sdbadds
                    )
                    weekly_folders.append(weekly_folder)
                except NoInstrumentError:
                    self.logger.warning(
                        f"Weekly folder {x}.{self.exchange} is not found, "
                        "check if folder name and ticker are the same"
                    )
        return weekly_folders

        
    def create(self, dry_run: bool = False):
        if not self.payload.get('path'):
            self.payload['path'] = deepcopy(self.option.instrument['path'])
        if dry_run:
            print(f"Dry run. New folder {self.payload['name']} to create:")
            pp(self.payload)
            self.payload['path'].append(
                f"<<new {self.option.ticker}.{self.option.exchange} {self.payload['name']} id>>"
            )
        elif self.option.instrument.get('_id'):
            create = asyncio.run(self.sdb.create(self.payload))
            if not create.get('_id'):
                self.option.logger.error(pformat(create))
                raise RuntimeError(
                    f"Can not create common weekly folder {self.option.ticker}: {create['message']}"
                )
            self.option.logger.debug(f'Result: {pformat(create)}')
            self.payload['_id'] = create['_id']
            self.payload['_rev'] = create['_rev']
            self.payload['path'].append(self.payload['_id'])
            new_record = pd.DataFrame([{
                key: val for key, val
                in self.payload.items()
                if key in self.tree_df.columns
            }], index=[self.payload['_id']])
            pd.concat([self.tree_df, new_record])
            self.tree_df.replace({np.nan: None})
            # self.option.tree.append(self.payload)

    def update(self, diff: dict, dry_run: bool = False):
        self.option.logger.info(
            f"{self.option.ticker}.{self.option.exchange}, {self.payload['name']}: "
            "following changes have been made:"
        )
        self.option.logger.info(pformat(diff))
        if dry_run:
            print(f"Dry run. The folder {self.payload['name']} to update:")
            pp(diff)
            return {}
        response = asyncio.run(self.sdb.update(self.payload))
        if response.get('message'):
            self.option.logger.warning(
                f"{self.option.ticker}.{self.option.exchange}, {self.payload['name']}: "
                "folder is not updated"
            )
            self.option.logger.warning(pformat(response))

    def mk_weeklies(self, recreate: bool = False, week_number: int = 0):
        '''
        · If week identifier is a number replace it with "$"
        (e.g. for weeklies ZW1, ZW2, ..., ZW5 type ZW$
        and for weeklies R1E, R2E, ..., R5E type R$E)
        · If week identifier is a letter replace it with "@"
        (e.g for weeklies Si/A, Si/B, ..., Si/E type Si/@):
        '''
        endings = ['', 'st', 'nd', 'rd', 'th', 'th']
        letters = ' ABCDE'
        for num in range(1, 6):
            # in case if we want to create only one weekly_folder
            if week_number and num != week_number:
                continue
            existing = next((
                ex_num for ex_num, x
                in enumerate(self.weekly_folders)
                if x.week_number == num
            ), None)
            shortname = self.option.shortname if self.option.shortname else self.option.instrument.get('description')
            shortname = re.sub(r'( )?[Oo]ptions( )?(on )?', '', shortname)
            ticker_template = self.templates.get('ticker')
            if not ticker_template:
                self.option.logger.warning('No weekly ticker template have been provided, weekly folders are not created')
                return None
            if '$' in ticker_template:
                weekly_ticker = weekly_name = f"{ticker_template.replace('$', str(num))}"
            elif '@' in ticker_template:
                weekly_ticker = weekly_name = f"{ticker_template.replace('@', letters[num])}"
            else:
                weekly_name = f"{num}{endings[num]} Week"
            new_weekly = Option(
                ticker=weekly_ticker,
                exchange=self.option.exchange,
                shortname=shortname,
                parent_folder=self.payload,
                week_number=num,
                recreate=recreate,
                reload_cache=False,
                bo=self.bo,
                sdb=self.sdb,
                sdbadds=self.sdbadds

            )
            new_weekly.instrument['name'] = weekly_name
            if not new_weekly.instrument.get('_id') or recreate:
                if [x for x in self.templates if x != 'ticker']:
                    feed_providers = [
                        x[0] for x
                        in asyncio.run(
                            self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
                        )
                    ]
                    broker_providers = [
                        x[0] for x
                        in asyncio.run(
                            self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
                        )
                    ]
                    for provider in self.templates:
                        if provider not in feed_providers + broker_providers:
                            continue
                        for item, value in self.templates[provider].items():
                            if not isinstance(value, str):
                                continue
                            if '$' in value:
                                self.templates[provider][item] = value.replace('$', str(num))
                            if '@' in value:
                                self.templates[provider][item] = value.replace('@', letters[num])
                        new_weekly.set_provider_overrides(provider, **self.templates[provider])

            if isinstance(existing, int) and recreate:
                self.weekly_folders[existing] = new_weekly
            elif existing is None:
                if recreate:
                    self.option.logger.warning(
                        f"{new_weekly.ticker}.{new_weekly.exchange} week folder is not found, it will be created as new"
                    )
                self.weekly_folders.append(new_weekly)