import asyncio
import datetime as dt
import pandas as pd
import numpy as np
import logging
import json
from copy import deepcopy
from dataclasses import dataclass, field
from deepdiff import DeepDiff
from libs.async_symboldb import SymbolDB
from libs.backoffice import BackOffice
from libs.async_sdb_additional import SDBAdditional
from pprint import pformat, pp
from typing import Optional, Union
from libs.sdb_instruments import (
    Instrument,
    Derivative,
    Future,
    FutureExpiration,
    ExpirationError,
    format_maturity
)
import re



@dataclass
class Spread(Derivative):
    # series parameters
    ticker: str
    exchange: str
    shortname: Optional[str] = None
    calendar_type: str = 'FORWARD'
    parent_folder: Union[str, dict] = None
    env: str = 'prod'

    # init parameters
    reload_cache: bool = True
    recreate: bool = False
    silent: bool =  False

    # class instances
    bo: BackOffice = None
    sdb: SymbolDB = None
    sdbadds: SDBAdditional = None

    series_payload: dict = field(default_factory=dict)
    # non-init vars
    instrument_type: str = 'SPREAD'
    instrument: dict = field(init=False, default_factory=dict)
    reference: dict = field(init=False, default_factory=dict)
    series_tree: list[dict] = field(init=False, default_factory=list)
    skipped: set = field(init=False, default_factory=set)
    allowed_expirations: list = field(init=False, default_factory=list)


    def __post_init__(self):
        self.first_ticker = None
        self.second_ticker = None
        if len(self.ticker.split('-')) == 2: 
            self.spread_type = 'SPREAD'
            self.first_ticker, self.second_ticker = self.ticker.split('-')[:2]
        elif len(self.ticker.split('-')) == 1:
            self.spread_type = 'CALENDAR_SPREAD'
        else:
            raise RuntimeError(f'Wrong ticker: {self.ticker}. Should look like TICKER or TICKER1-TICKER2')

        self.contracts: list[SpreadExpiration] = []
        self.leg_futures: list[FutureExpiration] = []
        self.new_expirations: list[SpreadExpiration] = []
        self.update_expirations: list[SpreadExpiration] = []

        # super sets up following self.vars:
            # self.schema,
            # self.instrument,
            # self.reference,
            # self.contracts,
            # self.leg_futures
        super().__init__(self)
        self.__set_contracts()
        self._align_expiry_la_lt(self.contracts, self.update_expirations)


    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __repr__(self):
        return f"Spread({self.ticker}.{self.exchange}, {self.spread_type=})"

    def __set_contracts(self):

        self.contracts = [
            SpreadExpiration(self, payload=x) for x
            in self.series_tree
            if x['path'][:-1] == self.instrument['path']
            and not x['isAbstract']
        ]
        if self.spread_type in ['CALENDAR', 'CALENDAR_SPREAD']:
            self.gap_folders = [
                x for x
                in self.series_tree
                if x['path'][:-1] == self.instrument['path']
                and x['isAbstract']
                and re.match(r'\d{1,2} month', x['name'])
            ]
            for gf in self.gap_folders:
                self.contracts.extend([
                    SpreadExpiration(self, payload=x) for x
                    in self.series_tree
                    if x['path'][:-1] == gf['path']
                    and not x['isAbstract']
                ])

            try:
                future = Future(self.ticker, self.exchange, env=self.env, reload_cache=False)
                self.leg_futures = future.contracts
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
                    self.leg_futures += future.contracts
                except Exception as e:
                    self.logger.error(
                        f"{self.ticker}.{self.exchange}: {e.__class__.__name__}: {e}"
                    )
                    self.logger.error(
                        f'{leg_ticker}.{self.exchange} '
                        'futures are not found in sdb! Create them in first place'
                    )

    def find_calendar_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            near_maturity: str = None,
            far_maturity: str = None
        ):
        present_expirations: list[tuple[FutureExpiration, Future]] = []
        expiration_date = None
        if isinstance(expiration, str):
            try:
                expiration_date = dt.date.fromisoformat(expiration)
            except ValueError:
                pass
            except AttributeError:
                pass
            if expiration_date is None and near_maturity is None:
                near_maturity = expiration
        elif isinstance(expiration, dt.datetime):
            expiration_date = expiration.date()
        elif isinstance(expiration, dt.date):
            expiration_date = expiration
        near_maturity = format_maturity(near_maturity)
        near_symbolic = self._date_to_symbolic(near_maturity)
        far_maturity = format_maturity(far_maturity)
        far_symbolic = self._date_to_symbolic(far_maturity)
        try_symbol_id_fwd = f"{self.ticker}.{self.exchange}.CS/{near_symbolic}-{far_symbolic}"
        try_symbol_id_rev = f"{self.ticker}.{self.exchange}.RS/{near_symbolic}-{far_symbolic}"
        present_expirations = [
            num for num, x
            in enumerate(self.update_expirations)
            if (
                (
                    x.expiration == expiration_date
                    or x.near_maturity == near_maturity
                ) and x.far_maturity == far_maturity
            ) or x.get_expiration()[1] == try_symbol_id_fwd  # expiration: Z2021
            or x.get_expiration()[1] == try_symbol_id_rev  # expiration: Z2021
        ]
        if len(present_expirations) == 1:
            present_expiration = self.update_expirations.pop(present_expirations[0])
            return present_expiration, self
        elif len(present_expirations) > 1:
            self.logger.error(
                'More than one expiration have been found, try to narrow search criteria'
            )
            return None, None
        # if nothing is found, search in spread.contracts

        present_expirations = [
            x for x
            in self.contracts
            if (
                (
                    (
                        x.expiration == expiration_date
                        or x.near_maturity == near_maturity
                    ) and x.far_maturity == far_maturity
                ) or x.get_expiration()[1] == try_symbol_id_fwd  # expiration: Z2021
                or x.get_expiration()[1] == try_symbol_id_rev  # expiration: Z2021
            ) and x.instrument.get('isTrading') is not False
        ]
        if len(present_expirations) == 1:
            present_expiration = present_expirations[0]
            return present_expiration, self
        elif len(present_expirations) > 1:
            self.logger.error(
                'More than one expiration have been found, try to narrow search criteria'
            )
            return None, None
        return None, self

    def find_product_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None
        ):
        present_expirations: list[tuple[FutureExpiration, Future]] = []
        expiration_date = None
        if isinstance(expiration, str):
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
        try_symbol_id = f"{self.ticker}.{self.exchange}.{maturity}"
        maturity = format_maturity(maturity)
        present_expirations = [
            num for num, x
            in enumerate(self.update_expirations)
            if x.expiration == expiration_date
            or x.maturity == maturity
            or x.get_expiration()[1] == try_symbol_id  # expiration: Z2021
        ]
        if len(present_expirations) == 1:
            present_expiration = self.update_expirations.pop(present_expirations[0])
            return present_expiration, self
        elif len(present_expirations) > 1:
            self.logger.error(
                'More than one expiration have been found, try to narrow search criteria'
            )
            return None, None
        # if nothing is found, search in future.contracts

        present_expirations = [
            x for x
            in self.contracts
            if (
                x.expiration == expiration_date
                or x.maturity == maturity
                or x.get_expiration()[1] == try_symbol_id  # expiration: Z2021
            ) and x.instrument.get('isTrading') is not False
        ]
        if len(present_expirations) == 1:
            present_expiration = present_expirations[0]
            return present_expiration, self
        elif len(present_expirations) > 1:
            self.logger.error(
                'More than one expiration have been found, try to narrow search criteria'
            )
            return None, None
        return None, self
    
    def add_payload(
            self,
            payload: dict,
            skip_if_exists: bool = True,
            overwrite_old: bool = False
        ):
        leg_gap = None
        if self.spread_type == 'SPREAD' and (
            not payload.get('expiry')
            or not payload.get('maturityDate')
        ):
            
            self.logger.error("bad data")
            return {}

        elif self.spread_type == 'CALENDAR_SPREAD' and (
            not payload.get('expiry')
            or not payload.get('nearMaturityDate')
            or not payload.get('farMaturityDate')
        ):
        
            self.logger.error("bad data")
            return {}

        # get expiration date
        exp_date = self.sdb.sdb_to_date(payload['expiry'])
        if self.spread_type == 'SPREAD':
            maturity = format_maturity(payload['maturityDate'])
            existing_exp, series = self.find_product_expiration(exp_date, maturity)
        elif self.spread_type == 'CALENDAR_SPREAD':
            if 'leg_gap' in payload:
                leg_gap = payload.pop('leg_gap')
            near_maturity = format_maturity(payload['nearMaturityDate'])
            far_maturity = format_maturity(payload['farMaturityDate'])

            existing_exp, series = self.find_calendar_expiration(
                exp_date,
                near_maturity,
                far_maturity
            )

        if existing_exp:
            if skip_if_exists:
                self.skipped.add(exp_date.isoformat())
                return {}
            current_version = asyncio.run(self.sdb.get(existing_exp.instrument['_id']))
            reference = deepcopy(current_version)
            if overwrite_old:
                payload.update({key: val for key, val in current_version.items() if key[0] == '_'})
                update_contract = SpreadExpiration(
                    self,
                    payload=payload,
                    leg_gap=leg_gap
                )
            else:
                current_version.update(payload)
                update_contract = SpreadExpiration(
                    self,
                    payload=current_version,
                    leg_gap=leg_gap
                )
            update_contract.set_la_lt()
            diff: dict = DeepDiff(reference, update_contract.instrument)
            if diff:
                self.logger.info(
                    f'{self.ticker}.{self.exchange} {exp_date.isoformat()}: '
                    'following changes have been made:'
                )
                self.logger.info(pformat(diff))
                self.update_expirations.append(update_contract)
            return {'updated': update_contract.get_expiration()[1], 'diff': diff}

        if self.allowed_expirations:
            if self.spread_type == 'SPREAD':
                symbolic = self._date_to_symbolic(exp_date.isoformat()[:7])
            if self.spread_type == 'CALENDAR_SPREAD':
                symbolic = f"{self._date_to_symbolic(near_maturity)}-{self._date_to_symbolic(far_maturity)}"

            if not (
                    exp_date.isoformat() in self.allowed_expirations
                    or symbolic in self.allowed_expirations
                ):
                self.logger.info(
                    f"Allowed expirations are set and {exp_date.isoformat()} "
                    f"or {symbolic} are not there"
                )
                return {}

        new_contract = SpreadExpiration(
            self,
            payload=payload,
            leg_gap=leg_gap
        )
        new_contract.set_la_lt()
        self.new_expirations.append(new_contract)
        return {'created': new_contract.get_expiration()[1]}

    def add(
            self,
            exp_date: str,
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            skip_if_exists: bool = True,
            **kwargs
        ):
        if self.spread_type == 'SPREAD':
            existing_exp, series = self.find_product_expiration(exp_date, maturity)
        elif self.spread_type == 'CALENDAR_SPREAD':
            existing_exp, series = self.find_calendar_expiration(exp_date, near_maturity, far_maturity)
        if existing_exp and skip_if_exists:
            self.skipped.add(exp_date)
            return {}
        elif existing_exp:
            if not maturity and existing_exp.get('maturityDate'):
                maturity = format_maturity(existing_exp['maturityDate'])
            current_version = asyncio.run(self.sdb.get(existing_exp.instrument['_id']))
            reference = deepcopy(current_version)
            update_contract = SpreadExpiration(
                self,
                exp_date,
                maturity,
                near_maturity,
                far_maturity,
                payload=current_version
            )
            diff = update_contract.build_expiration_dict(reference=reference, **kwargs)
            if diff:
                self.logger.info(
                    f'{self.ticker}.{self.exchange} {exp_date}: following changes have been made:'
                )
                self.logger.info(pformat(diff))
                self.update_expirations.append(update_contract)
                return {'updated': update_contract.get_expiration()[1], 'diff': diff}
            else:
                self.logger.info(f'No new data for existing expiration: {exp_date}')
                return {}

        if self.allowed_expirations:
            symbolic = self._date_to_symbolic(exp_date[:7])
            if not (
                    exp_date in self.allowed_expirations
                    or symbolic in self.allowed_expirations
                ):
                self.logger.info(f"Allowed expirations are set and {exp_date} is not in it")
                return {}

        new_contract = SpreadExpiration(
            self,
            exp_date,
            maturity,
            near_maturity,
            far_maturity,
            payload={}
        )
        self.new_expirations.append(new_contract)
        return {'created': new_contract.get_expiration()[1]}

    def create_gap_folder(self, gap: int, sibling_folder: dict, dry_run: bool = False):
        new_folder = deepcopy(sibling_folder)
        for udl_field in [x for x in sibling_folder if x[0] == '_']:
            new_folder.pop(udl_field)
        new_folder['path'].pop(-1)

        str_gap = str(gap) if gap > 10 else f'0{gap}'
        new_folder['name'] = re.sub(r'\d{1,2}', str_gap, new_folder['name'])
        sibling_months = int(re.search(r'\d{1,2}', sibling_folder['name']).group())
        sibling_leggap = next((
            val for key, val
            in sibling_folder.get('brokers', {}).get('providerOverrides', {}).items()
            if key == 'legGap'
        ), None)
        multiplier = sibling_months / sibling_leggap if sibling_leggap else 1
        true_leg_gap = gap * multiplier
        for prov, overrides in new_folder['brokers']['providerOverrides'].items():
            if overrides.get('legGap'):
                new_folder['brokers']['providerOverrides'][prov]['legGap'] = true_leg_gap
        if dry_run:
            print(f"Dry run. New folder {new_folder['name']} to create:")
            pp(new_folder)
            new_folder['path'].append(f"<<new {self.ticker}.{self.exchange} folder id>>")
        else:
            create = asyncio.run(self.sdb.create(new_folder))
            if not create.get('_id'):
                self.logger.error(pformat(create))
                return None
            self.logger.debug(f'Result: {pformat(create)}')
            new_folder['_id'] = create['_id']
            new_folder['_rev'] = create['_rev']
            new_folder['path'].append(new_folder['_id'])
            new_record = pd.DataFrame([{
                key: val for key, val
                in self.payload.items()
                if key in self.tree_df.columns
            }], index=[self.payload['_id']])
            pd.concat([self.tree_df, new_record])
            self.tree_df.replace({np.nan: None})

            # self.tree.append(new_folder)
        return new_folder

    def post_to_sdb(self, dry_run=True) -> dict:
        """Sends to SymbolDB absent expirations.
        If symbol doesn't exist, creates it."""
        new_folders = []
        try_again_series = False
        # Create folder if needed
        if not self.instrument.get('_id'):
            self.create(dry_run)
        
        create_result = ''
        update_result = ''
        diff = DeepDiff(self.reference, self.instrument)
        if diff:
            self.update(diff, dry_run)
        else:
            self.logger.info(f"No changes were made for {self.ticker}.{self.exchange}.*")

        # Create expirations
        for new in self.new_expirations:
            new.instrument['path'] = deepcopy(self.instrument['path'])
            if self.spread_type == 'CALENDAR_SPREAD' and self.gap_folders:
                try:
                    days_delta =  (
                        dt.date.fromisoformat(f"{new.far_maturity}-01") - \
                        dt.date.fromisoformat(f"{new.near_maturity}-01")
                    ).days
                    month_gap = int(round(days_delta / 30.41, 0)) # 30.41 = 365 / 12
                    gap_folder = next(
                        (
                            x for x in self.gap_folders
                            if re.search(r'(?P<month>\d{1,2}) month', x['name'])
                            and int(re.search(r'(?P<month>\d{1,2}) month', x['name']).group('month')) == month_gap
                        ), None)
                    if gap_folder:
                        new.instrument['path'].append(gap_folder['_id'])
                    else:
                        new_folder = self.create_gap_folder(month_gap, self.gap_folders[0], dry_run)
                        self.gap_folders.append(new_folder)
                        new_folders.append(new_folder)
                        new.instrument['path'].append(new_folder['_id'])
                except Exception:
                    self.logger.error(
                        "Cannot determine month gap"
                    )
        if dry_run and self.new_expirations:
            print(f"Dry run, new expirations to create:")
            pp([x.get_expiration()[1] for x in self.new_expirations])
        if dry_run and self.update_expirations:
            print(f"Dry run, expirations to update:")
            pp([x.get_expiration()[1] for x in self.update_expirations])
        if dry_run:
            return {}

        if self.new_expirations:
            create_result = asyncio.run(self.sdb.batch_create(
                input_data=[x.get_expiration()[0] for x in self.new_expirations]
            ))
        else:
            create_result = 'Nothing to do'
        if self.update_expirations:
            update_result = asyncio.run(self.sdb.batch_update(
                input_data=[x.get_expiration()[0] for x in self.update_expirations]
            ))
        else:
            update_result = 'Nothing to do'
        report = {f'{self.ticker}.{self.exchange}': {}}
        if new_folders:
             report[f'{self.ticker}.{self.exchange}'].setdefault(
                'created',
                []
            ).extend([x['name'] for x in new_folders])
        if create_result == 'Nothing to do':
            pass
        elif create_result:
            if isinstance(create_result, str):
                create_result = json.loads(create_result)
            self.logger.error(
                f'problems with creating new expirations: {pformat(create_result)}'
            )
            report[f'{self.ticker}.{self.exchange}'].update({
                'create_error': create_result.get('description')
            })
        else:
            report[f'{self.ticker}.{self.exchange}'].setdefault(
                'created',
                []
            ).extend([x.get_expiration()[1] for x in self.new_expirations])
        if update_result == 'Nothing to do':
            pass
        elif update_result:
            if isinstance(update_result, str):
                update_result = json.loads(update_result)
            self.logger.error(
                f'problems with updating expirations: {pformat(update_result)}'
            )
            report[f'{self.ticker}.{self.exchange}'].update({
                'update_error': update_result.get('description')
            })
        else:
            report[f'{self.ticker}.{self.exchange}'].setdefault(
                'updated',
                []
            ).extend([x.get_expiration()[1] for x in self.update_expirations])
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

class SpreadExpiration(Instrument):
    def __init__(
            self,
            spread: Spread,
            expiration_date: Union[str, dt.date, dt.datetime] = None,

            #product parameters
            maturity: str = None,

            #calendar parameters
            near_maturity: str = None,
            far_maturity: str = None,
            calendar_type: str = None,
            leg_gap: int = None,
            payload: dict = None,
        ):
        self.env = spread.env
        self.spread = spread
        self.spread_type = spread.spread_type
        
        if not calendar_type:
            self.calendar_type = spread.calendar_type
        else:
            self.calendar_type = calendar_type
        super().__init__(
            instrument_type=spread.instrument_type,
            env=spread.env,
            sdb=spread.sdb,
            sdbadds=spread.sdbadds,
            spread_type=self.spread_type,
            silent=spread.silent
        )
        self.ticker = spread.ticker
        self.first_ticker = spread.first_ticker
        self.second_ticker = spread.second_ticker
        self.exchange = spread.exchange
        self.leg_futures = spread.leg_futures
        self.leg_gap = leg_gap
        if payload is None:
            payload = {}
        self.instrument: dict = payload
        if expiration_date:
            if isinstance(expiration_date, str):
                try:
                    self.expiration = dt.date.fromisoformat(expiration_date)
                except ValueError:
                    if not self.instrument:
                        raise ExpirationError(f'Invalid expiration date format: {expiration_date}')
                    self.expiration = self.sdb.sdb_to_date(self.instrument['expiry'])
            elif isinstance(expiration_date, dt.date):
                self.expiration = expiration_date
            elif isinstance(expiration_date, dt.datetime):
                self.expiration = expiration_date.date()
        elif self.instrument.get('expiry'):
            self.expiration = self.sdb.sdb_to_date(self.instrument['expiry'])
        else:
            raise ExpirationError(f'Invalid expiration date format: {expiration_date}')
        if self.spread_type == 'SPREAD':
            if maturity:
                self.maturity = format_maturity(maturity)
                if not self.maturity:
                    raise ExpirationError(f'Cannot format maturity: {maturity}')
            elif self.instrument.get('maturityDate'):
                if int(payload['maturityDate']['month']) < 10:
                    self.maturity = f"{payload['maturityDate']['year']}-0{payload['maturityDate']['month']}"
                else:
                    self.maturity = f"{payload['maturityDate']['year']}-{payload['maturityDate']['month']}"
                if self.instrument['maturityDate'].get('day'):
                    if int(payload['maturityDate']['day']) < 10:
                        self.maturity += f"-0{payload['maturityDate']['day']}"
                    else:
                        self.maturity += f"-{payload['maturityDate']['day']}"
            else:
                self.maturity = self.expiration.strftime('%Y-%m')
        if self.spread_type == 'CALENDAR_SPREAD':
            if near_maturity:
                self.near_maturity = format_maturity(near_maturity)
                if not self.near_maturity:
                    raise ExpirationError(f'Cannot format near_maturity: {near_maturity}')
            elif self.instrument.get('nearMaturityDate'):
                if int(payload['nearMaturityDate']['month']) < 10:
                    self.near_maturity = f"{payload['nearMaturityDate']['year']}-0{payload['nearMaturityDate']['month']}"
                else:
                    self.near_maturity = f"{payload['nearMaturityDate']['year']}-{payload['nearMaturityDate']['month']}"
                if self.instrument['nearMaturityDate'].get('day'):
                    if int(payload['nearMaturityDate']['day']) < 10:
                        self.near_maturity += f"-0{payload['nearMaturityDate']['day']}"
                    else:
                        self.near_maturity += f"-{payload['nearMaturityDate']['day']}"
            else:
                self.near_maturity = self.expiration.strftime('%Y-%m')
            if far_maturity:
                self.far_maturity = format_maturity(far_maturity)
                if not self.far_maturity:
                    raise ExpirationError(f'Cannot format far_maturity: {far_maturity}')
            elif self.instrument.get('farMaturityDate'):
                if int(payload['farMaturityDate']['month']) < 10:
                    self.far_maturity = f"{payload['farMaturityDate']['year']}-0{payload['farMaturityDate']['month']}"
                else:
                    self.far_maturity = f"{payload['farMaturityDate']['year']}-{payload['farMaturityDate']['month']}"
                if self.instrument['farMaturityDate'].get('day'):
                    if int(payload['farMaturityDate']['day']) < 10:
                        self.far_maturity += f"-0{payload['farMaturityDate']['day']}"
                    else:
                        self.far_maturity += f"-{payload['farMaturityDate']['day']}"
            if not self.near_maturity:
                raise ExpirationError('Near maturity is not set')
            if not self.far_maturity:
                raise ExpirationError('Far maturity is not set')
            if self.far_maturity < self.near_maturity:
                raise ExpirationError(
                    f'{self.far_maturity=} is earlier than {self.near_maturity=}'
                )


        self.compiled_parent = asyncio.run(self.sdbadds.build_inheritance(
            [self.spread.compiled_parent, self.spread.instrument],
            include_self=True
        ))
        if calendar_type:
            self.calendar_type = calendar_type
        elif self.instrument.get('spreadType'):
            self.calendar_type = self.instrument['spreadType']
        else:
            self.calendar_type = self.compiled_parent.get('spreadType')
        if not self.instrument:
            self.build_expiration_dict()
        if not self.instrument.get('path'):
            self.instrument['path'] = deepcopy(self.spread.instrument['path'])
            if not self.spread.instrument.get('_id'):
                self.instrument['path'].append('<<new series id>>')

    def __repr__(self):
        if self.spread_type == 'SPREAD':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.{self._date_to_symbolic(self.maturity)}, "
                f"{self.expiration.isoformat()})"
            )
        elif self.spread_type == 'CALENDAR_SPREAD' and self.calendar_type == 'FORWARD':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.CS/"
                f"{self._date_to_symbolic(self.near_maturity)}-{self._date_to_symbolic(self.far_maturity)}, "
                f"{self.expiration.isoformat()}, {self.calendar_type=})"
            )
        elif self.spread_type == 'CALENDAR_SPREAD' and self.calendar_type == 'REVERSE':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.RS/"
                f"{self._date_to_symbolic(self.near_maturity)}-{self._date_to_symbolic(self.far_maturity)}, "
                f"{self.expiration.isoformat()}, {self.calendar_type=})"
            )

    def __eq__(self, other):
        return (
            self.expiration == other.expiration
            and (
                (
                    self.far_maturity is None
                    and other.far_maturity is None
                )
                or self.far_maturity == other.far_maturity
            )
            and self.ticker == other.ticker
            and self.exchange == other.exchange
        )
    
    def __gt__(self, other: object) -> bool:
        if self.far_maturity:
            return (
                self.expiration > other.expiration
                or (
                    self.expiration == other.expiration
                    and self.far_maturity > other.far_maturity
                )
            )
        else:
            return self.expiration > other.expiration

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")
    
    def mk_product_legs(self):
        first_leg = next((
            x for x
            in self.leg_futures
            if x.ticker == self.first_ticker
            and x.maturity == self.maturity
        ), None)
        second_leg = next((
            x for x
            in self.leg_futures
            if x.ticker == self.second_ticker
            and x.maturity == self.maturity
        ), None)
        if first_leg and second_leg:
            legs = [
                {
                    'quantity': 1,
                    'exanteId': first_leg.get_expiration()[1]
                },
                {
                    'quantity': -1,
                    'exanteId': second_leg.get_expiration()[1]
                }
            ]
            return legs
        elif not first_leg:
            self.logger.error(
                f'{self.first_ticker}.{self.exchange}.{self._date_to_symbolic(self.maturity)} '
                'future is not found in sdb!'
            )
        elif not second_leg:
            self.logger.error(
                f'{self.second_ticker}.{self.exchange}.{self._date_to_symbolic(self.maturity)} '
                'future is not found in sdb!'
            )
        return None

    def mk_calendar_legs(self):
        first_leg = next((
            x for x
            in self.leg_futures
            if x.maturity == self.near_maturity
        ), None)
        second_leg = next((
            x for x
            in self.leg_futures
            if x.maturity == self.far_maturity
        ), None)
        if first_leg and second_leg:
            if self.calendar_type == 'FORWARD':
                legs = [
                    {
                        'quantity': 1,
                        'exanteId': first_leg.get_expiration()[1]
                    },
                    {
                        'quantity': -1,
                        'exanteId': second_leg.get_expiration()[1]
                    }
                ]
            elif self.calendar_type == 'REVERSE':
                legs = [
                    {
                        'quantity': -1,
                        'exanteId': first_leg.get_expiration()[1]
                    },
                    {
                        'quantity': 1,
                        'exanteId': second_leg.get_expiration()[1]
                    }
                ]
            return legs
        elif not first_leg:
            self.logger.error(
                f'{self.first_ticker}.{self.exchange}.{self._date_to_symbolic(self.maturity)} '
                'future is not found in sdb!'
            )
        elif not second_leg:
            self.logger.error(
                f'{self.second_ticker}.{self.exchange}.{self._date_to_symbolic(self.maturity)} '
                'future is not found in sdb!'
            )
        return None

    def build_expiration_dict(self, reference: dict = None, **kwargs):
        if not self.instrument:
            self.instrument = {
                'isAbstract': False,
                'name': None,
                'maturityDate': {},
                'expiry': {}

            }
            reference = {}
        elif not reference:
            reference = deepcopy(self.instrument)
        self.instrument.update({
            'expiry': {
                'year': self.expiration.year,
                'month': self.expiration.month,
                'day': self.expiration.day
            }
        })
        if self.spread_type == 'SPREAD':
            self.instrument.update({
                'maturityDate': {
                    'month': int(self.maturity.split('-')[1]),
                    'year': int(self.maturity.split('-')[0])
                }
            })
            if not self.instrument.get('name'):
                self.instrument.update({
                    'name': self.maturity
                })
            legs = self.mk_product_legs()
            
        if self.spread_type == 'CALENDAR_SPREAD':
            self.instrument.update({
                'nearMaturityDate': {
                    'month': int(self.near_maturity.split('-')[1]),
                    'year': int(self.near_maturity.split('-')[0])
                },
                'farMaturityDate': {
                    'month': int(self.far_maturity.split('-')[1]),
                    'year': int(self.far_maturity.split('-')[0])
                }
            })
            if not self.instrument.get('name'):
                self.instrument.update({
                    'name': f"{self.near_maturity} {self.far_maturity}"
                })
            if self.calendar_type != self.spread.calendar_type:
                self.instrument.update({
                    'spreadType': self.calendar_type
                })

            legs = self.mk_calendar_legs()
        if legs:
            self.instrument.update({
                'legs': legs
            })
        else:
            pass
        if self.instrument.get('isTrading') is not None:
            self.instrument.pop('isTrading')
        self.set_la_lt()
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))
        diff = DeepDiff(reference, self.instrument)
        return diff

    def set_la_lt(self):
        if self.spread.set_la:
            self.instrument['lastAvailable'] = self.sdb.date_to_sdb(
                self.sdb.sdb_to_date(self.instrument['expiry']) + dt.timedelta(days=3)
            )
            if isinstance(self.spread.set_la, str):
                self.instrument['lastAvailable']['time'] = self.spread.set_la
            

        if self.spread.set_lt:
            self.instrument['lastTrading'] = deepcopy(self.instrument['expiry'])
            if isinstance(self.spread.set_lt, str):
                self.instrument['lastTrading']['time'] = self.spread.set_lt


    def get_expiration(self):
        if self.spread_type == 'CALENDAR_SPREAD':
            symbolic_date = f"{self._date_to_symbolic(self.near_maturity)}-{self._date_to_symbolic(self.far_maturity)}"
            if self.calendar_type == 'FORWARD':
                exante_id = f"{self.ticker}.{self.exchange}.CS/{symbolic_date}"
            if self.calendar_type == 'REVERSE':
                exante_id = f"{self.ticker}.{self.exchange}.RS/{symbolic_date}"
        elif self.spread_type == 'SPREAD':
            symbolic_date = f"{self._date_to_symbolic(self.maturity)}"
            exante_id = f"{self.ticker}.{self.exchange}.{symbolic_date}"
        return [self.instrument, exante_id]