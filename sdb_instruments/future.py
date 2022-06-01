import asyncio
import datetime as dt
import logging
import json
from copy import copy, deepcopy
from dataclasses import dataclass, field
from deepdiff import DeepDiff
from libs.async_symboldb import SymbolDB
from libs.backoffice import BackOffice
from libs.async_sdb_additional import Months, SDBAdditional, SdbLists
from pprint import pformat, pp
from typing import Dict, Optional, Union
from .derivative import (
    Derivative,
    ExpirationError,
    format_maturity
)

from .instrument import Instrument

@dataclass
class Future(Derivative):
    # series parameters
    ticker: str
    exchange: str
    shortname: Optional[str] = None
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

    # non-init vars
    instrument_type = 'FUTURE'
    instrument: dict = field(init=False, default_factory=dict)
    reference: dict = field(init=False, default_factory=dict)
    series_tree: list[dict] = field(init=False, default_factory=list)

    skipped: set = field(init=False, default_factory=set)
    allowed_expirations: list = field(init=False, default_factory=list)

    def __post_init__(self):
        self.contracts: list[FutureExpiration] = []
        self.new_expirations: list[FutureExpiration] = []
        self.update_expirations: list[FutureExpiration] = []

        # super sets up following self.vars:
            # self.schema,
            # self.instrument,
            # self.reference,
            # self.contracts
        super().__init__(self)
        self.__set_contracts()
        self._align_expiry_la_lt(self.contracts, self.update_expirations)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __repr__(self):
        return f"Future({self.ticker}.{self.exchange})"

    def __set_contracts(self):
        if self.instrument:
            self.contracts = [
                FutureExpiration(self, payload=x) for x
                in self.series_tree
                if x['path'][:-1] == self.instrument['path']
                and not x['isAbstract']
            ]
            # common weekly folders where single week folders are stored
            # in most cases only one is needed but there are cases like EW.CME
        else:
            self.contracts = []

    def find_expiration(
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

        # prepare expiration_date as dt.date
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
        
        # prepare maturity as YYYY-MM
        maturity = format_maturity(maturity)
        # symbolic_maturity as Z2021
        symbolic_maturity = self._date_to_symbolic(maturity)
        try_symbol_id = f"{self.ticker}.{self.exchange}.{symbolic_maturity}"
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
        if not payload.get('expiry') \
            or not payload.get('maturityDate'):
            
            self.logger.error("bad data")
            return {}
        
        # get expiration date
        exp_date = self.sdb.sdb_to_date(payload['expiry'])
        maturity = format_maturity(payload['maturityDate'])
        existing_exp, series = self.find_expiration(exp_date, maturity)
        if existing_exp:
            if skip_if_exists:
                self.skipped.add(exp_date.isoformat())
                return {}
            current_version = asyncio.run(self.sdb.get(existing_exp.instrument['_id']))
            reference = deepcopy(current_version)
            if overwrite_old:
                payload.update({key: val for key, val in current_version.items() if key[0] == '_'})
                update_contract = FutureExpiration(
                    self,
                    payload=payload
                )
            else:
                current_version.update(payload)
                update_contract = FutureExpiration(
                    self,
                    payload=current_version
                )
            update_contract.set_la_lt()
            diff: dict = DeepDiff(reference, update_contract.instrument)
            if diff:
                self.logger.info(
                    f'{update_contract.get_expiration()[1]}: '
                    'following changes have been made:'
                )
                self.logger.info(pformat(diff))
                self.update_expirations.append(update_contract)
            return {'updated': update_contract.get_expiration()[1], 'diff': diff}

        if self.allowed_expirations:
            symbolic = self._date_to_symbolic(exp_date.strftime('%Y-%m'))

            if not (
                    exp_date.isoformat() in self.allowed_expirations
                    or symbolic in self.allowed_expirations
                ):
                self.logger.info(
                    f"Allowed expirations are set and {exp_date.isoformat()} "
                    f"or {symbolic} are not there"
                )
                return {}

        new_contract = FutureExpiration(
            self,
            payload=payload
        )
        new_contract.set_la_lt()
        self.new_expirations.append(new_contract)
        return {'created': new_contract.get_expiration()[1]}

    def add(
            self,
            exp_date: str,
            maturity: str,
            skip_if_exists: bool = True,
            **kwargs
        ):
        existing_exp, series = self.find_expiration(exp_date, maturity)
        if existing_exp and skip_if_exists:
            self.skipped.add(exp_date)
            return {}
        elif existing_exp:
            if not maturity and existing_exp.get('maturityDate'):
                maturity = format_maturity(existing_exp['maturityDate'])
            current_version = asyncio.run(self.sdb.get(existing_exp.instrument['_id']))
            reference = deepcopy(current_version)
            update_contract = FutureExpiration(
                self,
                exp_date,
                maturity,
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

        new_contract = FutureExpiration(
            self,
            exp_date,
            maturity,
            payload={}
        )
        self.new_expirations.append(new_contract)
        return {'created': new_contract.get_expiration()[1]}

    def post_to_sdb(self, dry_run=True) -> dict:
        """Sends to SymbolDB absent expirations.
        If symbol doesn't exist, creates it."""
        
        try_again_series = False
        # Create folder if need
        if not self.instrument.get('_id'):
            self.create(dry_run)
        
        create_result = ''
        update_result = ''
        diff = DeepDiff(self.reference, self.instrument)
        if diff:
            self.update(diff, dry_run)
        else:
            self.logger.info(f"{self.ticker}.{self.exchange}.*: No changes have been made")

        # Create expirations
        for new in self.new_expirations:
            new.instrument['path'] = deepcopy(self.instrument['path'])
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
            report[f'{self.ticker}.{self.exchange}'].update({
                'created': [x.get_expiration()[1] for x in self.new_expirations]
            })
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
            report[f'{self.ticker}.{self.exchange}'].update({
                'updated': [x.get_expiration()[1] for x in self.update_expirations],
            })
        if report and try_again_series:
            response = asyncio.run(self.sdb.update(self.instrument))
            if response.get('message'):
                self.logger.error(f'instrument {self.ticker} is not updated:')
                self.logger.error(pformat(response))
        if not dry_run:
            self.clean_up_times()
        if [x for x, y in report.items() if y.get('created')]:
            self.logger.info('All good, reloading tree cache...')
            self.force_tree_reload()
        return report

class FutureExpiration(Instrument):
    def __init__(
            self,
            future: Future,
            expiration_date: Union[str, dt.date, dt.datetime] = None,
            maturity: str = None,
            payload: dict = None,
        ):
        self.env = future.env
        self.future = future
        super().__init__(
            instrument_type=future.instrument_type,
            env=self.env,
            sdb=future.sdb,
            sdbadds=future.sdbadds,
            silent=future.silent
        )
        self.ticker = future.ticker
        self.exchange = future.exchange
        if payload is None:
            payload = {}
        self.instrument = payload
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
        self.compiled_parent = asyncio.run(self.sdbadds.build_inheritance(
            [self.future.compiled_parent, self.future.instrument],
            include_self=True
        ))
        if not self.instrument:
            self.build_expiration_dict()
        if not self.instrument.get('path'):
            self.instrument['path'] = deepcopy(self.future.instrument['path'])
            if not self.future.instrument.get('_id'):
                self.instrument['path'].append('<<new series id>>')

    def __repr__(self):
        return (
            f"FutureExpiration({self.ticker}.{self.exchange}.{self._date_to_symbolic(self.maturity)}, "
            f"{self.expiration.isoformat()})"
        )

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def build_expiration_dict(self, reference: dict = None, **kwargs) -> dict:
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
            },
            'maturityDate': {
                'month': int(self.maturity.split('-')[1]),
                'year': int(self.maturity.split('-')[0])
            }
        })
        if not self.instrument.get('name'):
            self.instrument.update({
                'name': self.maturity
            })
        if self.instrument.get('isTrading') is not None:
            self.instrument.pop('isTrading')
        self.set_la_lt()
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))
        diff = DeepDiff(reference, self.instrument)
        return diff

    def set_la_lt(self):
        if self.future.set_la:
            self.instrument['lastAvailable'] = self.sdb.date_to_sdb(
                self.sdb.sdb_to_date(self.instrument['expiry']) + dt.timedelta(days=3)
            )
            if isinstance(self.future.set_la, str):
                self.instrument['lastAvailable']['time'] = self.future.set_la
            

        if self.future.set_lt:
            self.instrument['lastTrading'] = deepcopy(self.instrument['expiry'])
            if isinstance(self.future.set_lt, str):
                self.instrument['lastTrading']['time'] = self.future.set_lt

    def get_expiration(self):
        symbolic_date = self._date_to_symbolic(self.maturity)
        exante_id = f"{self.ticker}.{self.exchange}.{symbolic_date}"
        return [self.instrument, exante_id]