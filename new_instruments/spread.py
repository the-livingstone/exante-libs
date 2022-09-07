import asyncio
from copy import deepcopy
import datetime as dt
from deepdiff import DeepDiff
import json
import logging
import numpy as np
import pandas as pd
from pprint import pformat, pp
import re
from typing import Union

from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional
from libs.backoffice import BackOffice
from libs.new_instruments import (
    InitThemAll,
    Instrument,
    Derivative,
    Future,
    FutureExpiration,
    ExpirationError,
    NoInstrumentError,
    NoExchangeError,
    get_uuid_by_path
)

class Spread(Derivative):
    def __init__(
            self,
            # series parameters
            ticker: str,
            exchange: str,
            instrument: dict = None,
            reference: dict = None,
            calendar_type: str = 'FORWARD',
            series_tree: list[dict] = None,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env: str = 'prod'
        ):
        self.env = env
        (
            self.bo,
            self.sdb,
            self.sdbadds,
            self.tree_df
        ) = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env,
            reload_cache=False
        ).get_instances
        self.ticker = ticker
        self.exchange = exchange
        self.first_ticker = None
        self.second_ticker = None
        if len(self.ticker.split('-')) == 2: 
            self.spread_type = 'SPREAD'
            self.first_ticker, self.second_ticker = self.ticker.split('-')[:2]
        elif len(self.ticker.split('-')) == 1:
            self.spread_type = 'CALENDAR_SPREAD'
        else:
            raise RuntimeError(
                f'Wrong ticker: {self.ticker}. Should look like TICKER or TICKER1-TICKER2'
            )
        self.calendar_type = calendar_type

        self.instrument = instrument
        super().__init__(
            ticker=ticker,
            exchange=exchange,
            instrument_type=self.spread_type,
            instrument=self.instrument,
            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            reload_cache=False,
            calendar_type=self.calendar_type
        )
        if reference is None:
            reference = {}
        self.reference = reference
        self.skipped = set()
        self.allowed_expirations = []

        self.new_expirations: list[SpreadExpiration] = []
        self.leg_futures: list[FutureExpiration] = []
        self.series_tree = series_tree
        self.contracts, self.gap_folders = self.__set_contracts(series_tree)
        self.leg_futures = self.__set_gap_folders()
        self._align_expiry_la_lt(self.contracts)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")
    
    @property
    def series_name(self):
        return f"{self.ticker}.{self.exchange}"

    def __repr__(self):
        return f"Spread({self.series_name}, {self.spread_type=})"

    @classmethod
    def from_sdb(
            cls,
            ticker: str,
            exchange: str,
            parent_folder_id: str = None,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            reload_cache: bool = True,
            env: str = 'prod'
        ):
        bo, sdb, sdbadds, tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env,
            reload_cache=reload_cache,
        ).get_instances
        if not parent_folder_id:
            parent_folder_id = get_uuid_by_path(
                ['Root', 'SPREAD', exchange],
                tree_df
            )
            if not parent_folder_id:
                raise NoExchangeError(f'{exchange=} does not exist in SymbolDB')
        instrument, series_tree = Derivative._find_series(
            ticker,
            parent_folder_id,
            sdb=sdb,
            tree_df=tree_df,
            env=env
        )
        if not instrument:
            raise NoInstrumentError(
                f'{ticker}.{exchange} series does not exist in SymbolDB'
            )
        
        return cls(
            ticker=ticker,
            exchange=exchange,
            instrument=instrument,
            reference=deepcopy(instrument),
            series_tree=series_tree,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )

    @classmethod
    def from_scratch(
            cls,
            ticker: str,
            exchange: str,
            shortname: str,
            calendar_type: str = 'FORWARD',
            parent_folder_id: str = None,
            recreate: bool = False,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            reload_cache: bool = True,
            env: str = 'prod',
            **kwargs
        ):
        bo, sdb, sdbadds, tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env,
            reload_cache=reload_cache
        ).get_instances
        if not parent_folder_id:
            parent_folder_id = get_uuid_by_path(
                ['Root', 'SPREAD', exchange],
                tree_df
            )
            if not parent_folder_id:
                raise NoExchangeError(f'{exchange=} does not exist in SymbolDB')            

        parent_folder = asyncio.run(sdb.get(parent_folder_id))
        if not parent_folder or not parent_folder.get('isAbstract'):
            raise NoInstrumentError(f"Bad {parent_folder_id=}")
        reference, series_tree = Derivative._find_series(
            ticker,
            parent_folder_id,
            sdb=sdb,
            tree_df=tree_df,
            env=env
        )
        if reference:
            if not recreate:
                raise RuntimeError(
                    f'{ticker}.{exchange} series already exist in SymbolDB'
                )
            kwargs.update({
                key: val for key, val
                in reference.items()
                if key[0] == '_' or key == 'path'
            })
        instrument = Derivative.create_series_dict(
            ticker,
            exchange,
            shortname,
            parent_folder,
            **kwargs
        )
        instrument.update({
            'description': f'{shortname} Spreads'
        })

        spread = cls(
            ticker=ticker,
            exchange=exchange,
            instrument=instrument,
            reference=deepcopy(reference),
            series_tree=series_tree,
            calendar_type=calendar_type,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )
        if spread.spread_type == 'CALENDAR_SPREAD' and calendar_type == 'REVERSE':
            instrument.update({
                'spreadType': 'REVERSE'
            })
        elif spread.spread_type == 'SPREAD':
            instrument.update({
                'type': 'FUTURE'
            })
        return spread

    @classmethod
    def from_dict(
            cls,
            payload: dict,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            reload_cache: bool = True,
            env: str = 'prod',
        ):
        bo, sdb, sdbadds, tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env,
            reload_cache=reload_cache
        ).get_instances
        check_path = get_uuid_by_path(
                payload.get('path', []),
                tree_df
            )
        if not check_path:
            raise NoInstrumentError(f"Bad path: {payload.get('path')}")
        if payload['path'][1] != get_uuid_by_path(['Root', 'FUTURE'], tree_df):
            raise NoInstrumentError(f"Bad path: {sdbadds.show_path(payload.get('path'))}")

        if payload.get('_id') and payload['path'][-1] == payload['_id']:
            parent_folder_id = payload['path'][-2]
        else:
            parent_folder_id = payload['path'][-1]
        ticker = payload.get('ticker')
        # get exchange folder _id from path (Root -> Future -> EXCHANGE), check its name in tree_df
        exchange_df = tree_df[tree_df['_id'] == payload['path'][2]]
        if exchange_df.empty:
            raise NoInstrumentError(
                f"Bad path: exchange folder with _id {payload['path'][2]} is not found"
            )
        exchange = exchange_df.iloc[0]['name']
        parent_folder = asyncio.run(sdb.get(parent_folder_id))
        if not parent_folder or not parent_folder.get('isAbstract'):
            raise NoInstrumentError(f"Bad {parent_folder_id=}")
        reference, series_tree = Derivative._find_series(
            ticker,
            exchange,
            parent_folder_id,
            sdb=sdb,
            tree_df=tree_df,
            env=env
        )

        if reference.get('spreadType') == 'REVERSE' or payload.get('spreadType') == 'REVERSE':
            calendar_type = 'REVERSE'
        else:
            calendar_type = 'FORWARD'

        return cls(
            ticker=ticker,
            exchange=exchange,
            instrument=payload,
            reference=deepcopy(reference),
            series_tree=series_tree,
            calendar_type=calendar_type,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )

    def __set_contracts(self, series_tree: list[dict]):
        contracts: list[SpreadExpiration] = [
            SpreadExpiration.from_dict(self, instrument=x, reference=x) for x
            in series_tree
            if x['path'][:-1] == self.instrument['path']
            and not x['isAbstract']
        ]
        gap_folders = []
        if self.spread_type in ['CALENDAR', 'CALENDAR_SPREAD']:
            gap_folders = [
                x for x
                in series_tree
                if x['path'][:-1] == self.instrument['path']
                and x['isAbstract']
                and re.match(r'\d{1,2} month', x['name'])
            ]
            for gf in gap_folders:
                contracts.extend([
                    SpreadExpiration.from_dict(self, instrument=x, reference=x) for x
                    in series_tree
                    if x['path'][:-1] == gf['path']
                    and not x['isAbstract']
                ])
        return contracts, gap_folders

    def __set_gap_folders(self):
        leg_futures: list[FutureExpiration] = []
        tickers = self.ticker.split('-')
        for leg_ticker in tickers:
            try:
                future = Future.from_sdb(
                    leg_ticker,
                    self.exchange,
                    env=self.env,
                    reload_cache=False
                )
                leg_futures += future.contracts
            except Exception as e:
                self.logger.error(
                    f"{self.ticker}.{self.exchange}: {e.__class__.__name__}: {e}"
                )
                raise NoInstrumentError(
                    f'{leg_ticker}.{self.exchange} '
                    'futures are not found in sdb! Create them in first place'
                )
        return leg_futures

    def find_product_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None,
            uuid: str = None
        ):
        """
        Find existing expiration in self.contracts
        :param expiration: expiration date as ISO-string or datetime object
        :param maturity: maturity as date-like string (2022-08) or some kind of symbolic (Q2022, Q22)
        :param uuid: _id of sdb instrument
        :return: tuple of FutureExpiration object if found and corresponding Future object.
            None, None if more than one expiration in contracts satisfies searching criteria
        """
        if uuid:
            present_expiration = next(
                (
                    num for num, x
                    in enumerate(self.contracts)
                    if x.instrument['_id'] == uuid
                ),
                None
            )
            if present_expiration is None:
                self.logger.warning(
                    f"{self.series_name}: "
                    f"expiration with _id={uuid} is not found!"
                )
            return present_expiration, self
        expiration_date = self.normalize_date(expiration)        
        # prepare maturity as YYYY-MM
        maturity = self.format_maturity(maturity)
        # symbolic_maturity as Z2021
        symbol_id = f"{self.series_name}.{self._maturity_to_symbolic(maturity)}"
        present_expirations = [
            num for num, x
            in enumerate(self.contracts)
            if (
                x.expiration == expiration_date
                or x.maturity == maturity
                or x.contract_name == symbol_id  # expiration: Z2021
            ) and x.instrument.get('isTrading') is not False
        ]
        if len(present_expirations) == 1:
            return present_expirations[0], self
        elif len(present_expirations) > 1:
            self.logger.error(
                'More than one expiration have been found, try to narrow search criteria'
            )
            return None, None
        # if nothing is found, search in future.contracts
        return None, self

    def find_calendar_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            near_maturity: str = None,
            far_maturity: str = None,
            uuid: str = None
        ):
        if uuid:
            present_expiration = next(
                (
                    num for num, x
                    in enumerate(self.contracts)
                    if x.instrument['_id'] == uuid
                ),
                None
            )
            if present_expiration is None:
                self.logger.warning(
                    f"{self.series_name}: "
                    f"expiration with _id={uuid} is not found!"
                )
            return present_expiration, self
        expiration_date = self.normalize_date(expiration)        
        # prepare maturity as YYYY-MM
        near_maturity = self.format_maturity(near_maturity)
        far_maturity = self.format_maturity(far_maturity)
        near_symbolic = self._maturity_to_symbolic(near_maturity)
        far_symbolic = self._maturity_to_symbolic(far_maturity)
        # symbolic_maturity as Z2021
        symbol_id_fwd = f"{self.series_name}.CS/{near_symbolic}-{far_symbolic}"
        symbol_id_rev = f"{self.series_name}.RS/{near_symbolic}-{far_symbolic}"
        
        present_expirations = [
            num for num, x
            in enumerate(self.contracts)
            if (
                (
                    (
                        x.expiration == expiration_date
                        or x.near_maturity == near_maturity
                    ) and x.far_maturity == far_maturity
                )
                or x.contract_name == symbol_id_fwd
                or x.contract_name == symbol_id_rev
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
        """
        Add new expiration (if it does not exist in sdb) or update existing expiration with given dict
        If self.spread_type == 'SPREAD':
        mandatory fields: {
            'expiry': {
                'year': int,
                'month': int,
                'day': int
            },
            'maturityDate': {
                'year': int,
                'month': int
            }
        }
        If self.spread_type == 'CALENDAR_SPREAD':
        mandatory fields: {
            'expiry': {
                'year': int,
                'month': int,
                'day': int
            },
            'nearMaturityDate': {
                'year': int,
                'month': int
            }
            'farMaturityDate': {
                'year': int,
                'month': int
            }
        }
        :param payload: dict to create/update expiration
        :param skip_if_exists: if True create new expirations only, do nothing if contract already exists
        :param owerwrite_old: if True replace all data in existing contract with given one (except for _id and _rev)
            else update existing contract with given data (other fields stay unmodified)
        :return: dict {'created': symbolId} in case of creation or dict {'updated': symbolId, 'diff': diff} in case of update existing
        """
        if not payload.get('expiry'):
            self.logger.error(f"Bad data: {pformat(payload)}")
            return {}
        exp_date = self.normalize_date(payload['expiry'])
        
        leg_gap = None
        if self.spread_type == 'SPREAD':
            if not payload.get('maturityDate'):
                self.logger.error(f"Bad data: {pformat(payload)}")
                return {}
        # get expiration date
            maturity = self.format_maturity(payload['maturityDate'])
            existing_exp, series = self.find_product_expiration(exp_date, maturity, payload.get('_id'))
        elif self.spread_type == 'CALENDAR_SPREAD':
            if not payload.get('nearMaturityDate') or not payload.get('farMaturityDate'):
                self.logger.error(f"Bad data: {pformat(payload)}")
                return {}
            if 'leg_gap' in payload:
                leg_gap = payload.pop('leg_gap')
            near_maturity = self.format_maturity(payload['nearMaturityDate'])
            far_maturity = self.format_maturity(payload['farMaturityDate'])
            existing_exp, series = self.find_calendar_expiration(
                exp_date,
                near_maturity,
                far_maturity,
                payload.get('_id')
            )
        else:
            raise RuntimeError(f'Wrong {self.spread_type=}, should be SPREAD or CALENDAR_SPREAD')

        if existing_exp is not None:
            if skip_if_exists:
                self.skipped.add(exp_date.isoformat())
                return {}
            series.contracts[existing_exp].leg_gap = leg_gap
            if not payload.get('path'):
                payload['path'] = series.contracts[existing_exp].instrument['path']
            elif payload['path'][:len(series.instrument['path'])] != series.instrument['path']:
                self.logger.error(f"Bad path: {self.sdbadds.show_path(payload['path'])}")
                return {}

            if overwrite_old:
                payload.update({
                    key: val for key, val
                    in series.contracts[existing_exp].instrument.items()
                    if key[0] == '_' or key == 'path'
                })
                series.contracts[existing_exp].instrument = payload
            else:
                series.contracts[existing_exp].instrument.update(payload)
            if series.contracts[existing_exp].instrument.get('isTrading'):
                series.contracts[existing_exp].instrument.pop('isTrading')
            series.contracts[existing_exp].set_la_lt()
            diff = series.contracts[existing_exp].get_diff()
            if diff:
                self.logger.info(
                    f'{series.contracts[existing_exp].contract_name}: '
                    'following changes have been made:'
                )
                self.logger.info(pformat(diff))
            return {
                'updated': series.contracts[existing_exp].contract_name,
                'diff': diff
            }

        if series.allowed_expirations:
            if self.spread_type == 'SPREAD':
                symbolic = self._maturity_to_symbolic(maturity)
            elif self.spread_type == 'CALENDAR_SPREAD':
                near = self._maturity_to_symbolic(near_maturity)
                far = self._maturity_to_symbolic(far_maturity)
                # symbolic_maturity as Z2021
                symbolic = f"{near}-{far}"
            else:
                symbolic = ''
            if not (
                    exp_date.isoformat() in series.allowed_expirations
                    or symbolic in series.allowed_expirations
                ):
                self.logger.info(
                    f"Allowed expirations are set and {exp_date.isoformat()} "
                    f"or {symbolic} are not there"
                )
                return {}
        if not payload.get('path'):
            payload['path'] = series.instrument['path']
            if self.gap_folders:
                try:
                    days_delta =  (
                        dt.date.fromisoformat(f"{far_maturity}-01") - \
                        dt.date.fromisoformat(f"{near_maturity}-01")
                    ).days
                    month_gap = int(round(days_delta / 30.41, 0)) # 30.41 = 365 / 12
                    gap_folder = next(
                        (
                            x for x in self.gap_folders
                            if re.search(r'(?P<month>\d{1,2}) month', x['name'])
                            and int(re.search(r'(?P<month>\d{1,2}) month', x['name']).group('month')) == month_gap
                        ), None)
                    if gap_folder:
                        payload['path'].append(gap_folder['_id'])
                    else:
                        payload['path'].append(f'<<{month_gap} month folder>>')
                except Exception:
                    self.logger.error(
                        "Cannot determine month gap"
                    )

        elif payload['path'][:len(series.instrument['path'])] != series.instrument['path']:
            self.logger.error(f"Bad path: {self.sdbadds.show_path(payload['path'])}")
            return {}

        new_contract = SpreadExpiration.from_dict(
            series,
            payload,
            leg_gap=leg_gap
        )
        new_contract.set_la_lt(series, new_contract.instrument)
        if new_contract in series.new_expirations:
            self.logger.warning(
                f"{new_contract} is already in list of new expirations. "
                "Replacing it with newer version")
            series.new_expirations.remove(new_contract)
        series.new_expirations.append(new_contract)
        return {'created': new_contract.contract_name}

    def add(
            self,
            exp_date: Union[str, dt.date, dt.datetime],
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            leg_gap: int = None,
            uuid: str = None,
            skip_if_exists: bool = True,
            overwrite_old: bool = False,
            **kwargs
        ):
        """
        Create and add new expiration (if it does not exist in sdb) using given expiration date, maturity and other custom params
        or update existing expiration with custom params
        :param exp_date: expiration date (ISO str or date object)
        :param maturity: maturity str as numeric (like '2022-08') or symbolic (like Q2022), use only for self.spread_type == 'SPREAD'
        :param near_maturity: maturity str as numeric (like '2022-08') or symbolic (like Q2022), use only for self.spread_type == 'CALENDAR_SPREAD'
        :param far_maturity: maturity str as numeric (like '2022-08') or symbolic (like Q2022), use only for self.spread_type == 'CALENDAR_SPREAD'
        :param uuid: _id of existing expiration
        :param skip_if_exists: if True create new expirations only, do nothing if contract already exists
        :param owerwrite_old: if True replace all data in existing contract with created from scratch (except for _id and _rev)
            else update existing contract with given custom params in kwargs (other fields stay unmodified)
        :return: dict {'created': symbolId} in case of creation or dict {'updated': symbolId, 'diff': diff} in case of update existing
        """
        if self.spread_type == 'SPREAD':
            existing_exp, series = self.find_product_expiration(exp_date, maturity, uuid=uuid)
        elif self.spread_type == 'CALENDAR_SPREAD':
            existing_exp, series = self.find_calendar_expiration(
                exp_date,
                near_maturity,
                far_maturity,
                uuid=uuid
            )
        if existing_exp is not None:
            if skip_if_exists:
                self.skipped.add(exp_date)
                return {}
            if not overwrite_old:
                for field, val in kwargs.items():
                    series.contracts[existing_exp].set_field_value(val, field.split('/'))
            else:
                kwargs.update({
                    key: val for key, val
                    in series.contracts[existing_exp].instrument.items()
                    if key[0] == '_' or key == 'path'
                })
                series.contracts[existing_exp] = SpreadExpiration.from_scratch(
                    series,
                    expiration_date=exp_date,
                    maturity=maturity,
                    near_maturity=near_maturity,
                    far_maturity=far_maturity,
                    leg_gap=leg_gap,
                    reference=series.contracts[existing_exp].reference
                    **kwargs
                )
            diff = series.contracts[existing_exp].get_diff()
            if diff:
                self.logger.info(
                    f'{series.contracts[existing_exp].contract_name}: '
                    'following changes have been made:'
                )
                self.logger.info(pformat(diff))
                return {
                    'updated': series.contracts[existing_exp].contract_name,
                    'diff': diff
                }
            else:
                self.logger.info(f'No new data for existing expiration: {exp_date}')
                return {}
        if series.allowed_expirations:
            if self.spread_type == 'SPREAD':
                symbolic = self._maturity_to_symbolic(maturity)
            elif self.spread_type == 'CALENDAR_SPREAD':
                near = self._maturity_to_symbolic(near_maturity)
                far = self._maturity_to_symbolic(far_maturity)
                # symbolic_maturity as Z2021
                symbolic = f"{near}-{far}"
            else:
                symbolic = ''
            if not (
                    exp_date.isoformat() in series.allowed_expirations
                    or symbolic in series.allowed_expirations
                ):
                self.logger.info(
                    f"Allowed expirations are set and {exp_date.isoformat()} "
                    f"or {symbolic} are not there"
                )
                return {}

        new_contract = SpreadExpiration.from_scratch(
            self,
            exp_date,
            maturity,
            near_maturity,
            far_maturity,
            leg_gap,
            **kwargs
        )
        if new_contract in self.new_expirations:
            self.logger.warning(
                f"{new_contract} is already in list of new expirations. "
                "Replacing it with newer version")
            self.new_expirations.remove(new_contract)
        self.new_expirations.append(new_contract)
        return {'created': new_contract.contract_name}

    def create_gap_folder(self, gap: int, sibling_folder: dict, dry_run: bool):
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
            return None
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
                in new_folder.items()
                if key in self.tree_df.columns
            }], index=[new_folder['_id']])
            pd.concat([self.tree_df, new_record])
            self.tree_df.replace({np.nan: None})
        return new_folder

    def replace_id_fillers(self, dry_run: bool):
        if dry_run:
            return None
        for contract in self.new_expirations + self.contracts:
            series_filler = next((
                num for num, x
                in enumerate(contract.instrument['path'])
                if x == '<<series_id>>'
            ), None)
            if series_filler is not None:
                contract.instrument['path'][series_filler] = self.instrument['_id']
            gap_folder_filler = next((
                (num, int(re.match(r'<<(\d{1,2}) month folder>>', x).groups()[0])) for num, x
                in enumerate(contract.instrument['path'])
                if re.match(r'<<(\d{1,2}) month folder>>', x)
            ), (None, None))
            if gap_folder_filler[0] is not None:
                gap_folder = next((
                    x for x
                    in self.gap_folders
                    if (re.search(rf'0{gap_folder_filler[1]}', x) and gap_folder_filler < 10)
                    or (re.search(rf'{gap_folder_filler[1]}', x) and gap_folder_filler >= 10)
                ), None)
                if not gap_folder:
                    self.logger.error(
                        f'Gap folder for {gap_folder_filler[1]} does not exist in sdb!'
                    )
                    self.logger.error(
                        f'Contract {contract} will not be created (or updated)'
                    )
                    contract.instrument = {}
                contract.instrument['path'][gap_folder_filler[0]] = gap_folder['_id']
        self.new_expirations = [x for x in self.new_expirations if x.instrument]
        self.contracts = [x for x in self.contracts if x.instrument]


    def post_to_sdb(self, dry_run=True) -> dict:
        """
        · creates (if doesn't exist in sdb) or updates (if there is a diff relative to the self.reference) the series folder from self.instrument dict
        · updates existing expirations from self.contracts if there is some diff between contract.reference and contract.instrument
        · creates new expirations from self.new_expirations on base of FutureExpiration().instrument dict
        :param dry_run: if True prints out what is to be created and updated, post nothing to sdb
        :return: dict
        """
        create_result = ''
        update_result = ''
        report = {}
        try_again_series = False
        diff = DeepDiff(self.reference, self.instrument)
        # Create folder if need
        if not self.instrument.get('_id'):
            self.create(dry_run)
        elif diff:
            self.update(diff, dry_run)
        else:
            self.logger.info(f"{self.series_name}.*: No changes have been made")

        gap_folders_to_create = set([
            x.instrument['path'][-1] for x
            in self.new_expirations
            if re.match(r'<<\d{1,2} month folder>>', x.instrument['path'][-1])
        ] + [
            x.instrument['path'][-2] for x
            in self.contracts
            if re.match(r'<<\d{1,2} month folder>>', x.instrument['path'][-2])
        ])
        for gf in gap_folders_to_create:
            month_gap = int(re.match(r'<<(\d{1,2}) month folder>>', gf).groups()[0])
            new_gap_folder = self.create_gap_folder(
                month_gap,
                self.gap_folders[0],
                dry_run
            )
            if new_gap_folder:
                self.gap_folders.append(new_gap_folder)
        self.replace_id_fillers(dry_run)
        update_expirations = [
            x for x
            in self.contracts
            if x.expiration >= dt.date.today()
            and x.get_diff()
        ]

        if dry_run:
            if self.new_expirations:
                print(f"Dry run, new expirations to create:")
                pp([x.contract_name for x in self.new_expirations])
            if update_expirations:
                print(f"Dry run, expirations to update:")
                pp([x.contract_name for x in update_expirations])
            return {}
        if self.new_expirations:
            create_result = asyncio.run(self.sdb.batch_create(
                input_data=[x.instrument for x in self.new_expirations]
            ))
            if create_result:
                if isinstance(create_result, str):
                    create_result = json.loads(create_result)
                self.logger.error(
                    f'problems with creating new expirations: {pformat(create_result)}'
                )
                report.setdefault(self.series_name, {}).update({
                    'create_error': create_result.get('description')
                })
            else:
                report.setdefault(self.series_name, {}).update({
                    'created': [x.contract_name for x in self.new_expirations]
                })
        if update_expirations:
            update_result = asyncio.run(self.sdb.batch_update(
                input_data=[x.instrument for x in update_expirations]
            ))
            if update_result:
                if isinstance(update_result, str):
                    update_result = json.loads(update_result)
                self.logger.error(
                    f'problems with updating expirations: {pformat(update_result)}'
                )
                report.setdefault(self.series_name, {}).update({
                    'update_error': update_result.get('description')
                })
            else:
                report.setdefault(self.series_name, {}).update({
                    'updated': [x.contract_name for x in update_expirations],
                })
        if report and try_again_series:
            response = asyncio.run(self.sdb.update(self.instrument))
            if response.get('message'):
                self.logger.error(f'instrument {self.ticker} is not updated:')
                self.logger.error(pformat(response))
        if not dry_run:
            self.clean_up_times()
        return report


class SpreadExpiration(Instrument):
    def __init__(
            self,
            spread: Spread,
            instrument: dict,
            reference: dict = None,
            leg_gap: int = None,
            reload_cache: bool = False,
            **kwargs
        ):
        self.ticker = spread.ticker
        self.first_ticker = spread.first_ticker
        self.second_ticker = spread.second_ticker
        self.exchange = spread.exchange
        self.series_name = spread.series_name
        self.spread_type = spread.spread_type
        self.leg_futures = spread.leg_futures
        self.leg_gap = leg_gap
        self.instrument = instrument
        if self.instrument.get('spreadType'):
            self.calendar_type = self.instrument['spreadType']
        elif spread.calendar_type:
            self.calendar_type = spread.calendar_type
        else:
            self.calendar_type = self.compiled_parent.get('spreadType')
        if reference is None:
            reference = {}
        self.reference = reference
        self.expiration = self.normalize_date(instrument['expiry'])

        self.maturity = self.format_maturity(instrument.get('maturityDate'))
        self.near_maturity = self.format_maturity(instrument.get('nearMaturityDate'))
        self.far_maturity = self.format_maturity(instrument.get('farMaturityDate'))

        super().__init__(
            instrument=instrument,
            instrument_type=self.spread_type,
            parent=spread,
            env=spread.env,
            sdb=spread.sdb,
            sdbadds=spread.sdbadds,
            reload_cache=reload_cache
        )
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))

    @classmethod
    def from_scratch(
            cls,
            spread: Spread,
            expiration_date: Union[str, dt.date, dt.datetime],
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            calendar_type: str = None,
            leg_gap: int = None,

            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        if not reference:
            reference = {}
        expiration = Instrument.normalize_date(expiration_date)
        maturity = Instrument.format_maturity(maturity)
        near_maturity = Instrument.format_maturity(near_maturity)
        far_maturity = Instrument.format_maturity(far_maturity)
        instrument = SpreadExpiration.create_expiration_dict(
            spread,
            expiration,
            maturity,
            near_maturity,
            far_maturity,
            calendar_type,
            **kwargs
        )
        return cls(
            spread,
            instrument,
            deepcopy(reference),
            leg_gap=leg_gap,
            reload_cache=reload_cache,
            **kwargs
        )

    @classmethod
    def from_dict(
            cls,
            spread: Spread,
            instrument: dict,
            leg_gap: int = None,
            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        if not reference:
            reference = {}
        if instrument.get('isTrading') is not None:
            instrument.pop('isTrading')
        if not instrument.get('path'):
            instrument['path'] = spread.instrument['path']
            if spread.spread_type == 'CALENDAR_SPREAD':
                near_maturity = Instrument.format_maturity(instrument.get('nearMaturityDate'))
                far_maturity = Instrument.format_maturity(instrument.get('farMaturityDate'))
                gf_id = SpreadExpiration.get_gap_folder_id(
                    spread,
                    near_maturity,
                    far_maturity
                )
                if gf_id:
                    instrument['path'].append(gf_id)
        else:
            if instrument['path'][:len(spread.instrument['path'])] != spread.instrument['path']:
                raise ExpirationError(
                    f"Bad path: {spread.sdbadds.show_path(instrument['path'])}"
                )
        return cls(
            spread,
            instrument,
            deepcopy(reference),
            leg_gap=leg_gap,
            reload_cache=reload_cache,
            **kwargs
        )

    def __repr__(self):
        if self.spread_type == 'SPREAD':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.{self._maturity_to_symbolic(self.maturity)}, "
                f"{self.expiration.isoformat()})"
            )
        elif self.spread_type == 'CALENDAR_SPREAD' and self.calendar_type == 'FORWARD':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.CS/"
                f"{self._maturity_to_symbolic(self.near_maturity)}-{self._maturity_to_symbolic(self.far_maturity)}, "
                f"{self.expiration.isoformat()}, {self.calendar_type=})"
            )
        elif self.spread_type == 'CALENDAR_SPREAD' and self.calendar_type == 'REVERSE':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.RS/"
                f"{self._maturity_to_symbolic(self.near_maturity)}-{self._maturity_to_symbolic(self.far_maturity)}, "
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

    @property
    def contract_name(self):
        if self.spread_type == 'CALENDAR_SPREAD':
            symbolic_date = f"{self._maturity_to_symbolic(self.near_maturity)}-{self._maturity_to_symbolic(self.far_maturity)}"
            if self.calendar_type == 'REVERSE':
                return f"{self.ticker}.{self.exchange}.RS/{symbolic_date}"
            return f"{self.ticker}.{self.exchange}.CS/{symbolic_date}"
        else:
            symbolic_date = f"{self._maturity_to_symbolic(self.maturity)}"
            return f"{self.ticker}.{self.exchange}.{symbolic_date}"

    @staticmethod
    def create_expiration_dict(
            spread: Spread,
            expiration: dt.date,
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            calendar_type: str = None,
            **kwargs
        ) -> dict:
        instrument = {
            'isAbstract': False,
            'expiry': {
                'year': expiration.year,
                'month': expiration.month,
                'day': expiration.day
            },
            'path': deepcopy(spread.instrument['path'])
        }
        if spread.instrument['path'][-1] != spread.instrument.get('_id'):
            instrument['path'].append('<<series_id>>')

        if spread.spread_type == 'CALENDAR_SPREAD':
            if not near_maturity or not far_maturity:
                raise ExpirationError(
                    f'Both {near_maturity=} and {far_maturity=} '
                    'should be set for a calendar spread contract'
                )
            instrument.update({
                'nearMaturityDate': {
                    'month': int(near_maturity.split('-')[1]),
                    'year': int(near_maturity.split('-')[0])
                },
                'farMaturityDate': {
                    'month': int(far_maturity.split('-')[1]),
                    'year': int(far_maturity.split('-')[0])
                },
                'name': f"{near_maturity} {far_maturity}"
            })
            if calendar_type is not None and calendar_type != spread.calendar_type:
                instrument.update({
                    'spreadType': calendar_type
                })
            gf_id = SpreadExpiration.get_gap_folder_id(
                spread,
                near_maturity,
                far_maturity
            )
            if gf_id:
                instrument['path'].append(gf_id)

            legs = SpreadExpiration.mk_calendar_legs(
                spread,
                near_maturity,
                far_maturity,
                calendar_type
            )
        else:
            if not maturity:
                raise ExpirationError(
                    f'{maturity=} should be set for a product spread contract'
                )
            instrument.update({
                'maturityDate': {
                    'month': int(maturity.split('-')[1]),
                    'year': int(maturity.split('-')[0])
                },
                'name': maturity
            })
            legs = SpreadExpiration.mk_product_legs(
                spread,
                maturity
            )
            
        if not legs:
            raise ExpirationError(
                f'Legs are not set, cannot create contract'
            )
        instrument['legs'] = legs
        [
            instrument.update({key: val}) for key, val
            in kwargs.items() if len(key.split('/')) == 1
        ]
        SpreadExpiration.set_la_lt(spread, instrument)
        return instrument

    @staticmethod
    def set_la_lt(spread: Spread, instrument: dict):
        if spread.set_la:
            instrument['lastAvailable'] = spread.sdb.date_to_sdb(
                spread.sdb.sdb_to_date(instrument['expiry']) + dt.timedelta(days=3)
            )
            if isinstance(spread.set_la, str):
                instrument['lastAvailable']['time'] = spread.set_la
            

        if spread.set_lt:
            instrument['lastTrading'] = deepcopy(instrument['expiry'])
            if isinstance(spread.set_lt, str):
                instrument['lastTrading']['time'] = spread.set_lt

    @staticmethod
    def get_gap_folder_id(
            spread: Spread,
            near_maturity: str,
            far_maturity: str
        ):
        days_delta =  (
            dt.date.fromisoformat(f"{far_maturity}-01") - \
            dt.date.fromisoformat(f"{near_maturity}-01")
        ).days
        month_gap = int(round(days_delta / 30.41, 0)) # 30.41 = 365 / 12
        gap_folder = next(
            (
                x for x in spread.gap_folders
                if re.search(r'(?P<month>\d{1,2}) month', x['name'])
                and int(re.search(
                    r'(?P<month>\d{1,2}) month',
                    x['name']).group('month')
                ) == month_gap
            ), None)
        if gap_folder:
            return gap_folder['_id']
        elif spread.gap_folders:
            return f'<<{month_gap} month folder>>'

    @staticmethod
    def mk_product_legs(
            spread: Spread,
            maturity: str
        ):
        first_leg = next((
            x for x
            in spread.leg_futures
            if x.ticker == spread.first_ticker
            and x.maturity == maturity
        ), None)
        second_leg = next((
            x for x
            in spread.leg_futures
            if x.ticker == spread.second_ticker
            and x.maturity == maturity
        ), None)
        if first_leg and second_leg:
            legs = [
                {
                    'quantity': 1,
                    'exanteId': first_leg.contract_name
                },
                {
                    'quantity': -1,
                    'exanteId': second_leg.contract_name
                }
            ]
            return legs
        elif not first_leg:
            spread.logger.error(
                f'{spread.first_ticker}.{spread.exchange}.{spread._maturity_to_symbolic(maturity)} '
                'future is not found in sdb!'
            )
        elif not second_leg:
            spread.logger.error(
                f'{spread.second_ticker}.{spread.exchange}.{spread._maturity_to_symbolic(maturity)} '
                'future is not found in sdb!'
            )
        return None

    @staticmethod
    def mk_calendar_legs(
            spread: Spread,
            near_maturity: str,
            far_maturity: str,
            calendar_type: str = None
        ):
        if calendar_type is None:
            calendar_type = spread.calendar_type
        first_leg = next((
            x for x
            in spread.leg_futures
            if x.maturity == near_maturity
        ), None)
        second_leg = next((
            x for x
            in spread.leg_futures
            if x.maturity == far_maturity
        ), None)
        if first_leg and second_leg:
            if spread.calendar_type == 'FORWARD':
                legs = [
                    {
                        'quantity': 1,
                        'exanteId': first_leg.contract_name
                    },
                    {
                        'quantity': -1,
                        'exanteId': second_leg.contract_name
                    }
                ]
            elif spread.calendar_type == 'REVERSE':
                legs = [
                    {
                        'quantity': -1,
                        'exanteId': first_leg.contract_name
                    },
                    {
                        'quantity': 1,
                        'exanteId': second_leg.contract_name
                    }
                ]
            return legs
        elif not first_leg:
            spread.logger.error(
                f'{spread.series_name}.{spread._maturity_to_symbolic(near_maturity)} '
                'future is not found in sdb!'
            )
        elif not second_leg:
            spread.logger.error(
                f'{spread.series_name}.{spread._maturity_to_symbolic(far_maturity)} '
                'future is not found in sdb!'
            )
        return None

    def get_diff(self) -> dict:
        return DeepDiff(self.reference, self.instrument)

    def get_expiration(self) -> tuple[dict, str]:
        return self.instrument, self.contract_name