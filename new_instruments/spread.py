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
from libs.backoffice import BackOffice
from libs.replica_sdb_additional import SDBAdditional
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
            series_tree: list[dict] = None,
            calendar_type: str = 'FORWARD',
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env: str = 'prod'
        ):
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
        self.env = env
        (
            self.bo,
            self.sdb,
            self.sdbadds
        ) = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env
        ).get_instances

        super().__init__(
            ticker=ticker,
            exchange=exchange,
            instrument_type=self.spread_type,
            instrument=instrument,
            reference=reference,
            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            calendar_type=self.calendar_type
        )
        self.skipped = set()
        self.allowed_expirations = []

        self.new_expirations: list[SpreadExpiration] = []
        self.leg_futures: list[FutureExpiration] = []
        self.series_tree = series_tree
        self.contracts, self.gap_folders = self.__set_contracts(series_tree)
        self.leg_futures = self.__set_leg_futures()
        self._align_expiry_la_lt()

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
            env: str = 'prod'
        ):
        bo, sdb, sdbadds = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env
        ).get_instances
        if not parent_folder_id:
            parent_folder_id = get_uuid_by_path(
                ['Root', 'SPREAD', exchange],
                sdbadds.engine
            )
            if not parent_folder_id:
                raise NoExchangeError(f'{exchange=} does not exist in SymbolDB')
            parent_folder_id = sdbadds.uuid2str(parent_folder_id)
        instrument, series_tree = Derivative._find_series(
            ticker,
            parent_folder_id,
            sdb=sdb,
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
            env: str = 'prod',
            **kwargs
        ):
        bo, sdb, sdbadds = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env
        ).get_instances
        if not parent_folder_id:
            parent_folder_id = get_uuid_by_path(
                ['Root', 'SPREAD', exchange],
                sdbadds.engine
            )
            if not parent_folder_id:
                raise NoExchangeError(f'{exchange=} does not exist in SymbolDB')
            parent_folder_id = sdbadds.uuid2str(parent_folder_id)
        parent_folder = asyncio.run(sdb.get(parent_folder_id))
        if not parent_folder or not parent_folder.get('isAbstract'):
            raise NoInstrumentError(f"Bad {parent_folder_id=}")
        reference, series_tree = Derivative._find_series(
            ticker,
            parent_folder_id,
            sdb=sdb,
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
            recreate: bool = False,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env: str = 'prod',
        ):
        bo, sdb, sdbadds = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env
        ).get_instances
        if len(payload['path']) < 3:
            raise NoInstrumentError(f"Bad path: {payload.get('path')}")
        check_parent_df = pd.read_sql(
            'SELECT id as _id, path '
            'FROM instruments '
            f"WHERE id = '{payload['path'][-2 if payload.get('_id') else -1]}'",
            sdbadds.engine
        )
        if check_parent_df.empty:
            raise NoInstrumentError(f"Bad path: {payload.get('path')}")
        parent_path = [
            sdbadds.uuid2str(x) for x
            in check_parent_df.iloc[0]['path']
        ]
        spread_fld_id = sdbadds.uuid2str(get_uuid_by_path(['Root', 'SPREAD'], sdbadds.engine))
        if not parent_path == payload['path'][:len(parent_path)]:
            raise NoInstrumentError(f"Bad path: {sdbadds.show_path(payload.get('path'))}")
        if payload['path'][1] != spread_fld_id:
            raise NoInstrumentError(f"Bad path: {sdbadds.show_path(payload.get('path'))}")

        if payload.get('_id') and payload['path'][-1] == payload['_id']:
            parent_folder_id = payload['path'][-2]
        else:
            parent_folder_id = payload['path'][-1]
        ticker = payload.get('ticker')
        # get exchange folder _id from path (Root -> Future -> EXCHANGE), check its name in tree_df
        exchange_df = pd.read_sql(
            'SELECT id as _id, "extraData" as extra '
            'FROM instruments '
            f"WHERE id = '{payload['path'][2]}'",
            sdbadds.engine
        )
        if exchange_df.empty:
            raise NoInstrumentError(
                f"Bad path: exchange folder with _id {payload['path'][2]} is not found"
            )
        exchange = exchange_df.iloc[0]['extra']['name']
        parent_folder = asyncio.run(sdb.get(parent_folder_id))
        if not parent_folder or not parent_folder.get('isAbstract'):
            raise NoInstrumentError(f"Bad {parent_folder_id=}")
        reference, series_tree = Derivative._find_series(
            ticker,
            parent_folder_id,
            sdb=sdb,
            env=env
        )
        if reference:
            if not recreate:
                raise RuntimeError(
                    f'{ticker}.{exchange} series already exist in SymbolDB'
                )
            payload.update({
                key: val for key, val
                in reference.items()
                if key[0] == '_' or key == 'path'
            })

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
        contracts: list[SpreadExpiration] = []
        gap_folders = []
        if self.spread_type in ['CALENDAR', 'CALENDAR_SPREAD']:
            gap_folders = [
                x for x
                in series_tree
                if x['path'][:-1] == self.instrument['path']
                and x['isAbstract']
                and re.match(r'\d{1,2} month', x['name'])
            ]
        for parent_folder in [self.instrument] + gap_folders:
            contract_dicts = [
                x for x
                in series_tree
                if x['path'][:-1] == parent_folder['path']
                and not x['isAbstract']
                and x.get('isTrading') is not False
            ]
            for item in contract_dicts:
                try:
                    contracts.append(
                        SpreadExpiration.from_dict(self, instrument=item, reference=item)
                    )
                except Exception as e:
                    # Don't bother with old shit
                    expiration_date = self.sdb.sdb_to_date(item.get('expiry', {}))
                    if expiration_date and dt.date.today() - expiration_date < dt.timedelta(days=1100): # ~3 years
                        raise e
                    message = f"{self.ticker}.{self.exchange}: {e.__class__.__name__}: {e}"
                    self.logger.info(message)
                    self.logger.info(
                        f"Cannot initialize contract {item['name']=}, {item['_id']=}."
                        "Anyway, it's too old to worry about"
                    )

        return sorted(contracts), gap_folders

    def __set_leg_futures(self):
        leg_futures: list[FutureExpiration] = []
        tickers = self.ticker.split('-')
        for leg_ticker in tickers:
            try:
                future = Future.from_sdb(
                    leg_ticker,
                    self.exchange,
                    bo=self.bo,
                    sdb=self.sdb,
                    sdbadds=self.sdbadds,
                    env=self.env
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
                    if x._id == uuid
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
                    if x._id == uuid
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

    def get_product_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None,
            uuid: str = None
        ):
        num, series = self.find_product_expiration(
            expiration,
            maturity=maturity,
            uuid=uuid
            )
        return series.contracts[num] if num is not None and series is not None else None

    def get_calendar_expiration(
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
        num, series = self.find_calendar_expiration(
            expiration,
            near_maturity=near_maturity,
            far_maturity=far_maturity,
            uuid=uuid
            )
        return series.contracts[num] if num is not None and series is not None else None

    def __update_existing_contract(
            self,
            series: 'Spread',
            num: int,
            calendar_type: str = None,
            leg_gap: int = None,
            overwrite_old: bool = False,
            payload: dict = None,
            **kwargs
        ):
        exp_date = series.contracts[num].expiration
        maturity = series.contracts[num].maturity
        near_maturity = series.contracts[num].near_maturity
        far_maturity = series.contracts[num].far_maturity
        if overwrite_old:
            if payload:
                leg_gap = payload.pop('leg_gap', None)
                if leg_gap:
                    series.contracts[num]._leg_gap = leg_gap
                if payload.get('spreadType'):
                    calendar_type = payload['spreadType']
                else:
                    calendar_type = series.calendar_type
                series.contracts[num]._calendar_type = calendar_type

                payload.update({
                    key: val for key, val
                    in series.contracts[num].instrument.items()
                    if key[0] == '_' or key in ['path']
                })
                series.contracts[num]._instrument = payload
            else:
                kwargs.update({
                    key: val for key, val
                    in series.contracts[num].instrument.items()
                    if key[0] == '_'
                })
                series.contracts[num] = SpreadExpiration.from_scratch(
                    series,
                    expiration_date=exp_date,
                    maturity=maturity,
                    near_maturity=near_maturity,
                    far_maturity=far_maturity,
                    calendar_type=calendar_type,
                    leg_gap=leg_gap,
                    reference=series.contracts[num].reference
                    **kwargs
                )
        elif payload:
            series.contracts[num]._instrument.update(payload)
        else:
            for field, val in kwargs.items():
                series.contracts[num].set_field_value(val, field.split('/'))

        series.contracts[num].instrument.pop('isTrading', None)
        diff = series.contracts[num].get_diff()
        if diff:
            self.logger.info(
                f'{series.contracts[num].contract_name}: '
                'following changes have been made:'
            )
            self.logger.info(pformat(diff))
        return {
            'updated': series.contracts[num].contract_name,
            'diff': diff
        }

    def __create_new_contract(
            self,
            series: 'Spread',
            exp_date: dt.date,
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            leg_gap: int = None,
            calendar_type: str = 'FORWARD',
            payload: dict = None,
            **kwargs
        ):
        if series.allowed_expirations:
            symbolic = ''
            if payload:
                if self.spread_type == 'SPREAD':
                    symbolic = self._maturity_to_symbolic(
                        self.format_maturity(
                            payload['maturityDate']
                        )
                    )
                elif self.spread_type == 'CALENDAR_SPREAD':
                    near = self._maturity_to_symbolic(
                        self.format_maturity(
                            payload['near_maturity']
                        )
                    )
                    far = self._maturity_to_symbolic(
                        self.format_maturity(
                            payload['far_maturity']
                        )
                    )
                    # symbolic_maturity as Z2021
                    symbolic = f"{near}-{far}"
            else:
                if self.spread_type == 'SPREAD':
                    symbolic = self._maturity_to_symbolic(maturity)
                elif self.spread_type == 'CALENDAR_SPREAD':
                    near = self._maturity_to_symbolic(near_maturity)
                    far = self._maturity_to_symbolic(far_maturity)
                    # symbolic_maturity as Z2021
                    symbolic = f"{near}-{far}"
            if not (
                    exp_date.isoformat() in series.allowed_expirations
                    or symbolic in series.allowed_expirations
                ):
                self.logger.info(
                    f"Allowed expirations are set and {exp_date.isoformat()} "
                    f"or {symbolic} are not there"
                )
                return {}
        if payload:
            leg_gap = payload.pop('leg_gap', None)
            new_contract = SpreadExpiration.from_dict(
                series,
                payload,
                leg_gap=leg_gap
            )
        else:
            new_contract = SpreadExpiration.from_scratch(
                series,
                expiration_date=exp_date,
                maturity=maturity,
                near_maturity=near_maturity,
                far_maturity=far_maturity,
                calendar_type=calendar_type,
                leg_gap=leg_gap,
                **kwargs
            )
        if new_contract in series.new_expirations:
            self.logger.warning(
                f"{new_contract} is already in list of new expirations. "
                "Replacing it with newer version")
            series.new_expirations.remove(new_contract)
        series.new_expirations.append(new_contract)
        return {'created': new_contract.contract_name}

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
        
        leg_gap = payload.pop('leg_gap', None)
        if self.spread_type == 'SPREAD':
            if not payload.get('maturityDate'):
                self.logger.error(f"Bad data: {pformat(payload)}")
                return {}
        # get expiration date
            maturity = self.format_maturity(payload['maturityDate'])
            near_maturity = None
            far_maturity = None
            existing_exp, series = self.find_product_expiration(
                exp_date,
                maturity,
                payload.get('_id')
            )
        elif self.spread_type == 'CALENDAR_SPREAD':
            if not payload.get('nearMaturityDate') or not payload.get('farMaturityDate'):
                self.logger.error(f"Bad data: {pformat(payload)}")
                return {}
            near_maturity = self.format_maturity(payload['nearMaturityDate'])
            far_maturity = self.format_maturity(payload['farMaturityDate'])
            maturity = None
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
            update = self.__update_existing_contract(
                series,
                existing_exp,
                leg_gap=leg_gap,
                overwrite_old=overwrite_old,
                payload=payload
            )
            return update
        create = self.__create_new_contract(
            series,
            exp_date=exp_date,
            maturity=maturity,
            near_maturity=near_maturity,
            far_maturity=far_maturity,
            leg_gap=leg_gap,
            payload=payload
        )
        return create

    def add(
            self,
            exp_date: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
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
        exp_date = self.normalize_date(exp_date)
        maturity = self.format_maturity(maturity)
        near_maturity = self.format_maturity(near_maturity)
        far_maturity = self.format_maturity(far_maturity)
        if self.spread_type == 'SPREAD':
            existing_exp, series = self.find_product_expiration(
                exp_date,
                maturity,
                uuid=uuid
            )
        elif self.spread_type == 'CALENDAR_SPREAD':
            existing_exp, series = self.find_calendar_expiration(
                exp_date,
                near_maturity,
                far_maturity,
                uuid=uuid
            )
        if existing_exp is not None:
            if skip_if_exists:
                self.skipped.add(series.contracts[existing_exp].contract_name)
                return {}
            update = self.__update_existing_contract(
                series,
                existing_exp,
                leg_gap=leg_gap,
                overwrite_old=overwrite_old,
                **kwargs
            )
            return update
        create = self.__create_new_contract(
            series,
            exp_date=exp_date,
            maturity=maturity,
            near_maturity=near_maturity,
            far_maturity=far_maturity,
            leg_gap=leg_gap,
            calendar_type=series.calendar_type,
            **kwargs
        )
        return create

    def create_gap_folder(self, gap: int, sibling_folder: dict, dry_run: bool):
        new_folder = {
            key: val for key, val
            in deepcopy(sibling_folder).items()
            if key[0] != '_'
        }
        new_folder['path'].pop(-1)

        str_gap = f'{gap:0>2}'
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
            self.wait_for_sdb()
            create = asyncio.run(self.sdb.create(new_folder))
            if not create.get('_id'):
                self.logger.error(pformat(create))
                return None
            self.logger.debug(f'Result: {pformat(create)}')
            new_folder['_id'] = create['_id']
            new_folder['_rev'] = create['_rev']
            new_folder['path'].append(create['_id'])
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
                    if re.search(rf'{gap_folder_filler[1]:0>2}', x)
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
        update_expirations = [
            x for x
            in self.contracts
            if x.expiration >= dt.date.today()
            and x.get_diff()
        ]
        self.reduce_instrument()
        diff = DeepDiff(self.reference, self.instrument)
        # Create folder if need
        if not self._id:
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
        # self.replace_id_fillers(dry_run)
        update_expirations = [
            x for x
            in self.contracts
            if x.expiration >= dt.date.today()
            and x.get_diff()
        ]

        if self.new_expirations and dry_run:
            print(f"Dry run, new expirations to create:")
            pp([x.contract_name for x in self.new_expirations])
            report.setdefault(self.series_name, {}).update({
                'to_create': [x.contract_name for x in self.new_expirations]
            })
        elif self.new_expirations:
            self.wait_for_sdb()
            create_result = asyncio.run(self.sdb.batch_create(
                input_data=[x.get_instrument for x in self.new_expirations]
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
        if update_expirations and dry_run:
            print(f"Dry run, expirations to update:")
            pp([x.contract_name for x in update_expirations])
            report.setdefault(self.series_name, {}).update({
                'to_update': [x.contract_name for x in update_expirations]
            })
        elif update_expirations:
            self.wait_for_sdb()
            update_result = asyncio.run(self.sdb.batch_update(
                input_data=[x.get_instrument for x in update_expirations]
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
        if report and try_again_series and not dry_run:
            self.wait_for_sdb()
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
            expiration: dt.date,
            instrument: dict,
            reference: dict = None,
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            calendar_type: str = None,
            leg_gap: int = None,
            **kwargs
        ):
        self.ticker = spread.ticker
        self.first_ticker = spread.first_ticker
        self.second_ticker = spread.second_ticker
        self.exchange = spread.exchange
        self.series_name = spread.series_name
        self.spread_type = spread.spread_type
        self.leg_futures = spread.leg_futures
        self.spread = spread

        self.expiration = expiration
        self.maturity = maturity
        self.near_maturity = near_maturity
        self.far_maturity = far_maturity
        self._leg_gap = leg_gap
        
        if self.instrument.get('spreadType'):
            self.calendar_type = self.instrument['spreadType']
        elif spread.calendar_type:
            self.calendar_type = spread.calendar_type
        else:
            self.calendar_type = self.compiled_parent.get('spreadType')
        self._instrument = instrument

        super().__init__(
            instrument=instrument,
            reference=reference,
            instrument_type=self.spread_type,
            parent=spread,
            env=spread.env,
            sdb=spread.sdb,
            sdbadds=spread.sdbadds
        )
        self.set_la_lt()
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))

    @classmethod
    def from_scratch(
            cls,
            spread: Spread,
            expiration_date: Union[
                str,
                dt.date,
                dt.datetime
            ],
            maturity: str = None,
            near_maturity: str = None,
            far_maturity: str = None,
            calendar_type: str = None,
            leg_gap: int = None,
            reference: dict = None,
            **kwargs
        ):
        if not reference:
            reference = {}
        expiration = Instrument.normalize_date(expiration_date)
        maturity = Instrument.format_maturity(maturity)
        near_maturity = Instrument.format_maturity(near_maturity)
        far_maturity = Instrument.format_maturity(far_maturity)
        if spread.spread_type == 'SPREAD' and not maturity:
            raise ExpirationError(f'maturity should be set for {spread.spread_type=}')
        if spread.spread_type == 'CALENDARSPREAD' \
            and not (near_maturity and far_maturity):
            raise ExpirationError(
                f'both near_maturity and far_maturity should be set for {spread.spread_type=}'
            )
        return cls(
            spread,
            expiration,
            maturity=maturity,
            near_maturity=near_maturity,
            far_maturity=far_maturity,
            instrument={},
            reference=deepcopy(reference),
            calendar_type=calendar_type,
            leg_gap=leg_gap,
            **kwargs
        )
    
    @classmethod
    def from_dict(
            cls,
            spread: Spread,
            instrument: dict,
            reference: dict = None,
            leg_gap: int = None,
            **kwargs
        ):
        if not reference:
            reference = {}
        instrument.pop('isTrading', None)
        expiration = Instrument.normalize_date(instrument.get('expiry', {}))
        maturity = Instrument.format_maturity(instrument.get('maturityDate', {}))
        near_maturity = Instrument.format_maturity(instrument.get('nearMaturityDate', {}))
        far_maturity = Instrument.format_maturity(instrument.get('farMaturityDate', {}))
        calendar_type = instrument.get('spreadType', spread.calendar_type)
        if spread.spread_type == 'SPREAD' and not maturity:
            raise ExpirationError(f'maturity should be set for {spread.spread_type=}')
        if spread.spread_type == 'CALENDARSPREAD' \
            and not (near_maturity and far_maturity):
            raise ExpirationError(
                f'both near_maturity and far_maturity should be set for {spread.spread_type=}'
            )
        return cls(
            spread,
            expiration=expiration,
            instrument=instrument,
            reference=deepcopy(reference),
            maturity=maturity,
            near_maturity=near_maturity,
            far_maturity=far_maturity,
            calendar_type=calendar_type,
            leg_gap=leg_gap,
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
                f"{self._maturity_to_symbolic(self.near_maturity)}-"
                f"{self._maturity_to_symbolic(self.far_maturity)}, "
                f"{self.expiration.isoformat()}, {self.calendar_type=})"
            )
        elif self.spread_type == 'CALENDAR_SPREAD' and self.calendar_type == 'REVERSE':
            return (
                f"SpreadExpiration({self.ticker}.{self.exchange}.RS/"
                f"{self._maturity_to_symbolic(self.near_maturity)}-"
                f"{self._maturity_to_symbolic(self.far_maturity)}, "
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
            symbolic_date = (
                f"{self._maturity_to_symbolic(self.near_maturity)}-"
                f"{self._maturity_to_symbolic(self.far_maturity)}"
            )
            if self.calendar_type == 'REVERSE':
                return f"{self.ticker}.{self.exchange}.RS/{symbolic_date}"
            return f"{self.ticker}.{self.exchange}.CS/{symbolic_date}"
        else:
            symbolic_date = f"{self._maturity_to_symbolic(self.maturity)}"
            return f"{self.ticker}.{self.exchange}.{symbolic_date}"

    @property
    def path(self):
        if self.spread.instrument.get('_id'):
            p = deepcopy(self.spread.instrument['path'])
        else:
            p = deepcopy(self.spread.instrument['path']) + ['<<series_folder_id>>']
        if self.spread.gap_folders:
            gf_id = self.get_gap_folder_id()
            if gf_id:
                p.append(gf_id)
        if not self.instrument.get('_id'):
            return p
        return p + [self.instrument['_id']]

    @property
    def get_custom_fields(self) -> dict:
        return {
            key: val for key, val
            in self._instrument.items()
            if key not in [
                'isAbstract',
                'name',
                'expiry',
                'maturityDate',
                'nearMaturityDate',
                'farMaturityDate',
                'path',
                'legs'
            ]
        }

    @property
    def legs(self):
        return self.mk_product_legs() if self.spread_type == 'SPREAD' else self.mk_calendar_legs()

    @property
    def get_instrument(self):
        instrument_dict = {
            'isAbstract': False,
            'expiry': {
                'year': self.expiration.year,
                'month': self.expiration.month,
                'day': self.expiration.day
            },
            'path': self.path,
            'legs': self.legs
        }
        if self.spread_type == 'SPREAD':
            instrument_dict.update({
                'name': self.maturity,
                'maturityDate': {
                    'month': int(self.maturity.split('-')[1]),
                    'year': int(self.maturity.split('-')[0])
                }
            })
        elif self.spread_type == 'CALENDAR_SPREAD':
            instrument_dict.update({
                'name': f"{self.near_maturity} {self.far_maturity}",
                'nearMaturityDate': {
                    'month': int(self.near_maturity.split('-')[1]),
                    'year': int(self.near_maturity.split('-')[0])
                },
                'farMaturityDate': {
                    'month': int(self.far_maturity.split('-')[1]),
                    'year': int(self.far_maturity.split('-')[0])
                }
            })

    def set_la_lt(self):
        if self.spread.set_la:
            self.set_field_value(
                self.sdb.date_to_sdb(
                    self.expiration + dt.timedelta(days=3)
                ),
                ['lastAvailable']
            )
            self.set_field_value(self.spread.set_la, ['lastAvailable', 'time'])
        if self.spread.set_lt:
            self.set_field_value(
                self.sdb.date_to_sdb(self.expiration),
                ['lastTrading']
            )
            self.set_field_value(self.spread.set_lt, ['lastTrading', 'time'])

    def get_gap_folder_id(self):
        days_delta =  (
            dt.date.fromisoformat(f"{self.far_maturity}-01") - \
            dt.date.fromisoformat(f"{self.near_maturity}-01")
        ).days
        month_gap = int(round(days_delta / 30.41, 0)) # 30.41 = 365 / 12
        gap_folder = next(
            (
                x for x in self.spread.gap_folders
                if re.search(r'(?P<month>\d{1,2}) month', x['name'])
                and int(re.search(
                    r'(?P<month>\d{1,2}) month',
                    x['name']).group('month')
                ) == month_gap
            ), None)
        if gap_folder:
            return gap_folder['_id']
        elif self.spread.gap_folders:
            return f'<<{month_gap} month folder>>'

    def mk_product_legs(self):
        first_leg = next((
            x for x
            in self.spread.leg_futures
            if x.ticker == self.spread.first_ticker
            and x.maturity == self.maturity
        ), None)
        second_leg = next((
            x for x
            in self.spread.leg_futures
            if x.ticker == self.spread.second_ticker
            and x.maturity == self.maturity
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
            self.logger.error(
                f'{self.spread.first_ticker}.{self.spread.exchange}.'
                f'{self.spread._maturity_to_symbolic(self.maturity)} '
                'future is not found in sdb!'
            )
        elif not second_leg:
            self.logger.error(
                f'{self.spread.second_ticker}.{self.spread.exchange}.'
                f'{self.spread._maturity_to_symbolic(self.maturity)} '
                'future is not found in sdb!'
            )
        return None

    def mk_calendar_legs(self):
        if calendar_type is None:
            calendar_type = self.spread.calendar_type
        first_leg = next((
            x for x
            in self.spread.leg_futures
            if x.maturity == self.near_maturity
        ), None)
        second_leg = next((
            x for x
            in self.spread.leg_futures
            if x.maturity == self.far_maturity
        ), None)
        if first_leg and second_leg:
            if self.spread.calendar_type == 'FORWARD':
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
            elif self.spread.calendar_type == 'REVERSE':
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
            self.logger.error(
                f'{self.spread.series_name}.'
                f'{self.spread._maturity_to_symbolic(self.near_maturity)} '
                'future is not found in sdb!'
            )
        elif not second_leg:
            self.logger.error(
                f'{self.spread.series_name}.'
                f'{self.spread._maturity_to_symbolic(self.far_maturity)} '
                'future is not found in sdb!'
            )
        return None

    def get_diff(self) -> dict:
        return DeepDiff(self.reference, self.instrument)

    def get_expiration(self) -> tuple[dict, str]:
        return self.instrument, self.contract_name