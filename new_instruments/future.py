import asyncio
import datetime as dt
import logging
import json
from copy import copy, deepcopy
from deepdiff import DeepDiff
from pandas import DataFrame
from libs.async_symboldb import SymbolDB
from libs.backoffice import BackOffice
from libs.async_sdb_additional import SDBAdditional
from pprint import pformat, pp
from typing import Union
from libs.new_instruments import (
    Instrument,
    InitThemAll,
    Derivative,
    ExpirationError,
    NoInstrumentError,
    NoExchangeError,
    get_uuid_by_path
)

class Future(Derivative):
    """
    usage:
    · if series exists in sdb and it's totally or mostly ok:
        use from_sdb constructor
    · if series does not exist in sdb and you have params to create it:
        use from_scratch constructor
    · if series does not exist in sdb and you have fully defined dict of document to create (including path):
        use from_dict constructor
    · if series exists in sdb and it should be recreated dropping all old settings:
        use from_scratch or from_dict constructor with recreate=True

    attrs:
    there are no specific attrs for Future, take a look on common attrs in Derivative class
    """


    def __init__(
            self,
            # series parameters
            ticker: str,
            exchange: str,
            instrument: dict = None,
            reference: dict = None,
            series_tree: list[dict] = None,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            tree_df: DataFrame = None,
            env: str = 'prod'
        ):
        self.ticker = ticker
        self.exchange = exchange
        (
            self.bo,
            self.sdb,
            self.sdbadds,
            self.tree_df
        ) = InitThemAll(
            bo,
            sdb,
            sdbadds,
            tree_df,
            env,
            reload_cache=False
        ).get_instances

        self.instrument_type = 'FUTURE'
        self.instrument = instrument
        super().__init__(
            ticker=ticker,
            exchange=exchange,
            instrument_type='FUTURE',
            instrument=self.instrument,
            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            tree_df=tree_df,
            reload_cache=False
        )
        if reference is None:
            reference = {}
        self.reference = reference
        self.skipped = set()
        self.allowed_expirations = []

        self.new_expirations: list[FutureExpiration] = []
        self.series_tree = series_tree
        self.contracts = self.__set_contracts(series_tree)
        self._align_expiry_la_lt(self.contracts)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")
    
    @property
    def series_name(self):
        return f"{self.ticker}.{self.exchange}"

    def __repr__(self):
        return f"Future({self.series_name})"


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
            tree_df: DataFrame = None,
            reload_cache: bool = True,
            env: str = 'prod'
        ):
        """
        retreives Future series from sdb,
        raises NoExchangeError if exchange does not exist in sdb,
        raises NoInstrumentError if ticker is not found for given exchange
        :param ticker:
        :param exchange:
        :param parent_folder_id: specify in case of ambiguous results of finding series or series located not in Root → FUTURE folder,
            feel free to leave empty
        :param bo: BackOffice class instance
        :param sdb: SymbolDB (async) class instance
        :param sdbadds: SDBAdditional class instance
        :param tree_df: sdb tree DataFrame
        :param reload_cache: load fresh tree_df if tree_df is not given in params
        :param env: environment
        """
        bo, sdb, sdbadds, tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            tree_df,
            env,
            reload_cache=reload_cache
        ).get_instances
        if not parent_folder_id:
            parent_folder_id = get_uuid_by_path(
                ['Root', 'FUTURE', exchange],
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
            tree_df=tree_df,
            env=env
        )

    @classmethod
    def from_scratch(
            cls,
            ticker: str,
            exchange: str,
            shortname: str,
            parent_folder_id: str = None,
            recreate: bool = False,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            tree_df: DataFrame = None,
            reload_cache: bool = True,
            env: str = 'prod',
            **kwargs
        ):
        """
        creates new series document with given ticker, shortname and other fields as kwargs,
        raises NoExchangeError if exchange does not exist in sdb,
        raises NoInstrumentError if bad parent_folder_id was given,
        raises RuntimeError if series exists in sdb and recreate=False
        :param ticker:
        :param exchange:
        :param shortname:
        :param parent_folder_id: specify if new series should be placed in destination that is not the third level folder
            (i.e. other than Root → FUTURE → <<exchange>>)
        :param recreate: if series exists in sdb drop all settings and replace it with newly created document
        :param bo: BackOffice class instance
        :param sdb: SymbolDB (async) class instance
        :param sdbadds: SDBAdditional class instance
        :param tree_df: sdb tree DataFrame
        :param reload_cache: load fresh tree_df if tree_df is not given in params
        :param env: environment
        :param kwargs: fields, that could be validated via sdb_schemas
            deeper layer fields could be pointed using path divided by '/' e.g. {'identifiers/ISIN': value} 

        """
        bo, sdb, sdbadds, tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            tree_df,
            env,
            reload_cache=reload_cache,
        ).get_instances
        if not parent_folder_id:
            parent_folder_id = get_uuid_by_path(
                ['Root', 'FUTURE', exchange],
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
            'description': f'{shortname} Futures'
        })

        return cls(
            ticker=ticker,
            exchange=exchange,
            instrument=instrument,
            reference=deepcopy(reference),
            series_tree=series_tree,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            tree_df=tree_df,
            env=env
        )

    @classmethod
    def from_dict(
            cls,
            payload: dict,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            tree_df: DataFrame = None,
            reload_cache: bool = True,
            env: str = 'prod'
        ):
        """
        >>> CONTINUE EDITING FROM HERE
        creates new series document with given ticker, shortname and other fields as kwargs,
        raises NoExchangeError if exchange does not exist in sdb,
        raises NoInstrumentError if bad path is given in payload,
        raises RuntimeError if series exists in sdb and recreate=False
        :param shortname:
        :param recreate: if series exists in sdb drop all settings and replace it with newly created document
        :param bo: BackOffice class instance
        :param sdb: SymbolDB (async) class instance
        :param sdbadds: SDBAdditional class instance
        :param tree_df: sdb tree DataFrame
        :param reload_cache: load fresh tree_df if tree_df is not given in params
        :param env: environment
        :param kwargs: fields, that could be validated via sdb_schemas
            deeper layer fields could be pointed using path divided by '/' e.g. {'identifiers/ISIN': value} 

        """

        bo, sdb, sdbadds, tree_df = InitThemAll(
            bo,
            sdb,
            sdbadds,
            tree_df,
            env,
            reload_cache=reload_cache
        ).get_instances
        if len(payload['path']) < 3:
            raise NoInstrumentError(f"Bad path: {payload.get('path')}")
        check_parent_df = tree_df[tree_df['_id'] == payload['path'][-1]]
        if check_parent_df.empty:
            raise NoInstrumentError(f"Bad path: {payload.get('path')}")
        if not check_parent_df.iloc[0]['path'] == payload['path']:
            raise NoInstrumentError(f"Bad path: {sdbadds.show_path(payload.get('path'))}")
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
            raise NoExchangeError(
                f"Bad path: exchange folder with _id {payload['path'][2]} is not found"
            )
        exchange = exchange_df.iloc[0]['name']
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

        return cls(
            ticker=ticker,
            exchange=exchange,
            instrument=payload,
            reference=deepcopy(reference),
            series_tree=series_tree,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            tree_df=tree_df,
            env=env
        )

    def __set_contracts(self, series_tree: list[dict]):
        contracts: list[FutureExpiration] = []
        contract_dicts = [
            x for x
            in series_tree
            if x['path'][:-1] == self.instrument['path']
            and not x['isAbstract']
            and x.get('isTrading') is not False
        ]
        for item in contract_dicts:
            try:
                contracts.append(
                    FutureExpiration.from_dict(self, instrument=item, reference=item)
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
        return sorted(contracts)

    def find_expiration(
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

    def get_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None,
            uuid: str = None
        ):
        num, series = self.find_expiration(
            expiration,
            maturity=maturity,
            uuid=uuid
            )
        return series.contracts[num] if num is not None and series is not None else None

    def add_payload(
            self,
            payload: dict,
            skip_if_exists: bool = True,
            overwrite_old: bool = False
        ):
        """
        Add new expiration (if it does not exist in sdb) or update existing expiration with given dict
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
        :param payload: dict to create/update expiration
        :param skip_if_exists: if True create new expirations only, do nothing if contract already exists
        :param owerwrite_old: if True replace all data in existing contract with given one (except for _id and _rev)
            else update existing contract with given data (other fields stay unmodified)
        :return: dict {'created': symbolId} in case of creation or dict {'updated': symbolId, 'diff': diff} in case of update existing
        """
        if not payload.get('expiry') \
            or not payload.get('maturityDate'):
            
            self.logger.error(f"Bad data: {pformat(payload)}")
            return {}
        
        # get expiration date
        exp_date = self.normalize_date(payload['expiry'])
        maturity = self.format_maturity(payload['maturityDate'])
        existing_exp, series = self.find_expiration(exp_date, maturity, payload.get('_id'))
        if existing_exp is not None:
            if skip_if_exists:
                self.skipped.add(exp_date.isoformat())
                return {}
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
            symbolic = self._maturity_to_symbolic(maturity)

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
        elif payload['path'][:len(series.instrument['path'])] != series.instrument['path']:
            self.logger.error(f"Bad path: {self.sdbadds.show_path(payload['path'])}")
            return {}

        new_contract = FutureExpiration.from_dict(
            series,
            payload
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
            maturity: str,
            uuid: str = None,
            skip_if_exists: bool = True,
            overwrite_old: bool = False,
            **kwargs
        ):
        """
        Create and add new expiration (if it does not exist in sdb) using given expiration date, maturity and other custom params
        or update existing expiration with custom params
        :param exp_date: expiration date (ISO str or date object)
        :param maturity: maturity str as numeric (like '2022-08') or symbolic (like Q2022)
        :param uuid: _id of existing expiration
        :param skip_if_exists: if True create new expirations only, do nothing if contract already exists
        :param owerwrite_old: if True replace all data in existing contract with created from scratch (except for _id and _rev)
            else update existing contract with given custom params in kwargs (other fields stay unmodified)
        :return: dict {'created': symbolId} in case of creation or dict {'updated': symbolId, 'diff': diff} in case of update existing
        """
        existing_exp, series = self.find_expiration(exp_date, maturity, uuid)
        if existing_exp is not None:
            if skip_if_exists:
                self.skipped.add(exp_date)
                return {}
            if not maturity and series.contracts[existing_exp].instrument.get('maturityDate'):
                maturity = self.format_maturity(series.contracts[existing_exp].instrument['maturityDate'])
            if not overwrite_old:
                for field, val in kwargs.items():
                    series.contracts[existing_exp].set_field_value(val, field.split('/'))
            else:
                kwargs.update({
                    key: val for key, val
                    in series.contracts[existing_exp].instrument.items()
                    if key[0] == '_' or key == 'path'
                })
                series.contracts[existing_exp] = FutureExpiration.from_scratch(
                    series,
                    expiration_date=exp_date,
                    maturity=maturity,
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

        if self.allowed_expirations:
            symbolic = self._maturity_to_symbolic(self.format_maturity(maturity))
            if not (
                    exp_date in self.allowed_expirations
                    or symbolic in self.allowed_expirations
                ):
                self.logger.info(f"Allowed expirations are set and {exp_date} or {symbolic} is not in it")
                return {}

        new_contract = FutureExpiration.from_scratch(
            self,
            exp_date,
            maturity,
            **kwargs
        )
        if new_contract in self.new_expirations:
            self.logger.warning(
                f"{new_contract} is already in list of new expirations. "
                "Replacing it with newer version")
            self.new_expirations.remove(new_contract)
        self.new_expirations.append(new_contract)
        return {'created': new_contract.contract_name}

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
        self.instrument = self.reduce_instrument()
        diff = DeepDiff(self.reference, self.instrument)
        # Create folder if need
        if not self.instrument.get('_id'):
            self.create(dry_run)
        elif diff:
            self.update(diff, dry_run)
        else:
            self.logger.info(f"{self.series_name}.*: No changes have been made")

        # Create expirations
        if self.new_expirations and dry_run:
            print(f"Dry run, new expirations to create:")
            pp([x.contract_name for x in self.new_expirations])
            report.setdefault(self.series_name, {}).update({
                'to_create': [x.contract_name for x in self.new_expirations]
            })
        elif self.new_expirations:
            self.wait_for_sdb()
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
        if update_expirations and dry_run:
            print(f"Dry run, expirations to update:")
            pp([x.contract_name for x in update_expirations])
            report.setdefault(self.series_name, {}).update({
                'to_update': [x.contract_name for x in update_expirations]
            })
        elif update_expirations:
            self.wait_for_sdb()
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
        if report and try_again_series and not dry_run:
            self.wait_for_sdb()
            response = asyncio.run(self.sdb.update(self.instrument))
            if response.get('message'):
                self.logger.error(f'instrument {self.ticker} is not updated:')
                self.logger.error(pformat(response))
        if not dry_run:
            self.clean_up_times()
        return report

class FutureExpiration(Instrument):
    def __init__(
            self,
            future: Future,
            instrument: dict,
            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        self.ticker = future.ticker
        self.exchange = future.exchange
        self.series_name = future.series_name
        self.instrument = instrument
        if reference is None:
            reference = {}
        self.reference = reference
        self.expiration = self.normalize_date(instrument['expiry'])
        self.maturity = self.format_maturity(instrument['maturityDate'])
        super().__init__(
            instrument=instrument,
            instrument_type='FUTURE',
            parent=future,
            env=future.env,
            sdb=future.sdb,
            sdbadds=future.sdbadds,
            reload_cache=reload_cache
        )
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))


    @classmethod
    def from_scratch(
            cls,
            future: Future,
            expiration_date: Union[str, dt.date, dt.datetime],
            maturity: str = None,
            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        if not reference:
            reference = {}
        expiration = Instrument.normalize_date(expiration_date)
        maturity = Instrument.format_maturity(maturity)
        instrument = FutureExpiration.create_expiration_dict(
            future,
            expiration,
            maturity,
            **kwargs
        )
        return cls(
            future,
            instrument,
            deepcopy(reference),
            reload_cache=reload_cache,
            **kwargs
        )

    @classmethod
    def from_dict(
            cls,
            future: Future,
            instrument: dict,
            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        if not reference:
            reference = {}
        if instrument.get('isTrading') is not None:
            instrument.pop('isTrading')
        if not instrument.get('path'):
            instrument['path'] = future.instrument['path']
        else:
            if instrument['path'][:len(future.instrument['path'])] != future.instrument['path']:
                raise ExpirationError(
                    f"Bad path: {future.sdbadds.show_path(instrument['path'])}"
                )
        return cls(
            future,
            instrument,
            deepcopy(reference),
            reload_cache=reload_cache,
            **kwargs
        )

    def __repr__(self):
        return (
            f"FutureExpiration({self.contract_name}, "
            f"{self.expiration.isoformat()})"
        )

    def __eq__(self, other: object) -> bool:
        return (self.expiration == other.expiration and self.maturity == other.maturity)
    
    def __gt__(self, other: object) -> bool:
        return self.expiration > other.expiration


    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @property
    def contract_name(self):
        return f"{self.ticker}.{self.exchange}.{self._maturity_to_symbolic(self.maturity)}"

    @staticmethod
    def create_expiration_dict(
            future: Future,
            expiration: dt.date,
            maturity: str,
            **kwargs
        ) -> dict:
        instrument = {
            'isAbstract': False,
            'name': maturity,
            'expiry': {
                'year': expiration.year,
                'month': expiration.month,
                'day': expiration.day
            },
            'maturityDate': {
                'month': int(maturity.split('-')[1]),
                'year': int(maturity.split('-')[0])
            },
            'path': future.instrument['path']

        }
        [
            instrument.update({key: val}) for key, val
            in kwargs.items() if len(key.split('/')) == 1
        ]
        FutureExpiration.set_la_lt(future, instrument)
        return instrument

    @staticmethod
    def set_la_lt(future: Future, instrument: dict):
        if future.set_la:
            instrument['lastAvailable'] = future.sdb.date_to_sdb(
                future.sdb.sdb_to_date(instrument['expiry']) + dt.timedelta(days=3)
            )
            if isinstance(future.set_la, str):
                instrument['lastAvailable']['time'] = future.set_la
            

        if future.set_lt:
            instrument['lastTrading'] = deepcopy(instrument['expiry'])
            if isinstance(future.set_lt, str):
                instrument['lastTrading']['time'] = future.set_lt

    def get_diff(self) -> dict:
        return DeepDiff(self.reference, self.instrument)

    def get_expiration(self) -> tuple[dict, str]:
        return self.instrument, self.contract_name