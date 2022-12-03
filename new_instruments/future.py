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
        self._align_expiry_la_lt()

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
            recreate: bool = False,
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
        return series.contracts[num] if num is not None and series else None



    def __update_existing_contract(
            self,
            series,
            num: int,
            overwrite_old: bool = False,
            payload: dict = None,
            **kwargs
        ):
        exp_date = series.contracts[num].expiration
        maturity = series.contracts[num].maturity
        if overwrite_old:
            if payload:
                payload.update({
                    key: val for key, val
                    in series.contracts[num].instrument.items()
                    if key[0] == '_' or key in ['path', 'strikePrices']
                })
                series.contracts[num].instrument = payload
            else:
                kwargs.update({
                    key: val for key, val
                    in series.contracts[num].instrument.items()
                    if key[0] == '_'
                })
                series.contracts[num] = FutureExpiration.from_scratch(
                    series,
                    expiration_date=exp_date,
                    maturity=maturity,
                    reference=series.contracts[num].reference
                    **kwargs
                )
        else:
            if payload:
                series.contracts[num].instrument.update(payload)
            else:
                for field, val in kwargs.items():
                    series.contracts[num].set_field_value(val, field.split('/'))

        if series.contracts[num].instrument.get('isTrading'):
            series.contracts[num].instrument.pop('isTrading')
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
            series,
            exp_date: dt.date,
            maturity: str,
            payload: dict = None,
            **kwargs
        ):
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
        if payload:
            new_contract = FutureExpiration.from_dict(
                series,
                payload
            )
        else:
            new_contract = FutureExpiration.from_scratch(
                series,
                expiration_date=exp_date,
                maturity=maturity,
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
        Add new expiration (if it does not exist in sdb)
        or update existing expiration with given dict
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
        :param skip_if_exists: if True do not update existing expiration
        :param owerwrite_old: if True replace all data in existing contract with given one (except for _id and _rev)
            else update existing contract with given data (other fields stay unmodified)
        :return: dict {'created': symbolId} in case of creation
            or dict {'updated': symbolId, 'diff': diff} in case of update existing
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
                self.skipped.add(series.contracts[existing_exp].contract_name)
                return {}
            update = self.__update_existing_contract(
                series,
                existing_exp,
                overwrite_old,
                payload=payload
            )
            return update
        create = self.__create_new_contract(
            series,
            exp_date,
            maturity,
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
            uuid: str = None,
            skip_if_exists: bool = True,
            overwrite_old: bool = False,
            **kwargs
        ):
        """
        Create and add new expiration (if it does not exist in sdb)
        using given expiration date, maturity and other custom params
        or update existing expiration with custom params
        :param exp_date: expiration date (ISO str or date object)
        :param maturity: maturity str as numeric (like '2022-08') or symbolic (like Q2022)
        :param uuid: _id of existing expiration
        :param skip_if_exists: if True do not update existing expiration
        :param owerwrite_old: if True replace all data in existing contract with created from scratch (except for _id and _rev)
            else update existing contract with given custom params in kwargs (other fields stay unmodified)
        :return: dict {'created': symbolId} in case of creation or dict {'updated': symbolId, 'diff': diff} in case of update existing
        """
        exp_date = self.normalize_date(exp_date)
        maturity = self.format_maturity(maturity)
        existing_exp, series = self.find_expiration(exp_date, maturity, uuid)
        if existing_exp is not None:
            if skip_if_exists:
                self.skipped.add(series.contracts[existing_exp].contract_name)
                return {}
            update = self.__update_existing_contract(
                series,
                existing_exp,
                overwrite_old,
                **kwargs
            )
            return update
        create = self.__create_new_contract(
            series,
            exp_date,
            maturity,
            **kwargs
        )
        return create


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

        # adjust paths
        # for contract in self.new_expirations + update_expirations:
        #     contract.instrument.update({
        #         'path': contract.path
        #     })
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

class FutureExpiration(Instrument):
    def __init__(
            self,
            future: Future,
            expiration: dt.date,
            maturity: str,
            custom_fields: dict = None,
            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        if custom_fields is None:
            custom_fields = {}
        if reference is None:
            reference = {}
        self.ticker = future.ticker
        self.exchange = future.exchange
        self.series_name = future.series_name
        self.future = future

        self.expiration = expiration
        self.maturity = maturity
        self.instrument = custom_fields
        self.instrument = self.get_instrument

        self.reference = reference
        super().__init__(
            instrument=self.instrument,
            instrument_type='FUTURE',
            parent=future,
            env=future.env,
            sdb=future.sdb,
            sdbadds=future.sdbadds,
            reload_cache=reload_cache
        )
        self.set_la_lt()
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))

    @classmethod
    def from_scratch(
            cls,
            future: Future,
            expiration_date: Union[
                str,
                dt.date,
                dt.datetime
            ],
            maturity: str = None,
            reference: dict = None,
            reload_cache: bool = False,
            **kwargs
        ):
        if not reference:
            reference = {}
        expiration = Instrument.normalize_date(expiration_date)
        maturity = Instrument.format_maturity(maturity)
        return cls(
            future,
            expiration,
            maturity,
            reference=deepcopy(reference),
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
        expiration = Instrument.normalize_date(instrument.get('expiry', {}))
        maturity = Instrument.format_maturity(instrument.get('maturityDate', {}))
        return cls(
            future,
            expiration,
            maturity,
            custom_fields=instrument,
            reference=deepcopy(reference),
            reload_cache=reload_cache,
            **kwargs
        )

    def __repr__(self):
        return (
            f"FutureExpiration({self.contract_name}, "
            f"{self.expiration.isoformat()})"
        )

    def __eq__(self, other: object) -> bool:
        return (
            self.expiration == other.expiration 
            and self.maturity == other.maturity
        )
    
    def __gt__(self, other: object) -> bool:
        return self.expiration > other.expiration


    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @property
    def contract_name(self) -> str:
        return f"{self.ticker}.{self.exchange}.{self._maturity_to_symbolic(self.maturity)}"

    @property
    def path(self) -> list[str]:
        if self.future.instrument.get('_id'):
            p = deepcopy(self.future.instrument['path'])
        else:
            p = deepcopy(self.future.instrument['path']) + ['<<series_folder_id>>']
        if not self.instrument.get('_id'):
            return p
        return p + [self.instrument['_id']]

    @property
    def get_custom_fields(self) -> dict:
        return {
            key: val for key, val
            in self.instrument.items()
            if key not in [
                'isAbstract',
                'name',
                'expiry',
                'maturityDate',
                'path'
            ]
        }

    @property
    def get_instrument(self) -> dict:
        instrument_dict = {
            'isAbstract': False,
            'name': self.maturity,
            'expiry': {
                'year': self.expiration.year,
                'month': self.expiration.month,
                'day': self.expiration.day
            },
            'maturityDate': {
                'month': int(self.maturity.split('-')[1]),
                'year': int(self.maturity.split('-')[0])
            },
            'path': self.path
        }
        if len(self.maturity.split('.')) == 3:
            instrument_dict['maturityDate'].update({
                'day': int(self.maturity.split('-')[2])
            })
        instrument_dict.update(self.get_custom_fields)
        return instrument_dict

    def set_la_lt(self):
        if self.future.set_la:
            self.set_field_value(
                self.sdb.date_to_sdb(
                    self.expiration + dt.timedelta(days=3)
                ),
                ['lastAvailable']
            )
            self.set_field_value(self.future.set_la, ['lastAvailable', 'time'])
        if self.future.set_lt:
            self.set_field_value(
                self.sdb.date_to_sdb(self.expiration),
                ['lastTrading']
            )
            self.set_field_value(self.future.set_lt, ['lastTrading', 'time'])

    def get_diff(self) -> dict:
        return DeepDiff(self.reference, self.get_instrument)

    def get_expiration(self) -> tuple[dict, str]:
        return self.get_instrument, self.contract_name