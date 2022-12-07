import asyncio
from copy import deepcopy
import datetime as dt
from deepdiff import DeepDiff
import json
import logging
import pandas as pd
from pprint import pformat, pp
import re
from typing import Dict, Union

from libs.async_symboldb import SymbolDB
from libs.backoffice import BackOffice
from libs.replica_sdb_additional import SDBAdditional, SdbLists
from libs.new_instruments import (
    Instrument,
    InitThemAll,
    Derivative,
    ExpirationError,
    NoExchangeError,
    NoInstrumentError,
    get_uuid_by_path
)

class Option(Derivative):
    def __init__(
        self,
        # series parameters
        ticker: str,
        exchange: str,
        instrument: dict = None,
        reference: dict = None,
        series_tree: list[dict] = None,
        option_type: str = None,
        week_number: int = 0,
        parent: Instrument = None,
        # class instances
        bo: BackOffice = None,
        sdb: SymbolDB = None,
        sdbadds: SDBAdditional = None,
        env: str = 'prod'
    ):
        self.ticker = ticker
        self.exchange = exchange
        self.option_type = option_type
        self.week_number = week_number
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
            instrument_type='OPTION',
            instrument=instrument,
            reference=reference,
            parent=parent,
            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds
        )
        self.skipped = set()
        self.allowed_expirations = []

        self.new_expirations: list[OptionExpiration] = []
        self.series_tree = series_tree
        self.contracts, self.weekly_commons = self.__set_contracts(
            series_tree,
            week_number=week_number
        )
        self._align_expiry_la_lt()

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @property
    def series_name(self):
        return f"{self.ticker}.{self.exchange}"

    def __repr__(self):
        week_indication = "Monthly" if not self.week_number else f"Week {self.week_number}"
        return f"Option({self.ticker}.{self.exchange}, {self.option_type=}, {week_indication} series)"


    @classmethod
    def from_sdb(
            cls,
            ticker: str,
            exchange: str,
            parent_folder_id: str = None,
            option_type: str = None,
            week_number: int = 0,
            parent: Instrument = None,
            parent_tree: list[dict] = None,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
            env: str = 'prod'
        ):
        """
        retreives Option series from sdb,
        raises NoExchangeError if exchange does not exist in sdb,
        raises NoInstrumentError if ticker is not found for given exchange
        :param ticker:
        :param exchange:
        :param parent_folder_id: specify in case of ambiguous results of finding series
            or series located not in Root → OPTION/OPTION ON FUTURE folder,
            feel free to leave empty
        :param option_type: 'OPTION' or 'OPTION ON FUTURE',
            in most cases sets automatically but sometimes may be helpful to specify
        :param week_number: 0 if series is monthly, 1-5 for corresponding week of month
        :param parent_tree: (optional) full heirs tree for monthly series to reduce sdb requests
        :param bo: BackOffice class instance
        :param sdb: SymbolDB (async) class instance
        :param sdbadds: SDBAdditional class instance
        :param env: environment
        """
        bo, sdb, sdbadds = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env
        ).get_instances
        if parent:
            parent_folder_id = parent._id
            option_type = parent.option_type
        else:
            if not parent_folder_id:
                parent_folder_id, option_type = Option._find_parent_folder_id(
                    ticker,
                    exchange,
                    sdb,
                    sdbadds,
                    option_type=option_type
                )
            if not option_type:
                pf_df = pd.read_sql(
                    'SELECT id as _id, path '
                    'FROM instruments '
                    f"WHERE id = '{parent_folder_id}'",
                    sdbadds.engine
                )
                if pf_df.empty:
                    raise NoInstrumentError(f'Invalid {parent_folder_id=}')
                option_folder_id = pd.read_sql(
                    'SELECT id as _id, path, "extraData" as extra '
                    'FROM instruments '
                    f"WHERE id = '{pf_df.iloc[0]['path'][1]}'",
                    sdbadds.engine
                )
                option_type = option_folder_id.iloc[0]['extra']['name']
            if option_type not in ['OPTION', 'OPTION ON FUTURE']:
                raise NoInstrumentError(f'Invalid {option_type=}')

        instrument, series_tree = Derivative._find_option_series(
            ticker,
            parent_folder_id,
            parent_tree=parent_tree,
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
            option_type=option_type,
            week_number=week_number,
            parent=parent,

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
            parent_folder_id: str = None,
            option_type: str = None,
            week_number: int = 0,
            parent: Instrument = None,
            underlying_id: str = None,
            recreate: bool = False,
            # class instances
            bo: BackOffice = None,
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None,
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
            (i.e. other than Root → OPTION/OPTION ON FUTURE → <<exchange>>)
        :param option_type: 'OPTION' or 'OPTION ON FUTURE' in most cases no need to specify it, but sometimes helpful
        :param week_number: sets week number for weekly option series creation
        :param underlying_id: symbolId of existing symbol in SDB representing underlying for option series
        :param recreate: if series exists in sdb drop all settings and replace it with newly created document
        :param bo: BackOffice class instance
        :param sdb: SymbolDB (async) class instance
        :param sdbadds: SDBAdditional class instance
        :param env: environment
        :param kwargs: fields, that could be validated via sdb_schemas
            deeper layer fields could be pointed using path divided by '/' e.g. {'identifiers/ISIN': value} 

        """
        bo, sdb, sdbadds = InitThemAll(
            bo,
            sdb,
            sdbadds,
            env
        ).get_instances
        if parent:
            parent_folder_id = parent._id
            option_type = parent.option_type
            parent_folder = parent.instrument
        else:
            if not parent_folder_id:
                parent_folder_id, option_type = Option._find_parent_folder_id(
                    ticker,
                    exchange,
                    sdb,
                    sdbadds,
                    option_type=option_type
                )
            parent_folder = asyncio.run(sdb.get(parent_folder_id))
            if not parent_folder or not parent_folder.get('isAbstract'):
                raise NoInstrumentError(f"Bad {parent_folder_id=}")
        reference, series_tree = Derivative._find_option_series(
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
            'description': f'Options on {shortname}'
        })

        if not week_number:
            instrument.update({
                'underlying': ticker
            })
        if underlying_id:
            underlying_sym = pd.read_sql(
                'SELECT min("dataId") '
                'FROM compiled_instruments_ids '
                f"WHERE dataId = '{underlying_id}'"
                'GROUP BY "instrumentId"',
                sdbadds.engine
            )
            if not underlying_sym.empty:
                instrument.update({
                    'underlyingId': {
                        'id': underlying_id,
                        'type': 'symbolId'
                    }
                })
        if exchange == 'CBOE':
            instrument.update({
                'feeds': {
                    'gateways': Derivative.less_loaded_feeds('CBOE', env)
                }
            })
            if not instrument.get('underlyingId'):
                try:
                    udl_ticker = ticker[:-1] if ticker[-1].isdecimal() else ticker
                    underlying_stock = asyncio.run(sdb.get_v2(
                        rf'{udl_ticker}\.(NASDAQ|NYSE|AMEX|ARCA|BATS)',
                        is_expired=False,
                        fields=['symbolId']
                    ))[0]['symbolId']
                    instrument.update({
                        'underlyingId': {
                            'id': underlying_stock,
                            'type': 'symbolId'
                        }
                    })
                except (IndexError, KeyError):
                    logging.warning(f'Can not find underlyingId for {ticker}')
        elif option_type == 'OPTION':
            logging.warning(
                f"Underlying for {ticker}.{exchange} is not set!"
            )

        return cls(
            ticker=ticker,
            exchange=exchange,
            instrument=instrument,
            reference=deepcopy(reference),
            series_tree=series_tree,
            option_type=option_type,
            week_number=week_number,
            parent=parent,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )

    @classmethod
    def from_dict(
            cls,
            payload: dict,
            recreate: bool = False,
            week_num: int = 0,
            parent: Instrument = None,
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
        if not parent_path == payload['path'][:len(parent_path)]:
            raise NoInstrumentError(f"Bad path: {sdbadds.show_path(payload.get('path'))}")
        if payload['path'][1] not in [
            get_uuid_by_path(['Root', 'OPTION'], sdbadds.engine),
            get_uuid_by_path(['Root', 'OPTION ON FUTURE'], sdbadds.engine)
            ]:
            raise NoInstrumentError(f"Bad path: {sdbadds.show_path(payload.get('path'))}")
        ticker = payload.get('ticker')
        if parent:
            parent_folder_id = parent._id
            exchange = parent.exchange
            parent_folder = parent.instrument
            option_type = parent.option_type
        else:
            if payload.get('_id') and payload['path'][-1] == payload['_id']:
                parent_folder_id = payload['path'][-2]
            else:
                parent_folder_id = payload['path'][-1]
            # get exchange folder _id from path (Root -> OPTION/OPTION ON FUTURE -> EXCHANGE), check its name in tree_df
            exchange_df = pd.read_sql(
                'SELECT id as _id, "extraData" as extra '
                'FROM instruments '
                f"WHERE id = '{payload['path'][2]}'",
                sdbadds.engine
            )
            if exchange_df.empty:
                raise NoExchangeError(
                    f"Bad path: exchange folder with _id {payload['path'][2]} is not found"
                )
            exchange = exchange_df.iloc[0]['extra']['name']
            parent_folder = asyncio.run(sdb.get(parent_folder_id))
            if not parent_folder or not parent_folder.get('isAbstract'):
                raise NoInstrumentError(f"Bad {parent_folder_id=}")
            option_type = sdbadds.uuid_to_name(parent_folder['path'][1])[0]
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

        return cls(
            ticker=ticker,
            exchange=exchange,
            instrument=payload,
            reference=deepcopy(reference),
            series_tree=series_tree,
            option_type=option_type,
            week_number=week_num,

            bo=bo,
            sdb=sdb,
            sdbadds=sdbadds,
            env=env
        )

    @staticmethod
    def _find_parent_folder_id(
            ticker: str,
            exchange: str,

            sdb: SymbolDB,
            sdbadds: SDBAdditional,
            option_type: str = None
        ):
        """
        parent folder is generally an exchange folder, first level deeper after option type
        e.g.: Root → OPTION ON FUTURE → CBOT
        or: Root → OPTION → CBOE
        method returns tuple (parent_folder_id, option_type)
        In case of existing ticker: method works as intended
        In case of non-existent ticker: method return values in two cases:
            · exchange is present only inside one option_type folder
            · option_type is given (e.g. EUREX is both in OPTION and OPTION ON FUTURE)
        otherwise NoExchangeError exception is thrown
        """
        # decide if option_type is OPTION or OPTION ON FUTURE
        for o_type in [option_type, 'OPTION', 'OPTION ON FUTURE']:
            # check if we have an existing path to exchange inside selected option_type
            parent_folder_id = get_uuid_by_path(
                    ['Root', o_type, exchange],
                    sdbadds.engine
                )
            if parent_folder_id:
                tree_part = pd.read_sql(
                    'SELECT id as _id, "extraData" as extra, '
                    '"isAbstract" as is_abstract, "isTrading" as is_trading '
                    'FROM instruments '
                    f"WHERE path[3] = '{parent_folder_id}' "
                    'AND "isAbstract" = true',
                    sdbadds.engine
                )
                tree_part['name'] = tree_part.apply(
                    lambda row: row['extra'].get('name'),
                    axis=1
                )
                # sometimes we have same exchange both in OPTION and OPTION ON FUTURE
                # check if we have ticker in selected exchange
                ticker_is_here = tree_part.loc[
                    (tree_part['name'] == ticker) &
                    (tree_part['is_abstract']) &
                    (tree_part['is_trading'] != False)
                ]
                if not ticker_is_here.empty:
                    option_type = o_type
                    break
        if parent_folder_id and option_type:
            return parent_folder_id, option_type

        # In my ideal world folders OPTION and OPTION ON FUTURE
        # contain only folders with exchange names
        # as they appear in exante id
        # but... here's slow and dirty hack for real world
        possible_exchanges = [
            x[1] for x
            in asyncio.run(
                sdbadds.get_list_from_sdb(SdbLists.EXCHANGES.value)
            )
            if x[0] == exchange
        ]
        opt_id = get_uuid_by_path(
            ['Root', 'OPTION'], sdbadds.engine
        )
        oof_id = get_uuid_by_path(
            ['Root', 'OPTION ON FUTURE'], sdbadds.engine
        )
        exchange_folders = asyncio.run(sdb.get_heirs(
            sdbadds.uuid2str(opt_id),
            fields=['name', 'exchangeId', 'path']))
        exchange_folders += asyncio.run(sdb.get_heirs(
            sdbadds.uuid2str(oof_id),
            fields=['name', 'exchangeId', 'path']))
        possible_exchange_folders = [
            x for x in exchange_folders 
            if x['exchangeId'] in possible_exchanges
        ]
        if len(possible_exchange_folders) < 1:
            raise NoExchangeError(f'{exchange=} does not exist in SymbolDB')
        elif len(possible_exchange_folders) == 1:
            parent_folder = possible_exchange_folders[0]
            parent_folder_id = parent_folder['_id']
            option_type = 'OPTION ON FUTURE' if parent_folder['path'][1] == oof_id else 'OPTION'
        else:
            ticker_folders = [
                x for pef in possible_exchange_folders for x
                in asyncio.run(
                    sdb.get_heirs(
                        pef['_id'],
                        fields=['path'],
                        recursive=True
                    )
                )
                if x['name'] == ticker
            ]
            if len(ticker_folders) == 1:
                parent_folder_id = ticker_folders[0]['path'][2]
                option_type = 'OPTION ON FUTURE' if ticker_folders[0]['path'][1] == oof_id else 'OPTION'
            else:
                raise NoExchangeError(
                    f'{ticker}.{exchange}: cannot select exchange folder'
                )
        return parent_folder_id, option_type

    def __set_contracts(self, series_tree: list[dict], week_number: int = 0):
        contracts: list[OptionExpiration] = []
        contract_dicts = [
            x for x
            in series_tree
            if x['path'][:-1] == self._instrument['path']
            and not x['isAbstract']
            and x.get('isTrading') is not False
        ]
        for item in contract_dicts:
            try:
                contracts.append(
                    OptionExpiration.from_dict(self, instrument=item, reference=item)
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

        # common weekly folders where single week folders are stored
        # in most cases only one is needed but there are cases like EW.CME
        if not week_number:
            weekly_common_folders = [
                x for x in series_tree
                if x['path'][:-1] == self._instrument['path']
                and 'weekly' in x['name'].lower()
                and x['isAbstract']
            ]
            weekly_commons: list[WeeklyCommon] = []
            for wcf in weekly_common_folders:
                wc = WeeklyCommon.from_dict(self, payload=wcf, reference=wcf)
                if wc:
                    weekly_commons.append(wc)
        else:
            weekly_commons = []
        return sorted(contracts), weekly_commons

    def find_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None,
            week_num: Union[int, bool] = None,
            ticker: str = None,
            uuid: str = None
        ):
        """
        Find existing expiration in self.contracts across all weeklies if called from Monthly instance
        :param expiration: expiration date as ISO-string or datetime object
        :param maturity: maturity as date-like string (2022-08) or some kind of symbolic (Q2022, Q22)
        :param uuid: _id of sdb instrument
        :return: tuple of OptionExpiration object if found and corresponding Future object.
            None, None if more than one expiration in single list (update_expirations or contracts) satisfies searching criteria
        """
        search_pool = [self]
        for weekly_common in self.weekly_commons:
            search_pool.extend(weekly_common.weekly_folders)
        if uuid:
            for opt in search_pool:
                present_expiration = next(
                    (
                        num for num, x
                        in enumerate(opt.contracts)
                        if x._id == uuid
                    ),
                    None
                )
                if present_expiration:
                    return present_expiration, opt

            if present_expiration is None:
                self.logger.warning(
                    f"{self.series_name}: "
                    f"expiration with _id={uuid} is not found!"
                )
                return None, self
        present_expirations = []

        expiration_date = self.normalize_date(expiration)        
        # prepare maturity as YYYY-MM
        maturity = self.format_maturity(maturity)
        # symbolic_maturity as Z2021
        if week_num is True and not self.week_number:
            search_pool.pop[0] # don't look in monthlies
        if isinstance(week_num, int):
            search_pool = [x for x in search_pool if x.week_number == week_num]
        if ticker:
            search_pool = [x for x in search_pool if x.ticker == ticker]
        for opt in search_pool:
            symbol_id = f"{opt.series_name}.{opt._maturity_to_symbolic(maturity)}"
            present_expirations.extend([
                (num, opt) for num, x
                in enumerate(opt.contracts)
                if (
                    x.expiration == expiration_date
                    or x.maturity == maturity
                    or x.contract_name == symbol_id  # expiration: Z2021
                ) and x._instrument.get('isTrading') is not False
            ])
            
        if len(present_expirations) == 1:
            return present_expirations[0]
        elif len(present_expirations) > 1:
            self.logger.error(
                "More than one expiration have been found: \n" +
                pformat(present_expirations) + '\n' +
                "try to narrow search criteria"
            )
            return None, None
        else:
            return None, self

    def get_expiration(
            self,
            expiration: Union[
                str,
                dt.date,
                dt.datetime
            ] = None,
            maturity: str = None,
            week_num: Union[int, bool] = None,
            ticker: str = None,
            uuid: str = None
        ):
        num, series = self.find_expiration(
            expiration,
            maturity=maturity,
            week_num=week_num,
            ticker=ticker,
            uuid=uuid
            )
        return series.contracts[num] if num is not None and series else None

    def _set_target_folder(
            self,
            week_num: int = None,
            ticker: str = None
        ):
        if self.week_number == week_num:
            return self
        if not self.week_number and not week_num:
            return self
        if self.week_number:
            self.logger.error(
                f"You should run this method from monthly instance, {self=}"
            )
            return None
        search_pool = [self]
        if (week_num or ticker != self.ticker) and not self.weekly_commons:
            raise NoInstrumentError(
                f"{self.series_name}: no weekly_common folders have been found"
            )
        for weekly_common in self.weekly_commons:
            search_pool.extend(weekly_common.weekly_folders)
        
        if ticker:
            ticker_opts = [x for x in search_pool if x.ticker == ticker]
            if not ticker_opts:
                raise NoInstrumentError(
                    f"No option folder with {ticker=} have been found"
                )
            if len(ticker_opts) == 1:
                return ticker_opts[0]
            else:
                search_pool = ticker_opts
        if week_num:
            week_opts = [x for x in search_pool if x.week_number == week_num]
            if not week_opts:
                if ticker:
                    raise NoInstrumentError(
                        f"No option folder with {week_num=}, {ticker=} have been found"
                    )
                raise NoInstrumentError(
                    f"No option folder with {week_num=} have been found"
                )
            if len(week_opts) == 1:
                return week_opts[0]
            else:
                raise ExpirationError(
                    f"More than one folder have been found: {week_opts}, "
                    "try to narrow search criteria"
                )
    
    def __update_existing_contract(
            self,
            series: 'Option',
            num: int,
            overwrite_old: bool = False,
            payload: dict = None,
            strikes: dict = None,
            **kwargs
        ):
        if payload:
            strikes = payload.pop('strikePrices')
        exp_date = series.contracts[num].expiration
        maturity = series.contracts[num].maturity
        if overwrite_old:
            added, removed, preserved = series.contracts[num].refresh_strikes(
                strikes,
                hard=True
            )
            if removed is None or removed.get('not_updated'):
                self.logger.warning(
                    f"{series.contracts[num].contract_name}: strikes are not updated!"
                )
            new_strikes = series.contracts[num].strikes
            if payload:
                payload.update({
                    key: val for key, val
                    in series.contracts[num]._instrument.items()
                    if key[0] == '_'
                })
                series.contracts[num]._instrument = payload
            else:
                kwargs.update({
                    key: val for key, val
                    in series.contracts[num]._instrument.items()
                    if key[0] == '_'
                })
                series.contracts[num] = OptionExpiration.from_scratch(
                    series,
                    expiration_date=exp_date,
                    maturity=maturity,
                    strikes=new_strikes,
                    reference=series.contracts[num].reference
                    **kwargs
                )
        else:
            series.contracts[num].add_strikes(strikes)
            if payload:
                series.contracts[num]._instrument.update(payload)
            else:
                for field, val in kwargs.items():
                    series.contracts[num].set_field_value(val, field.split('/'))

        series.contracts[num]._instrument.pop('isTrading', None)
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
            series: 'Option',
            exp_date: dt.date,
            maturity: str,
            payload: dict = None,
            strikes: dict = None,
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
            new_contract = OptionExpiration.from_dict(
                series,
                payload
            )
        else:
            new_contract = OptionExpiration.from_scratch(
                series,
                expiration_date=exp_date,
                maturity=maturity,
                strikes=strikes,
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
            overwrite_old: bool = False,
            week_num: Union[bool, int] = None,
            ticker: str = None
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
            },
            'strikePrices': {
                'CALL': [
                    {
                        'strikePrice': float,
                        'isAvailable': bool
                    },
                    ...
                ]
                'PUT': [
                    {
                        'strikePrice': float,
                        'isAvailable': bool
                    },
                    ...
                ]
            }
        }
        :param payload: dict to create/update expiration
        :param skip_if_exists: if True create new expirations only, do nothing if contract already exists
        :param owerwrite_old: if True replace all data in existing contract with given one (except for _id and _rev)
            else update existing contract with given data (other fields stay unmodified)
        :param week_num: 0, None or False to create monthly expiration, 1-5 to specify week if weekly,
            True to select weekly folder automatically by expiration day of month
        :param ticker: None to create monthly expiration, weekly ticker to select folder with corresponding ticker
        :return: dict {'created': symbolId} in case of creation or dict {'updated': symbolId, 'diff': diff} in case of update existing
        """
        # basic validation
        if not payload.get('expiry') \
            or not payload.get('maturityDate') \
            or not payload.get('strikePrices'):

            self.logger.error(f"Bad data: {pformat(payload)}")
            return {}

        # normalize variables
        exp_date = self.normalize_date(payload['expiry'])
        maturity = self.format_maturity(payload['maturityDate'])
        if week_num is True:
            week_num = int((exp_date.day - 1)/7) + 1
        elif isinstance(week_num, int) and week_num > 5:
            raise ExpirationError(
                f"Week number could not be greater than 5, {week_num=}"
            )
        if self.week_number:
            if week_num and week_num != self.week_number:
                self.logger.error(
                    f"You are trying to find {week_num=} expiration inside {self} instance. "
                    f"Please call this method either from Monthly instance or {week_num} week instance"
                )
                return {}

        # check if contract already exists
        existing_exp, series = self.find_expiration(
            exp_date,
            maturity,
            week_num=week_num,
            ticker=ticker,
            uuid=payload.get('_id')
        )
        if existing_exp is not None:
            # update existing
            if skip_if_exists:
                series.skipped.add(series.contracts[existing_exp].contract_name)
                return {}
            update = self.__update_existing_contract(
                series,
                existing_exp,
                overwrite_old,
                payload=payload
            )
            return update
        # contract does not exist
        # find a place where to create a new one
        series = self._set_target_folder(week_num, ticker)
        if series is None:
            self.logger.error(
                f"Series folder is not set ({self=}, {week_num=}), "
                "expiration is not added"
            )
            return {}
        create = self.__create_new_contract(
            series,
            exp_date,
            maturity,
            payload=payload
        )
        return create

    def add(
            self,
            exp_date: Union[str, dt.date, dt.datetime],
            strikes: dict,
            maturity: str = None,
            skip_if_exists: bool = True,
            overwrite_old: bool = False,
            week_num: Union[bool, int] = None,
            ticker: str = None,
            **kwargs
        ):
        # normalize variables
        exp_date = self.normalize_date(exp_date)
        maturity = self.format_maturity(maturity)
        if week_num is True:
            week_num = int((exp_date.day - 1)/7) + 1
        elif isinstance(week_num, int) and week_num > 5:
            raise ExpirationError(f"Week number could not be greater than 5, {week_num=}")
        if self.week_number:
            if week_num and week_num != self.week_number:
                self.logger.error(
                    f"You are trying to find {week_num=} expiration inside {self} instance. "
                    f"Please call this method either from Monthly instance or {week_num} week instance"
                )
                return {}
        existing_exp, series = self.find_expiration(
            exp_date,
            maturity,
            week_num=week_num,
            ticker=ticker
        )
        if existing_exp is not None:
            if skip_if_exists:
                series.skipped.add(exp_date.isoformat())
                return {}
            update = self.__update_existing_contract(
                series,
                existing_exp,
                overwrite_old,
                strikes=strikes,
                **kwargs
            )
            return update
        series = self._set_target_folder(week_num, ticker)
        if series is None:
            self.logger.error(
                f"Series folder is not set ({self=}, {week_num=}), "
                "expiration is not added"
            )
            return {}
        create = self.__create_new_contract(
            series,
            exp_date,
            maturity,
            strikes=strikes,
            **kwargs
        )
        return create

    def refresh_strikes(
            self,
            exp_date: Union[str, dt.date, dt.datetime],
            strikes: dict,
            maturity: str = None,
            hard: bool = False,
            force: bool = False,
            week_num: Union[bool, int] = None,
            ticker: str = None
        ):
        exp_date = self.normalize_date(exp_date)
        maturity = self.format_maturity(maturity)
        if week_num is True:
            week_num = int((exp_date.day - 1)/7) + 1
        if self.week_number:
            if week_num and week_num != self.week_number:
                self.logger.error(
                    f"You are trying to find {week_num=} expiration inside {self} instance. "
                    f"Please call this method either from Monthly instance or {week_num} week instance"
                )
                return {}
        existing_exp, series = self.find_expiration(
            exp_date,
            maturity,
            week_num=week_num,
            ticker=ticker
        )
        if existing_exp is None:
            self.logger.warning(
                f"{self.series_name}: expiration {exp_date=}, {maturity=}, {week_num=}, {ticker=} "
                "is not found, no strikes have been updated"
            )
            return None, None, None, None
        added, removed, preserved = series.contracts[existing_exp].refresh_strikes(
            strikes,
            force=force,
            hard=hard
        )
        if removed is None or removed.get('not_updated'):
            self.logger.warning(
                f"{series.contracts[existing_exp].contract_name}: "
                "strikes are not updated!"
            )
        return series.contracts[existing_exp], added, removed, preserved

    def set_underlying(self, symbol_id) -> bool:
        # check if symbol_id is not an uuid
        if self.sdb.is_uuid(symbol_id):
            self.logger.error(
                f"SymbolId is expected, not an _id! Underlying id is not set"
            )
            return False
        # check if symbol exists in sdb
        underlying_sym = pd.read_sql(
            'SELECT min("dataId") '
            'FROM compiled_instruments_ids '
            f"WHERE dataId = '{symbol_id}'"
            'GROUP BY "instrumentId"',
            self.sdbadds.engine
        )
        if not underlying_sym.empty:
            self.underlying_dict = {
                        'id': symbol_id,
                        'type': 'symbolId'
                    }
            return True
        self.logger.error(
            f'{symbol_id} does not exist in sdb! Underlying id is not set'
        )
        return False

    def create_weeklies(
            self,
            templates: dict,
            common_name: str = 'Weekly',
            recreate: bool = False,
            week_number: int = 0
        ):
        if self.week_number:
            self.logger.error(f"{self.ticker}.{self.exchange}: Cannot create weeklies inside weekly folder")
            return None
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
            weekly_common = WeeklyCommon.from_scratch(
                self,
                templates=templates,
                common_name=common_name,
                recreate=recreate
            )
            self.weekly_commons.append(weekly_common)
        elif recreate:
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
            self._instrument = self.create_series_dict()
            self._instrument.update({
                key: val for key, val in self.reference.items() if key[0] == '_'
            })
            return None
        elif week_number not in range(6):
            self.logger.error(
                'week number must be specified as a number between 0 and 5 '
                '(0 to recreate all week folders)'
            )
            return None
        if weekly_templates is None:
            self.logger.warning(
                'weekly template is not specified '
                '(use $ to identify week as a number, @ as a letter)'
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
        """
        · creates (if doesn't exist in sdb) or updates (if there is a diff relative to the self.reference) the series folder from self._instrument dict
        · updates existing expirations from self.contracts if there is some diff between contract.reference and contract._instrument
        · creates new expirations from self.new_expirations on base of FutureExpiration()._instrument dict
        :param dry_run: if True prints out what is to be created and updated, post nothing to sdb
        :return: dict
        """
        
        if self.week_number:
            return {'error': 'Call post_to_sdb() method only from main series instance (not weeklies)'}
        report = {}
        try_again_series = False
        self.reduce_instrument()
        diff = DeepDiff(self.reference, self.instrument)
        # Create folder if need
        if not self._id:
            self.create(dry_run)
        elif diff:
            self.update(diff, dry_run)
        else:
            self.logger.info(f"{self.series_name}.*: No changes have been made")

        # Create common folder for weekly subfolders
        for wc in self.weekly_commons:
            if not wc._id:
                wc.create(dry_run)
            elif wc.get_diff():
                wc.update(wc.get_diff(), dry_run)
            # Create weekly subfolders
            for wf in wc.weekly_folders:
                if not wf._id:
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
            create_result = ''
            update_result = ''
            update_expirations = [
                x for x
                in target.contracts
                if x.expiration >= dt.date.today()
                and x.get_diff()
            ]
            # Check if series folder has been changed
            target.reduce_instrument()
            diff = DeepDiff(target.reference, target.instrument)
            if diff:
                target.update(diff, dry_run)
            else:
                self.logger.info(f"No changes were made for {target.series_name}.*")
            for new in target.new_expirations:
                if target.option_type == 'OPTION ON FUTURE':
                    if not new.get_instrument.get('underlyingId', {}).get('id'):
                        self.logger.warning(f"Underlying for {new.contract_name} is not set!")
            if target.new_expirations and dry_run:
                print(f"Dry run, new expirations to create:")
                pp([x.contract_name for x in target.new_expirations])

                report.setdefault(target.series_name, {}).update({
                    'to_create': [x.contract_name for x in target.new_expirations]
                })
            elif target.new_expirations:
                self.wait_for_sdb()
                create_result = asyncio.run(self.sdb.batch_create(
                    input_data=[x.get_instrument for x in target.new_expirations]
                ))
                if create_result:
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
                    report.setdefault(target.series_name, {}).update({
                        'create_error': create_result.get('description')
                    })
                else:
                    report.setdefault(target.series_name, {}).update({
                        'created': [x.contract_name for x in target.new_expirations]
                    })
            if update_expirations and dry_run:
                print(f"Dry run, expirations to update:")
                pp([x.contract_name for x in update_expirations])
                report.setdefault(target.series_name, {}).update({
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
                    report.setdefault(target.series_name, {}).update({
                        'update_error': update_result.get('description')
                    })
                else:
                    report.setdefault(target.series_name, {}).update({
                        'updated': [x.contract_name for x in update_expirations]
                    })
        if report and try_again_series and not dry_run:
            self.wait_for_sdb()
            response = asyncio.run(self.sdb.update(self._instrument))
            if response.get('message'):
                self.logger.error(f'instrument {self.ticker} is not updated:')
                self.logger.error(pformat(response))

        if not dry_run:
            self.clean_up_times()
        return report


class OptionExpiration(Instrument):
    def __init__(
            self,
            option: Option,
            expiration: dt.date,
            maturity: str,
            strikes: dict,
            instrument: dict,
            underlying: str = None,
            reference: dict = None,
            **kwargs
        ):
        self.ticker = option.ticker
        self.exchange = option.exchange
        self.series_name = option.series_name
        self.option = option

        self.expiration = expiration
        self.maturity = maturity
        self.option_type = option.option_type
        self.week_number = option.week_number
        self._instrument = instrument
        self._strikes = self.build_strikes(strikes, self._instrument.get('_id'))
        self._underlying = None
        self.set_underlying(underlying)

        super().__init__(
            instrument=self.get_instrument,
            reference=reference,
            instrument_type='OPTION',
            parent=option,
            env=option.env,
            sdb=option.sdb,
            sdbadds=option.sdbadds
        )
        self.set_la_lt()
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))

    @classmethod
    def from_scratch(
            cls,
            option: Option,
            expiration_date: Union[
                str,
                dt.date,
                dt.datetime
            ],
            maturity: str = None,
            strikes: dict = None,
            reference: dict = None,
            underlying: str = None,
            **kwargs
        ):
        if not reference:
            reference = {}
        expiration = Instrument.normalize_date(expiration_date)
        maturity = Instrument.format_maturity(maturity)
        if option.week_number:
            if int((expiration.day - 1)/7) + 1 != option.week_number:
                raise ExpirationError(
                    f"You should not create a contract expiring at {expiration.isoformat()} "
                    f"as a {option.week_number} week contract"
                )
        return cls(
            option,
            expiration,
            maturity,
            strikes=strikes,
            instrument={},
            underlying=underlying,
            reference=deepcopy(reference),
            **kwargs
        )

    @classmethod
    def from_dict(
            cls,
            option: Option,
            instrument: dict,
            reference: dict = None,
            **kwargs
        ):
        if not reference:
            reference = {}
        instrument.pop('isTrading', None)
        expiration = Instrument.normalize_date(instrument.get('expiry', {}))
        maturity = Instrument.format_maturity(instrument.get('maturityDate', {}))
        strikes = instrument.get('strikePrices', {})
        underlying = instrument.get('underlyingId', {}).get('id')
        return cls(
            option,
            expiration,
            maturity,
            strikes=strikes,
            underlying=underlying,
            instrument=instrument,
            reference=deepcopy(reference),
            **kwargs
        )

    def __repr__(self):
        week_indication = "Monthly" if not self.week_number else f"Week {self.week_number}"
        return (
            f"OptionExpiration({self.contract_name}, "
            f"{self.expiration.isoformat()}, {week_indication})"
        )

    def __eq__(self, other):
        return (
            self.expiration == other.expiration
            and self.ticker == other.ticker
            and self.exchange == other.exchange
        )
    
    def __gt__(self, other: object) -> bool:
        self.expiration > other.expiration 

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @property
    def contract_name(self):
        return f"{self.ticker}.{self.exchange}.{self._maturity_to_symbolic(self.maturity)}"

    @property
    def path(self):
        if self.option._id:
            p = self.option.instrument['path']
        else:
            p = self.option.instrument['path'] + ['<<series_folder_id>>']
        if not self._id:
            return p
        return p + [self._id]

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
                'path',
                'strikePrices',
                'underlyingId'
            ]
        }

    @property
    def strikes(self) -> dict[str, list[dict]]:
        return deepcopy(self._strikes)

    @property
    def underlying(self) -> str:
        return self._underlying

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
            'path': self.path,
            'strikePrices': self.strikes,
        }
        if len(self.maturity.split('-')) == 3:
            instrument_dict['maturityDate'].update({
                'day': int(self.maturity.split('-')[2])
            })
        if self.underlying:
            instrument_dict.update({
                'underlyingId': {
                    'id': self.underlying,
                    'type': 'symbolId'
                }
            })
        instrument_dict.update(self.get_custom_fields)
        return instrument_dict

    def set_la_lt(self):
        if self.option.set_la:
            self.set_field_value(
                self.sdb.date_to_sdb(
                    self.expiration + dt.timedelta(days=3)
                ),
                ['lastAvailable']
            )
            self.set_field_value(self.option.set_la, ['lastAvailable', 'time'])
        if self.option.set_lt:
            self.set_field_value(
                self.sdb.date_to_sdb(self.expiration),
                ['lastTrading']
            )
            self.set_field_value(self.option.set_lt, ['lastTrading', 'time'])

    @staticmethod
    def build_strikes(strikes: dict, uuid: str = None) -> Dict[str, list[dict]]:
        def format_strike(raw_strike: dict) -> dict:
            strike_price = {
                'strikePrice': float(raw_strike['strikePrice']),
                'isAvailable': raw_strike.get('isAvailable', True)
            }
            if raw_strike.get('identifiers'):
                strike_price.update({'identifiers': raw_strike['identifiers']})
            for key in ['ISIN', 'FIGI']:
                if key in raw_strike:
                    strike_price.setdefault(
                        'identifiers', {}
                    ).update(raw_strike[key])
            return strike_price

        if uuid:
            strikes.setdefault('CALL', [])
            strikes.setdefault('PUT', [])
            return strikes
        if not isinstance(strikes.get('CALL'), (set, list)) \
            or not isinstance(strikes.get('PUT'), (set, list)):
            raise ExpirationError(
                f"Strikes are invalid: {pformat(strikes)}"
            )
        strikeprices_is_proper_dict = all(
            (
                isinstance(strike, dict)
                and strike.get('strikePrice')
            ) for strike
            in strikes['CALL'] + strikes['PUT']
        )
        if strikeprices_is_proper_dict:
            prepared_strikes = {
                side: [
                    format_strike(strike) for strike
                    in strikes[side]
                ] for side
                in ['PUT', 'CALL']
            }
            return prepared_strikes
        else:
            prepared_strikes = {
                side: [
                    {
                        'strikePrice': float(strike),
                        'isAvailable': True
                    } for strike in strikes.get(side)
                ] for side
                in ['PUT', 'CALL']
            }
            return prepared_strikes

    def set_underlying(self, underlying: str = None):
        if not underlying:
            if self.underlying:
                self.logger.warning(
                        f'{self.contract_name}: '
                        f'underlying has been removed'
                    )
            self._underlying = None
            return
        existing_future_df = pd.read_sql(
            'SELECT cid."instrumentId" as _id, ci."expiryTime" as expiry_time, min(cid."dataId") as symbol_id '
            'FROM compiled_instruments_ids cid '
            'LEFT JOIN compiled_instruments ci ON cid."instrumentId" = ci."instrumentId" '
            f"WHERE cid.\"dataId\" = '{underlying}'"
            'GROUP BY cid."instrumentId", ci."expiryTime"',
            self.option.sdbadds.engine
        )
        if existing_future_df.empty:
            self.logger.warning(
                f'{self.contract_name}: '
                f'{underlying} is not found in sdb, {self.underlying=}'
            )
            return
        udl_dt_expiry = existing_future_df.iloc[0]['expiry_time'].to_pydatetime()
        try:
            instr_dt_expiry = dt.datetime.fromisoformat(
                self.option.sdbadds.compile_expiry_time(
                    self.get_instrument
                )[:-1]
            )
        except TypeError:
            logging.warning(
                f'Cannot compile {self.contract_name} expiry time!'
            )
            return
        if instr_dt_expiry > udl_dt_expiry:
            logging.warning(
                f"{underlying} could not be an underlying "
                f"for {self.contract_name} as it expires earlier"
            )
            return
        self._underlying = underlying

    def add_strikes(self, strikes: dict) -> dict:
        strikes = self.build_strikes(strikes)
        added = {}
        for side in ['PUT', 'CALL']:
            new_strikes = [
                x for x
                in strikes[side]
                if x.get('strikePrice') not in [
                    strike['strikePrice'] for strike
                    in self.strikes[side]
                ]
            ]
            added.update({side: new_strikes})
            self._strikes[side].extend(new_strikes)
            self._strikes[side] = sorted(
                self.strikes[side],
                key=lambda sp: sp['strikePrice']
            )
        self.logger.info('New strikes:\n' + pformat(added))
        return added

    def _strike_exante_id(self, strike: float, side: str) -> str:
        if re.search(r'\.0$', str(strike)):
            strike = int(strike)
        underline = str(strike).replace('.', '_')
        return f"{self.contract_name}.{side[0]}{underline}"

    def refresh_strikes(
            self,
            strikes: dict,
            force: bool = False,
            hard: bool = False,
            reload_cache: bool = False,
            consider_demo: bool = True
        ):
        """
        strikes is a dict of active strikes that should stay
        and the rest should be removed with the exception
        of used symbols, they also should stay

        updating strikes process is divided into two separate functions
        because adding new strikes is fast and could be done on demand.
        Removing non-tradable strikes requires to check their presence in
        used symbols and it is slow, so it may be done automatically on schedule
        """

        # general idea of metod is that: resulting self strikes are given strikes plus those found in used_symbols

        if hard:
            # slooow
            self.used_symbols = asyncio.run(
                self.sdbadds.load_used_symbols(
                    reload_cache,
                    consider_demo
                )
            )

        MIN_STRIKES_ACCEPTABLE = 7
        MIN_INTERSECTION = (
            len(self.strikes['CALL']) + 
            len(self.strikes['PUT']) - 16
        )
        preserved = {}
        added = {}
        removed = {}
        cant_touch_this = {}

        strikes = self.build_strikes(strikes)

        # check if refresh is safe enough: check combined length of strikes and intersection
        strikes_len = len(strikes['PUT']) + len(strikes['CALL'])
        strikes_intersection = len(
            {
                x['strikePrice'] for x
                in strikes['PUT']
            }.intersection({
                x['strikePrice'] for x
                in self.strikes['PUT']
            })
        ) + len(
            {
                x['strikePrice'] for x
                in strikes['CALL']
            }.intersection({
                x['strikePrice'] for x
                in self.strikes['CALL']
            })
        )
        if not force and (
                strikes_len < MIN_STRIKES_ACCEPTABLE
                or strikes_intersection < MIN_INTERSECTION
            ):
            self.logger.warning(
                f"{self.ticker}.{self.exchange} {self.maturity}: "
                "Provided list and existing strikes do not look similar enough, "
                "no strikes removed"
            )
            return None, {'not_updated': True}, None
        
        # Now action
        for side in ['PUT', 'CALL']:
            if hard:
                # search existing strikes in used_symbols
                cant_touch_this.setdefault(side, set()).update([
                    x for x 
                    in self.strikes[side]
                    if self._strike_exante_id(x['strikePrice'], side) in self.used_symbols
                ])
                # preserved are the non-tradable strikes that we cannot remove due to the presense in used_symbols
                # can't_touch - new_strikes
                preserved.setdefault(side, set()).update(
                    {
                        x['strikePrice'] for x in cant_touch_this[side]
                    } - 
                    {
                        x['strikePrice'] for x in strikes[side]
                    }
                )
                # self_strikes - new_strikes - can't_touch
                removed.setdefault(side, set()).update(
                    {
                        x['strikePrice'] for x in self.strikes[side]
                    } - 
                    {
                        x['strikePrice'] for x in strikes[side]
                    } -
                    {
                        x['strikePrice'] for x in cant_touch_this[side]
                    }
                )
                # add preserved strikes to given strikes
                strikes[side].extend([
                    x for x
                    in cant_touch_this[side]
                    if x['strikePrice'] not in [
                        y['strikePrice'] for y in strikes[side]
                    ]
                ])
            else:
                preserved = {
                    'PUT': set(),
                    'CALL': set()
                }
                removed.setdefault(side, set()).update(
                    {
                        x['strikePrice'] for x in self.strikes[side]
                        if x.get('isAvailable') is not False # already disabled
                    } - 
                    {
                        x['strikePrice'] for x in strikes[side]
                    }
                )
            # new_strikes - self_strikes
            added.setdefault(side, set()).update(
                {
                    x['strikePrice'] for x in strikes[side]
                } -
                {
                    x['strikePrice'] for x in self.strikes[side]
                    if x.get('isAvailable') is not False
                }
            )
            if hard:
                # replace self_strikes with given ones enriched with preserved
                self._strikes[side] = sorted(
                    strikes[side],
                    key=lambda sp: sp['strikePrice']
                )
        if not hard:
            self.enable_strikes(removed, enable=False)
            self.enable_strikes(added, enable=True)

        if preserved.get('PUT') or preserved.get('CALL'):
            self.logger.info(
                f"{self.ticker}.{self.exchange} {self.expiration}: "
                f"cannot remove following strikes as they are present "
                f"in used symbols {preserved}"
            )
        return added, removed, preserved

    def enable_strikes(self, strikes: dict, enable: bool = True):
        """
        Sets isAvailable flag on strikes in self._instrument['strikePrices']
        on base of given strikes, adds new strikes if any stikes in given dict are absent in self._instrument
        :param strikes: dict {'CALL': list[float], 'PUT': list[float]} for which isAvailable flag should be updated
        :param enable: sets isAvailable: True if True, False if False or None
        """
        for side in ['PUT', 'CALL']:
            self._strikes.setdefault(side, [])
            side_strikes_nums = [
                num for num, x
                in enumerate(self.strikes[side])
                if x['strikePrice'] in strikes.get(side, [])
            ]
            new_strikes = list({
                x for x
                in strikes.get(side, [])
                if x not in [
                    y['strikePrice'] for y
                    in self.strikes[side]
                ]
            })
            [
                self._strikes[side][num].update({
                    'isAvailable': True if enable else False
                }) for num
                in side_strikes_nums
            ]
            [
                self._strikes[side].append({
                    'strikePrice': strike,
                    'isAvailable': True if enable else False
                }) for strike
                in new_strikes
            ]
            self._strikes[side] = sorted(
                self.strikes[side],
                key=lambda s: s['strikePrice']
            )

    def get_diff(self) -> dict:
        strikes_diff = {}
        for side in ['PUT', 'CALL']:
            added = [
                x['strikePrice'] for x
                in self.strikes.get(side, [])
                if x.get('isAvailable')
                and x['strikePrice'] not in [
                    y['strikePrice'] for y
                    in self.reference['strikePrices'][side]
                    if x.get('isAvailable')                        
                ]
            ]
            if added:
                strikes_diff.setdefault('added', {}).setdefault(side, []).extend(added)
            removed = [
                    x['strikePrice'] for x
                    in self.reference.get('strikePrices', {}).get(side, [])
                    if x.get('isAvailable')
                    and x['strikePrice'] not in [
                        y['strikePrice'] for y
                        in self.strikes[side]
                        if x.get('isAvailable')                        
                    ]
                ]
            if removed:
                strikes_diff.setdefault('removed', {}).setdefault(side, []).extend(removed)

        diff: dict = DeepDiff(
            {
                key: val for key, val
                in self.reference.items()
                if key != 'strikePrices'
            }, 
            {
                key: val for key, val
                in self.get_instrument.items()
                if key != 'strikePrices'
            }
        )
        if strikes_diff:
            diff.update(strikes_diff)
        return diff

    def get_expiration(self):
        return self.get_instrument, self.contract_name


class WeeklyCommon(Instrument):
    def __init__(
            self,
            option: Option,
            common_name: str = 'Weekly',
            templates: dict = None,
            instrument: dict = None,
            reference: dict = None
        ):
        if templates is None:
            templates = {}
        self.templates = templates
        self.option = option
        self.option_type = self.option.option_type
        self.exchange = self.option.exchange
        self.common_name = common_name
        self.weekly_folders: list[Option] = []
        self._instrument = instrument

        super().__init__(
            instrument=self.get_instrument,
            reference=reference,
            instrument_type='OPTION',
            parent=option,
            env=option.env,
            sdb=option.sdb,
            sdbadds=option.sdbadds
        )

    @classmethod
    def from_dict(
            cls,
            option: Option,
            payload: dict,
            reference: dict = None
        ):
        if reference is None:
            reference = {}
        common_name = payload.get('name')
        cw = cls(
            option,
            common_name=common_name,
            instrument=payload,
            reference=deepcopy(reference)
        )
        cw.weekly_folders = cw.__find_weekly_folders()
        if not cw.weekly_folders:
            return None
        cw.templates = {
            'ticker': re.sub(
                r'[12345]',
                '$',
                cw.weekly_folders[0].ticker
            )
        }
        return cw

    @classmethod
    def from_scratch(
            cls,
            option: Option,
            templates: dict,
            reference: dict = None,
            common_name='Weekly',
            recreate: bool = False,
            dry_run: bool = False
        ):
        if reference is None:
            reference = {}
        instrument = {
            key: val for key, val
            in reference.items()
            if key[0] == '_'
        }
        cw = cls(
            option,
            common_name,
            templates=templates,
            instrument=instrument,
            reference=deepcopy(reference)
        )
        if cw._id:
            cw.update(dry_run)
            cw.__find_weekly_folders()
        else:
            cw.create(dry_run)
            cw.mk_weeklies(recreate)
        return cw

    def __repr__(self):
        return f"WeeklyCommon({self.option.series_name}, {self.common_name=})"

    def __find_weekly_folders(self):
        weekly_folders: list[Option] = []
        existing_tickers = [
            x['ticker'] for x
            in self.option.series_tree
            if x.get('ticker')
            and x['path'][:-1] == self.path
            and x['isAbstract']
        ]
        for x in existing_tickers:
            if x and re.search(r'[12345]', x):
                try:
                    weekly_folder = Option.from_sdb(
                        ticker=x,
                        exchange=self.option.exchange,
                        parent_folder_id=self.payload.get('_id'),
                        week_number=int(re.search(r'[12345]', x).group()),
                        parent_tree=self.option.series_tree,
                        bo=self.option.bo,
                        sdb=self.option.sdb,
                        sdbadds=self.option.sdbadds
                    )
                    weekly_folders.append(weekly_folder)
                except NoInstrumentError:
                    self.logger.warning(
                        f"Weekly folder {x}.{self.option.exchange} is not found, "
                        "check if folder name and ticker are the same"
                    )
        return weekly_folders

    @property
    def get_custom_fields(self) -> dict:
        return {
            key: val for key, val
            in self._instrument.items()
            if key not in [
                'isAbstract',
                'name',
                'path'
            ]
        }

    @property
    def path(self):
        if self.option._id:
            p = self.option.instrument['path']
        else:
            p = self.option.instrument['path'] + ['<<series_folder_id>>']
        if not self._id:
            return p
        return p + [self._id]

    @property
    def get_instrument(self):
        instrument_dict = {
            'isAbstract': True,
            'name': self.common_name,
            'path': self.path
        }
        instrument_dict.update(self.get_custom_fields)
        return instrument_dict

    def create(self, dry_run: bool = False):
        if dry_run:
            print(f"Dry run. New folder {self.get_instrument['name']} to create:")
            pp(self.get_instrument)
            self._instrument['_id'] = (
                f"<<new {self.option.series_name} "
                f"{self.get_instrument['name']} id>>"
            )
            self._instrument['path'].append(
                f"<<new {self.option.series_name} "
                f"{self.get_instrument['name']} id>>"
            )

        elif self.option._id:
            self.option.wait_for_sdb()
            create = asyncio.run(self.option.sdb.create(self.get_instrument))
            if not create.get('_id'):
                self.option.logger.error(pformat(create))
                raise RuntimeError(
                    f"Can not create common weekly folder {self.option.ticker}: "
                    f"{create['message']}"
                )
            self.option.logger.debug(f'Result: {pformat(create)}')
            self._instrument['_id'] = create['_id']
            self._instrument['_rev'] = create['_rev']
            self._instrument['path'].append(create['_id'])
            self._reference = deepcopy(self.get_instrument)

    def update(self, diff: dict, dry_run: bool = False):
        self.option.logger.info(
            f"{self.option.ticker}.{self.option.exchange}, {self.get_instrument['name']}: "
            "following changes have been made:"
        )
        self.option.logger.info(pformat(diff))
        if dry_run:
            print(f"Dry run. The folder {self.get_instrument['name']} to update:")
            pp(diff)
            return {}
        self.option.wait_for_sdb()
        response = asyncio.run(self.option.sdb.update(self.get_instrument))
        if response.get('message'):
            self.option.logger.error(
                f"{self.option.ticker}.{self.option.exchange}, {self.get_instrument['name']}: "
                "folder is not updated"
            )
            self.option.logger.error(pformat(response))
        else:
            self._reference = deepcopy(self.get_instrument)


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
            shortname = self.option._instrument.get('description')
            shortname = re.sub(r'( )?[Oo]ptions( )?([Oo]n )?', '', shortname)
            shortname += f" {num}{endings[num]} Week"
            ticker_template: str = self.templates.get('ticker')
            if not ticker_template:
                self.option.logger.warning(
                    'No weekly ticker template have been provided, weekly folders are not created'
                )
                return None
            if '$' in ticker_template:
                weekly_ticker = weekly_name = f"{ticker_template.replace('$', str(num))}"
            elif '@' in ticker_template:
                weekly_ticker = weekly_name = f"{ticker_template.replace('@', letters[num])}"
            else:
                weekly_name = f"{num}{endings[num]} Week"
            new_weekly = Option.from_scratch(
                ticker=weekly_ticker,
                exchange=self.option.exchange,
                shortname=shortname,
                parent_folder_id=self._id,
                week_number=num,
                parent=self,
                bo=self.option.bo,
                sdb=self.option.sdb,
                sdbadds=self.option.sdbadds,

                name=weekly_name
            )
            # new_weekly._instrument['name'] = weekly_name
            if new_weekly._id and not recreate:
                continue
            feed_providers = [
                x[0] for x
                in asyncio.run(
                    self.option.sdbadds.get_list_from_sdb(
                        SdbLists.FEED_PROVIDERS.value
                    )
                )
            ]
            broker_providers = [
                x[0] for x
                in asyncio.run(
                    self.option.sdbadds.get_list_from_sdb(
                        SdbLists.BROKER_PROVIDERS.value
                    )
                )
            ]
            for provider in self.templates:
                if provider not in feed_providers + broker_providers:
                    continue
                overrides: dict = deepcopy(self.templates[provider])
                overrides = {
                    item: value.replace(
                            '$', str(num)
                        ).replace(
                            '@', letters[num]
                        ) if isinstance(value, str) else value for item, value
                    in overrides.items()
                }
                new_weekly.set_provider_overrides(provider, **overrides)

            if isinstance(existing, int) and recreate:
                self.weekly_folders[existing] = new_weekly
            elif existing is None:
                if recreate:
                    self.option.logger.warning(
                        f"{new_weekly.ticker}.{new_weekly.exchange} "
                        "week folder is not found, it will be created as new"
                    )
                self.weekly_folders.append(new_weekly)

    def get_diff(self):
        return DeepDiff(self.reference, self.get_instrument)