import asyncio
import logging
from copy import copy, deepcopy
from dataclasses import dataclass, field
from deepdiff import DeepDiff
from libs import sdb_schemas_cprod as cdb_schemas
from libs import sdb_schemas as sdb_schemas
from libs.sdb_instruments import Instrument, NoInstrumentError, NoExchangeError
from libs.sdb_instruments.instrument import stock_exchange_mapping
from pprint import pformat, pp
from typing import Dict, Optional, Union


@dataclass
class Stock(Instrument):
    # instrument parameters 
    ticker: str
    exchange: str
    shortname: Optional[str] = None
    identifiers: Optional[dict] = None
    parent_folder: Optional[str] = None
    env: str = 'prod'

    # init parameters
    recreate: bool = False
    silent: bool = False

    # non-init vars
    instrument: dict = field(init=False, default_factory=dict)
    reference: dict = field(init=False, default_factory=dict)


    def __post_init__(self):
        super().__init__(
            ticker=self.ticker,
            exchange=self.exchange,
            instrument={},
            instrument_type='STOCK',
            shortname=self.shortname,
            parent_folder=self.parent_folder,
            env=self.env
        )
        cfd_folder_id = asyncio.run(
            self.sdb.get_uuid_by_path(
                ['Root', 'CFD', 'STOCK'],
                self.tree
            )
        )
        stock_folder = next(
            x for x 
            in self.tree 
            if len(x['path']) == 2 
            and x['name'] == 'STOCK'
        )
        self.cfd_reqired_exchanges = [
            x for x in self.tree
            if len(x['path']) > 2
            and x['path'][-2] == cfd_folder_id
            and x['isAbstract']
        ]
        stock_folder_heirs = [
            x for x in self.tree
            if len(x['path']) > 2
            and x['path'][:2] == stock_folder['path']
        ]
        self.additional_folder_requirements = [
            x for x in stock_folder_heirs
            if len(x['path']) == 3
            and [
                y for y in stock_folder_heirs
                if y['isAbstract']
                and len(y['path']) > len(x['path'])
                and y['path'][:len(x['path'])] == x['path']
            ]
        ]
        self.instrument = asyncio.run(self.sdb.get(f'{self.ticker}.{self.exchange}'))
        if not self.instrument and not self.shortname:
            raise NoInstrumentError("Cannot create instrument without shortname")
        kwargs = {}
        if self.identifiers:
            for i_type, i_val in self.identifiers.items():
                kwargs.update({f"identifiers/{i_type}": i_val})
        if self.shortname:
            kwargs.update({
                'shortName': self.shortname,
                'description': self.shortname
            })
        if self.instrument:
            self.exchange_folder = asyncio.run(self.sdbadds.build_inheritance(
                self.instrument, include_self=False
            ))
            for underlined in [x for x in self.exchange_folder if x[0] == '_']:
                self.exchange_folder.pop(underlined)

        else:
            if self.custom_destination:
                exchange_folder_uuid = self.custom_destination
            elif self.exchange not in stock_exchange_mapping:
                exchange_folder_uuid = asyncio.run(self.sdb.get_uuid_by_path(
                    ['Root', 'STOCK', self.exchange], self.tree
                ))
            else:
                exchange_folder_uuid = asyncio.run(self.sdb.get_uuid_by_path(
                    ['Root', 'STOCK', stock_exchange_mapping[self.exchange]], self.tree
                ))
            if not exchange_folder_uuid:
                exchange_folder_uuid = self.advanced_search()
            self.exchange_folder = asyncio.run(self.sdbadds.build_inheritance(
                exchange_folder_uuid, include_self=True
            ))
            if len(self.exchange_folder['path']) > 3:
                self.child_folder = self.exchange_folder['name']
            for underlined in [x for x in self.exchange_folder if x[0] == '_']:
                self.exchange_folder.pop(underlined)
        try:
            self.add(**kwargs)
        except NoInstrumentError:
            raise NoInstrumentError(
                f"You should specify either child_folder (name): "
                f"""{[
                    x['name'] for x
                    in asyncio.run(self.sdb.get_heirs(self.exchange_folder['_id']))
                    if x['isAbstract']
                ]} or custom_destination (uuid)""")

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def advanced_search(self):
        stock_folder_id = asyncio.run(self.sdb.get_uuid_by_path(
            ['Root', 'STOCK'], self.tree
        ))
        all_stock_folders = [
            (x, next((
                y[0] for y in self.sdbadds.get_list_from_sdb('exchanges')
                if y[1] == x['exchangeId']), None)
            ) for x in self.tree
            if len(x['path']) > 2
            and x['path'][1] == stock_folder_id
            and x['isAbstract']
            and x.get('exchangeId')
        ]
        possible_stock_folders = [x for x in all_stock_folders if x[1] == self.exchange]
        if len(possible_stock_folders) == 1:
            return possible_stock_folders[0]
        elif not possible_stock_folders:
            raise NoExchangeError(f"Exchange {self.exchange} is not found in stocks")
        else:
            raise NoExchangeError(
                f"Cannot define stock exchange folder unambigously "
                f"{[x['name'] for x in possible_stock_folders]}"
            )
    
    def add(self, **kwargs):
        if not self.instrument:
            reference = {}
            self.instrument = {
                'isAbstract': False,
                'name': self.ticker,
                'ticker': self.ticker,
                'path': copy(self.exchange_folder['path']),
            }
            if self.exchange in self.additional_folder_requirements \
                and not self.custom_destination:

                additional_folder = next((
                    x for x in self.tree
                    if x['isAbstract']
                    and self.child_folder
                    and x['path'][-2] == self.exchange_folder['_id']
                    and x['name'] == self.child_folder
                ), None)
                if not additional_folder:
                    raise NoInstrumentError(
                        f"You should specify additional folder name: "
                        f"""{[
                            x['name'] for x
                            in asyncio.run(self.sdb.get_heirs(self.exchange_folder['_id']))
                            if x['isAbstract']
                        ]}""")
                self.instrument['path'].append(additional_folder['_id'])
                self.exchange_folder = asyncio.run(self.sdbadds.build_inheritance([
                    self.exchange_folder,
                    additional_folder
                ]))
                
        for field, val in kwargs.items():
            self.set_field_value(val, field.split('/'))

    def post_to_sdb(self, dry_run: bool = False):
        cfd_response = {'_id': True}
        if self.exchange in [x['name'] for x in self.cfd_reqired_exchanges]:
            cfd = deepcopy(self.instrument)
            for f in ['feeds', 'brokers']:
                if cfd.get(f):
                    cfd.pop(f)
            cfd_folder = next((
                x['path'] for x
                in self.cfd_reqired_exchanges
                if x['name'] == self.exchange
            ))
            if self.child_folder:
                cfd_folder.append(
                    next((
                        x['_id'] for x
                        in self.cfd_reqired_exchanges
                        if x['path'][-2] == cfd_folder['path'][-1]
                        and x['name'] == self.child_folder
                    ), None)
                )
                if cfd_folder[-1] is None:
                    raise NoExchangeError(
                        f"Cfd folder {self.exchange}/{self.child_folder} is not found"
                    )
            cfd['path'] = cfd_folder
            cfd_instrument = Instrument(sdb_schemas.CfdSchema, cfd)
            cfd_response = cfd_instrument.post_instrument(dry_run)
        if isinstance(cfd_response, dict) and cfd_response.get('_id'):
            response = super().post_instrument(dry_run)
            return response
        else:
            return cfd_response

