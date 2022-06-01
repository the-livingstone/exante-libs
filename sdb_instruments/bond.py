import asyncio
import datetime as dt
import logging
from copy import copy, deepcopy
from dataclasses import dataclass
from deepdiff import DeepDiff
from libs import sdb_schemas_cprod as cdb_schemas
from libs import sdb_schemas as sdb_schemas
from libs.sdb_handy_classes import Instrument, NoInstrumentError, get_part, BOND_REGIONS
from pprint import pformat, pp
import re

@dataclass
class Bond(Instrument):
    isin: str
    payload: dict = None
    exchange: str = None
    env: str = 'prod'
    
    def __post_init__(self):
        if self.env == 'prod':
            self.schema = sdb_schemas.BondSchema
        elif self.env == 'cprod':
            self.schema = sdb_schemas.BondSchema
        if not self.payload:
            self.payload = {}
        super().__init__(self.schema, self.payload, env=self.env)
        self.pay_freqs = [
            ('none', 0),
            ('annually', 1),
            ('semiannually', 2),
            ('quarterly', 4),
            ('monthly', 12)
        ]
        # self.required_fields = [
        #     'identifiers/ISIN',
        #     'identifiers/FIGI',
        #     'expiry',
        #     'maturityDate',
        #     'currency',
        #     'couponRate',
        #     'issuerType',
        #     'paymentFrequency',
        #     'country',
        #     'countryRisk',
        #     'exchangeId'
        # ]

        self.ticker = self.isin
        self.bond_folder = next(
            x for x in self.tree 
            if len(x['path']) == 2 and x['name'] == 'BOND'
        )
        self.__find_bond()

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def us_date_to_dt(usdate: str) -> dt.date:
        if usdate == 'PERP':
            return dt.date(2099, 1, 1)
        month, day, year = usdate.split('/')
        if len(year) == 2:
            return dt.date(year=int('20' + year), month=int(month), day=int(day))
        else:
            return dt.date(year=int(year), month=int(month), day=int(day))

    def iso_date_to_us(self, isodate: str) -> str:
        if isodate == 'PERP':
            return 'PERP'
        else:
            year, month, day = isodate.split('-')
            return f"{month}/{day}/{year[-2:]}"

    def __find_bond(self):
        possible_exchanges = [
                x[1] for x
                in self.sdbadds.get_list_from_sdb('exchanges')
                if x[0] == self.exchange
            ]
        bonds = [
            x for x in self.tree
            if x['name'] == self.isin
            or x['ticker'] == self.isin
            or x.get('identifiers', {}).get('ISIN') == self.isin 
        ]
        if self.exchange and not self.instrument:
            for b in bonds:
                if b.get('exchangeId') in possible_exchanges:
                    self.instrument = b
                    return True
                elif b.get('exchangeId'):
                    continue
                elif asyncio.run(self.sdbadds.build_inheritance(b['_id'])).get('exchangeId') in possible_exchanges:
                    self.instrument = b
                    return True
                else:
                    continue
            return False
        else:
            if not len(bonds):
                return False
            elif len(bonds) == 1:
                self.instrument = bonds[0]
                return True
            else:
                self.logger.error(
                    f"Cannot choose bond unambigously. "
                    f"Found possible instruments: {[x['symbolId'] for x in bonds]}"
                )
                raise NoInstrumentError("Cannot choose bond unambigously")

    def add(self, isin: str, expiration: str = None, abbreviated_name: str = None, force_changes=False, **kwargs):
        if self.instrument.get('_id') and len([x for x in self.instrument if x[0] == '_']) < 4:
            self.instrument = asyncio.run(self.sdb.get(self.instrument['_id']))
            reference = deepcopy(self.instrument)
        elif not self.instrument.get('_id'):
            self.instrument = {
                'isAbstract': False,
                'isTrading': True,
                'name': isin,
                'ticker': isin,
                'identifiers': {
                    'ISIN': isin,
                },
                'path': self.bond_folder['path']
            }
            reference = {}
        else:
            reference = deepcopy(self.instrument)
        # if not self.instrument.get('_id') or force_changes:
        for field, value in kwargs.items():
            if not get_part(self.instrument, field.split('/')) or force_changes:
                result = self.set_field_value(value, field.split('/'))
        if not self.instrument.get('expiry') \
            or not self.instrument.get('maturityDate') \
            or force_changes:
            
            if re.match(r'\d{1,2}\/\d{1,2}\/\d{4}|PERP', expiration):
                self.instrument.update({
                    'expiry': self.sdb.date_to_sdb(self.us_date_to_dt(expiration)),
                    'maturityDate': self.sdb.date_to_sdb(self.us_date_to_dt(expiration))
                })
            elif re.match(r'\d{4}-\d{2}-\d{2}', expiration):
                self.instrument.update({
                    'expiry': self.sdb.date_to_sdb(dt.date.fromisoformat(expiration)),
                    'maturityDate': self.sdb.date_to_sdb(dt.date.fromisoformat(expiration))
                })
            if not self.instrument.get('shortName') \
                or not re.search(r'\d{1,2}\/\d{1,2}\/\d{4}|PERP', self.instrument['shortName']) \
                or force_changes:
                
                self.compile_description(abbreviated_name, expiration, **kwargs)
        
        subfolder_id = None
        if self.instrument.get('country') and self.instrument['country'] not in ['GB', 'US']:
            subfolder_name = next((x for x in BOND_REGIONS if self.instrument['country'] in BOND_REGIONS[x]), None)
            if subfolder_name:
                self.logger.info(f'Selected subfolder {subfolder_name}')
                subfolder_id = next((
                    x['_id'] for x
                    in self.tree
                    if x['path'][:2] == self.bond_folder['path']
                    and x['name'] == subfolder_name
                ), None)
                if subfolder_id:
                    self.instrument['path'].append(subfolder_id)
                else:
                    self.logger.warning(f'Subfolder {subfolder_name} is not found in sdb')
        elif self.instrument.get('country') in ['GB', 'US'] and self.instrument.get('issuerType'):
            subst = {
                'GB': 'UK',
                'US': 'US',
                'corporate': 'Corporate',
                'government': 'Sovereign'
            }
            folder_name = f"{subst[self.instrument['country']]} {subst[self.instrument['issuerType']]}"
            subfolder_id = next((
                x['_id'] for x
                in self.tree
                if x['path'][:2] == self.bond_folder['path']
                and x['name'] == folder_name
            ), None)
            if subfolder_id:
                self.instrument['path'].append(subfolder_id)
            else:
                self.logger.warning(
                    f"Subfolder {folder_name} is not found in sdb"
                )
        else:
            self.logger.warning('Check country and issuerType fields')
        if not subfolder_id:
            self.logger.warning('Cannot select subfolder for this bond, pls do it manually')

        # validation = self.validate_instrument()
        # if validation is not True:
        #     self.instrument = reference
        # return validation

    def compile_description(self, abbreviated: str, expiration: str = None, **kwargs):
        if self.instrument.get('couponRate') is not None:
            cr = self.instrument['couponRate']
        elif kwargs.get('couponRate') is not None:
            cr = kwargs['couponRate']
        else:
            self.logger.warning('No coupon rate, cannot compile shortName')
            return False
        
        if expiration is not None:
            us_date = self.iso_date_to_us(expiration)
            # convert iso (yyyy-mm-dd) to us (mm/dd/yyyy), PERP if perpetual
        elif self.instrument.get('expiry') is not None:
            us_date = self.iso_date_to_us(
                self.sdb.sdb_to_date(
                    self.instrument['expiry']
                ).isoformat()
            )
            # convert expiry dict to us format date (mm/dd/yyyy), PERP if it's perpetual
        else:
            self.logger.warning('No expiration date, cannot compile shortName')
            return False

        if cr%1: # eliminate fractional part if it's 0
            self.instrument['shortName'] = f"{abbreviated} {cr} {us_date}"
        else:
            self.instrument['shortName'] = f"{abbreviated} {int(cr)} {us_date}"
        if self.instrument.get('paymentFrequency'):
            pf_in_word = next(x[0] for x in self.pay_freqs if x[1] == self.instrument['paymentFrequency'])
            self.instrument['description'] = f"{self.instrument['shortName']}, {pf_in_word}"
        else:
            self.instrument['description'] = self.instrument['shortName']

