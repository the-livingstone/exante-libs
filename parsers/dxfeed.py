#!/usr/bin/env python3

'''
https://downloads.dxfeed.com/specifications/dxFeed_Instrument_Profile_Format.pdf
https://downloads.dxfeed.com/specifications/dxFeed-Symbol-Guide.pdf
'''

import requests
import json
import csv
import logging

class DxFeed:
    def __init__(
            self,
            cred_file='/etc/support/auth/dxfeed.json',
            scheme='US',
            ipf='https://tools.dxfeed.com/ipf'
        ):
        self.set_region(scheme, cred_file)
        self.ipf = ipf

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")


    def set_region(self, region='US', cred_file='/etc/support/auth/dxfeed.json'):
        with open(cred_file, "r") as cf:
            auth = json.load(cf)
        if region not in auth:
            self.logger.error(
                f"region should be selected from list: {list(auth.keys())}, falling back to US"
            )
            self.auth = (auth['US']['user'], auth['US']['pwd'])
        else:
            self.auth = (auth[region]['user'], auth[region]['pwd'])
        self.session = requests.Session()
        self.session.auth = self.auth
        self.scheme = region

    @staticmethod
    def load_scheme(tpy, scheme) -> list:
        scheme[0] = tpy
        return scheme

    @staticmethod
    def __select(data, cfi=None, isin=None, opol=None, currency=None,
                 country=None, expiration=None, eurex_underlying=None) -> list:
        """
        Select record from data using params
        :param data: list from where will select data
        :param cfi: CFI code
        :param isin: ISIN code
        :param opol: MIC code
        :param currency: currency
        :param country: country
        :return: selected data as list
        """
        params = {param[0]: param[1] for param in locals().items() if param[0] != 'data' and param[1] is not None}
        for param in params.keys():
            if params[param]:
                data = [row for row in data if row[param.upper()] == params[param]]
        return data

    def get(self, TYPE: list = None, SYMBOL: list = None, CURRENCY: list = None, mode: str = 'dict', **kwargs):
        """
        Get data from ipf with params
        :param TYPE: list of type patterns, e.g. * or INDEX,STOCK
        :param SYMBOL: list of symbol patterns, e.g. * or IBM,AAPL,SPY
        :param CURRENCY: list of currency patterns, e.g. * or EUR,USD
        :param mode: type of return objects, can be: dict, list or raw
        :param kwargs: other fields for filtering purpouses
            (see https://downloads.dxfeed.com/specifications/dxFeed-Symbol-Guide.pdf)
        :return: list of objects defined by 'mode' param or string if 'mode' is raw
        """
        params = {
            'TYPE': ','.join(TYPE) if TYPE is not None else None,
            'SYMBOL': ','.join(SYMBOL) if SYMBOL is not None else None,
            'CURRENCY': ','.join(CURRENCY) if CURRENCY is not None else None
        }
        for el in kwargs:
            if kwargs[el] is not None:
                if isinstance(kwargs[el], list):
                    param = ','.join(kwargs[el])
                elif isinstance(kwargs[el], str):
                    param = kwargs[el]
                else:
                    raise Exception(f'Unknown parameter type of {el} - {kwargs[el]}')
                params[el] = param

        if mode == 'raw':
            res = self.session.get(url=self.ipf, params=params)
            if res.text == '':
                logging.info(f'Received null-string from ipf! Result is: {res}')
            return res.text
        else:
            type_keys_tuples = {}
            data = []
            stream = self.session.get(url=self.ipf, params=params, stream=True)
            for line in stream.iter_lines(decode_unicode=True):
                if line == '' or line == '##':
                    continue
                decoded_line = next(csv.reader([line], skipinitialspace=True))

                if mode == 'list':
                    data.append(decoded_line)
                else:
                    if decoded_line[0].startswith('#') and decoded_line[0].endswith('TYPE'):
                        type_ = decoded_line[0].split('#')[1].split(':')[0]
                        decoded_line[0] = 'TYPE'
                        type_keys_tuples[type_] = decoded_line
                    else:
                        if type_keys_tuples and decoded_line:
                            if decoded_line[0] in type_keys_tuples:
                                data.append(dict(zip(type_keys_tuples[decoded_line[0]], decoded_line)))
                        else:
                            raise RuntimeError(
                                'Did not find strings in ipf that define keys')
            stream.close()
            if len(data) == 0:
                logging.info(f'Nothing found with params {params}, try using wildcards, like * or ?')

            return data

    def search_stock(self, ticker: list = None, description=None, cfi=None, isin=None):
        """
        Search STOCK in IPF
        :param ticker: list of symbol patterns, e.g. * or IBM,AAPL,SPY
        :param description: string with patterns, e.g 'Apple', 'A'
        :param cfi: CFI code as string for search
        :param isin: ISIN code
        :return: list [dict1, dict2] or None
        """
        data = self.get(['STOCK'], ticker, mode='list')

        if data:
            scheme = self.load_scheme('TYPE', data.pop(0))
            data = [{key: record[i] for i, key in enumerate(scheme)} for record in data if len(record) > 1]
            if description:
                data = [row for row in data if description in row['DESCRIPTION']]
            return self.__select(data, cfi=cfi, isin=isin)
        return []

    def search_etf(self, ticker: list = None, description=None, isin=None):
        """
        Search ETF in IPF
        :param ticker: list of symbol patterns, e.g. * or IBM,AAPL,SPY
        :param description: string with patterns, e.g 'Apple', 'A'
        :param isin: ISIN code only for EU
        :return: list [dict1, dict2] or None
        """
        data = self.get(['ETF'], ticker, mode='list')

        if data:
            scheme = self.load_scheme('TYPE', data.pop(0))
            data = [{key: record[i] for i, key in enumerate(scheme)} for record in data if len(record) > 1]
            if description:
                data = [row for row in data if description in row['DESCRIPTION']]
            return self.__select(data, isin=isin if self.scheme == 'EU' else None)
        return []

    def search_future(self, ticker: list = None, products: list = None,
                      isin=None, description=None, mic=None, cfi=None):
        """
        Search FUTURE in IPF
        :param ticker: list of symbol patterns, e.g. * or /ES
        :param products: list of products patterns, e.g. /ESZ3 is returned for /ES
        :param isin: ISIN code
        :param description: string with patterns, e.g 'Apple', 'A'
        :param mic: exchange MIC code
        :param cfi: CFI code as string for search
        :return: list [dict1, dict2] or None
        """
        data = self.get(['FUTURE'], ticker, mode='list', PRODUCT=products)

        if data:
            scheme = self.load_scheme('TYPE', data.pop(0))
            data = [{key: record[i] for i, key in enumerate(scheme)} for record in data if len(record) > 1]
            if description:
                data = [row for row in data if description in row['DESCRIPTION']]
            return self.__select(data, cfi=cfi, opol=mic, isin=isin)
        return []

    def search_spread(self, ticker: list = None):
        """
        Search SPREAD in IPF
        :param ticker: list of symbol patterns, e.g. * or /ES
        :return: list [dict1, dict2] or None
        """
        data = self.get(['SPREAD'], ticker, mode='list')

        if data:
            scheme = self.load_scheme('TYPE', data.pop(0))
            data = [{key: record[i] for i, key in enumerate(scheme)} for record in data if len(record) > 1]
            return data
        return []

    def search_option(self, ticker: list = None, products: list = None, description=None, mic=None, cfi=None):
        """
        Search OPTION in IPF
        :param ticker: list of symbol patterns, e.g. * or /ES
        :param products: list of products patterns, e.g. /ESZ3 is returned for /ES
        :param description: string with patterns, e.g 'Apple', 'A'
        :param mic: exchange MIC code
        :param cfi: CFI code as string for search
        :return: list [dict1, dict2] or None
        """
        data = self.get(['OPTION'], ticker, mode='list', PRODUCT=products)

        if data:
            scheme = self.load_scheme('TYPE', data.pop(0))
            data = [{key: record[i] for i, key in enumerate(scheme)} for record in data if len(record) > 1]
            if description:
                data = [row for row in data if description in row['DESCRIPTION']]
            return self.__select(data, cfi=cfi, opol=mic)
        return []
