import logging
import pandas as pd
import re
import requests

class IceUnreachableError(Exception):
    pass

class Ice:
    SPEC_URL = 'https://www.theice.com/api/productguide/spec/{}/'
    ALL_ICE_SYMBOLS = 'https://www.theice.com/api/productguide/info/codes/all/csv'
    MIC_CODES = {
        'US': [
            'ICUS', # ICE US
            'IFUS', # US futures
            'IFED', # US Energy Division
        ],
        'EU': [
            'ICEU', # ICE Europe
            'IFLL', # LIFFE futures
            'IFEU', # Europe futures
            'IFLX',  # LIFFE commodities
            'IFLO', # LIFFE options
        ],
        'SG': [
            'IFSG'  # Singapore futures
        ],
    }
        # the rest are not interesting:
        # NDEX - Endex Europe futures (we can't execute these)
        # IMEQ - Amsterdam equity options
        # NDCM - UK gas spots
        # NDXS - European gas spots
        # Bilateral
        # NGXC - US Natural Gas Exchange
    
    def __init__(self) -> None:
        self.product_id_re = re.compile(r'http[s]?://www.theice.com/products/(?P<product_id>\d+)')
        url_formatting_re = re.compile(r'\"=HYPERLINK\(\"\"(?P<url>http[s]?://www.theice.com/products/\d+)\"\",\"\"(?P<product>.*)\"\"\)\"')
        response = requests.get(self.ALL_ICE_SYMBOLS)
        if not response.ok:
            raise IceUnreachableError(f'Cannot get data from {self.ALL_ICE_SYMBOLS}')
        strings = response.text.splitlines()
        # remove excel formulae, remove (notes in parentheses), split 'PRODUCT' column into 'URL' and 'PRODUCT'
        strings[0] = '"URL",' + strings[0].replace(' (Click to open in Browser)', '')
        strings[0] = strings[0][1:-1] # strip quotes 
        strings[0] = strings[0].split('","')
        for i, s in enumerate(strings[1:], start=1):
            # replace excel formula with valid url and product name
            url_match = re.match(url_formatting_re, s)
            s = f"\"{url_match.group('url')}\",\"{url_match.group('product')}\"{s[url_match.end():]}"
            # get rid of unsafe html symbol codes
            s = re.sub(r'(&\w+\;|&\#\d+\;)', '', s)
            # finally for aestetic reasons get rid of double spaces
            s = s.replace('  ', ' ')[1:-1] # and strip qoutes at the beginning and at the end
            strings[i] = s.split('","')
        # now we're good to convert this pile of text into data
        self.all_ice_products = pd.DataFrame.from_records(strings[1:], columns=strings[0])
        self.__clear_useless()

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __get(self, product_id):
        response = requests.get(self.SPEC_URL.format(product_id))
        if response.ok:
            return response.json()
        self.logger.debug(f'{response.status_code}: {response.text}')
        return {}

    def __clear_useless(self):
        filter_out = {
            'GROUP': [
                'Single Stock Options',
                'Endex Derivatives'
            ],
            'MIC CODE': [
                'NDEX',
                'NGXC',
                'ICES',
                'IEPA',
                'IMEQ'
            ],
            'CLEARING VENUE': [
                'NGXC'
            ]
        }
        for key, val in filter_out.items():
            for item in val:
                self.all_ice_products = self.all_ice_products.loc[
                    self.all_ice_products[key] != item
                ]


    def get_contract_spec(
            self,
            sym_type: str,
            symbol: str = None,
            additional: dict = {},
            # product_id: str = None,
            # url: str = None,
            # key_words: str = None
        ):
        if sym_type in ['OPTION', 'FUTURE']:
            instrument_type = sym_type
        elif sym_type == 'OPTION ON FUTURE':
            instrument_type = 'OPTION'
        else:
            self.logger.error(f"sym_type should be one of 'FUTURE', 'OPTION' or 'OPTION ON FUTURE'")
            return {}
        if additional.get('url'):
            match = re.match(self.product_id_re, additional['url'])
            if match:
                return self.__get(match.group('product_id'))
            else:
                self.logger.error('malformed url, nothing is found')
        if additional.get('product_id'):
            return self.__get(additional['product_id'])
        keyword_list = []
        if additional.get('description'):
            # decide how to split
            if ', ' in additional['description']:
                keyword_list = additional['description'].split(', ')
            elif ',' in additional['description']:
                keyword_list = additional['description'].split(',')
            else:
                keyword_list = additional['description'].split(' ')
        if additional.get('country_code') and additional['country_code'] in self.MIC_CODES:
            allowed_mics = self.MIC_CODES[additional['country_code']]
        else:
            allowed_mics = [x for y in self.MIC_CODES.values() for x in y]
        found = [x[1] for x
            in self.all_ice_products[self.all_ice_products['PHYSICAL'] == symbol].iterrows()
            if x[1]['MIC CODE'] in allowed_mics]
        if keyword_list:
            filtered = reversed(
                sorted(
                    [
                        x for x in found
                        if next((
                            y for y in keyword_list
                            if y.lower() in x['PRODUCT'].lower()
                        ), None)
                    ],
                    key=lambda f: len([
                        y for y in keyword_list
                        if y.lower() in f['PRODUCT'].lower()
                    ])
                )
            )
        else:
            filtered = found
        sym_spec_list = []
        for f in filtered:
            self.logger.info(f"{symbol} - {f['PRODUCT']}")
            f_id = re.match(self.product_id_re, f['URL']).group('product_id')
            f_spec = self.__get(f_id)
            if f_spec['productSpecType'] == instrument_type.capitalize():
                sym_spec_list.append(f_spec)
                self.logger.info(f"{f_spec['specName']} is added to the list")
        if not sym_spec_list:
            self.logger.error(f"nothing has been found for {symbol} | {additional} | {sym_type}")
            return {}
        elif len(sym_spec_list) == 1:
            return sym_spec_list[0]
        else:
            self.logger.error(f"found more than one spec, consider some additional parameters")
            self.logger.error('available options: product_id, url, description')
            return {}
            

