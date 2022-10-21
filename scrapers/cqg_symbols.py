import difflib
import logging
import re
import pandas as pd

from libs.derivative_adder import DerivativeAdder, DerivativeType


class CqgSymbols:
    cqg_url = 'http://help.cqg.com/fcm/symbols.xlsx'
    exchange_mapping = {
        'Australian Stock Exchange': 'ASX',
        'CBOE Futures Exchange (CFE)': 'CBOE',
        'CBOT E-Mini Event Contracts': 'CBOT',
        'CBOT/Globex': 'CBOT',
        'CME E-Mini Event Contracts': 'CME',
        'CME Event Contracts': 'CME',
        'COMEX/Globex (COMEXG)': 'COMEX',
        'Comex Event Contracts': 'COMEX',
        'Eurex': 'EUREX',
        'Hong Kong Futures Exchange (HKFE)': 'HKEX',
        'ICE Futures Europe': 'ICE',
        'ICE Futures Europe Commodities - Softs': 'ICE',
        'ICE Futures Europe � Financials': 'ICE',
        'ICE Futures Singapore': 'ICE',
        'ICE Futures U.S.': 'ICE',
        # 'IFM CBOT': 'CBOT',
        # 'IFM CME': 'CME',
        # 'IFM Comex': 'COMEX',
        # 'IFM ICE US': 'ICE',
        # 'IFM NYMEX': 'NYMEX',
        'Osaka Securities Exchange (OSE)': 'OE',
        'Singapore Exchange (SGX)': 'SGX'
    }

    type_mapping = {
        # 'Bundle',
        # 'Butterfly',
        'Calendar spread': 'calendar_spread',
        # 'Cash Spot',
        # 'Condor',
        # 'Crack',
        # 'Double Butterfly',
        # 'Equity',
        # 'Fixed Income',
        # 'Forex',
        # 'Forward',
        'Future': 'future',
        'Futures': 'future',
        'Inter-commodity Spread of Strips': 'spread',
        # 'Inter-commodity calendar spread',
        'Inter-commodity spread': 'spread',
        'Net change quoted inter-commodity spread': 'spread',
        'Options': 'option',
        'Pack': 'future',
        # 'Pack Spread',
        'Reduced tick calendar spread': 'calendar_spread',
        # 'Relative Daily Future',
        # 'Spread of Strips (average of leg prices)',
        # 'Strategy as a single instrument',
        # 'Strip'
    }

    def __init__(self):
        self.symbols_df = pd.read_excel(self.cqg_url, skiprows=4)
        self.symbols_df.drop(
            index=self.symbols_df.loc[self.symbols_df.apply(
                lambda row: row['Exchange'] not in self.exchange_mapping,
                axis=1
            )].index,
            inplace=True
        )
        self.symbols_df.drop(
            index=self.symbols_df.loc[self.symbols_df.apply(
                lambda row: row['Instrument'] not in self.type_mapping,
                axis=1
            )].index,
            inplace=True
        )
        for cqg_exch, sdb_exch in self.exchange_mapping.items():
            self.symbols_df.loc[self.symbols_df['Exchange'] == cqg_exch, 'Exchange'] = sdb_exch
        for cqg_type, sdb_type in self.type_mapping.items():
            self.symbols_df.loc[self.symbols_df['Instrument'] == cqg_type, 'Instrument'] = sdb_type

        self.symbols_df['Description'] = self.symbols_df.apply(lambda row: row['Description'].strip(), axis=1)
    
    def get_series_details(self, series: str, instrument_type: str):
        ticker, exchange = series.split('.')[:2]
        da = DerivativeAdder.from_sdb(ticker, exchange, derivative=instrument_type)
        description = da.series.instrument.get('shortName', '')
        return description, exchange

    def get_symbolname(
            self,
            series: str = None,
            instrument_type: str = None,
            description: str = None,
            exchange: str = None
        ):
        def get_suggestions(description: str, df: pd.DataFrame):
            prepared = re.sub(
                r'([Ff]uture(s)?( )?([Oo]n )?|[Oo]ption(s)?( )?([Oo]n )?|[Ss]pread(s)?( )?([Oo]n )?)',
                '',
                description
            )
            result = difflib.get_close_matches(prepared, df['Description'].to_list(), cutoff=0.82) # 0.82
            numbers = re.findall(r'\d+', description)
            if numbers:
                result = [x for x in result if all(num in x for num in numbers)]
            return result

        def filter_currencies(match: re.Match, df_description: str):
            if match.group('first_ccy') in df_description and match.group('second_ccy'):
                return True
            return False

        special_words = [
            'TAS',
            'NTR',
            'Mini',
            'Micro',
            'Weekly',
            'Ultra',
            'Dividend',
            'FTSE',
            'MSCI'
        ]
        if instrument_type.replace(' ', '_') == 'OPTION_ON_FUTURE':
            instrument_type = 'OPTION'
        if instrument_type not in DerivativeType.__members__:
            logging.warning(f"{instrument_type=} is invalid")
            return None
        if series:
            description, exchange = self.get_series_details(series, instrument_type)
        if not description or not exchange:
            logging.warning(f'Invalid {description=} or {exchange=}')
            return None
        candidates_df = self.symbols_df[
            (self.symbols_df['Instrument'] == instrument_type.lower()) &
            (self.symbols_df['Exchange'] == exchange)
        ]
        for sw in special_words:
            if sw.lower() in description.lower():
                candidates_df = candidates_df[candidates_df.apply(
                    lambda row: sw in row['Description'],
                    axis=1
                )]
            else:
                candidates_df = candidates_df[candidates_df.apply(
                    lambda row: sw not in row['Description'],
                    axis=1
                )]
        match_currencies = re.search(
            r'(?P<first_ccy>[A-Z]{3})\/(?P<second_ccy>[A-Z]{3})',
            description
        )
        if match_currencies:
            candidates_df = candidates_df[candidates_df.apply(
                lambda row: filter_currencies(match_currencies, row['Description']),
                axis=1
            )]
        suggestions_df = candidates_df[candidates_df.apply(
            lambda row: row['Description'] in get_suggestions(description, candidates_df)[:1],
            axis=1
        )]
        return suggestions_df
        # return suggestions_df['Symbol'].to_list()
