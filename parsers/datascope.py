#!/usr/bin/env python3

# Module for work with Datascope DSS API
# Authors: Alexandr Tarasov, Noxilie Volkov
# Reviewers:
# License: BSD
# Links:
# https://developers.refinitiv.com/en/api-catalog/datascope-select/datascope-select-rest-api/tutorials
# https://select.datascope.refinitiv.com/DataScope/Home

from email import header
from urllib import response
import requests
import re
import logging
from datetime import datetime
from json import load, dump, JSONDecodeError

class DatascopeAuthError(Exception):
    pass


class Datascope:
    """
    Class for using Datascope DSS API
    More information about all methods:
    https://developers.refinitiv.com/en/api-catalog/datascope-select/datascope-select-rest-api/tutorials

    or use web-app from Datascope [we have access to InstrumentSearch, EquitySearch, FuturesAndOptionsSearch]:
    https://select.datascope.refinitiv.com/DataScope/Home
    """
    url = {
        'base': 'https://selectapi.datascope.refinitiv.com/RestApi/v1',
        'auth': '/Authentication/RequestToken',
        'equity': '/Search/EquitySearch',
        'future_and_option': '/Search/FuturesAndOptionsSearch',
        'instrument': '/Search/InstrumentSearch',
        'extraction': '/Extractions/ExtractWithNotes',
        'instrument_lists': '/Extractions/InstrumentLists',
        'report_templates': '/Extractions/TickHistoryIntradaySummariesReportTemplates',
        'schedules': '/Extractions/Schedules',
        'report_extractions': '/Extractions/ReportExtractions'
    }

    composite_fields = [
        'Asset Category',
        'Asset Category Description',
        'Bank Qualified Flag',
        'CFI Code',
        'Common Code',
        'Company Name',
        'Contributor Code',
        'Currency Code',
        'Currency Code Description',
        'Exchange Code',
        'Exchange Description',
        'Exercise Style',
        'Expiration Date',
        'First Notice Day',
        'Instrument ID',
        'Instrument ID Type',
        'Issuer Country Code',
        'Issuer Name',
        'Last Trading Day',
        'Local Code',
        'Lot Size',
        'Lot Units',
        'Market Code',
        'Market Code Description',
        'Market MIC',
        'Market Segment Name',
        'Method of Delivery',
        'Primary Trading RIC',
        'Quote Currency Code',
        'RIC',
        'RIC Root',
        'Round Lot Size',
        'Security Description',
        'Security Long Description',
        'Tick Value',
        'Ticker',
        'Underlying RIC',
        'Underlying Security Description',
    ]

    default_cred_file = '/etc/support/auth/reuters.json'

    def __init__(self, cred_file: str = None, user: str = None, password: str = None,
                 token_file: str = None, request_page_size: int = 1000):
        """
        For Datascope initialization we need a token. Token expires within 24 hours.
        If you use it more often you can set token_file and token will be saved into file.
        If you do not want to save a token set only user/password or crefile.

        Generated Exception without processing: DatascopeAuthError

        :param token_file: full path to token_file
        :param cred_file: full path to file with credentials
        :param user: username
        :param password: password
        :param request_page_size: limit item on one page, pls not set more if you not understand what it means
        """
        self.headers = {
            'Content-Type': 'application/json; odata=minimalmetadata'
        }
        credentials = {}
        if cred_file:
            try:
                with open(cred_file) as cf:
                    credentials = load(cf)
            except FileNotFoundError:
                self.logger.error(f'Can not open credentials file! {cred_file}')
        if not credentials and user is not None and password is not None:
            credentials = {'Username': user, 'Password': password}

        if not credentials:
            try:
                with open(self.default_cred_file) as cf:
                    credentials = load(cf)
            except FileNotFoundError:
                self.logger.info(f'Can not open default credentials file! {self.default_cred_file}')

        if token_file:
            try:
                with open(token_file) as tf:
                    data = load(tf)
            except FileNotFoundError:
                data = {'error': 'error'}
            expiry = data.get('expiry', 0)
            token = data.get('value')
            if expiry > datetime.utcnow().timestamp() + 1800:
                token = data['value']
            elif expiry <= datetime.utcnow().timestamp() + 1800:
                token = self.__get_token(credentials)
                expiry = datetime.utcnow().timestamp() + 86400
                with open(token_file, 'w') as tf:
                    dump({'expiry': expiry, 'value': token}, tf)
        else:
            token = self.__get_token(credentials)

        self.logger.debug(f'Token: {token}')
        self.headers.update({'Authorization': f'Token {token}'})
        self.page_size = request_page_size

    @property
    def page_size(self):
        return self.__page_size

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @page_size.setter
    def page_size(self, val):
        self.__page_size = int(val)
        self.headers['Prefer'] = f'odata.maxpagesize={self.__page_size}'

    def __get_token(self, credentials):
        """
        Getting token from Datascope.
        Generated exception DatascopeAuthError
        :param credentials: credentials data
        :return: token as string
        """
        if not credentials:
            raise DatascopeAuthError('Datascope credentials are not set')
        url = self.url['base'] + self.url['auth']
        token_data = {'Credentials': credentials}

        try:
            return requests.post(url, json=token_data, headers=self.headers).json()['value']
        except KeyError as error:
            raise DatascopeAuthError(f'Can not get token, error: {error}')

    def __post(self, data: dict, api='instrument', entity_id=None, handle='') -> list:
        """
        Make request to Datascope.
        :param data: dictionary with request params.
        :param api: api type [equity, future_and_option, instrument]
        :return: list of data
        """
        if api == 'extraction':
            payload = {'ExtractionRequest': data}
            res_type = 'Contents'
        elif api in ['instrument', 'equity', 'future_and_option']:
            payload = {'SearchRequest': data}
            res_type = 'value'
        elif api in ['instrument_lists', 'schedules']:
            payload = data
            res_type = None

        url = self.url['base'] + self.url[api]
        if entity_id:
            url += f"('{entity_id}')"
        url += handle
        result = []
        notes = []
        self.logger.debug(f'full url: {url}')
        self.logger.debug(f'headers: {self.headers}')
        self.logger.debug(f'payload: {payload}')
        try:
            response = {'@odata.nextlink': url}
            while '@odata.nextlink' in response:
                response = requests.post(response['@odata.nextlink'], json=payload, headers=self.headers).json()
                if 'Notes' in response:
                    notes.extend(response['Notes'])
                if res_type:
                    result.extend(response[res_type])
                else:
                    result = response
        except Exception as error:
            result = [error]
            self.logger.error(f"{error.__class__.__name__}: {error}")
        self.logger.debug(f'Found {len(result)} items:\n{result}')
        if notes:
            self.logger.debug(f'Additional result notes:\n{notes}')
        return result
    
    def __get(self, api: str, entity_id=None, handle=''):
        url = self.url['base'] + self.url[api]
        if entity_id:
            url += f"('{entity_id}')"
        url += handle
        self.logger.debug(f'full url: {url}')
        self.logger.debug(f'headers: {self.headers}')
        try:
            response = requests.get(url, headers=self.headers).json()
        except Exception as error:
            response = error
            self.logger.error(f"{error.__class__.__name__}: {error}")
        if isinstance(response, dict) and 'value' in response:
            return response['value']
        return response

    def __put(self, data, api: str, entity_id=None, handle=''):
        url = self.url['base'] + self.url[api]
        if entity_id:
            url += f"('{entity_id}')"
        url += handle
        self.logger.debug(f'full url: {url}')
        self.logger.debug(f'headers: {self.headers}')
        self.logger.debug(f'payload: {data}')
        try:
            response = requests.put(url, json=data, headers=self.headers)
            try:
                response = response.json()
            except  JSONDecodeError:
                response = response.status_code
        except Exception as error:
            response = error
            self.logger.error(f"{error.__class__.__name__}: {error}")
        return response

    def get_fields(self, report_name):
        """
        Get available fields for certain report type
        :param report_name: report name [Composite, TermsAndConditions, Owners, CorporateActions]
        return: list of fields with descriptions
        """
        response = requests.get(
            f"{self.url['base']}/Extractions/GetValidContentFieldTypes(ReportTemplateType="
            f"DataScope.Select.Api.Extractions.ReportTemplates.ReportTemplateTypes'{report_name}')",
            headers=self.headers
        ).json()
        return response['value']

    def search(self, identifier, base_type='Ric', search_type='Ric', groups: list = None):
        """
        Basic search.
        :param identifier: Datascope search string smth like 'LSO*' or 'AAPL.OQ'
        :param base_type: identifier type [ric, sedol, isin and other]
        :param search_type: search identifier type
        :param groups: list of searching groups [Equities, FuturesAndOptions and other]
        :return:
        """
        data = {
            'InstrumentTypeGroups': groups if groups else [
                'CollatetizedMortgageObligations',
                'Commodities',
                'Equities',
                'FuturesAndOptions',
                'GovCorp',
                'MortgageBackedSecurities',
                'Money',
                'Municipals',
                'Funds'
            ],
            'IdentifierType': base_type,
            'Identifier': identifier,
            'PreferredIdentifierType': search_type
        }
        return self.__post(data=data, api='instrument')

    def equity(self, identifier, ticker=None, currency: list = None, exchange_codes: list = None,
               base_type='Ric', search_type='Ric', only_active=True, asset_category: list = None):
        """
        Advanced Equities search.
        :param identifier: Datascope search string smth like 'LSO*' or 'AAPL.OQ'
        :param ticker: exchange ticker
        :param currency: currency
        :param exchange_codes: Datascope Exchange Code
        :param base_type: identifier type [ric, sedol, isin and other]
        :param search_type: search identifier type
        :param only_active:  asset status. Active or all
        :param asset_category: list of asset category
        :return:
        """
        data = {
            'AssetCategoryCodes': asset_category,
            'CurrencyCodes': currency,
            'ExchangeCodes': exchange_codes,
            'AssetStatus': 'Active' if only_active else None,
            'Ticker': ticker,
            'IdentifierType': base_type,
            'Identifier': identifier,
            'PreferredIdentifierType': search_type
        }
        return self.__post(data=data, api='equity')

    def futures(self, identifier, underlying=None, currency=None,
                exchange_codes=None, only_active=True, expiration=None, suffix=None):
        """
        Searching Futures RIC by RIC
        :param identifier: base RIC smth like LCO without maturity and any other symbols
        :param underlying: RIC of the underlying asset
        :param currency: currency
        :param exchange_codes: Datascope Exchange Code
        :param only_active: asset status. Active or all
        :param expiration: expiration date in ISO format 'YYYY-MM-DD' or datetime 'YYYY-MM-DDTHH:MM:SS.uuuZ'
        :return: dictionary like {exchange:ticker:maturity}
        """
        result = {}
        if suffix:
            regexp = re.compile(rf'^(?P<ticker>{identifier})(?P<maturity>[FGHJKMNQUVXZ]\d){suffix}$')
        else:
            regexp = re.compile(rf'^(?P<ticker>{identifier})(?P<maturity>[FGHJKMNQUVXZ]\d)$')
        exp_payload = None
        if expiration:
            exp_payload = {
                '@odata.type': '#DataScope.Select.Api.Search.DateValueComparison',
                'ComparisonOperator': 'Equals',
                'Value': expiration
            }
        data = {
            'AssetStatus': 'Active' if only_active else None,
            'CurrencyCodes': currency,
            'FuturesAndOptionsType': 'Futures',
            'ExchangeCodes': exchange_codes,
            'IdentifierType': 'Ric',
            'Identifier': f'{identifier}*',
            'PreferredIdentifierType': 'Ric',
            'UnderlyingRic': underlying,
            'ExpirationDate': exp_payload
        }
        for item in self.__post(data=data, api='future_and_option'):
            matched = regexp.match(item['Identifier'])
            if 'ExpirationDate' in item and item['ExpirationDate'] \
                    and matched and matched.group('ticker') == identifier:
                exc = item['Source']
                expiry = item['ExpirationDate'][:10]
                if exc not in result:
                    result[exc] = {}
                if identifier not in result[exc]:
                    result[exc][identifier] = {}
                result[exc][identifier].update({expiry: {'MMY': matched.group('maturity')}})
        return result

    def options(self, identifier, underlying=None, currency=None,
                exchange_codes=None, only_active='Active', option_type=None, expiration=None, suffix=None):
        """
        Searching Options RIC by RIC
        :param identifier: base RIC smth like LCO without maturity and any other symbols
        :param underlying: RIC of the underlying asset
        :param currency: currency
        :param exchange_codes: Datascope Exchange Code
        :param only_active: asset status. Active or all
        :param option_type: 'Option' or 'FuturesOnOptions'
        :param expiration: expiration date in ISO format 'YYYY-MM-DD' or datetime 'YYYY-MM-DDTHH:MM:SS.uuuZ'
        :return: dictionary like {exchange:tiker:maturity:CALL/PUT:[]}
        """
        result = {}
        if suffix:
            regexp = re.compile(rf'^(?P<ticker>{identifier})(?P<strike>\d+)(?P<maturity>\w\d){suffix}$')
        else:
            regexp = re.compile(rf'^(?P<ticker>{identifier})(?P<strike>\d+)(?P<maturity>\w\d)$')
        exp_payload = None
        if expiration:
            exp_payload = {
                '@odata.type': '#DataScope.Select.Api.Search.DateValueComparison',
                'ComparisonOperator': 'Equals',
                'Value': expiration
            }
        data = {
            'AssetStatus': 'Active' if only_active else None,
            'CurrencyCodes': currency,
            'FuturesAndOptionsType': option_type if option_type else 'Options',
            'ExchangeCodes': exchange_codes,
            'IdentifierType': 'Ric',
            'Identifier': f'{identifier}*',
            'PreferredIdentifierType': 'Ric',
            'UnderlyingRic': underlying,
            'ExpirationDate': exp_payload
        }
        for item in self.__post(data=data, api='future_and_option'):
            matched = regexp.match(item['Identifier'])
            if item.get('ExpirationDate') is not None \
                    and matched and matched.group('ticker') == identifier:
                exc = item['Source']
                expiry = item['ExpirationDate'][:10]
                if exc not in result:
                    result[exc] = {}
                if identifier not in result[exc]:
                    result[exc][identifier] = {}
                if expiry not in result[exc][identifier]:
                    result[exc][identifier][expiry] = {'CALL': [], 'PUT': []}
                if item['PutCallCode'] == 'Call':
                    result[exc][identifier][expiry]['CALL'].append(float(item['StrikePrice']))
                else:
                    result[exc][identifier][expiry]['PUT'].append(float(item['StrikePrice']))
                result[exc][identifier][expiry]['ric'] = item['Identifier']
        return result

    def futures_raw(self, identifier, underlying=None, currency=None, exchange_codes=None,
                    base_type='Ric', search_type='Ric', only_active=True):
        """

        Advanced Futures search
        :param identifier: search string like LCOZ0
        :param underlying: RIC of the underlying asset
        :param currency: currency
        :param exchange_codes: Datascope Exchange Code
        :param base_type: identifier type [ric, sedol, isin and other]
        :param search_type: search identifier type
        :param only_active: asset status. Active or all
        :return: list of Datascope data
        """
        data = {
            'AssetStatus': 'Active' if only_active else None,
            'CurrencyCodes': currency,
            'FuturesAndOptionsType': 'Futures',
            'ExchangeCodes': exchange_codes,
            'IdentifierType': base_type.capitalize(),
            'Identifier': identifier,
            'PreferredIdentifierType': search_type.capitalize(),
            'UnderlyingRic': underlying
        }
        return self.__post(data=data, api='future_and_option')

    def options_raw(self, identifier, underlying=None, currency=None, exchange_codes=None,
                    base_type='Ric', search_type='Ric', only_active=True, option_type=None):
        """
        Advanced Options search
        :param identifier: search string like LCO*0 or LCO10000H1
        :param underlying: RIC of the underlying asset
        :param currency: currency
        :param exchange_codes: Datascope Exchange Code
        :param base_type: identifier type [ric, sedol, isin and other]
        :param search_type: search identifier type
        :param only_active: asset status. Active or all
        :param option_type:
        :return: list of Datascope data
        """
        data = {
            'AssetStatus': 'Active' if only_active else None,
            'CurrencyCodes': currency,
            'FuturesAndOptionsType': option_type if option_type else 'Options',
            'ExchangeCodes': exchange_codes,
            'IdentifierType': base_type.capitalize(),
            'Identifier': identifier,
            'PreferredIdentifierType': search_type.capitalize(),
            'UnderlyingRic': underlying
        }
        return self.__post(data=data, api='future_and_option')

    def composite(self, identifiers: list, fields: list = None, base_type='Isin'):
        """
        Get composite report data (includes terms_n_conditions fields)
        :param identifiers: list of identifiers
        :param fields: list of fields of composite report to get
        :param base_type: identifier type [ric, sedol, isin and other]
        return list of Datascope data
        """
        if fields is None:
            fields = ['Ticker']
        if fields[0] == 'all':
            fields = self.composite_fields
        data = {
            '@odata.type': '#DataScope.Select.Api.Extractions.ExtractionRequests.CompositeExtractionRequest',
            'ContentFieldNames': fields,
            'IdentifierList': {
                '@odata.type': '#DataScope.Select.Api.Extractions.ExtractionRequests.InstrumentIdentifierList',
                'InstrumentIdentifiers': [
                    {'Identifier': el, 'IdentifierType': base_type} for el in identifiers
                ]
            }
        }
        return self.__post(data=data, api='extraction')

    def terms_n_conditions(self, identifiers: list, fields: list = None, base_type='Isin'):
        """
        Get terms_n_conditions report data
        :param identifiers: list of identifiers
        :param fields: list of fields of composite report to get
        :param base_type: identifier type [ric, sedol, isin and other]
        return list of Datascope data
        """
        if fields is None:
            fields = ['Ticker']
        data = {
            '@odata.type': '#DataScope.Select.Api.Extractions.ExtractionRequests.TermsAndConditionsExtractionRequest',
            'ContentFieldNames': fields,
            'IdentifierList': {
                '@odata.type': '#DataScope.Select.Api.Extractions.ExtractionRequests.InstrumentIdentifierList',
                'InstrumentIdentifiers': [
                    {'Identifier': el, 'IdentifierType': base_type} for el in identifiers
                ]
            }
        }
        return self.__post(data=data, api='extraction')

    # def get_instrument_lists(self):
    #     """
    #     Get all instrument lists
    #     """
    #     return self.__get('instrument_lists')


    def get_instrument_list(self, list_id: str = None):
        """
        Get single instrument 
        :param list_id: Instrument list id
        """
        return self.__get('instrument_lists', list_id)
    
    
    def create_instrument_list(self, list_name: str = datetime.now().strftime("%Y%m%d-%H%M%S")):
        """
        Create new instrument list
        :param list_name: name of instrument list
        """
        payload = {
            'Name': list_name
        }
        return self.__post(payload, 'instrument_lists')


    def add_instrument_to_list(self, instruments: list, list_id: str):
        """
        Add instrument to existing instrument list by RIC
        :param instruments: list of instrument RICs
        :param list_id: instrument list id
        """
        handle = f"/DataScope.Select.Api.Extractions.InstrumentListAppendIdentifiers"
        identifiers = [
            {
                'Identifier': ric,
                'IdentifierType': 'Ric'
            }
            for ric in instruments
        ]
        payload = {
            'Identifiers': identifiers,
            'KeepDuplicates': 'false'            
        }
        return self.__post(payload, 'instrument_lists', list_id, handle)
        

    # def get_intraday_report_templates(self):
    #     """
    #     Get all TickHistoryIntradaySummaries Report Templates
    #     """
    #     return self.__get('report_templates')
    

    def get_intraday_report_template(self, template_id: str = None):
        """
        Get single TickHistoryIntradaySummaries Report Template
        :param template_id: target template id
        """
        return self.__get('report_templates', template_id)

    
    def update_report_dates(self, template_id: str, start_date: str = None, end_date: str = None):
        """
        Update query date interval in template
        :param template_id: template id for update
        :param start_date: query should start from datetime
        :param end_date: qery should end to datetime 
        """
        template_data = self.get_intraday_report_template(template_id)
        if not start_date:
            start_date = '2000-01-01T00:00:00.000Z'
        if not end_date:
            end_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        try:
            template_data['Condition']['QueryStartDate'] = start_date
            template_data['Condition']['QueryEndDate'] = end_date
            response = self.__put(template_data, 'report_templates', template_id)
        except KeyError as error:
            response = f'Error while get report template data: {error}, {template_data}'
            self.logger.error(f"{error.__class__.__name__}: {error}")
        return response


    # def get_all_schedules(self):
    #     """
    #     Get all extraction schedules 
    #     """
    #     return self.__get('schedules')
        

    def get_schedule(self, schedule_id: str = None):
        """
        Get single extraction schedules
        :param schedule_id: extraction schedule id
        """
        return self.__get('schedules', schedule_id)


    def create_extraction_schedule(self, template_id: str, list_id: str, schedule_file_name: str):
        """
        Create new immediate schedule to extraction
        :param schedule_file_name: set name for new schedule and extracted file
        :param list_id: instrument list id
        :param template_id: extraction template id
        """
        payload = {
            "Name": schedule_file_name,
            "OutputFileName": schedule_file_name,
            "TimeZone": "UTC",
            "Recurrence": {
                "@odata.type": "#DataScope.Select.Api.Extractions.Schedules.SingleRecurrence",
                "IsImmediate": 'true'
            },
            "Trigger": {
                "@odata.type": "#DataScope.Select.Api.Extractions.Schedules.ImmediateTrigger",
                "LimitReportToTodaysData": 'false'
            },
            "ListId": list_id,
            "ReportTemplateId": template_id
        }
        return self.__post(payload, 'schedules')


    def get_completed_extraction(self, schedule_id: str):
        """
        Returns the list of completed extractions for this schedule.
        :param schedule_id: extraction schedule id 
        """
        return self.__get('schedules', schedule_id, '/CompletedExtractions')


    def get_extracted_files_info(self, report_extraction_id: str):
        """
        A collection of files that belong to this extraction report
        :param report_extraction_id: report extraction id from compleated extraction of schedule
        """
        return self.__get('report_extractions', report_extraction_id, '/Files')

    
    def download_extracted_file(self, extracted_file_id: str, extracted_file_path: str):
        """
        Download the content of the extracted file. Since the download is a stream, 
        the client can read the content as if it were any other file. Not valid
        unless content exists. For Report files both full and partial, not that the may have changed since this file was produced.
        :param extracted_file_id: 
        """
        url = self.url['base'] + f"/Extractions/ExtractedFiles('{extracted_file_id}')/$value"
        local_filename = extracted_file_path
        with requests.get(url, stream=True, headers=self.headers) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        return local_filename


if __name__ == "__main__":
    print('Datascope API version 0.4.0')
