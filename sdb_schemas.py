import asyncio
from copy import deepcopy
import datetime as dt
from functools import reduce
import operator
from pprint import pformat
from pydantic import BaseModel, Field, validator, root_validator
from typing import Dict, List, Optional, Union
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional, SdbLists
import luhn
import logging

class SchemaLookupError(Exception):
    pass

type_mapping = {
    'boolean': bool,
    'integer': int,
    'number': float,
    'string': str,
    'array': list,
    'object': dict
}

class SchemaNavigation:
    schema_types = [
        'boolean',
        'integer',
        'number',
        'string',
        'array',
        'object'
    ]

    def __init__(self, schema: BaseModel) -> None:
        self.schema = schema.schema()
        self.references = {'root': self.__fish_out_refs()}
        self.references.update(
            {
                definition: self.__fish_out_refs(definition) for definition
                in self.schema['definitions'] if self.__fish_out_refs(definition)
            }
        )

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def __fish_out_refs(self, definition: str = None) -> dict:
        '''
        This lil' func converts this:
        {
            'SomeDefinition': {
                'title': 'someDefinition',
                'alias': 'someDefinition',
                'properties': {
                    'someField': {
                        'title': someField',
                        'alias': someField',
                        'allOf': [
                            {
                                '$ref': '#/definitions/SomeOtherDefinition'
                            }
                        ]
                    }
                }
            }
        }
        into this:
        {
            'SomeDefinition': {
                'someField': ['SomeOtherDefinition']
            }
        }
        '''
        lookup_section = self.schema['definitions'].get(definition) if definition else self.schema
        if not lookup_section:
            self.logger.warning(f"definition {definition} is not found in {self.schema['title']}")
            return dict()
        references = dict()
        for item, props in lookup_section['properties'].items():
            reference = next((
                [
                    list(x.values())[0].split('/')[-1] for x in val if '$ref' in x.keys()
                ] for key, val
                in props.items() if key in ['allOf', 'anyOf']),
                next((
                    [y.split('/')[-1] for x, y in val.items() if x == '$ref'] for key, val
                    in props.items() if key in ['additionalProperties', 'items']
                    ), None)
            )
            if reference:
                references.update({item: reference})
        return references

    def schema_lookup(self, path: list, **kwargs) -> list:
        '''
        method that shows what kind of part is on the given path in schema
        :param schema: schema to use
        :param path: keys of nested dicts (list indices are ignored)
        :param kwargs: are given to eliminate ambiguity in case of anyOf type {<fieldname>: <definition (or type)>}
        :return: properties of field [{<field_properties_set1>}, {<field_properties_set2>}, ...]
        '''
        result = []
        tree = self.schema
        additional = False
        for num, p in enumerate(path):
            # if there is providerOverrides key or number of gateway/account in list, just ignore it
            if isinstance(p, int) or additional:
                additional = False
                continue
            # how we deal with 'anyOf': when we face such item for the first time,
            # we iterate over all listed options and try to find the correct path among them
            # then it's time for magic:
            # · we take a fork from listed option, and call method recursively with
            #   same path but with preselected 'anyOf' option (we write its content to kwargs)
            # · if fork terminates earlier than path, we reject it by removing from kwargs
            # · if fork offers an acceptable path we write it into helping dict
            #   so when we face another fork we don't iteratate on failed but go strait to accceptable one 
            tree = tree.get('properties', {}).get(p, {})

            if tree.get('additionalProperties'):
                if num < len(path) - 1:
                    tree = tree['additionalProperties']
                    additional = True
                else:
                    break
            # check if any 'anyOf' options in kwargs
            if tree.get('anyOf') and kwargs.get(p) in tree['anyOf']:
                tree = deepcopy(kwargs[p])
                key = next(x for x in tree)
            # if we don't have any helping options, iterate over braches
            elif tree.get('anyOf'):
                for item in tree['anyOf']:
                    try:
                        kwargs.update({p: deepcopy(item)})
                        fork_lookup = self.schema_lookup(path=path, **kwargs)
                        result.extend(fork_lookup)
                    except SchemaLookupError as e:
                        pass
                break
            if tree.get('allOf'):
                subtree_path = tree['allOf'][0]['$ref'].split('/')[1:]
            elif tree.get('$ref'):
                subtree_path = tree['$ref'].split('/')[1:]
            elif tree.get('type') == 'array' and tree['items'].get('$ref') and num != len(path) - 1:
                subtree_path = tree['items']['$ref'].split('/')[1:]
            elif tree.get('type') != 'object':
                # it means we won't go any deeper with this branch
                # if it's the last path item we append it to final result,
                # otherwise abandon it
                if p in kwargs:
                    kwargs[p].pop(key)
                break
            else:
                raise SchemaLookupError('/'.join([str(x) for x in path[:num+1]]))
            tree = reduce(operator.getitem, subtree_path, self.schema)
        if not tree.get('anyOf') and (not path or num == len(path) - 1):
            result.append(tree)
        return result

    def find_path(self, target: str, *args) -> list:
        '''
        somehow opposite to schema_lookup: finds a path to a given field is schema
        :param target: field name or path/to/field (could be partial enough to eliminate ambiguity)
        :param args: field names out of path order that must be included (another option for disambiguation)
        :return: full path to field or None if cannot decide
        '''
        
        """
        presume that given path is a consistent part of actual path
        counting from the end, e.g. if actual path is:
        feeds/providerOverrides/04a47f56b3d29913fdaea70beb9da503/reutersProperties/quoteRic/base
        we consider following string as valid input:
        reutersProperties/quoteRic/base

        let's try to find the first fieldname in the root of schema
        if not, we'll try to find the definition where the fieldname is mentioned
        then try to find a field, where this definition is mentioned
        then cycle one more time
        """

        path = target.split('/')
        # check if the first field of path is in the root of schema
        if next((x for x in self.schema['properties'] if x == path[0]), None):
            # check if it is valid path
            if len(self.schema_lookup(path)) == 1:
                return path

        # ignore providerId in providerOverrides and list indices
        lookup_item = next(
            p for p in path
            if not p.isdecimal()
            and p not in [x[1] for x in ValidationLists.feed_providers]
            and p not in [x[1] for x in ValidationLists.broker_providers]
        )
        definitions = [
            key for key, val in self.schema['definitions'].items()
            if lookup_item in val['properties']
        ]
        # exactly one definition is wanted
        # otherwise the corresponding field should be provided in target
        if len(definitions) > 1:
            if 'FeedOverrides' in definitions and 'feeds' in args:
                definition = 'FeedOverrides'
            elif 'BrokerOverrides' in definitions and 'brokers' in args:
                definition = 'BrokerOverrides'
            else:
                definition = next((
                    x for x in definitions
                    if x.lower() in [y.lower() for y in args]
                ), None)
                if not definition:
                    self.logger.warning('Ambiguous input, provide more detailed path')
                    return None
        elif not definitions:
            self.logger.warning(f"path {path} is not found in {self.schema['title']}")
            return None
        else:
            definition = definitions[0]
        while True:
            # again, only one mention is expected, otherwise disambiguation required
            # transformation references to mentions, suppose definition is 'Ric',
            # then references:
            # {
            #   'ReutersProperties': {
            #       'ric': ['Ric'],
            #       'quoteRic': ['Ric'],
            #       'tradeRic': ['Ric']
            #   }
            # }
            # transform into mentions:
            # {
            #   'ReutersProperties': ['ric', 'quoteRic', 'tradeRic']
            # }
            mentions = {}
            for ref_name, ref_content in self.references.items():
                filter_section = [
                    fieldname for fieldname
                    in ref_content
                    if definition in ref_content[fieldname]
                ]
                if filter_section:
                    mentions.update({
                        ref_name: filter_section
                    })
            if not mentions:
                # this is unlikely but anyway
                self.logger.warning(f"path {path} is not found in {self.schema['title']}")
                return None
            if mentions.get('FeedOverrides') and mentions.get('BrokerOverrides'):
                if 'feeds' in args:
                    definition = 'FeedOverrides'
                elif 'brokers' in args:
                    definition = 'BrokerOverrides'
                else:
                    definition = None
            else:
                definition = next((
                    x for x in mentions
                    if x.lower() in [y.lower() for y in args]
                    or len(mentions) == 1), None)
            possible_fields = list()
            if not definition:
                # what's going on here:
                # mentions.values() is a list of lists (iterable, to be precise),
                # x for y in [list(), list(), ...] for x in y
                # alllows us to iterate over inner lists values
                possible_fields = [
                    x for y in mentions.values() for x in y if x in args
                ]
                if not possible_fields or len(possible_fields) > 1:
                    self.logger.warning(f'{possible_fields}')
                    self.logger.warning(f'Ambiguous input, provide more detailed path')
                    if len(mentions) > 1:
                        self.logger.warning(f"{lookup_item} mentioned in {pformat(mentions)}")
                    return None
                definition = next(x for x in mentions if possible_fields[0] in mentions[x])
            new_entry = possible_fields[0] if possible_fields else mentions[definition][0]
            if new_entry in ['providerOverrides', 'gateways', 'accounts']:
                path.insert(0, 'dummy')
            path.insert(0, new_entry)
            if definition == 'root':
                break
        if self.schema_lookup(path):
            return path

class ValidationLists:
    countries = {
        'Afghanistan': 'AF',
        'Albania': 'AL',
        'Algeria': 'DZ',
        'American Samoa': 'AS',
        'Andorra': 'AD',
        'Angola': 'AO',
        'Anguilla': 'AI',
        'Antarctica': 'AQ',
        'Antigua and Barbuda': 'AG',
        'Argentina': 'AR',
        'Armenia': 'AM',
        'Aruba': 'AW',
        'Australia': 'AU',
        'Austria': 'AT',
        'Azerbaijan': 'AZ',
        'Bahamas': 'BS',
        'Bahrain': 'BH',
        'Bangladesh': 'BD',
        'Barbados': 'BB',
        'Belarus': 'BY',
        'Belgium': 'BE',
        'Belize': 'BZ',
        'Benin': 'BJ',
        'Bermuda': 'BM',
        'Bhutan': 'BT',
        'Bolivia': 'BO',
        'Bonaire, Sint Eustatius and Saba': 'BQ',
        'Bosnia and Herzegovina': 'BA',
        'Botswana': 'BW',
        'Bouvet Island': 'BV',
        'Brazil': 'BR',
        'British Indian Ocean Territory': 'IO',
        'Brunei Darussalam': 'BN',
        'Bulgaria': 'BG',
        'Burkina Faso': 'BF',
        'Burundi': 'BI',
        'Cabo Verde': 'CV',
        'Cambodia': 'KH',
        'Cameroon': 'CM',
        'Canada': 'CA',
        'Cayman Islands': 'KY',
        'Central African Republic': 'CF',
        'Chad': 'TD',
        'Chile': 'CL',
        'China': 'CN',
        'Christmas Island': 'CX',
        'Cocos Islands': 'CC',
        'Colombia': 'CO',
        'Comoros': 'KM',
        'Congo (the Democratic Republic)': 'CD',
        'Congo': 'CG',
        'Cook Islands': 'CK',
        'Costa Rica': 'CR',
        'Croatia': 'HR',
        'Cuba': 'CU',
        'Cura\u00e7ao': 'CW',
        'Cyprus': 'CY',
        'Czechia': 'CZ',
        'C\u00f4te d\'Ivoire': 'CI',
        'Denmark': 'DK',
        'Djibouti': 'DJ',
        'Dominica': 'DM',
        'Dominican Republic': 'DO',
        'Ecuador': 'EC',
        'Egypt': 'EG',
        'El Salvador': 'SV',
        'Equatorial Guinea': 'GQ',
        'Eritrea': 'ER',
        'Estonia': 'EE',
        'Eswatini': 'SZ',
        'Ethiopia': 'ET',
        'Falkland Islands': 'FK',
        'Faroe Islands': 'FO',
        'Fiji': 'FJ',
        'Finland': 'FI',
        'France': 'FR',
        'French Guiana': 'GF',
        'French Polynesia': 'PF',
        'French Southern Territories': 'TF',
        'Gabon': 'GA',
        'Gambia': 'GM',
        'Georgia': 'GE',
        'Germany': 'DE',
        'Ghana': 'GH',
        'Gibraltar': 'GI',
        'Greece': 'GR',
        'Greenland': 'GL',
        'Grenada': 'GD',
        'Guadeloupe': 'GP',
        'Guam': 'GU',
        'Guatemala': 'GT',
        'Guernsey': 'GG',
        'Guinea': 'GN',
        'Guinea-Bissau': 'GW',
        'Guyana': 'GY',
        'Haiti': 'HT',
        'Heard Island and McDonald Islands': 'HM',
        'Holy See': 'VA',
        'Honduras': 'HN',
        'Hong Kong': 'HK',
        'Hungary': 'HU',
        'Iceland': 'IS',
        'India': 'IN',
        'Indonesia': 'ID',
        'Iran': 'IR',
        'Iraq': 'IQ',
        'Ireland': 'IE',
        'Isle of Man': 'IM',
        'Israel': 'IL',
        'Italy': 'IT',
        'Jamaica': 'JM',
        'Japan': 'JP',
        'Jersey': 'JE',
        'Jordan': 'JO',
        'Kazakhstan': 'KZ',
        'Kenya': 'KE',
        'Kiribati': 'KI',
        'Korea (North)': 'KP',
        'Korea (South)': 'KR',
        'Kuwait': 'KW',
        'Kyrgyzstan': 'KG',
        'Lao People\'s Democratic Republic': 'LA',
        'Latvia': 'LV',
        'Lebanon': 'LB',
        'Lesotho': 'LS',
        'Liberia': 'LR',
        'Libya': 'LY',
        'Liechtenstein': 'LI',
        'Lithuania': 'LT',
        'Luxembourg': 'LU',
        'Macao': 'MO',
        'Madagascar': 'MG',
        'Malawi': 'MW',
        'Malaysia': 'MY',
        'Maldives': 'MV',
        'Mali': 'ML',
        'Malta': 'MT',
        'Marshall Islands': 'MH',
        'Martinique': 'MQ',
        'Mauritania': 'MR',
        'Mauritius': 'MU',
        'Mayotte': 'YT',
        'Mexico': 'MX',
        'Micronesia': 'FM',
        'Moldova': 'MD',
        'Monaco': 'MC',
        'Mongolia': 'MN',
        'Montenegro': 'ME',
        'Montserrat': 'MS',
        'Morocco': 'MA',
        'Mozambique': 'MZ',
        'Myanmar': 'MM',
        'Namibia': 'NA',
        'Nauru': 'NR',
        'Nepal': 'NP',
        'Netherlands': 'NL',
        'New Caledonia': 'NC',
        'New Zealand': 'NZ',
        'Nicaragua': 'NI',
        'Niger': 'NE',
        'Nigeria': 'NG',
        'Niue': 'NU',
        'Norfolk Island': 'NF',
        'North Macedonia': 'MK',
        'Northern Mariana Islands': 'MP',
        'Norway': 'NO',
        'Oman': 'OM',
        'Pakistan': 'PK',
        'Palau': 'PW',
        'Palestine': 'PS',
        'Panama': 'PA',
        'Papua New Guinea': 'PG',
        'Paraguay': 'PY',
        'Peru': 'PE',
        'Philippines': 'PH',
        'Pitcairn': 'PN',
        'Poland': 'PL',
        'Portugal': 'PT',
        'Puerto Rico': 'PR',
        'Qatar': 'QA',
        'Romania': 'RO',
        'Russian Federation': 'RU',
        'Rwanda': 'RW',
        'R\u00e9union': 'RE',
        'Saint Barth\u00e9lemy': 'BL',
        'Saint Helena, Ascension and Tristan da Cunha': 'SH',
        'Saint Kitts and Nevis': 'KN',
        'Saint Lucia': 'LC',
        'Saint Martin (French part)': 'MF',
        'Saint Pierre and Miquelon': 'PM',
        'Saint Vincent and the Grenadines': 'VC',
        'Samoa': 'WS',
        'San Marino': 'SM',
        'Sao Tome and Principe': 'ST',
        'Saudi Arabia': 'SA',
        'Senegal': 'SN',
        'Serbia': 'RS',
        'Seychelles': 'SC',
        'Sierra Leone': 'SL',
        'Singapore': 'SG',
        'Sint Maarten (Dutch part)': 'SX',
        'Slovakia': 'SK',
        'Slovenia': 'SI',
        'Solomon Islands': 'SB',
        'Somalia': 'SO',
        'South Africa': 'ZA',
        'South Georgia and the South Sandwich Islands': 'GS',
        'South Sudan': 'SS',
        'Spain': 'ES',
        'Sri Lanka': 'LK',
        'Sudan': 'SD',
        'Suriname': 'SR',
        'Svalbard and Jan Mayen': 'SJ',
        'Sweden': 'SE',
        'Switzerland': 'CH',
        'Syrian Arab Republic': 'SY',
        'Taiwan': 'TW',
        'Tajikistan': 'TJ',
        'Tanzania': 'TZ',
        'Thailand': 'TH',
        'Timor-Leste': 'TL',
        'Togo': 'TG',
        'Tokelau': 'TK',
        'Tonga': 'TO',
        'Trinidad and Tobago': 'TT',
        'Tunisia': 'TN',
        'Turkey': 'TR',
        'Turkmenistan': 'TM',
        'Turks and Caicos Islands': 'TC',
        'Tuvalu': 'TV',
        'Uganda': 'UG',
        'Ukraine': 'UA',
        'United Arab Emirates': 'AE',
        'United Kingdom': 'GB',
        'United States Minor Outlying Islands': 'UM',
        'United States of America': 'US',
        'Uruguay': 'UY',
        'Uzbekistan': 'UZ',
        'Vanuatu': 'VU',
        'Venezuela': 'VE',
        'Viet Nam': 'VN',
        'Virgin Islands (British)': 'VG',
        'Virgin Islands (U.S.)': 'VI',
        'Wallis and Futuna': 'WF',
        'Western Sahara': 'EH',
        'Yemen': 'YE',
        'Zambia': 'ZM',
        'Zimbabwe': 'ZW',
        '\u00c5land Islands': 'AX'
    }
    sdbadds = SDBAdditional()
    accounts = sdbadds.get_list_from_sdb(SdbLists.ACCOUNTS.value, id_only=False)
    gateways = sdbadds.get_list_from_sdb(SdbLists.GATEWAYS.value, id_only=False)
    broker_providers = sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
    feed_providers = sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
    currencies = sdbadds.get_list_from_sdb(SdbLists.CURRENCIES.value)
    exchanges = sdbadds.get_list_from_sdb(SdbLists.EXCHANGES.value, id_only=False)
    exec_schemes = sdbadds.get_list_from_sdb(SdbLists.EXECSCHEMES.value)
    market_data_groups = [x['marketDataGroup'] for x in asyncio.run(sdbadds.load_feed_permissions())]
    schedules = sdbadds.get_list_from_sdb(SdbLists.SCHEDULES.value)
    asset_classes = [
        'EQ',
        'CO',
        'IN',
        'IR',
        'CU'
    ]
    commodity_bases = [
        'EN',
        'AG',
        'IN',
        'EQ',
        'IR',
        'ME',
        'OT',
        'FX'
    ]
    coupon_types = [
        'VARIABLE',
        'ZERO COUPON',
        'FIXED',
        'FLOATING',
        'EXCHANGED',
        'ZERO',
        'FLAT TRADING',
        'STEP CPN',
        'DEFAULTED',
        'TO BE PRICED',
        'RANGE',
        'FUNGED',
        'STRIP',
        'PAY-IN-KIND',
        'FIXED TO FLOATER'
    ]
    coupon_period_types = [
        'Regular',
        'Long',
        'Short',
        'Normal',
        'Long First',
        'Short First',
        'Odd For Life'
    ]
    day_count_types = [
        '30/360',
        'ACT/ACT',
        'ACT/360',
        'ACT/365',
        'BUS DAYS/252',
        'ISMA-30/360',
        'ISMA-30/360 NONEOM',
        'Unknown',
        'GERMAN:30/360',
        'ACT/360 NON-EOM',
        'ACT/364',
        'ACT/365 NON-EOM',
        'ACT/ACT NON-EOM',
        'ISDA ACT/ACT',
        '30/360 NON-EOM',
        '30/365 German',
        'NL/365',
        'Actual/365 (Canadian Bond)'
    ]
    durations = [
        'DAY',
        'GOOD_TILL_CANCEL',
        'GOOD_TILL_TIME',
        'IMMEDIATE_OR_CANCEL',
        'FILL_OR_KILL',
        'AT_THE_OPENING',
        'AT_THE_CLOSE',
        'DISABLED'
    ]
    exercise_styles = [
        'AMERICAN',
        'EUROPEAN',
        'BERMUDIAN'
    ]
    identifier_types = [
        'PRIVATE',
        'ISIN',
        'CUSIP',
        'RIC',
        'SEDOL'
    ]
    interval_types = [
        'MAIN_SESSION',
        'PREMARKET',
        'AFTERMARKET'
    ]
    issuer_types = [
        'government',
        'corporate',
        'municipal',
        'other'
    ]
    legal_entities = [
        'CFSC',
        'Counterparty',
        'Cyprus',
        'GBXP',
        'Hong Kong',
        'Malta',
        'Mauritius',
        'OTC',
        'Panama',
        'PLBCF',
        'United Kingdom'
    ]
    margining_styles = [
        'FUTURES',
        'EQUITY'
    ]
    markup_units = [
        'MPI',
        'SCALE',
        'ABSOLUTE'
    ]
    maturity_types = [
        'NORMAL',
        'SINKABLE',
        'AT MATURITY',
        'CONV/CALL',
        'PERP/CALL',
        'CALLABLE',
        'CALL/SINK',
        'PERPETUAL',
        'CALL/PUT',
        'PERP/CALL/PUT',
        'PUTABLE',
        'SINK/EXT',
        'CONVERTIBLE',
        'SINK/PUT',
        'CALL/SINK/PUT',
        'CONV/PUT',
        'CONV/PUT/CALL',
        'CONV/PERP',
        'EXTENDIBLE'
    ]
    order_types = [
        'MARKET',
        'LIMIT',
        'STOP',
        'STOP_LIMIT',
        'ICEBERG',
        'TWAP',
        'TRAILING_STOP'
    ]
    replace_modes = [
        'DISABLED',
        'EMULATED',
        'NATIVE'
    ]
    source_types = [
        'bond',
        'bond_rrps',
        'bond_otc',
        'l1',
        'l2',
        'index'
    ]
    spread_types = [
        'FORWARD',
        'REVERSE'
    ]
    sym_types = [
        'BOND',
        'STOCK',
        'FUTURE',
        'FX_SPOT',
        'FOREX',
        'OPTION',
        'CALENDAR_SPREAD',
        'CFD',
        'FUND'
    ]

class Template(BaseModel):
    template: str = Field(
        alias='$template',
        title='$template'
    )

class SdbDate(BaseModel):
    day: Optional[int]
    month: int
    year: int
    time: Optional[str]

    @root_validator(allow_reuse=True)
    def check_date(cls, values: dict):
        day = values.get('day') if values.get('day') else 1
        strday = str(day) if day > 9 else f'0{day}'
        strmonth = str(values.get('month')) if values.get('month') > 9 else f"0{values.get('month')}"
        dt.date.fromisoformat(f"{values.get('year')}-{strmonth}-{strday}")
        if values.get('time'):
            if len(values['time'].split(':')[0]) == 1:
                dt.time.fromisoformat(f"0{values['time']}")
            else:
                dt.time.fromisoformat(values.get('time'))
        return values

class AdvancedSdbDate(BaseModel):
    day: Optional[Union[int, Template]]
    month: Union[int, Template]
    year: Union[int, Template]

    @root_validator(allow_reuse=True)
    def check_date(cls, values: dict):
        day = values.get('day') if values.get('day') else 1
        if isinstance(values.get('year'), int) and isinstance(values.get('month'), int) and isinstance(day, int):
            strday = str(day) if day > 9 else f'0{day}'
            strmonth = str(values.get('month')) if values.get('month') > 9 else f"0{values.get('month')}"
            dt.date.fromisoformat(f"{values.get('year')}-{strmonth}-{strday}")
        return values

class Identifiers(BaseModel):
    CUSIP: Optional[str]
    FIGI: Optional[str]
    ISIN: Optional[str]
    RIC: Optional[str]
    SEDOL: Optional[str]

    @validator('ISIN', allow_reuse=True)
    def check_isin(cls, isin):
        country_codes = list(ValidationLists.countries.values())
        country_codes += ['EU', 'XC', 'XS', 'AN', 'CS']
        if isin[:2] not in country_codes:
            raise ValueError(f"{isin[:2]} is invalid country code")
        elif len(isin) != 12:
            raise ValueError(f"{isin} is invalid (length is not 12 symbols)")
        numbers = []
        for i in isin:
            if i.isdecimal():
                numbers.append(int(i))
            else:
                numbers.append(int(ord(i)-55))
        if luhn.verify(''.join([str(x) for x in numbers])):
            return isin
        else:
            raise ValueError('Checksum is incorrect')

class Aodt(BaseModel):
    market: Optional[List[str]] = Field(
        alias='MARKET',
        title='MARKET',
        opts_list=ValidationLists.durations
    )
    limit: Optional[List[str]] = Field(
        alias='LIMIT',
        title='LIMIT',
        opts_list=ValidationLists.durations
    )
    stop: Optional[List[str]] = Field(
        alias='STOP',
        title='STOP',
        opts_list=ValidationLists.durations
    )
    stop_limit: Optional[List[str]] = Field(
        alias='STOP_LIMIT',
        title='STOP_LIMIT',
        opts_list=ValidationLists.durations
    )
    twap: Optional[List[str]] = Field(
        alias='TWAP',
        title='TWAP',
        opts_list=ValidationLists.durations
    )
    iceberg: Optional[List[str]] = Field(
        alias='ICEBERG',
        title='ICEBERG',

        opts_list=ValidationLists.durations
    )
    trailing_stop: Optional[List[str]] = Field(
        alias='TRAILING_STOP',
        title='TRAILING_STOP',
        opts_list=ValidationLists.durations
    )

    @validator('*', allow_reuse=True)
    def durations_validator(cls, given_durations):
        if 'DISABLED' in given_durations and len(given_durations) > 1:
            raise ValueError(
                    f"Section could not contain 'DISABLED' and other durations simultaneously")
        if not given_durations:
            raise ValueError(
                    f"Section could not be empty, fill with 'DISABLED' or with some durations if any")
        if next((x for x in given_durations if x not in ValidationLists.durations), None):
            raise ValueError(
                f"Section should contain only durations from list: {ValidationLists.durations}, or 'DISABLED'")
        return given_durations

class SyntheticSources(BaseModel):
    quote_lifetime: int = Field(
        alias='quoteLifetime',
        title='quoteLifetime'
    )
    symbol: str = Field(
        alias='symbol',
        title='symbol'
    )

class SyntheticFeed(BaseModel):
    enable_market_depth: Optional[bool] = Field(
        alias='enableMarketDepth',
        title='enableMarketDepth'
    )
    max_source_deviation: int = Field(
        alias='maxSourceDeviation',
        title='maxSourceDeviation'
    )
    min_spread: Optional[float] = Field(
        alias='minSpread',
        title='minSpread'
    )
    sources: List[SyntheticSources] = Field(
        alias='sources',
        title='sources'
    )

class SyntheticSettings(BaseModel):
    enable_market_depth: Optional[bool] = Field(
        alias='enableMarketDepth',
        title='enableMarketDepth'
    )
    max_source_deviation: float = Field(
        alias='maxSourceDeviation',
        title='maxSourceDeviation'
    )
    sources: SyntheticSources = Field(
        alias='sources',
        title='sources'
    )

class AuxCalcData(BaseModel):
    fetch_change: Optional[bool] = Field(
        alias='fetchChange',
        title='fetchChange'
    )
    fetch_daily_volume: Optional[bool] = Field(
        alias='fetchDailyVolume',
        title='fetchDailyVolume'
    )
    fetch_last_session_close: Optional[bool] = Field(
        alias='fetchLastSessionClose',
        title='fetchLastSessionClose'
    )
    fetch_session_open: Optional[bool] = Field(
        alias='fetchSessionOpen',
        title='fetchSessionOpen'
    )
    fetch_session_data: Optional[bool] = Field(
        alias='fetchSessionData',
        title='fetchSessionData'
    )
    fetch_volume_24: Optional[bool] = Field(
        alias='fetchVolume24',
        title='fetchVolume24'
    )

class ZeusCalcData(BaseModel):
    fetch_greeks: Optional[bool] = Field(
        alias='fetchGreeks',
        title='fetchGreeks'
    )
    fetch_implied_forward_price: Optional[bool] = Field(
        alias='fetchImpliedForwardPrice',
        title='fetchImpliedForwardPrice'
    )
    fetch_implied_volatility: Optional[bool] = Field(
        alias='fetchImpliedVolatility',
        title='fetchImpliedVolatility'
    )
    fetch_risk_free_rate: Optional[bool] = Field(
        alias='fetchRiskFreeRate',
        title='fetchRiskFreeRate'
    )
    fetch_theo_price: Optional[bool] = Field(
        alias='fetchTheoPrice',
        title='fetchTheoPrice'
    )
    fetch_volatility_index: Optional[bool] = Field(
        alias='fetchVolatilityIndex',
        title='fetchVolatilityIndex'
    )

class YtmLimits(BaseModel):
    min_ytm: Optional[float] = Field(
        alias='min',
        title='min'
    )
    max_ytm: Optional[float] = Field(
        alias='max',
        title='max'
    )

class BondDataMultipliers(BaseModel):
    aci_multiplier: Optional[float] = Field(
        alias='aciMultiplier',
        title='aciMultiplier'
    )
    ytm_multiplier: Optional[float] = Field(
        alias='ytmMultiplier',
        title='ytmMultiplier'
    )

class BondCalcData(BaseModel):
    aci_delta: Optional[float] = Field(
        alias='ACIDelta',
        title='ACIDelta'
    )
    bond_data_multipliers: Optional[BondDataMultipliers] = Field(
        alias='bondDataMultipliers',
        title='bondDataMultipliers'
    )
    fetch_aci: Optional[bool] = Field(
        alias='fetchAci',
        title='fetchAci'
    )
    fetch_dirty_price: Optional[bool] = Field(
        alias='fetchDirtyPrice',
        title='fetchDirtyPrice'
    )
    fetch_next_coupon_date: Optional[bool] = Field(
        alias='fetchNextCouponDate',
        title='fetchNextCouponDate'
    )
    fetch_prev_coupon_date: Optional[bool] = Field(
        alias='fetchPrevCouponDate',
        title='fetchPrevCouponDate'
    )
    fetch_ytm: Optional[bool] = Field(
        alias='fetchYtm',
        title='fetchYtm'
    )
    ytm_limits: Optional[YtmLimits] = Field(
        alias='ytmLimits',
        title='ytmLimits'
    )

class Ric(BaseModel):
    base: Optional[Union[str, Template]] = Field(
        alias='base',
        title='base'
    )
    far_leg_prefix: Optional[str] = Field(
        alias='farLegPrefix',
        title='farLegPrefix'
    )
    near_leg_prefix: Optional[str] = Field(
        alias='nearLegPrefix',
        title='nearLegPrefix'
    )
    option_separator: Optional[str] = Field(
        alias='optionSeparator',
        title='optionSeparator'
    )
    suffix: Optional[str] = Field(
        alias='suffix',
        title='suffix'
    )
    truncate_strike_price: Optional[bool] = Field(
        alias='truncateStrikePrice',
        title='truncateStrikePrice'
    )

class ReutersProperties(BaseModel):
    alt_trades: Optional[bool] = Field(
        alias='altTrades',
        title='altTrades'
    )
    bond_ric: Optional[str] = Field(
        alias='bondRic',
        title='bondRic'
    )
    flipped_bids: Optional[bool] = Field(
        alias='flippedBids',
        title='flippedBids'
    )
    log: Optional[bool] = Field(
        alias='log',
        title='log'
    )
    quote_ric: Optional[Union[str, Ric]] = Field(
        alias='quoteRic',
        title='quoteRic'
    )
    ric: Optional[Union[str, Ric]] = Field(
        alias='ric',
        title='ric'
    )
    source_type: str = Field(
        alias='sourceType',
        title='sourceType',
        opts_list=ValidationLists.source_types
    )
    trade_min_price: Optional[float] = Field(
        alias='tradeMinPrice',
        title='tradeMinPrice'
    )
    trade_ric: Optional[Union[str, Ric]] = Field(
        alias='tradeRic',
        title='tradeRic'
    )
    use_yields: Optional[bool] = Field(
        alias='useYields',
        title='useYields'
    )

    @validator('source_type', allow_reuse=True)
    def check_source_type(cls, source_type):

        if source_type not in ValidationLists.source_types:
            raise ValueError(f'sourceType should be one of those: {ValidationLists.source_types}')
        return source_type

class IdcProperties(BaseModel):
    log: Optional[bool] = Field(
        alias='log',
        title='log'
    )
    source: Optional[str] = Field(
        alias='source',
        title='source'
    )
    symbol_id: Optional[Union[str, Template]] = Field(
        alias='symbolId',
        title='symbolId'
    )

class DxfeedProperties(BaseModel):
    log: Optional[bool] = Field(
        alias='log',
        title='log'
    )
    mixed_order_book: Optional[bool] = Field(
        alias='mixedOrderBook',
        title='mixedOrderBook'
    )
    suffix: Optional[str] = Field(
        alias='suffix',
        title='suffix'
    )
    use_long_maturity_format: Optional[bool] = Field(
        alias='useLongMaturityFormat',
        title='useLongMaturityFormat'
    )

class DelayProperties(BaseModel):
    skip_default_feed: Optional[bool] = Field(
        alias='skipDefaultFeed',
        title='skipDefaultFeed'
    )

class HttpProperties(BaseModel):
    renew_interval_quote: Optional[int] = Field(
        alias='renewIntervalQuote',
        title='renewIntervalQuote'
    )

class CbondsProperties(BaseModel):
    source: Optional[str] = Field(
        alias='source',
        title='source'
    )

class LambdaHandler(BaseModel):
    description: Optional[str]
    parameters: Dict[str, Union[dict, str]] 
    transform: Optional[str]
    type: Optional[str]

class LambdaSettings(BaseModel):
    handler: LambdaHandler = Field(
        alias='handler',
        title='handler'
    )
    sources: Dict[str, Union[dict, str]]

class GeneratorSettings(BaseModel):
    bondDataChangeInterval: Optional[float] = Field(
        alias='bondDataChangeInterval',
        title='bondDataChangeInterval'
    )
    generateYield: Optional[bool] = Field(
        alias='generateYield',
        title='generateYield'
    )
    optionDataChangeInterval: Optional[float] = Field(
        alias='optionDataChangeInterval',
        title='optionDataChangeInterval'
    )
    priceChangeInterval: Optional[float] = Field(
        alias='priceChangeInterval',
        title='priceChangeInterval'
    )
    sizeChangeInterval: Optional[float] = Field(
        alias='sizeChangeInterval',
        title='sizeChangeInterval'
    )
    tradeInterval: Optional[float] = Field(
        alias='tradeInterval',
        title='tradeInterval'
    )

class SymbolIdentifier(BaseModel):
    identifier_type: str = Field(
        alias='type',
        title='type',
        opts_list=ValidationLists.identifier_types
    )
    identifier: Optional[Union[str, Template]] = Field(
        alias='identifier',
        title='identifier'
    )

    @validator('identifier_type', allow_reuse=True)
    def check_identifier_type(cls, identifier_type):
        if identifier_type not in ValidationLists.identifier_types:
            raise ValueError(f'identifier type should be one of those: {ValidationLists.identifier_types}')
        return identifier_type

class FeedOverrides(BaseModel):
    # provider properties
    cbonds_properties: Optional[CbondsProperties] = Field(
        alias='cbondsProperties',
        title='cbondsProperties'
    )
    delay_properties: Optional[DelayProperties] = Field(
        alias='delayProperties',
        title='delayProperties'
    )
    dxfeed_properties: Optional[DxfeedProperties] = Field(
        alias='dxfeedProperties',
        title='dxfeedProperties'
    )
    generator_settings: Optional[GeneratorSettings] = Field(
        alias='generatorSettings',
        title='generatorSettings'
    )
    http_properties: Optional[HttpProperties] = Field(
        alias='httpProperties',
        title='httpProperties'
    )
    idc_properties: Optional[IdcProperties] = Field(
        alias='idcProperties',
        title='idcProperties'
    )
    lambda_settings: Optional[LambdaSettings] = Field(
        alias='lambdaSettings',
        title='lambdaSettings'
    )
    reuters_properties: Optional[ReutersProperties] = Field(
        alias='reutersProperties',
        title='reutersProperties'
    )
    synthetic_settings: Optional[SyntheticSettings] = Field(
        alias='syntheticSettings',
        title='syntheticSettings'
    )

    # *calc data
    aux_calc_data: Optional[AuxCalcData] = Field(
        alias='auxCalcData',
        title='auxCalcData'
    )
    bond_calc_data: Optional[BondCalcData] = Field(
        alias='bondCalcData',
        title='bondCalcData'
    )
    zeus_calc_data: Optional[ZeusCalcData] = Field(
        alias='zeusCalcData',
        title='zeusCalcData'
    )

    # multipliers
    price_multiplier: Optional[float] = Field(
        alias='priceMultiplier',
        title='priceMultiplier'
    )
    quote_volume_multiplier: Optional[float] = Field(
        alias='quoteVolumeMultiplier',
        title='quoteVolumeMultiplier'
    )
    strike_price_multiplier: Optional[float] = Field(
        alias='strikePriceMultiplier',
        title='strikePriceMultiplier'
    )
    trade_price_multiplier: Optional[float] = Field(
        alias='tradePriceMultiplier',
        title='tradePriceMultiplier'
    )
    trade_volume_multiplier: Optional[float] = Field(
        alias='tradeVolumeMultiplier',
        title='tradeVolumeMultiplier'
    )
    quote_price_multiplier: Optional[float] = Field(
        alias='quotePriceMultiplier',
        title='quotePriceMultiplier'
    )
    volume_multiplier: Optional[float] = Field(
        alias='volumeMultiplier',
        title='volumeMultiplier'
    )

    # symbol related
    currency: Optional[str] = Field(
        alias='currency',
        title='currency',
        opts_list=[x[1] for x in ValidationLists.currencies]
    )
    exchange_name: Optional[str] = Field(
        alias='exchangeName',
        title='exchangeName'
    )
    leg_gap: Optional[int] = Field(
        alias='legGap',
        title='legGap'
    )
    markup: Optional[float] = Field(
        alias='markup',
        title='markup'
    )
    markup_unit: Optional[str] = Field(
        alias='markupUnit',
        title='markupUnit',
        opts_list=ValidationLists.markup_units
    )
    maturity_date: Optional[AdvancedSdbDate] = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    spread_type: Optional[str] = Field(
        alias='spreadType',
        title='spreadType',
        opts_list=ValidationLists.spread_types
    )
    symbol_name: Optional[Union[str, Template]] = Field(
        alias='symbolName',
        title='symbolName'
    )
    symbol_identifier: Optional[SymbolIdentifier] = Field(
        alias='symbolIdentifier',
        title='symbolIdentifier'
    )

    # quotes related
    delay_feed_depth: Optional[int] = Field(
        alias='delayFeedDepth',
        title='delayFeedDepth'
    )
    force_align_to_mpi: Optional[bool] = Field(
        alias='forceAlignToMpi',
        title='forceAlignToMpi'
    )
    max_quote_depth: Optional[int] = Field(
        alias='maxQuoteDepth',
        title='maxQuoteDepth'
    )
    min_allowed_volume: Optional[int] = Field(
        alias='minAllowedVolume',
        title='minAllowedVolume'
    )
    price_deviation: Optional[int] = Field(
        alias='priceDeviation',
        title='priceDeviation'
    )
    restart_on_absent_quotes_timeout: Optional[int] = Field(
        alias='restartOnAbsentQuotesTimeout',
        title='restartOnAbsentQuotesTimeout'
    )
    use_aci_correction: Optional[bool] = Field(
        alias='useAciCorrection',
        title='useAciCorrection'
    )
    use_bond_discount_quotation: Optional[bool] = Field(
        alias='useBondDiscountQuotation',
        title='useBondDiscountQuotation'
    )
    use_trades_as_quotes: Optional[bool] = Field(
        alias='useTradesAsQuotes',
        title='useTradesAsQuotes'
    )

    @validator('markup_unit', allow_reuse=True)
    def check_markup_unit(cls, markup_unit):
        if markup_unit not in ValidationLists.markup_units:
            raise ValueError(f'sourceType should be one of those: {ValidationLists.markup_units}')
        return markup_unit

    @validator('currency', allow_reuse=True)
    def check_currency(cls, currency):
        if currency not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{currency} is invalid currency')
        return currency

class Gateway(BaseModel):
    allow_fallback: Optional[bool] = Field(
        alias='allowFallback',
        title='allowFallback'
    )
    enabled: bool = Field(
        alias='enabled',
        title='enabled'
    )
    provider_id: str = Field(
        alias='providerId',
        title='providerId',
        opts_list=ValidationLists.feed_providers
    )

    @validator('provider_id', allow_reuse=True)
    def check_provider_id(cls, provider_id):
        if provider_id not in [x[1]['gateway']['providerId'] for x in ValidationLists.gateways]:
            raise ValueError(f'{provider_id} is invalid provider id')
        return provider_id

class Gateways(BaseModel):
    gateway: Gateway = Field(
        alias='gateway',
        title='gateway'
    )
    gateway_id: str = Field(
        alias='gatewayId',
        title='gatewayId',
        opts_list=[(x[0], x[1]['gatewayId']) for x in ValidationLists.gateways]
    )

    @validator('gateway_id', allow_reuse=True)
    def check_gateway_id(cls, gw_id):
        if gw_id not in [x[1]['gatewayId'] for x in ValidationLists.gateways]:
            raise ValueError(f'{gw_id} is invalid gateway id')
        return gw_id

class Feeds(BaseModel):
    feed_overrides: Optional[Dict[str, FeedOverrides]] = Field(
        alias='providerOverrides',
        title='providerOverrides',
        opts_list=ValidationLists.feed_providers
    )
    gateways: Optional[List[Gateways]] = Field(
        alias='gateways',
        title='gateways'
    )
    
    @validator('feed_overrides', allow_reuse=True)
    def check_feed_overrides(cls, overrides: dict):
        for provider_id in overrides.keys():
            if provider_id not in [x[1]['gateway']['providerId'] for x in ValidationLists.gateways]:
                raise ValueError(f'{provider_id} is invalid provider id')
        return overrides

class Constraints(BaseModel):

    allowed_entities: Optional[List[str]] = Field(
        alias='allowedEntities',
        title='allowedEntities',
        opts_list=ValidationLists.legal_entities
    )
    allowed_interval_types: Optional[List[str]] = Field(
        alias='allowedIntervalTypes',
        title='allowedIntervalTypes',
        opts_list=ValidationLists.interval_types
    )
    allowed_tags: Optional[List[str]] = Field(
        alias='allowedTags',
        title='allowedTags'
    )
    aodt: Optional[Aodt] = Field(
        alias='availableOrderDurationTypes',
        title='availableOrderDurationTypes'
    )
    broker_availability_required: Optional[bool] = Field(
        alias='brokerAvailabilityRequired',
        title='brokerAvailabilityRequired'
    )
    forbidden_entities: Optional[List[str]] = Field(
        alias='forbiddenEntities',
        title='forbiddenEntities',
        opts_list=ValidationLists.legal_entities
    )
    forbidden_side: Optional[str] = Field(
        alias='forbiddenSide',
        title='forbiddenSide',
        opts_list=['BUY', 'SELL']
    )
    forbidden_tags: Optional[List[str]] = Field(
        alias='forbiddenTags',
        title='forbiddenTags'
    )
    max_quantity: Optional[float] = Field(
        alias='maxQuantity',
        title='maxQuantity'
    )
    min_quantity: Optional[float] = Field(
        alias='minQuantity',
        title='minQuantity'
    )
    
    @validator('forbidden_entities', 'allowed_entities', allow_reuse=True)
    def check_entities(cls, items):
        for item in items:
            if item not in ValidationLists.legal_entities:
                raise ValueError(f'{item} is invalid legal entity, available options: {ValidationLists.legal_entities}')
        return items

    @validator('allowed_interval_types', allow_reuse=True)
    def check_interval_types(cls, items):
        for item in items:
            if item not in ValidationLists.interval_types:
                raise ValueError(f'{item} is invalid interval type, available options: {ValidationLists.interval_types}')
        return items

    @validator('forbidden_side', allow_reuse=True)
    def check_side(cls, side):
        if side is not None and side not in ['BUY', 'SELL']:
            raise ValueError('forbidden side should be either "BUY", "SELL", or None')
        return side

class MarkupReporting(BaseModel):
    user: Optional[str]
    account: Optional[str]

class BloombergProperties(BaseModel):
    underlying_code: Optional[Union[str, Template]] = Field(
        alias='underlyingCode',
        title='underlyingCode'
    )
    nom_borrow: Optional[bool] = Field(
        alias='nomBorrow',
        title='nomBorrow',
    )

class BrokerOverrides(BaseModel):

    # provider related
    bloomberg_properties: Optional[BloombergProperties] = Field(
        alias='bloombergProperties',
        title='bloombergProperties'
    )

    # symbol related
    currency: Optional[str] = Field(
        alias='currency',
        title='currency',
        opts_list=[x[1] for x in ValidationLists.currencies]
    )
    exchange_name: Optional[str] = Field(
        alias='exchangeName',
        title='exchangeName'
    )
    is_trading: Optional[bool] = Field(
        alias='isTrading',
        title='isTrading'
    )
    leg_gap: Optional[int] = Field(
        alias='legGap',
        title='legGap'
    )
    maturity_date: Optional[AdvancedSdbDate] = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    spread_type: Optional[str] = Field(
        alias='spreadType',
        title='spreadType',
        opts_list=ValidationLists.spread_types
    )
    symbol_identifier: Optional[SymbolIdentifier] = Field(
        alias='symbolIdentifier',
        title='symbolIdentifier'
    )
    symbol_name: Optional[Union[str, Template]] = Field(
        alias='symbolName',
        title='symbolName'
    )
    symbol_id_override: Optional[Union[str, Template]] = Field(
        alias='symbolIdOverride',
        title='symbolIdOverride'
    )

    # order related
    emulate_day: Optional[bool] = Field(
        alias='emulateDay',
        title='emulateDay'
    )
    emulate_gtc: Optional[bool] = Field(
        alias='emulateGtc',
        title='emulateGtc'
    )
    emulate_market: Optional[bool] = Field(
        alias='emulateMarket',
        title='emulateMarket'
    )
    emulate_stop: Optional[bool] = Field(
        alias='emulateStop',
        title='emulateStop'
    )
    execution_scheme_id: Optional[str] = Field(
        alias='executionSchemeId',
        title='executionSchemeId',
        opts_list=ValidationLists.exec_schemes
    )
    negate_price_on_buy: Optional[bool] = Field(
        alias='negatePriceOnBuy',
        title='negatePriceOnBuy'
    )
    min_lot_size: Optional[float] = Field(
        alias='minLotSize',
        title='minLotSize'
    )
    replace_mode: Optional[str] = Field(
        alias='replaceMode',
        title='replaceMode',
        opts_list=ValidationLists.replace_modes
    )

    # markup
    buy_markup: Optional[float] = Field(
        alias='buyMarkup',
        title='buyMarkup'
    )
    markup: Optional[float] = Field(
        alias='markup',
        title='markup'
    )
    markup_reporting: Optional[MarkupReporting] = Field(
        alias='markupReporting',
        title='markupReporting'
    )
    markup_unit: Optional[str] = Field(
        alias='markupUnit',
        title='markupUnit',
        opts_list=ValidationLists.markup_units
    )
    lambda_settings: Optional[LambdaSettings] = Field(
        alias='lambdaSettings',
        title='lambdaSettings'
    )

    # multipliers
    contract_multiplier: Optional[float] = Field(
        alias='contractMultiplier',
        title='contractMultiplier'
    )
    price_multiplier: Optional[float] = Field(
        alias='priceMultiplier',
        title='priceMultiplier'
    )
    strike_price_multiplier: Optional[float] = Field(
        alias='strikePriceMultiplier',
        title='strikePriceMultiplier'
    )
    volume_multiplier: Optional[float] = Field(
        alias='volumeMultiplier',
        title='volumeMultiplier'
    )

    @validator('currency', allow_reuse=True)
    def check_currency(cls, currency):
        if currency not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{currency} is invalid currency')
        return currency

    @validator('execution_scheme_id', allow_reuse=True)
    def check_execution_scheme(cls, scheme):
        if scheme not in [x[1] for x in ValidationLists.exec_schemes]:
            raise ValueError(f'{scheme} is not valid execution scheme id')
    
    @validator('replace_mode', allow_reuse=True)
    def check_replace_mode(cls, replace_mode):
        if replace_mode not in ValidationLists.replace_modes:
            raise ValueError(f'{replace_mode} is not valid replace mode')

class Account(BaseModel):
    provider_id: str = Field(
        alias='providerId',
        title='providerId',
        opts_list=ValidationLists.broker_providers
    )
    gateway_id: str = Field(
        alias='gatewayId',
        title='gatewayId',
        opts_list=[(x[0].split(': ')[1], x[1]['account']['gatewayId']) for x in ValidationLists.accounts]
    )
    execution_scheme_id: Optional[str] = Field(
        alias='executionSchemeId',
        title='executionSchemeId',
        opts_list=ValidationLists.exec_schemes
    )
    allow_fallback: Optional[bool] = Field(
        alias='allowFallback',
        title='allowFallback'
    )
    enabled: bool = Field(
        alias='enabled',
        title='enabled'
    )
    constraints: Optional[Constraints] = Field(
        alias='constraints',
        title='constraints'
    )
    @validator('provider_id', allow_reuse=True)
    def check_provider_id(cls, provider_id):
        if provider_id not in [x[1]['account']['providerId'] for x in ValidationLists.accounts]:
            raise ValueError(f'{provider_id} is not valid provider id')
        return provider_id

    @validator('gateway_id', allow_reuse=True)
    def check_gateway_id(cls, gateway_id):
        if gateway_id not in [x[1]['account']['gatewayId'] for x in ValidationLists.accounts]:
            raise ValueError(f'{gateway_id} is not valid gateway id')
        return gateway_id

    @validator('execution_scheme_id', allow_reuse=True)
    def check_execution_scheme(cls, scheme):
        if scheme not in [x[1] for x in ValidationLists.exec_schemes]:
            raise ValueError(f'{scheme} is not valid execution scheme id')
        return scheme
    
    @root_validator
    def check_trading_route(cls, values: dict):
        fb = values.get('allow_fallback')
        scheme = values.get('execution_scheme_id')
        if fb is not None and scheme is None:
            raise ValueError('Execution scheme is required')
        return values

class Accounts(BaseModel):
    account_id: str = Field(
        alias='accountId',
        title='accountId',
        opts_list=[(x[0], x[1]['accountId']) for x in ValidationLists.accounts]
    )
    account: Account = Field(
        alias='account',
        title='account'
    )

    @validator('account_id', allow_reuse=True)
    def check_account_id(cls, account_id):
        if account_id not in [x[1]['accountId'] for x in ValidationLists.accounts]:
            raise ValueError(f'{account_id} is not valid account id')
        return account_id

class Brokers(BaseModel):
    broker_overrides: Optional[Dict[str, BrokerOverrides]] = Field(
        alias='providerOverrides',
        title='providerOverrides',
        opts_list=ValidationLists.broker_providers
    )
    accounts: Optional[List[Accounts]] = Field(
        alias='accounts',
        title='accounts'
    )

    @validator('broker_overrides', allow_reuse=True)
    def check_broker_overrides(cls, overrides: dict):
        for provider_id in overrides.keys():
            if provider_id not in [x[1]['account']['providerId'] for x in ValidationLists.accounts]:
                raise ValueError(f'{provider_id} is invalid provider id')
        return overrides

class InstantMarkup(BaseModel):
    max_markup: Optional[int] = Field(
        alias='maxMarkup',
        title='maxMarkup'
    )
    max_volume: Optional[int] = Field(
        alias='maxVolume',
        title='maxVolume'
    )
    min_markup: Optional[int] = Field(
        alias='minMarkup',
        title='minMarkup'
    )

class InstantExecution(BaseModel):
    markup: Optional[List[InstantMarkup]] = Field(
        alias='markup',
        title='markup'
    )
    max_position_volume: Optional[int] = Field(
        alias='maxPositionVolume',
        title='maxPositionVolume'
    )
    order_price_max_deviation: Optional[int] = Field(
        alias='orderPriceMaxDeviation',
        title='orderPriceMaxDeviation'
    )
    quote_lifetime: Optional[int] = Field(
        alias='quoteLifetime',
        title='quoteLifetime'
    )
    report_errors: Optional[bool] = Field(
        alias='reportErrors',
        title='reportErrors'
    )
    volume_currency: Optional[str] = Field(
        alias='volumeCurrency',
        title='volumeCurrency',
        opts_list=[x[1] for x in ValidationLists.currencies]
    )

    @validator('volume_currency', allow_reuse=True)
    def check_currency(cls, currency):
        if currency not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{currency} is invalid currency')
        return currency

class QuoteFilters(BaseModel):
    max_spread: float = Field(
        alias='maxSpread',
        title='maxSpread'
    )

class AssetInformation(BaseModel):
    asset_class: Optional[str] = Field(
        alias='assetClass',
        title='assetClass',
        opts_list=ValidationLists.asset_classes
    )
    cfi: Optional[str] = Field(
        alias='CFI',
        title='CFI'
    )
    @validator('asset_class', allow_reuse=True)
    def check_asset_class(cls, asset_class):
        if asset_class not in ValidationLists.asset_classes:
            raise ValueError(f'{asset_class} is invalid asset class')
        return asset_class

class AssetInformations(BaseModel):
    asset_class: Optional[str] = Field(
        alias='assetClass',
        title='assetClass'
    )
    asset_sub_class: Optional[str] = Field(
        alias='assetSubClass',
        title='assetSubClass'
    )
    cfi: Optional[str] = Field(
        alias='CFI',
        title='CFI'
    )
    sector: Optional[str] = Field(
        alias='sector',
        title='sector'
    )

class Legs(BaseModel):
    exante_id: str = Field(
        alias='exanteId',
        title='exanteId'
    )
    quantity: int = Field(
        alias='quantity',
        title='quantity'
    )

class FatalRejects(BaseModel):
    code: str = Field(
        alias='code',
        title='code'
    )
    group: str = Field(
        alias='group',
        title='group'
    )

class AuxDataCalcSettings(BaseModel):
    change_calculation: Optional[bool] = Field(
        alias='changeCalculation',
        title='changeCalculation'
    )
    is_public: Optional[bool] = Field(
        alias='isPublic',
        title='isPublic'
    )
    volumes_calculation: Optional[bool] = Field(
        alias='volumesCalculation',
        title='volumesCalculation'
    )

class OrderAutomation(BaseModel):
    check_price_for_aux_limits: Optional[bool] = Field(
        alias='checkPriceForAuxLimits',
        title='checkPriceForAuxLimits'
    )
    fallback_timeout: Optional[int] = Field(
        alias='fallbackTimeout',
        title='fallbackTimeout'
    )
    fatal_rejects: Optional[List[FatalRejects]] = Field(
        alias='fatalRejects',
        title='fatalRejects'
    )
    force_automation: Optional[bool] = Field(
        alias='forceAutomation',
        title='forceAutomation'
    )
    market_emulation_distance: Optional[float] = Field(
        alias='marketEmulationDistance',
        title='marketEmulationDistance'
    )
    market_emulation_mode: Optional[str] = Field(
        alias='marketEmulationMode',
        title='marketEmulationMode'
    )
    market_emulation_use_aux_limits: Optional[bool] = Field(
        alias='marketEmulationUseAuxLimits',
        title='marketEmulationUseAuxLimits'
    )
    min_chunk_size: Optional[int] = Field(
        alias='minChunkSize',
        title='minChunkSize'
    )
    pending_place_delay: Optional[int] = Field(
        alias='pendingPlaceDelay',
        title='pendingPlaceDelay'
    )
    recurrent_attempts: Optional[int] = Field(
        alias='recurrentAttempts',
        title='recurrentAttempts'
    )
# continue reordering from here
class CommonSchema(BaseModel):  
    sym_type: str = Field(
        alias='type',
        title='type',
        opts_list=ValidationLists.sym_types
    )
    is_abstract: bool = Field(
        alias='isAbstract',
        title='isAbstract'
    )
    is_trading: bool = Field(
        alias='isTrading',
        title='isTrading'
    )
    asset_informations: Optional[AssetInformations] = Field(
        alias='assetInformations',
        title='assetInformations'
    )
    search_weight: Optional[int] = Field(
        alias='searchWeight',
        title='searchWeight'
    )
    path: List[str] = Field(
        alias='path',
        title='path'
    )
    name: str = Field(
        alias='name',
        title='name'
    )
    ticker: Optional[str] = Field(
        alias='ticker',
        title='ticker'
    )
    market_data_group: Optional[str] = Field(
        alias='marketDataGroup',
        title='marketDataGroup',
        opts_list=ValidationLists.market_data_groups
    )
    shortname: str = Field(
        alias='shortName',
        title='shortName'
    )
    description: Optional[str] = Field(
        alias='description',
        title='description'
    )
    identifiers: Optional[Identifiers] = Field(
        alias='identifiers',
        title='identifiers'
    )
    expiry: Optional[SdbDate] = Field(
        alias='expiry',
        title='expiry'
    )
    ompi: float = Field(
        alias='orderMinPriceIncrement',
        title='orderMinPriceIncrement'
    )
    fmpi: float = Field(
        alias='feedMinPriceIncrement',
        title='feedMinPriceIncrement'
    )
    currency: str = Field(
        alias='currency',
        title='currency',
        opts_list=[x[1] for x in ValidationLists.currencies]
    )
    country: Optional[str] = Field(
        alias='country',
        title='country',
        opts_list=[(x, y) for x, y in ValidationLists.countries.items()]
    )
    feeds: Optional[Feeds] = Field(
        alias='feeds',
        title='feeds'
    )
    brokers: Optional[Brokers] = Field(
        alias='brokers',
        title='brokers'
    )
    schedule_id: str = Field(
        alias='scheduleId',
        title='scheduleId',
        opts_list=ValidationLists.schedules
    )
    comments: Optional[str] = Field(
        alias='comments',
        title='comments'
    )
    exchange_link: Optional[str] = Field(
        alias='exchangeLink',
        title='exchangeLink'
    )
    use_historical_quotes: Optional[bool] = Field(
        alias='useHistoricalQuotes',
        title='useHistoricalQuotes'
    )
    min_order_quantity: Optional[int] = Field(
        alias='minOrderQuantity',
        title='minOrderQuantity'
    )
    min_lot_size: Optional[int] = Field(
        alias='minLotSize',
        title='minLotSize'
    )
    lot_size: Optional[float] = Field(
        alias='lotSize',
        title='lotSize'
    )
    contract_multiplier: float = Field(
        alias='contractMultiplier',
        title='contractMultiplier'
    )
    price_unit: Optional[float] = Field(
        alias='priceUnit',
        title='priceUnit'
    )
    face_value: Optional[float] = Field(
        alias='faceValue',
        title='faceValue'
    )
    initial_margin: Optional[float] = Field(
        alias='initialMargin',
        title='initialMargin'
    )
    maintenance_margin: Optional[float] = Field(
        alias='maintenanceMargin',
        title='maintenanceMargin'
    )
    units: Optional[str] = Field(
        alias='units',
        title='units'
    )
    value_date_delta: Optional[int] = Field(
        alias='valueDateDelta',
        title='valueDateDelta'
    )
    is_liquid: Optional[bool] = Field(
        alias='isLiquid',
        title='isLiquid'
    )
    is_replace_enabled: Optional[bool] = Field(
        alias='isReplaceEnabled',
        title='isReplaceEnabled'
    )
    has_negative_price: Optional[bool] = Field(
        alias='hasNegativePrice',
        title='hasNegativePrice'
    )
    is_robot_tradable: Optional[bool] = Field(
        alias='isRobotTradable',
        title='isRobotTradable'
    )
    quote_filters: QuoteFilters = Field(
        alias='quoteFilters',
        title='quoteFilters'
    )
    available_order_types: Optional[List[str]] = Field(
        alias='availableOrderTypes',
        title='availableOrderTypes',
        opts_list=ValidationLists.order_types
    )
    aodt: Aodt = Field(
        alias='availableOrderDurationTypes',
        title='availableOrderDurationTypes'
    )
    max_close_by_market_volume: Optional[int] = Field(
        alias='maxCloseByMarketVolume',
        title='maxCloseByMarketVolume'
    )
    leverage_rate: Optional[float] = Field(
        alias='leverageRate',
        title='leverageRate'
    )
    leverage_rate_short: Optional[float] = Field(
        alias='leverageRateShort',
        title='leverageRateShort'
    )
    extreme_leverage_rate: Optional[float] = Field(
        alias='extremeLeverageRate',
        title='extremeLeverageRate'
    )
    extreme_leverage_rate_short: Optional[float] = Field(
        alias='extremeLeverageRateShort',
        title='extremeLeverageRateShort'
    )
    aux_data_calc_settings: Optional[AuxDataCalcSettings] = Field(
        alias='auxDataCalcSettings',
        title='auxDataCalcSettings'
    )
    exchange_id: str = Field(
        alias='exchangeId',
        title='exchangeId',
        opts_list=ValidationLists.exchanges
    )
    instant_execution: Optional[InstantExecution] = Field(
        alias='instantExecution',
        title='instantExecution'
    )
    trade_data_available: Optional[bool] = Field(
        alias='tradeDataAvailable',
        title='tradeDataAvailable'
    )
    underlying: Optional[str] = Field(
        alias='underlying',
        title='underlying'
    )
    synthetic_feed: Optional[SyntheticFeed] = Field(
        alias='syntheticFeed',
        title='syntheticFeed'
    )
    symbol_id: Optional[str] = Field(
        alias='symbolId',
        title='symbolId'
    )
    quote_lifetime: Optional[int] = Field(
        alias='quoteLifetime',
        title='quoteLifetime'
    )
    delay_feed_depth: Optional[int] = Field(
        alias='delayFeedDepth',
        title='delayFeedDepth'
    )
    order_automation: Optional[OrderAutomation] = Field(
        alias='orderAutomation',
        title='orderAutomation'
    )

    @validator('market_data_group', allow_reuse=True)
    def check_market_data_group(cls, item):
        if item not in ValidationLists.market_data_groups:
            raise ValueError(f'{item} is invalid market_data_group')
        return item

    @validator('currency', allow_reuse=True)
    def check_currency(cls, item):
        if item not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{item} is invalid currency')
        return item

    @validator('country', allow_reuse=True)
    def check_country(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item

    @validator('schedule_id', allow_reuse=True)
    def check_schedule_id(cls, item):
        if item not in [x[1] for x in ValidationLists.schedules]:
            raise ValueError(f'{item} is invalid schedule id')
        return item

    @validator('available_order_types', allow_reuse=True)
    def check_available_order_types(cls, items):
        for item in items:
            if item not in ValidationLists.order_types:
                raise ValueError(f'{item} is invalid order type')
        return items

    @validator('exchange_id', allow_reuse=True)
    def check_exchange_id(cls, item):
        if item not in [x[1] for x in ValidationLists.exchanges]:
            raise ValueError(f'{item} is invalid exchange id')
        return item

class Rating(BaseModel):
    snp: Optional[str]
    moodys: Optional[str]
    fitch: Optional[str]

class BondCalcDataSettings(BaseModel):
    day_count_type: str = Field(
        default=None,
        alias='dayCountType',
        title='dayCountType',
        opts_list=ValidationLists.day_count_types
    )
    ytm_enabled: bool = Field(
        alias='ytmEnabled',
        title='ytmEnabled'
    )
    enabled: bool = Field(
        alias='enabled',
        title='enabled'
    )
    aci_enabled: bool = Field(
        alias='aciEnabled',
        title='aciEnabled'
    )
    @validator('day_count_type', allow_reuse=True)
    def check_day_count_type(cls, day_count_type):
        if day_count_type not in ValidationLists.day_count_types:
            raise ValueError(f'Day count type should be one of those: {ValidationLists.day_count_types}')
        return day_count_type

class BondSchema(CommonSchema):

    sym_type: str = Field(
        'BOND',
        const=True,
        alias='type',
        title='type'
    )
    issuer_type: str = Field(
        alias='issuerType',
        title='issuerType',
        opts_list=ValidationLists.issuer_types
    )
    coupon_rate: float = Field(
        alias='couponRate',
        title='couponRate'
    )
    payment_frequency: int = Field(
        alias='paymentFrequency',
        title='paymentFrequency'
    )
    maturity_date: SdbDate = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    last_trading: Optional[SdbDate] = Field(
        alias='lastTrading',
        title='lastTrading'
    )
    last_available: Optional[SdbDate] = Field(
        alias='lastAvailable',
        title='lastAvailable'
    )
    rating: Optional[Rating] = Field(
        alias='rating',
        title='rating'
    )
    payment_dates: Optional[Dict[str, List[float]]] = Field(
        alias='paymentDates',
        title='paymentDates'
    )
    is_sinkable: Optional[bool] = Field(
        alias='isSinkable',
        title='isSinkable'
    )
    amt_issued: Optional[int] = Field(
        alias='amtIssued',
        title='amtIssued'
    )
    amt_outstanding: Optional[int] = Field(
        alias='amtOutstanding',
        title='amtOutstanding'
    )
    floater: Optional[str] = Field(
        alias='floater',
        title='floater',
        opts_list=['Y', 'N']
    )
    coupon_dates: Optional[List[str]] = Field(
        alias='couponDates',
        title='couponDates'
    )
    first_coupon_period_type: Optional[str] = Field(
        alias='firstCouponPeriodType',
        title='firstCouponPeriodType',
        opts_list=ValidationLists.coupon_period_types
    )
    last_coupon_period_type: Optional[str] = Field(
        alias='lastCouponPeriodType',
        title='lastCouponPeriodType',
        opts_list=ValidationLists.coupon_period_types
    )
    bond_calc_data_settings: Optional[BondCalcDataSettings] = Field(
        alias='bondCalcDataSettings',
        title='bondCalcDataSettings'
    )
    redemption_value: int = Field(
        alias='redemptionValue',
        title='redemptionValue'
    )
    face_value: float = Field(
        alias='faceValue',
        title='faceValue'
    )
    next_coupon_date: Optional[SdbDate] = Field(
        alias='nextCouponDate',
        title='nextCouponDate'
    )
    first_coupon_date: Optional[SdbDate] = Field(
        alias='firstCouponDate',
        title='firstCouponDate'
    )
    coupon_type: Optional[str] = Field(
        alias='couponType',
        title='couponType',
        opts_list=ValidationLists.coupon_types
    )
    maturity_type: Optional[str] = Field(
        alias='maturityType',
        title='maturityType',
        opts_list=ValidationLists.maturity_types
    )
    process_accrued_interest: Optional[bool] = Field(
        alias='processAccruedInterest',
        title='processAccruedInterest'
    )
    previous_coupon_date: Optional[SdbDate] = Field(
        alias='previousCouponDate',
        title='previousCouponDate'
    )
    min_increment: Optional[int] = Field(
        alias='minIncrement',
        title='minIncrement'
    )
    country: str = Field(
        alias='country',
        title='country',
        opts_list=[(x, y) for x, y in ValidationLists.countries.items()]
    )
    country_risk: str = Field(
        alias='countryRisk',
        title='countryRisk',
        opts_list=[(x, y) for x, y in ValidationLists.countries.items()]
    )
    min_piece: Optional[int] = Field(
        alias='minPiece',
        title='minPiece'
    )
    sinking_fund_factor: Optional[int] = Field(
        alias='sinkingFundFactor',
        title='sinkingFundFactor'
    )
    issue_date: Optional[SdbDate] = Field(
        alias='issueDate',
        title='issueDate'
    )
    coupon_calculation_type: Optional[str] = Field(
        alias='couponCalculationType',
        title='couponCalculationType',
        # opts_list=ValidationLists.coupon
    )
    glits_ex_dividend_date: Optional[SdbDate] = Field(
        alias='glitsExDividendDate',
        title='glitsExDividendDate'
    )
    ex_dividend_schedule: Optional[List[str]] = Field(
        alias='exDividendSchedule',
        title='exDividendSchedule'
    )
    ex_dividend_days: Optional[int] = Field(
        alias='exDividendDays',
        title='exDividendDays'
    )
    tree_path_override: List[str] = Field(
        alias='treePathOverride',
        title='treePathOverride'
    )
    default_date: Optional[SdbDate] = Field(
        alias='defaultDate',
        title='defaultDate'
    )
    act_pre_first_days: Optional[int] = Field(
        alias='actPreFirstDays',
        title='actPreFirstDays'
    )
    issuer: Optional[str] = Field(
        alias='issuer',
        title='issuer'
    )
    sink_schedule_amount_type: Optional[str] = Field(
        alias='sinkScheduleAmountType',
        title='sinkScheduleAmountType',
        opts_list=['Percent', 'Cash']
    )
    sink_schedule: Optional[Dict[str, List[str]]] = Field(
        alias='sinkSchedule',
        title='sinkSchedule'
    )
    ex_dividend_calendar: Optional[str] = Field(
        alias='exDividendCalendar',
        title='exDividendCalendar'
    )
    step_up_date: Optional[SdbDate] = Field(
        alias='stepUpDate',
        title='stepUpDate'
    )
    step_up_coupon: Optional[float] = Field(
        alias='stepUpCoupon',
        title='stepUpCoupon'
    )
    coupon_type_specific: Optional[str] = Field(
        alias='couponTypeSpecific',
        title='couponTypeSpecific'
    )
    source_type: Optional[str] = Field(
        alias='sourceType',
        title='sourceType'
    )
    use_aci_correction: Optional[bool] = Field(
        alias='useACICorrection',
        title='useACICorrection'
    )
    sinkable: Optional[str] = Field(
        alias='sinkable',
        title='sinkable'
    )

    @validator('issuer_type', allow_reuse=True)
    def check_issuer_type(cls, item):
        if item not in ValidationLists.issuer_types:
            raise ValueError(f'{item} is invalid issuer_type')
        return item

    @validator('floater', allow_reuse=True)
    def check_floater(cls, item):
        if item not in ['Y', 'N']:
            raise ValueError(f'{item} is invalid floater')
        return item

    @validator('first_coupon_period_type', 'last_coupon_period_type', allow_reuse=True)
    def check_coupon_period_type(cls, item):
        if item not in ValidationLists.coupon_period_types:
            raise ValueError(f'{item} is invalid coupon period type')
        return item

    @validator('coupon_type', allow_reuse=True)
    def check_coupon_type(cls, item):
        if item not in ValidationLists.coupon_types:
            raise ValueError(f'{item} is invalid coupon_type')
        return item

    @validator('maturity_type', allow_reuse=True)
    def check_maturity_type(cls, item):
        if item not in ValidationLists.maturity_types:
            raise ValueError(f'{item} is invalid maturity_type')
        return item

    @validator('country_risk', allow_reuse=True)
    def check_country_risk(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item

    @validator('sink_schedule_amount_type', allow_reuse=True)
    def check_sink_schedule_amount_type(cls, item):
        if item not in ['Percent', 'Cash']:
            raise ValueError(f'{item} is invalid sink schedule amount type')
        return item

class UnderlyingId(BaseModel):
    instrument_type: str = Field(
        'symbolId',
        alias='type',
        title='type',
        const=True
    )
    instrument_id: str = Field(
        alias='id',
        title='id'
    )

class ExecutionMonitoring(BaseModel):
    size: Optional[float] = Field(
        alias='size',
        title='size'
    )
    price_multiplier: Optional[float] = Field(
        alias='priceMultiplier',
        title='priceMultiplier'
    )
    is_enabled: Optional[bool] = Field(
        alias='isEnabled',
        title='isEnabled'
    )
    aodt: Optional[Aodt] = Field(
        alias='availableOrderDurationTypes',
        title='availableOrderDurationTypes'
    )
    quote_freshness: Optional[int] = Field(
        alias='quoteFreshness',
        title='quoteFreshness'
    )
    interval: Optional[int] = Field(
        alias='interval',
        title='interval'
    )
    delay: Optional[int] = Field(
        alias='delay',
        title='delay'
    )

class StampDuty(BaseModel):
    eligible_for_stamp_duty: bool = Field(
        alias='eligibleForStampDuty',
        title='eligibleForStampDuty'
    )
    eligible_for_levy_fee: bool = Field(
        alias='eligibleForLevyFee',
        title='eligibleForLevyFee'
    )
    country_of_incorporation: str = Field(
        alias='countryOfIncorporation',
        title='countryOfIncorporation',
        opts_list=[(x, y) for x, y in ValidationLists.countries.items()]
    )
    override_sinc_data: bool = Field(
        alias='overrideSincData',
        title='overrideSincData'
    )

    @validator('country_of_incorporation', allow_reuse=True)
    def check_country_of_incorporation(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item

class LocalizedDescription(BaseModel):
    ru: Optional[str]

class StockSchema(CommonSchema):
    
    sym_type: str = Field(
        'STOCK',
        const=True,
        alias='type',
        title='type'
    )
    popular: Optional[bool] = Field(
        alias='popular',
        title='popular'
    )
    intraday_coefficient: Optional[float] = Field(
        alias='intradayCoefficient',
        title='intradayCoefficient'
    )
    exchange_name: Optional[str] = Field(
        alias='exchangeName',
        title='exchangeName'
    )
    country: Optional[str] = Field(
        alias='country',
        title='country',
        opts_list=[(x, y) for x, y in ValidationLists.countries.items()]
    )
    is_imported: Optional[bool] = Field(
        alias='isImported',
        title='isImported'
    )
    show_as_fund: Optional[bool] = Field(
        alias='showAsFund',
        title='showAsFund'
    )
    ticker_icon: Optional[str] = Field(
        alias='tickerIcon',
        title='tickerIcon'
    )
    commission_rule: Optional[str] = Field(
        alias='commissionRule',
        title='commissionRule'
    )
    apply_execution_scheme: Optional[bool] = Field(
        alias='applyExecutionScheme',
        title='applyExecutionScheme'
    )
    mic: Optional[str] = Field(
        alias='MIC',
        title='MIC'
    )
    quote_monitor_schedule_id: Optional[str] = Field(
        alias='quoteMonitorScheduleId',
        title='quoteMonitorScheduleId'
    )
    execution_monitoring: Optional[ExecutionMonitoring] = Field(
        alias='executionMonitoring',
        title='executionMonitoring'
    )
    max_market_order_value: Optional[int] = Field(
        alias='maxMarketOrderValue',
        title='maxMarketOrderValue'
    )
    stamp_duty: Optional[StampDuty] = Field(
        alias='stampDuty',
        title='stampDuty'
    )
    voice_trading_only: Optional[bool] = Field(
        alias='voiceTradingOnly',
        title='voiceTradingOnly'
    )
    rating: Optional[Rating] = Field(
        alias='rating',
        title='rating'
    )
    synthetic_feed: Optional[SyntheticFeed] = Field(
        alias='syntheticFeed',
        title='syntheticFeed'
    )
    order_price_max_deviation: Optional[float] = Field(
        alias='orderPriceMaxDeviation',
        title='orderPriceMaxDeviation'
    )
    max_price_deviation: Optional[float] = Field(
        alias='maxPriceDeviation',
        title='maxPriceDeviation'
    )
    localized_description: Optional[LocalizedDescription] = Field(
        alias='localizedDescription',
        title='localizedDescription'
    )
    stop_trigger_policy: Optional[str] = Field(
        alias='stopTriggerPolicy',
        title='stopTriggerPolicy'
    )
    price_unit: Optional[float] = Field(
        alias='priceUnit',
        title='priceUnit'
    )
    real_exchange_name: Optional[str] = Field(
        alias='realExchangeName',
        title='realExchangeName'
    )

    @validator('country', allow_reuse=True)
    def check_country(cls, item):
        if item not in ValidationLists.countries.values():
            raise ValueError(f'{item} is invalid country')
        return item

class FutureSchema(CommonSchema):

    sym_type: str = Field(
        'FUTURE',
        const=True,
        alias='type',
        title='type'
    )
    maturity_date: SdbDate = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    last_trading: Optional[SdbDate] = Field(
        alias='lastTrading',
        title='lastTrading'
    )
    last_available: Optional[SdbDate] = Field(
        alias='lastAvailable',
        title='lastAvailable'
    )
    underlying_id: Optional[UnderlyingId] = Field(
        alias='underlyingId',
        title='underlyingId'
    )
    legs: Optional[List[Legs]] = Field(
        alias='legs',
        title='legs'
    )
    is_physical_delivery: bool = Field(
        alias='isPhysicalDelivery',
        title='isPhysicalDelivery'
    )
    maturity_name: Optional[str] = Field(
        alias='maturityName',
        title='maturityName'
    )
    is_settle_pnl_on_expiry_date: Optional[bool] = Field(
        alias='isSettlePNLOnExpiryDate',
        title='isSettlePNLOnExpiryDate'
    )
    portfolio_margin: Optional[bool] = Field(
        alias='portfolioMargin',
        title='portfolioMargin'
    )
    first_notice_day: Optional[SdbDate] = Field(
        alias='firstNoticeDay',
        title='firstNoticeDay'
    )
    commodity_details: Optional[str] = Field(
        alias='commodityDetails',
        title='commodityDetails'
    )
    commodity_base: Optional[str] = Field(
        alias='commodityBase',
        title='commodityBase',
        opts_list=ValidationLists.commodity_bases
    )
    near_maturity_date: Optional[SdbDate] = Field(
        alias='nearMaturityDate',
        title='nearMaturityDate'
    )
    far_maturity_date: Optional[SdbDate] = Field(
        alias='farMaturityDate',
        title='farMaturityDate'
    )
    show_as_fund: Optional[bool] = Field(
        alias='showAsFund',
        title='showAsFund'
    )

    @validator('commodity_base', allow_reuse=True)
    def check_commodity_base(cls, item):
        if item not in ValidationLists.commodity_bases:
            raise ValueError(f'{item} is invalid commodity base')
        return item

class StrikePrice(BaseModel):
    strike_price: float = Field(
        alias='strikePrice',
        title='strikePrice'
    )
    is_available: Optional[bool] = Field(
        alias='isAvailable',
        title='isAvailable'
    )
    identifiers: Optional[Identifiers] = Field(
        alias='identifiers',
        title='identifiers'
    )

class StrikePrices(BaseModel):
    call: List[StrikePrice] = Field(
        alias='CALL',
        title='CALL'
    )
    put: List[StrikePrice] = Field(
        alias='PUT',
        title='PUT'
    )

class OptionSchema(CommonSchema):
 
    sym_type: str = Field(
        'OPTION',
        const=True,
        alias='type',
        title='type'
    )
    margining_style: str = Field(
        alias='marginingStyle',
        title='marginingStyle',
        opts_list=ValidationLists.margining_styles
    )
    strike_to_underlying_scale: Optional[float] = Field(
        alias='strikeToUnderlyingScale',
        title='strikeToUnderlyingScale'
    )
    is_physical_delivery: bool = Field(
        alias='isPhysicalDelivery',
        title='isPhysicalDelivery'
    )
    exercise_style: str = Field(
        alias='exerciseStyle',
        title='exerciseStyle',
        opts_list=ValidationLists.exercise_styles
    )
    underlying_id: UnderlyingId = Field(
        alias='underlyingId',
        title='underlyingId'
    )
    strike_prices: StrikePrices = Field(
        alias='strikePrices',
        title='strikePrices'
    )
    maturity_date: SdbDate = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    last_trading: Optional[SdbDate] = Field(
        alias='lastTrading',
        title='lastTrading'
    )
    last_available: Optional[SdbDate] = Field(
        alias='lastAvailable',
        title='lastAvailable'
    )
    price_unit: Optional[float] = Field(
        alias='priceUnit',
        title='priceUnit'
    )

    @validator('margining_style', allow_reuse=True)
    def check_margining_style(cls, item):
        if item not in ValidationLists.margining_styles:
            raise ValueError(f'{item} is invalid margining style')
        return item

    @validator('exercise_style', allow_reuse=True)
    def check_exercise_style(cls, item):
        if item not in ValidationLists.exercise_styles:
            raise ValueError(f'{item} is invalid exercise style')
        return item

    @root_validator
    def check_underlying(cls, items):
        sdb = SymbolDB()
        sdbadds = SDBAdditional()
        if not items.get('underlying_id'):
            raise ValueError('UnderlyingId is not set')
        udl_name = items['underlying_id'].instrument_id
        udl = asyncio.run(sdb.get(udl_name))
        date_expiry = dt.date(
            items['expiry'].year,
            items['expiry'].month,
            items['expiry'].day
        )
        if items['expiry'].time:
            if len(items['expiry'].time.split(':')[0]) == 1:
                time_expiry = f"0{items['expiry'].time}"
            else:
                time_expiry = items['expiry'].time
        else:
            time_expiry = '00:00:00'
        dt_expiry = dt.datetime.combine(
            date_expiry,
            dt.time.fromisoformat(time_expiry)
        )
        if not udl:
            raise ValueError(f'{udl_name} does not exist in sdb')
        compiled = asyncio.run(sdbadds.build_inheritance(udl, include_self=True))
        if compiled.get('isTrading') is False:
            raise ValueError(f'{udl_name} is not tradable')
        elif compiled.get('expiry'):
            udl_date = sdb.sdb_to_date(compiled['expiry'])
            if compiled['expiry'].get('time'):
                if len(compiled['expiry']['time'].split(':')[0]) == 1:
                    udl_time = f"0{compiled['expiry']['time']}"
                else:
                    udl_time = compiled['expiry']['time']
            else:
                time_expiry = '00:00:00'
            udl_expiry = dt.datetime.combine(
                udl_date,
                dt.time.fromisoformat(udl_time)
            )
            if udl_expiry < dt_expiry:
                raise ValueError(f'{udl_name} expires earlier than instrument')
        return items
    
    @root_validator
    def check_lt_la(cls, items): 
        date_expiry = dt.date(
            items['expiry'].year,
            items['expiry'].month,
            items['expiry'].day
        )
        if items['expiry'].time:
            if len(items['expiry'].time.split(':')[0]) == 1:
                time_expiry = f"0{items['expiry'].time}"
            else:
                time_expiry = items['expiry'].time
        else:
            time_expiry = '00:00:00'
        dt_expiry = dt.datetime.combine(
            date_expiry,
            dt.time.fromisoformat(time_expiry)
        )
        dt_last_trading = None
        dt_last_available = None
        if items.get('last_trading'):
            date_last_trading = dt.date(
                items['last_trading'].year,
                items['last_trading'].month,
                items['last_trading'].day
            )
            if items['last_trading'].time:
                if len(items['last_trading'].time.split(':')[0]) == 1:
                    time_last_trading = f"0{items['last_trading'].time}"
                else:
                    time_last_trading = items['last_trading'].time
            else:
                time_last_trading = '00:00:00'
            dt_last_trading = dt.datetime.combine(
                date_last_trading,
                dt.time.fromisoformat(time_last_trading)
            )
            if dt_expiry < dt_last_trading:
                raise ValueError('Expiry is less than last trading')
        if items.get('last_available'):
            date_last_available = dt.date(
                items['last_available'].year,
                items['last_available'].month,
                items['last_available'].day
            )
            if items['last_available'].time:
                if len(items['last_available'].time.split(':')[0]) == 1:
                    time_last_available = f"0{items['last_available'].time}"
                else:
                    time_last_available = items['last_available'].time
            else:
                time_last_available = '00:00:00'
            dt_last_available = dt.datetime.combine(
                date_last_available,
                dt.time.fromisoformat(time_last_available)
            )
            if dt_last_available < dt_expiry:
                raise ValueError('Last available is less than expiry')
        return items

class CalendarSpreadSchema(CommonSchema):

    sym_type: str = Field(
        'CALENDAR_SPREAD',
        const=True,
        alias='type',
        title='type'
    )
    spread_type: str = Field(
        alias='spreadType',
        title='spreadType',
        opts_list=ValidationLists.spread_types
    )
    near_maturity_date: SdbDate = Field(
        alias='nearMaturityDate',
        title='nearMaturityDate'
    )
    far_maturity_date: SdbDate = Field(
        alias='farMaturityDate',
        title='farMaturityDate'
    )
    legs: List[Legs] = Field(
        alias='legs',
        title='legs'
    )
    send_long_fix_originator: Optional[bool] = Field(
        alias='sendLongFixOriginator',
        title='sendLongFixOriginator'
    )
    country: Optional[str] = Field(
        alias='country',
        title='country'
    )
    apply_execution_scheme: Optional[bool] = Field(
        alias='applyExecutionScheme',
        title='applyExecutionScheme'
    )
    is_physical_delivery: bool = Field(
        alias='isPhysicalDelivery',
        title='isPhysicalDelivery'
    )
    stop_trigger_policy: Optional[str] = Field(
        alias='stopTriggerPolicy',
        title='stopTriggerPolicy'
    )
    leg_gap: Optional[int] = Field(
        alias='legGap',
        title='legGap'
    )
    first_notice_day: Optional[SdbDate] = Field(
        alias='firstNoticeDay',
        title='firstNoticeDay'
    )
    quote_monitor_schedule_id: Optional[str] = Field(
        alias='quoteMonitorScheduleId',
        title='quoteMonitorScheduleId'
    )
    trade_price_multiplier: Optional[int] = Field(
        alias='tradePriceMultiplier',
        title='tradePriceMultiplier'
    )

    @validator('spread_type', allow_reuse=True)
    def check_spread_type(cls, item):
        if item not in ValidationLists.spread_types:
            raise ValueError(f'{item} is invalid spread type')
        return item

class SpreadSchema(CommonSchema):

    sym_type: str = Field(
        'FUTURE',
        const=True,
        alias='type',
        title='type'
    )
    maturity_date: SdbDate = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    legs: List[Legs] = Field(
        alias='legs',
        title='legs'
    )
    send_long_fix_originator: Optional[bool] = Field(
        alias='sendLongFixOriginator',
        title='sendLongFixOriginator'
    )
    country: Optional[str] = Field(
        alias='country',
        title='country'
    )
    apply_execution_scheme: Optional[bool] = Field(
        alias='applyExecutionScheme',
        title='applyExecutionScheme'
    )
    is_physical_delivery: bool = Field(
        alias='isPhysicalDelivery',
        title='isPhysicalDelivery'
    )
    stop_trigger_policy: Optional[str] = Field(
        alias='stopTriggerPolicy',
        title='stopTriggerPolicy'
    )
    first_notice_day: Optional[SdbDate] = Field(
        alias='firstNoticeDay',
        title='firstNoticeDay'
    )
    quote_monitor_schedule_id: Optional[str] = Field(
        alias='quoteMonitorScheduleId',
        title='quoteMonitorScheduleId'
    )
    trade_price_multiplier: Optional[int] = Field(
        alias='tradePriceMultiplier',
        title='tradePriceMultiplier'
    )

class SyntheticSettings(BaseModel):
    sources: List[SyntheticSources] = Field(
        alias='sources',
        title='sources'
    )
    enable_market_depth: bool = Field(
        alias='enableMarketDepth',
        title='enableMarketDepth'
    )

class FxSpotSchema(CommonSchema):

    sym_type: str = Field(
        'FX_SPOT',
        const=True,
        alias='type',
        title='type'
    )
    maturity_name: str = Field(
        alias='maturityName',
        title='maturityName'
    )
    base_currency: str = Field(
        alias='baseCurrency',
        title='baseCurrency',
        opts_list=[x[1] for x in ValidationLists.currencies]
    )
    show_quote_as_fx: Optional[bool] = Field(
        alias='showQuoteAsFX',
        title='showQuoteAsFX'
    )
    inverted_pnl: Optional[bool] = Field(
        alias='invertedPnL',
        title='invertedPnL'
    )
    execution_monitoring: Optional[ExecutionMonitoring] = Field(
        alias='executionMonitoring',
        title='executionMonitoring'
    )
    synthetic_settings: Optional[Dict] = Field(
        alias='syntheticSettings',
        title='syntheticSettings'
    )

    @validator('base_currency', allow_reuse=True)
    def check_currency(cls, currency):
        if currency not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{currency} is invalid currency')
        return currency

class ForexSchema(CommonSchema):

    sym_type: str = Field(
        'FOREX',
        const=True,
        alias='type',
        title='type'
    )
    base_currency: str = Field(
        alias='baseCurrency',
        title='baseCurrency',
        opts_list=[x[1] for x in ValidationLists.currencies]
    )
    show_quote_as_fx: Optional[bool] = Field(
        alias='showQuoteAsFX',
        title='showQuoteAsFX'
    )
    apply_execution_scheme: Optional[bool] = Field(
        alias='applyExecutionScheme',
        title='applyExecutionScheme'
    )
    tree_path_override: Optional[List[str]] = Field(
        alias='treePathOverride',
        title='treePathOverride'
    )
    is_physical_delivery: Optional[bool] = Field(
        alias='isPhysicalDelivery',
        title='isPhysicalDelivery'
    )
    use_in_crossrates: Optional[bool] = Field(
        alias='useInCrossrates',
        title='useInCrossrates'
    )
    show_as_fund: Optional[bool] = Field(
        alias='showAsFund',
        title='showAsFund'
    )
    popular: Optional[bool] = Field(
        alias='popular',
        title='popular'
    )

    @validator('base_currency', allow_reuse=True)
    def check_currency(cls, currency):
        if currency not in [x[1] for x in ValidationLists.currencies]:
            raise ValueError(f'{currency} is invalid currency')
        return currency

class CfdAdditionalParameters(BaseModel):
    is_ipo: bool = Field(
        alias='isIPO',
        title='isIPO'
    )

class CfdSchema(CommonSchema):

    sym_type: str = Field(
        'CFD',
        const=True,
        alias='type',
        title='type'
    )
    show_quote_as_fx: Optional[bool] = Field(
        alias='showQuoteAsFX',
        title='showQuoteAsFX'
    )
    commodity_details: Optional[str] = Field(
        alias='commodityDetails',
        title='commodityDetails'
    )
    commodity_base: Optional[str] = Field(
        alias='commodityBase',
        title='commodityBase',
        opts_list=ValidationLists.commodity_bases
    )
    show_as_fund: Optional[bool] = Field(
        alias='showAsFund',
        title='showAsFund'
    )
    additional_parameters: Optional[CfdAdditionalParameters] = Field(
        alias='additionalParameters',
        title='additionalParameters'
    )
    apply_execution_scheme: Optional[bool] = Field(
        alias='applyExecutionScheme',
        title='applyExecutionScheme'
    )
    tree_path_override: Optional[List[str]] = Field(
        alias='treePathOverride',
        title='treePathOverride'
    )
    commission_rule: Optional[str] = Field(
        alias='commissionRule',
        title='commissionRule'
    )
    mic: Optional[str] = Field(
        alias='MIC',
        title='MIC'
    )
    exchange_name: Optional[str] = Field(
        alias='exchangeName',
        title='exchangeName'
    )
    quote_monitor_schedule_id: Optional[str] = Field(
        alias='quoteMonitorScheduleId',
        title='quoteMonitorScheduleId'
    )
    ticker_icon: Optional[str] = Field(
        alias='tickerIcon',
        title='tickerIcon'
    )
    rating: Optional[Rating] = Field(
        alias='rating',
        title='rating'
    )
    maturity_date: Optional[SdbDate] = Field(
        alias='maturityDate',
        title='maturityDate'
    )
    stamp_duty: Optional[StampDuty] = Field(
        alias='stampDuty',
        title='stampDuty'
    )
    popular: Optional[bool] = Field(
        alias='popular',
        title='popular'
    )
    real_exchange_name: Optional[str] = Field(
        alias='realExchangeName',
        title='realExchangeName'
    )
    intraday_coefficient: Optional[float] = Field(
        alias='intradayCoefficient',
        title='intradayCoefficient'
    )

    @validator('commodity_base', allow_reuse=True)
    def check_commodity_base(cls, item):
        if item not in ValidationLists.commodity_bases:
            raise ValueError(f'{item} is invalid commodity base')
        return item

class FundSchema(CommonSchema):

    sym_type: str = Field(
        'FUND',
        const=True,
        alias='type',
        title='type'
    )
    structural_asset_id: str = Field(
        alias='structuralAssetId',
        title='structuralAssetId'
    )
    show_as_fund: bool = Field(
        alias='showAsFund',
        title='showAsFund'
    )
    tree_path_override: Optional[List[str]] = Field(
        alias='treePathOverride',
        title='treePathOverride'
    )
    coupon_rate: float = Field(
        alias='couponRate',
        title='couponRate'
    )
    rating: Rating = Field(
        alias='rating',
        title='rating'
    )
    payment_frequency: int = Field(
        alias='paymentFrequency',
        title='paymentFrequency'
    )
    payment_dates: Dict[str, List[float]] = Field(
        alias='paymentDates',
        title='paymentDates'
    )
    voice_trading_only: bool = Field(
        alias='voiceTradingOnly',
        title='voiceTradingOnly'
    )
    bond_calc_data_settings: BondCalcDataSettings = Field(
        alias='bondCalcDataSettings',
        title='bondCalcDataSettings'
    )
    show_trades_chart_by_default: bool = Field(
        alias='showTradesChartByDefault',
        title='showTradesChartByDefault'
    )
    max_price_deviation: int = Field(
        alias='maxPriceDeviation',
        title='maxPriceDeviation'
    )
    ticker_icon: str = Field(
        alias='tickerIcon',
        title='tickerIcon'
    )
    show_quote_as_fx: bool = Field(
        alias='showQuoteAsFX',
        title='showQuoteAsFX'
    )
    is_available_for_clients: bool = Field(
        alias='isAvailableForClients',
        title='isAvailableForClients'
    )
    synthetic_feed: SyntheticFeed = Field(
        alias='syntheticFeed',
        title='syntheticFeed'
    )