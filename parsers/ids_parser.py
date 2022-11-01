"""
ICE Xml Parser
"""

from lxml import etree


gl, ci, mi, om, im, ix, ici, mm = ('global_information', 'country_information',
                               'master_information', 'organization_master',
                               'instrument_master', 'instrument_xref',
                               'instrument_country_information', 'market_master')


IDENT = {
    'isin': etree.XPath(f'{mi}/{ix}/*[@type_id="2"]/text()'),
    'figi': etree.XPath(f'{mi}/{ix}/*[@type_id="20"]/text()'),
    'sedol': etree.XPath(f'{mi}/{ix}/*[@type="SEDOL"]/text()'),
    'country': etree.XPath(f"{gl}/{ci}/{ici}/country_code/text()"),
    'countryRisk': etree.XPath(f"{mi}/{om}/org_country_code/text()"),
    'maturityDate': etree.XPath(f"debt/fixed_income/maturity_date/text()"),
    'mic': etree.XPath(f"{mi}/{mm}/market/mic/text()"),
    'ric': etree.XPath(f'{mi}/{ix}/*[@type="RIC"]/text()'),
    'name': etree.XPath(f'{mi}/{mm}/market/*[@type="Ticker IDC"]/text()'),
    'ticker': etree.XPath(f'{mi}/{mm}/market/*[@type="Ticker"]/text()'),
    'description': etree.XPath(f"{mi}/{im}/primary_name/text()"),
    'shortName': etree.XPath(f"{mi}/{im}/primary_name/text()"),
    'currency': etree.XPath(f"{mi}/{im}/primary_currency_code/text()"),
    'cfi': etree.XPath(f"{mi}/{im}/cfi_code/text()"),
    'issueDate': etree.XPath(f"{mi}/{im}/issue_date/text()"),
    # 'id': etree.XPath(f""),
    # 'feedMinPriceIncrement': etree.XPath(f""),
    # 'orderMinPriceIncrement': etree.XPath(f""),
    # 'lotSize': etree.XPath(f""),
    # 'includedIntoRegReporting': etree.XPath(f""),
    'gics_filds': etree.XPath(f"{gl}/organizationInformation/classifications/*[@type='gicsCode']/text()")
    # Add path to values 
}


class ICEXmlParser:
    """ Class to parse XML from ICE Data Service
    """

    def __init__(self, filename: str) -> None:
        self.filename = filename


    def __parse_elem(self, elem):
        idents = {}
        for item, value in IDENT.items():
            try:
                idents.update({item: value(elem)[0]})
            except Exception:
                pass
        return idents


    def parse_file(self):
        xml_data = etree.iterparse(source=self.filename, tag='instrument', events=('end',))
        parsed_instruments = []
        for _, elem in xml_data:
            try:
                parsed_instruments.append(self.__parse_elem(elem))
            except Exception:
                pass
        return parsed_instruments
