import datetime as dt
import json
import logging
import re
from typing import Optional, Union
import pandas as pd
from pydantic import BaseModel, Field, ValidationError, root_validator
import requests
from pprint import pformat, pp

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed
)

from libs.parsers.cprod_exchange_parser_base import ExchangeParser


class Ftx:
    url = 'https://ftx.com/api'
    default_cred_file = '/etc/support/auth/ftx.json'

    def __init__(self, cred_file: str = default_cred_file):
        with open(cred_file, 'r') as f:
            self.creds = json.load(f)

        self.session = requests.Session()
        self.session.mount(self.url, requests.adapters.HTTPAdapter())

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")


    @retry(
        retry=retry_if_exception_type((
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout
        )),
        stop=stop_after_attempt(10),
        wait=wait_fixed(10)
    )
    def __request(self, method, handle: str, jdata: dict = None, **params):
        # headers = {
        #     'FTX-KEY': self.creds['api_key']
        # }
        # ts = int(time.time() * 1000)
        # signature_payload = f'{ts}{method}/{handle}'
        # if jdata:
        #     signature_payload += str(jdata)
        # signature = hmac.new(
        #     self.creds['api_secret'].encode(),
        #     signature_payload.encode(),
        #     'sha256'
        # ).hexdigest()
        # headers['FTX-SIGN'] = signature
        # headers['FTX-TS'] = str(ts)
        try:
            response = method(f"{self.url}/{handle}", params=params, json=jdata) # headers=headers)
            if response.ok:
                return response
            else:
                self.logger.error(
                    f"Error code {response.status_code} while requesting"
                    "\n"
                    f"{response.url}"
                    "\n"
                    f"{response.text}"
                )
        except Exception as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise e
                
    def get(self, handle, params=None):
        """
        wrapper method for requests.get
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        return self.__request(method=self.session.get, handle=handle, params=params)

    def search_futures(
            self,
            ticker: str = None
        ):
        search = self.get('futures').json().get('result')
        if ticker:
            return [x for x in search if x.get('underlying') == ticker]
        else:
            return search


class FutureSchema(BaseModel):
    ticker: str
    exchange: str = Field(
        'FTX',
        const=True
    )
    name: str
    name_: str = Field(
        alias='name'
    )
    underlying: str
    description: str = Field(
        alias='underlyingDescription'
    )
    type_: str = Field(
        alias='type'
    )
    expiry: Optional[dt.date] = Field(
        alias='expiry'
    )
    maturity: Optional[str]
    maturityName: Optional[str]
    perpetual_: bool = Field(
        alias='perpetual'
    )
    feedMinPriceIncrement: float = Field(
        alias='priceIncrement'
    )
    orderMinPriceIncrement: float = Field(
        alias='priceIncrement'
    )
    lotSize: float = Field(
        alias='sizeIncrement'
    )
    minLotSize: float = Field(
        alias='sizeIncrement'
    )
    currency: str
    baseCurrency: str = Field(
        'USD',
        const=True
    )
    contractMultiplier: float = Field(
        1
    )

    @root_validator(pre=True)
    def normalize_derivative(cls, values: dict):
        values['ticker'] = values.get('underlying')
        if values['perpetual'] is False:
            values['maturity'] = values.get('expiry').split('T')[0]
            values['name'] = values['maturity']
            values['expiry'] = dt.date.fromisoformat(
                values.get('expiry').split('T')[0]
            )
        else:
            values['name'] = values.get('underlying')
            values['description'] += ' Perpetual'
            values['maturityName'] = 'PERPETUAL'

        values['currency'] = values.get('underlying')
        values['exchangeLink'] = f"https://ftx.com/trade/{values.get('name')}"
        return values

class Parser(Ftx, ExchangeParser):
    def __init__(self, cred_file='/etc/support/auth/ftx.json'):
        super().__init__(cred_file)

    def futures(
            self,
            series: str,
            overrides: dict = None,
            data: list[dict] = None,
            **kwargs
        ):
        contracts = []
        series_data = {}
        ticker, _ = series.split('.')[:2]
        data = self.search_futures(ticker)
        for d in data:
            try:
                contracts.append(FutureSchema(**d).dict())
            except ValidationError as valerr:
                self.logger.warning(
                    f"contract data {d.get('name')} is invalid: {pformat(valerr.errors())}"
                )

        data_df = pd.DataFrame(contracts)
        series_data.update({
            key: next(x for x in data_df[key]) for key
            in data_df.columns
            if key not in [
                'name_',
                'type_',
                'perpetual_',
                'expiry'
            ]
        })
        series_data.update({
            'ticker': ticker,
            'exchange': 'FTX'
        })
        self.logger.info(f"Folder settings:")
        self.logger.info(pformat(series_data))
        self.logger.info(f"Found contracts:")
        self.logger.info(pformat(contracts))
        return series_data, contracts

    def options(
            self,
            series: str,
            overrides: dict,
            product: str = 'OPTION',
            data: list[dict] = None,
            **kwargs
        ):
        raise NotImplementedError("options are currently unavailable")

    def spreads(
            self,
            series: str,
            overrides: dict = None,
            data: list[dict] = None,
            **kwargs
        ):
        raise NotImplementedError()
