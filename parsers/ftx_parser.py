import datetime as dt
from typing import Optional, Union
import pandas as pd
from pydantic import BaseModel, Field, ValidationError, root_validator
from pprint import pformat, pp

from libs.cp_apis.ftx import Ftx

from libs.parsers.cprod_exchange_parser_base import ExchangeParser

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
        data = self.search_future(ticker)
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
