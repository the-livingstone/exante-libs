import asyncio
import logging
from pprint import pp
import requests
from pydantic import BaseModel, Field, ValidationError, root_validator

from libs.async_sdb_additional import SDBAdditional



class KrakenScraper:
    kraken_assets = 'https://api.kraken.com/0/public/AssetPairs'
    sdbadds = SDBAdditional()
    currencies = asyncio.run(sdbadds.get_list_from_sdb(
        'currencies',
        additional_fields=['description']
    ))

    def get_fx_spots(self):
        response = requests.get(self.kraken_assets).json()
        fx_spots: dict = response['result']
        for p, val in fx_spots.items():
            try:
                instrument = KrakenFXSchema(**val).dict()
                yield instrument
            except ValidationError as val_err:
                logging.warning(val_err.errors())

class KrakenFXSchema(BaseModel):
    name: str
    isAbstract: bool = Field(
        False,
        const=True
    )
    currency: str
    baseCurrency: str
    minLotSize: float = Field(
        alias='ordermin'
    )
    shortName: str
    lotSize: float
    feedMinPriceIncrement: float
    orderMinPriceIncrement: float


    @root_validator(pre=True)
    def mk_sdb_instrument(cls, values: dict):
        values['baseCurrency'], values['currency'] = values['wsname'].split('/')
        sdb_currencies = [x[0] for x in KrakenScraper.currencies]
        for currency in ['baseCurrency', 'currency']:
            if values[currency] == '1INCH':
                values[currency] = 'ONEINCH'
            if values[currency] not in sdb_currencies:
                raise ValueError(f"{values[currency]=} does not exist in sdb")
        values['name'] = f"{values['baseCurrency']}/{values['currency']}"
        values['lotSize'] = float(values.get('tick_size', 0.01))*float(values.get('lot_multiplier', 1))
        values['feedMinPriceIncrement'] = float(values.get('tick_size', 0.01))
        values['orderMinPriceIncrement'] = float(values.get('tick_size', 0.01))

        base_ccy_descr = next((x[2] for x in KrakenScraper.currencies if x[0] == values['baseCurrency']), values['baseCurrency'])
        ccy_descr = next((x[2] for x in KrakenScraper.currencies if x[0] == values['currency']), values['currency'])
        values['shortName'] = f"{base_ccy_descr} / {ccy_descr}"

        return values
