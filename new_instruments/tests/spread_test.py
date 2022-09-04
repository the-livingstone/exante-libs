import pytest
from copy import deepcopy
from libs.async_sdb_additional import SDBAdditional
from libs.new_instruments import Spread, NoInstrumentError, NoExchangeError, Derivative
import datetime as dt

from libs.tests.test_libs.symboldb import SymbolDB
from libs.tests.test_libs.backoffice import BackOffice

EXISTING_EXCHANGE = 'CBOT'

EXISTING_PRODUCT_TICKER = 'KE-ZC'
EXISTING_PRODUCT_EXPIRATION = dt.date(2020, 8, 31)
EXISTING_PRODUCT_MATURITY = 'U2020'

EXISTING_CALENDAR_TICKER = 'KE'
EXISTING_CALENDAR_EXPIRATION = dt.date(2022, 7, 14)
EXISTING_CALENDAR_NEAR_MATURITY = 'N2022'
EXISTING_CALENDAR_FAR_MATURITY = 'N2023'

EXISTING_NEAR_LEG = 'K2019'
EXISTING_NEAR_LEG_EXPIRATION = dt.date(2019, 4, 30)
EXISTING_FAR_LEG = 'N2025'
BAD_FAR_LEG = 'M2025'

EXISTING_PRODUCT_LEG = 'Z2024'
EXISTING_PRODUCT_LEG_EXPIRATION = dt.date(2024, 11, 29)
BAD_PRODUCT_LEG = 'X2024'
BAD_PRODUCT_LEG_EXPIRATION = dt.date(2024, 10, 29)


NEW_TICKER = 'SOME_FUT'
EXISTING_NEW_TICKER = 'MES'
NEW_EXCHANGE = 'SOME_EXCH'
EXISTING_NEW_EXCHANGE = 'CME'
NEW_EXPIRATION = dt.date(2023, 10, 20)
NEW_MATURITY = 'V2023'

sdb = SymbolDB()
bo = BackOffice()
sdbadds = SDBAdditional(sdb=sdb, bo=bo, test=True)


# get existing series
def test_from_sdb_existing_series():
    existing_cal = Spread.from_sdb(
        EXISTING_CALENDAR_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    existing_prod = Spread.from_sdb(
        EXISTING_PRODUCT_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert existing_cal
    assert existing_cal.contracts
    assert existing_cal.instrument == existing_cal.reference
    assert existing_cal.leg_futures
    assert existing_cal.leg_futures[0].ticker == EXISTING_CALENDAR_TICKER
    assert existing_cal.leg_futures[0].exchange == EXISTING_EXCHANGE
    assert existing_cal.find_calendar_expiration(
        expiration=EXISTING_CALENDAR_EXPIRATION
    )[0] is None
    assert existing_cal.find_calendar_expiration(
        expiration=EXISTING_CALENDAR_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY
    )[0] is not None
    assert existing_cal.find_calendar_expiration(
        near_maturity=EXISTING_CALENDAR_NEAR_MATURITY,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY
    )[0] is not None
    assert existing_cal.find_product_expiration(maturity=EXISTING_PRODUCT_MATURITY)[0] is None

    assert existing_prod
    assert existing_prod.contracts
    assert existing_prod.first_ticker and existing_prod.second_ticker
    assert existing_prod.instrument == existing_prod.reference
    assert existing_prod.leg_futures
    assert next((
        x for x
        in existing_prod.leg_futures
        if x.ticker == EXISTING_PRODUCT_TICKER.split('-')[0]
    ), None)
    assert next((
        x for x
        in existing_prod.leg_futures
        if x.ticker == EXISTING_PRODUCT_TICKER.split('-')[1]
    ), None)
    assert existing_prod.leg_futures[0].exchange == EXISTING_EXCHANGE
    assert existing_prod.find_calendar_expiration(
        expiration=EXISTING_PRODUCT_EXPIRATION
    )[0] is None
    assert existing_prod.find_product_expiration(
        expiration=EXISTING_PRODUCT_EXPIRATION
    )[0] is not None
    assert existing_prod.find_product_expiration(maturity=EXISTING_PRODUCT_MATURITY)[0] is not None

def test_from_sdb_non_existing_series():
    try:
        new_spread = Spread.from_sdb(
            NEW_TICKER,
            EXISTING_EXCHANGE,
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoInstrumentError:
        new_spread = None
    assert new_spread is None
    try:
        new_spread = Spread.from_sdb(
            NEW_TICKER,
            NEW_EXCHANGE,
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoExchangeError:
        new_spread = None
    assert new_spread is None

def test_from_scratch_existing_series():
    existing_cal = Spread.from_scratch(
        EXISTING_CALENDAR_TICKER,
        EXISTING_EXCHANGE,
        recreate=True,
        shortname='New shortname',
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert existing_cal
    assert existing_cal.instrument.get('_id')
    assert existing_cal.contracts
    assert existing_cal.instrument != existing_cal.reference
    assert existing_cal.find_calendar_expiration(
        expiration=EXISTING_CALENDAR_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY
    )[0] is not None
    assert existing_cal.find_calendar_expiration(
        expiration=NEW_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY    
    )[0] is None

# try to create new series
def test_from_scratch_new_series():
    try:
        new_spread = Spread.from_scratch(
            NEW_TICKER,
            NEW_EXCHANGE,
            shortname='New shortname',
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoExchangeError:
        new_spread = None
    assert new_spread is None
    try:
        new_spread = Spread.from_scratch(
            NEW_TICKER,
            EXISTING_EXCHANGE,
            shortname='New shortname',
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoInstrumentError: # future SOME_FUT.CBOT does not exist
        new_spread = None
    assert new_spread is None

def test_add_existing_skip():
    existing_cal = Spread.from_sdb(
        EXISTING_CALENDAR_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    expiration_index = existing_cal.find_calendar_expiration(
        expiration=EXISTING_CALENDAR_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY
    )[0]
    expiration = existing_cal.contracts[expiration_index]
    reference = deepcopy(expiration.instrument)
    result = existing_cal.add(
        EXISTING_CALENDAR_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY,
        comments='somevalue'
    )
    assert result == {}
    assert reference == expiration.instrument
    assert expiration.reference == expiration.instrument
    assert not existing_cal.new_expirations

def test_add_existing_change():
    existing_cal = Spread.from_sdb(
        EXISTING_CALENDAR_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    expiration_index = existing_cal.find_calendar_expiration(
        expiration=EXISTING_CALENDAR_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY
    )[0]
    expiration = existing_cal.contracts[expiration_index]
    reference = deepcopy(expiration.instrument)
    result = existing_cal.add(
        EXISTING_CALENDAR_EXPIRATION,
        far_maturity=EXISTING_CALENDAR_FAR_MATURITY,
        skip_if_exists=False,
        comments='somevalue'
    )
    assert result.get('diff', {}).get('dictionary_item_added')
    assert reference != expiration.instrument
    assert expiration.reference != expiration.instrument
    assert not existing_cal.new_expirations

def test_add_new_legs_exist():
    existing_cal = Spread.from_sdb(
        EXISTING_CALENDAR_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    existing_prod = Spread.from_sdb(
        EXISTING_PRODUCT_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    cal_result = existing_cal.add(
        EXISTING_NEAR_LEG_EXPIRATION,
        near_maturity=EXISTING_NEAR_LEG,
        far_maturity=EXISTING_FAR_LEG

    )
    prod_result = existing_prod.add(
        EXISTING_PRODUCT_LEG_EXPIRATION,
        maturity=EXISTING_PRODUCT_LEG
    )


    cal_legs = existing_cal.new_expirations[0].instrument['legs']
    prod_legs = existing_prod.new_expirations[0].instrument['legs']

    assert cal_result.get('created')
    assert existing_cal.new_expirations
    assert existing_cal.new_expirations[0].expiration == EXISTING_NEAR_LEG_EXPIRATION
    assert existing_cal.new_expirations[0].near_maturity == Derivative.format_maturity(EXISTING_NEAR_LEG)
    assert existing_cal.new_expirations[0].far_maturity == Derivative.format_maturity(EXISTING_FAR_LEG)
    assert existing_cal.new_expirations[0].instrument['name'] == f'{Derivative.format_maturity(EXISTING_NEAR_LEG)} {Derivative.format_maturity(EXISTING_FAR_LEG)}'
    assert existing_cal.new_expirations[0].instrument['path'] == existing_cal.instrument['path']
    assert next(x['exanteId'] for x in cal_legs if x['quantity'] == 1) == f'{EXISTING_CALENDAR_TICKER}.{EXISTING_EXCHANGE}.{EXISTING_NEAR_LEG}'
    assert next(x['exanteId'] for x in cal_legs if x['quantity'] == -1) == f'{EXISTING_CALENDAR_TICKER}.{EXISTING_EXCHANGE}.{EXISTING_FAR_LEG}'

    assert prod_result.get('created')
    assert existing_prod.new_expirations
    assert existing_prod.new_expirations[0].expiration == EXISTING_PRODUCT_LEG_EXPIRATION
    assert existing_prod.new_expirations[0].maturity == Derivative.format_maturity(EXISTING_PRODUCT_LEG)
    assert existing_prod.new_expirations[0].instrument['name'] == Derivative.format_maturity(EXISTING_PRODUCT_LEG)
    assert existing_prod.new_expirations[0].instrument['path'] == existing_prod.instrument['path']
    assert next(x['exanteId'] for x in prod_legs if x['quantity'] == 1) == f'{existing_prod.first_ticker}.{EXISTING_EXCHANGE}.{EXISTING_PRODUCT_LEG}'
    assert next(x['exanteId'] for x in prod_legs if x['quantity'] == -1) == f'{existing_prod.second_ticker}.{EXISTING_EXCHANGE}.{EXISTING_PRODUCT_LEG}'

# test_from_sdb_existing_series()

# test_add_new_legs_exist()