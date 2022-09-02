import pytest
from copy import deepcopy
from libs.async_sdb_additional import SDBAdditional
from libs.new_instruments import Spread, NoInstrumentError, NoExchangeError
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


NEW_TICKER = 'SOME_FUT'
NEW_EXCHANGE = 'SOME_EXCH'
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

test_from_sdb_existing_series()