import pytest
from copy import deepcopy
from libs.async_sdb_additional import SDBAdditional
from libs.new_instruments import Future, NoInstrumentError, NoExchangeError
import datetime as dt

from libs.tests.test_libs.symboldb import SymbolDB
from libs.tests.test_libs.backoffice import BackOffice


EXISTING_TICKER = 'MES' # CME
EXISTING_EXCHANGE = 'CME'
EXISTING_EXPIRATION = dt.date(2023, 9, 15)
EXISTING_MATURITY = 'U2023'

NEW_TICKER = 'SOME_FUT'
NEW_EXCHANGE = 'SOME_EXCH'
NEW_EXPIRATION = dt.date(2023, 10, 20)
NEW_MATURITY = 'V2023'

sdb = SymbolDB()
bo = BackOffice()
sdbadds = SDBAdditional(sdb=sdb, bo=bo, test=True)


# get existing series
def test_from_sdb_existing_series():
    existing_fut = Future.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert existing_fut
    assert existing_fut.contracts
    assert existing_fut.instrument == existing_fut.reference
    assert existing_fut.find_expiration(expiration=EXISTING_EXPIRATION)[0] is not None
    assert existing_fut.find_expiration(maturity=EXISTING_MATURITY)[0] is not None
    assert existing_fut.find_expiration(expiration=NEW_EXPIRATION)[0] is None

# try to create new series
def test_from_sdb_non_existing_series():
    try:
        new_fut = Future.from_sdb(
            NEW_TICKER,
            EXISTING_EXCHANGE,
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoInstrumentError:
        new_fut = None
    assert new_fut is None
    try:
        new_fut = Future.from_sdb(
            NEW_TICKER,
            NEW_EXCHANGE,
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoExchangeError:
        new_fut = None
    assert new_fut is None

def test_from_scratch_existing_series():
    existing_fut = Future.from_scratch(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        recreate=True,
        shortname='New shortname',
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert existing_fut
    assert existing_fut.instrument.get('_id')
    assert existing_fut.contracts
    assert existing_fut.instrument != existing_fut.reference
    assert existing_fut.find_expiration(expiration=EXISTING_EXPIRATION)[0] is not None
    assert existing_fut.find_expiration(maturity=EXISTING_MATURITY)[0] is not None
    assert existing_fut.find_expiration(expiration=NEW_EXPIRATION)[0] is None

def test_from_scratch_new_series():
    try:
        new_fut = Future.from_scratch(
            NEW_TICKER,
            NEW_EXCHANGE,
            shortname='New shortname',
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoExchangeError:
        new_fut = None
    assert new_fut is None
    new_fut = Future.from_scratch(
        NEW_TICKER,
        EXISTING_EXCHANGE,
        shortname='New shortname',
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert new_fut

def test_add_existing_skip():
    existing_fut = Future.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    expiration_index = existing_fut.find_expiration(expiration=EXISTING_EXPIRATION)[0]
    expiration = existing_fut.contracts[expiration_index]
    reference = deepcopy(expiration.instrument)
    result = existing_fut.add(
        EXISTING_EXPIRATION,
        EXISTING_MATURITY,
        comments='somevalue'
    )
    assert result == {}
    assert reference == expiration.instrument
    assert expiration.reference == expiration.instrument
    assert not existing_fut.new_expirations

def test_add_existing_change():
    existing_fut = Future.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    expiration_index = existing_fut.find_expiration(expiration=EXISTING_EXPIRATION)[0]
    expiration = existing_fut.contracts[expiration_index]
    reference = deepcopy(expiration.instrument)
    result = existing_fut.add(EXISTING_EXPIRATION, EXISTING_MATURITY, skip_if_exists=False, comments='somevalue')
    assert result.get('diff', {}).get('dictionary_item_added')
    assert reference != expiration.instrument
    assert expiration.reference != expiration.instrument
    assert not existing_fut.new_expirations

def test_add_new():
    existing_fut = Future.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    result = existing_fut.add(NEW_EXPIRATION, NEW_MATURITY)
    assert result.get('created')
    assert existing_fut.new_expirations
    assert existing_fut.new_expirations[0].expiration == NEW_EXPIRATION
    assert existing_fut.new_expirations[0].maturity == '2023-10'
    assert existing_fut.new_expirations[0].instrument['name'] == '2023-10'
    assert existing_fut.new_expirations[0].instrument['path'] == existing_fut.instrument['path']

# test_from_sdb_existing_series()
# test_add_existing_skip()