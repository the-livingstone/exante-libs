import asyncio
import pytest
from copy import deepcopy
from libs.new_instruments import ExpirationError, Option, NoInstrumentError, NoExchangeError
import datetime as dt

from libs.async_sdb_additional import SDBAdditional

from libs.tests.test_libs.symboldb import SymbolDB
from libs.tests.test_libs.backoffice import BackOffice

EXISTING_TICKER = 'MES' # CME
EXISTING_EXCHANGE = 'CME'
EXISTING_EXPIRATION = dt.date(2022, 12, 16)
EXISTING_WEEKLY_EXPIRATION = dt.date(2022, 8, 5)
EXISTING_MATURITY = 'Z2022'
EXISTING_WEEKLY_MATURITY = 'Q2022'
EXISTING_WEEKLY_ID = '861543995ff24b59b1d0aa5b6ecd3cc5'

FIRST_WEEK_TICKER = 'EX1'

EXISTING_WEEKLY_EXPIRATION = dt.date(2022, 8, 5)
EXISTING_WEEKLY_MATURITY = 'Q2022'

GOOD_UNDERLYING = 'MES.CME.Z2022'
GOOD_WEEKLY_UNDERLYING = 'MES.CME.U2022'

NEW_TICKER = 'SOME_OPT'
NEW_EXCHANGE = 'SOME_EXCH'
NEW_EXPIRATION = dt.date(2023, 10, 20)
NEW_FIRST_WEEK_EXPIRATION = dt.date(2023, 10, 6)
NEW_MATURITY = 'V2023'

USED_MONTHLY = ['MES.CME.Z2022.P3930', 'MES.CME.Z2022.P3970']
NON_USED_MONTHLY = 'MES.CME.Z2022.C2250'
USED_WEEKLY = ['EX1.CME.Q2022.P3600', 'EX1.CME.Q2022.C3900']
NON_USED_WEEKLY = 'EX1.CME.Q2022.C2300'


SIMPLE_STRIKES = {
        'CALL': [
            100,
            200,
            300
        ],
        'PUT': [
            100,
            200,
            300
        ]
    }
COMPLEX_STRIKES = {
        'CALL': [
            {
                'strikePrice': 100,
                'ISIN': 'TW0005326003'
            },
            {
                'strikePrice': 200,
                'ISIN': 'TW0005326003'
            },
            {
                'strikePrice': 300,
                'ISIN': 'TW0005326003'
            }
        ],
        'PUT': [
            {
                'strikePrice': 100,
                'ISIN': 'TW0005326003'
            },
            {
                'strikePrice': 200,
                'ISIN': 'TW0005326003'
            },
            {
                'strikePrice': 300,
                'ISIN': 'TW0005326003'
            }
        ]
    }


sdb = SymbolDB()
bo = BackOffice()
sdbadds = SDBAdditional(sdb=sdb, bo=bo, test=True)
asyncio.run(sdbadds.load_tree(
    fields=['expiryTime'],
    reload_cache=True,
    return_dict=False
))
tree_df = sdbadds.tree_df

# static methods
def test_find_parent_folder_id():
    aapl_folder_id, aapl_option_type = Option._find_parent_folder_id(
        'AAPL',
        'CBOE',
        sdb,
        sdbadds,
        tree_df
    )
    
    mes_folder_id, mes_option_type = Option._find_parent_folder_id(
        'MES',
        'CME',
        sdb,
        sdbadds,
        tree_df
    )
    spx_folder_id, spx_option_type = Option._find_parent_folder_id(
        'SPX',
        'CBOE',
        sdb,
        sdbadds,
        tree_df
    )
    aa_folder_id, aa_option_type = Option._find_parent_folder_id(
        'AA',
        'CBOE',
        sdb,
        sdbadds,
        tree_df
    )
    mes_folder = sdb.get(mes_folder_id)
    spx_folder = sdb.get(spx_folder_id)
    aa_folder = sdb.get(aa_folder_id)

    assert mes_folder['name'] == 'CME'
    assert spx_folder['name'] == 'CBOE'
    assert aa_folder['name'] == 'CBOE'
    assert mes_option_type == 'OPTION ON FUTURE'
    assert spx_option_type == 'OPTION'
    assert aa_option_type == 'OPTION'



# get existing series
def test_from_sdb_existing_series():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert existing_opt
    assert existing_opt.contracts
    assert existing_opt.instrument == existing_opt.reference
    assert existing_opt.find_expiration(expiration=EXISTING_EXPIRATION)[0] is not None
    assert existing_opt.find_expiration(maturity=EXISTING_MATURITY)[0] is not None
    assert existing_opt.find_expiration(expiration=NEW_EXPIRATION)[0] is None

# try to create new series
def test_from_sdb_non_existing_series():
    try:
        new_opt = Option.from_sdb(
            NEW_TICKER,
            EXISTING_EXCHANGE,
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoInstrumentError:
        new_opt = None
    assert new_opt is None
    try:
        new_opt = Option.from_sdb(
            NEW_TICKER,
            NEW_EXCHANGE,
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoExchangeError:
        new_opt = None
    assert new_opt is None

def test_from_scratch_existing_series():
    existing_opt = Option.from_scratch(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        shortname='New shortname',
        recreate=True,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert existing_opt
    assert existing_opt.instrument.get('_id')
    assert existing_opt.contracts
    assert existing_opt.instrument != existing_opt.reference
    assert existing_opt.find_expiration(expiration=EXISTING_EXPIRATION)[0] is not None
    assert existing_opt.find_expiration(maturity=EXISTING_MATURITY)[0] is not None
    assert existing_opt.find_expiration(expiration=NEW_EXPIRATION)[0] is None

def test_from_scratch_new_series():
    try:
        new_opt = Option.from_scratch(
            NEW_TICKER,
            NEW_EXCHANGE,
            shortname='New shortname',
            sdb=sdb,
            bo=bo,
            sdbadds=sdbadds
        )
    except NoExchangeError:
        new_opt = None
    assert new_opt is None
    new_opt = Option.from_scratch(
        NEW_TICKER,
        EXISTING_EXCHANGE,
        shortname='New shortname',
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    assert new_opt

def test_add_existing_skip():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    expiration_index, series = existing_opt.find_expiration(
        expiration=EXISTING_EXPIRATION
    )
    expiration = series.contracts[expiration_index]
    strikes = deepcopy(expiration.instrument['strikePrices'])
    reference = deepcopy(expiration.instrument)
    result = existing_opt.add(
        EXISTING_EXPIRATION,
        strikes=strikes,
        maturity=EXISTING_MATURITY,
        comments='somevalue'
    )
    assert result == {}
    assert reference == expiration.instrument
    assert expiration.reference == expiration.instrument
    assert not existing_opt.new_expirations

def test_add_existing_change():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    expiration_index, series = existing_opt.find_expiration(expiration=EXISTING_EXPIRATION)
    expiration = series.contracts[expiration_index]
    strikes = deepcopy(expiration.instrument['strikePrices'])
    reference = deepcopy(expiration.instrument)
    result = existing_opt.add(
        EXISTING_EXPIRATION,
        strikes=strikes,
        maturity=EXISTING_MATURITY,
        skip_if_exists=False,
        comments='somevalue'
    )
    assert result.get('diff', {}).get('dictionary_item_added')
    assert reference != expiration.instrument
    assert expiration.reference != expiration.instrument
    assert not existing_opt.new_expirations

def test_add_new_monthly_simple_strikes():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    result = existing_opt.add(
        NEW_EXPIRATION,
        strikes=SIMPLE_STRIKES,
        maturity=NEW_MATURITY
    )
    assert result.get('created')
    assert existing_opt.new_expirations
    assert existing_opt.new_expirations[0].expiration == NEW_EXPIRATION
    assert existing_opt.new_expirations[0].maturity == '2023-10'
    assert existing_opt.new_expirations[0].instrument['name'] == '2023-10'
    assert existing_opt.new_expirations[0].instrument['path'] == existing_opt.instrument['path']
    assert existing_opt.new_expirations[0].instrument['strikePrices']['CALL'][0] == {
        'strikePrice': 100.0,
        'isAvailable': True
    }

def test_add_new_monthly_complex_strikes():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    result = existing_opt.add(
        NEW_EXPIRATION,
        strikes=COMPLEX_STRIKES,
        maturity=NEW_MATURITY
    )
    assert existing_opt.new_expirations[0].instrument['strikePrices']['CALL'][0] == {
        'strikePrice': 100.0,
        'isAvailable': True,
        'identifiers': {
            'ISIN': 'TW0005326003'
        }
    }
def test_add_new_weekly_complex_strikes():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    try:
        result = existing_opt.add(
            NEW_EXPIRATION,
            strikes=COMPLEX_STRIKES,
            maturity=NEW_MATURITY,
            week_num=1
        )
    except ExpirationError:
        result = 'Not created'
    assert result == 'Not created'
    result = existing_opt.add(
        NEW_FIRST_WEEK_EXPIRATION,
        strikes=COMPLEX_STRIKES,
        maturity=NEW_MATURITY,
        week_num=1
    )


    first_week_opt = next(
        x for x
        in existing_opt.weekly_commons[0].weekly_folders
        if x.week_number == 1
    )
    assert first_week_opt.ticker == FIRST_WEEK_TICKER
    assert first_week_opt.new_expirations
    assert first_week_opt.new_expirations[0].expiration == NEW_FIRST_WEEK_EXPIRATION
    assert first_week_opt.new_expirations[0].maturity == '2023-10'
    assert first_week_opt.new_expirations[0].instrument['name'] == '2023-10'
    assert first_week_opt.new_expirations[0].instrument['path'] == first_week_opt.instrument['path']
    assert first_week_opt.new_expirations[0].instrument['strikePrices']['CALL'][0] == {
        'strikePrice': 100.0,
        'isAvailable': True,
        'identifiers': {
            'ISIN': 'TW0005326003'
        }
    }
    assert not existing_opt.new_expirations

def test_find_weekly_by_id():
    existing_opt = Option.from_sdb(
        EXISTING_TICKER,
        EXISTING_EXCHANGE,
        sdb=sdb,
        bo=bo,
        sdbadds=sdbadds
    )
    num, series = existing_opt.find_expiration(
        uuid=EXISTING_WEEKLY_ID
    )
    found = series.contracts[num]
    assert found.instrument['name'] == '2022-08'
    assert series.ticker == FIRST_WEEK_TICKER
    assert series.week_number == 1


# test_from_sdb_existing_series()
# test_add_new_weekly_complex_strikes()

test_find_parent_folder_id()