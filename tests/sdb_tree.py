import asyncio
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional
from enum import Enum
import json

class SelectedInstruments(Enum):
    BOND = '8680711f828f4b1bb4dbada05e6fcf6b' # AT0000A0U3T4
    CFD_NASDAQ = '0539bb901ede6c0567115641e443bfee' # AAPL.US
    CFD_LSE = '0539bb8af3220513f45401c7c3e153cd' # BRBY.GB
    CFD_EURONEXT = '355a109eb70148f69eabd532b5e3380c' # AMG.NL
    FUTURE = '4dd6a49e9b824d7ab389780a2463b190' # MES.CME series
    FUTURE_KE = 'e034a18d82ab4a868fe099d915ec531e' # KE.CBOT series
    FUTURE_ZC = '04e8638eac22112605e6792504820d4d' # ZC.CBOT series
    OPTION_ON_FUTURE = '141dfc9ca2ac4afca4c6b55e24629d78' # MES.CME series (incl. weeklies)
    OPTION_CBOE_INDEX = '04d37b354452e768939c9641a53a590e' # SPX.CBOE series
    OPTION_CBOE_SINGLE_STOCK = '9ca4ff86757c49c79926a91c3acaaeda' # AA.CBOE series
    SPREAD_PRODUCT = '366530b3e0a0446088aaf867d88d02c5' # KE-ZC.CBOT series
    SPREAD_CALENDAR = '1b6c00bd9d4746de916d7cac3178b0bb' # KE.CBOT series
    STOCK_NASDAQ ='04b15fc10f4ac00e1ab649a30af65ec6' # AAPL.NASDAQ
    STOCK_LSE = '0518677376fc61bfe75d6e040607d1cc' # BRBY.LSE
    STOCK_EURONEXT = '2429f00c4c98474ab716123fd6d4aecb' # AMG.EURONEXT

sdb = SymbolDB('prod')
sdbadds = SDBAdditional('prod', sdb=sdb)
selected_tree = []
for uuid in SelectedInstruments:
    instr = asyncio.run(sdb.get(uuid.value))
    parents = asyncio.run(sdb.get_parents(uuid.value))
    heirs = asyncio.run(sdb.get_heirs(uuid.value, full=True, recursive=True))
    selected_tree.append(instr)
    selected_tree.extend(heirs)
    selected_tree.extend([x for x in parents if x not in selected_tree])
selected_tree = sorted(selected_tree, key=lambda p: ''.join(p['path']))
for i in selected_tree:
    i['symbolId'] = sdbadds.compile_symbol_id(i, cache=selected_tree) if i['isAbstract'] is False else None
    i['expiryTime'] = sdbadds.compile_expiry_time(i, cache=selected_tree) if i['isAbstract'] is False else None
with open('selected_tree.json', 'w') as f:
    json.dump(selected_tree, f, indent=4)