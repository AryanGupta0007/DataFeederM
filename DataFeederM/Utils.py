import re
from datetime import datetime

def expand_years(year_list):
    if not year_list or len(year_list) < 2:
        return year_list
    
    start, end = map(int, year_list[:2])
    return [str(y) for y in range(start, end + 1)]

class Utils:

    # ---------- REGEX PATTERNS (FIXED) ----------

    INDEX_OPTION_PATTERN  = re.compile(r"^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX|BANKEX)")
    
    INDEX_FUTURE_PATTERN  = re.compile(r"^(NIFTY|BANKNIFTY|FINNIFTY)(\d{2}[A-Z]{3}FUT|-I)")
    
    INDEX_SPOT_PATTERN    = re.compile(
        r"^(NIFTY 50|NIFTY BANK|NIFTY FIN SERVICE|NIFTY AUTO|NIFTY REALTY|"
        r"NIFTY PHARMA|NIFTY OIL AND GAS|NIFTY MEDIA|NIFTY IT|NIFTY FMCG|SENSEX)"
    )
    
    # OPTION ex: NIFTY25FEB2119500CE
    OPTION_PATTERN        = re.compile(
        r"^([A-Z0-9.&_\-]+)"          # symbol
        r"(\d{1,2}[A-Z]{3}\d{2})"     # expiry (fixed: \"d{2})
        r"(\d{1,7})"                  # strike
        r"(CE|PE)$"                   # option type
    )

    FUTURE_PATTERN        = re.compile(
        r"(\d{2}[A-Z]{3}FUT|-(I|II|III))$"
    )

    @classmethod
    def get_db_name(cls, sym):

        if cls.OPTION_PATTERN.search(sym):
            if cls.INDEX_OPTION_PATTERN.search(sym):
                return "index_options_db"   
            else:
                return "stock_options_db"   

        elif cls.FUTURE_PATTERN.search(sym):
            if cls.INDEX_FUTURE_PATTERN.search(sym):
                return "index_futures_db"   
            else:
                return "stock_futures_db"   

        else:
            if cls.INDEX_SPOT_PATTERN.search(sym):
                return "index_db"    
            else:
                return "stock_db"    

    @classmethod
    def get_collection_name(cls, ti, fmt="%Y"):
        return datetime.fromtimestamp(ti).strftime(fmt)
    
    @classmethod 
    def get_collections(cls, epochs: list):
        collections = []
        for e in epochs:
            collection = Utils.get_collection_name(e)
            collections.append(collection)
        
        collections = expand_years(collections)
        print(collections)
        import sys 
        sys.exit()
        return collections
    
    @classmethod 
    def get_dbs(cls, syms: list):
        dbs = {}
        for sym in syms:
            db = Utils.get_db_name(sym)
            dbs[sym] = db
        return dbs
    