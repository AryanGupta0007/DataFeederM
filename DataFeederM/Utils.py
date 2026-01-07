import re
from datetime import datetime
import requests 

def expand_years(year_list):
    if not year_list or len(year_list) < 2:
        return year_list
    
    start, end = map(int, year_list[:2])
    return [str(y) for y in range(start, end + 1)]

class Utils:

    
    INDEX_OPTION_PATTERN  = re.compile(r"^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX|BANKEX)")
    
    INDEX_FUTURE_PATTERN  = re.compile(r"^(NIFTY|BANKNIFTY|FINNIFTY)(\d{2}[A-Z]{3}FUT|-I)")
    
    INDEX_SPOT_PATTERN    = re.compile(
        r"^(NIFTY 50|NIFTY BANK|NIFTY FIN SERVICE|NIFTY AUTO|NIFTY REALTY|"
        r"NIFTY PHARMA|NIFTY OIL AND GAS|NIFTY MEDIA|NIFTY IT|NIFTY FMCG|SENSEX)"
    )
    
    OPTION_PATTERN        = re.compile(
        r"^([A-Z0-9.&_\-]+)"          
        r"(\d{1,2}[A-Z]{3}\d{2})"     
        r"(\d{1,7})"                  
        r"(CE|PE)$"                    
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

    @staticmethod
    def get_collection_name(ti, fmt="%Y"):
        return datetime.fromtimestamp(ti).strftime(fmt)
    
    @classmethod 
    def get_collections(cls, epochs: list):
        collections = []
        for e in epochs:
            collection = cls.get_collection_name(e)
            collections.append(collection)
        
        collections = expand_years(collections)
        print(collections)
        return collections
    
    @classmethod 
    def get_dbs(cls, syms: list):
        dbs = {}
        for sym in syms:
            db = cls.get_db_name(sym)
            dbs[sym] = db
        return dbs
    
    @staticmethod
    def login_orb(ORB_USERNAME, ORB_PASSWORD, ORB_URL):
        auth_data = {
            "username": ORB_USERNAME,
            "password": ORB_PASSWORD
        }
        response = requests.post(f"{ORB_URL}/api/auth/token", data=auth_data)
        token = response.json().get("access_token")
        return token
    
    @classmethod
    def get_collections_and_dbs(cls, syms:list, epochs:list):
        collections = cls.get_collections(epochs)
        dbs = cls.get_dbs(syms)
        return collections, dbs
    
    @staticmethod
    def find_request_orb(ORB_URL, payload, accesstoken):
        headers = {
            'Authorization': f'Bearer {accesstoken}'
            }
        response = requests.post(f"{ORB_URL}/api/data/find", headers=headers, json=payload)
        data = response.json()
        if data["status_code"] == 200:
            return data['data']
        else:
            raise Exception(data)
        