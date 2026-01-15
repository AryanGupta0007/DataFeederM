prod=True 
if prod:
    from .Utils import Utils
else:
    from Utils import Utils
    
    
class GetData:
    def get_options_daily_data(ORB_URL, accesstoken, syms, date, month,  year, current_month_only=True):
        output = {}
        for sym in syms:
            if sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
                db = "index_options_db"
            else:
                db = "stock_options_db"
            from datetime import datetime, timezone
            start_dt = f"{year}-{month}-{int(date) if int(date) > 10 else f"0{date}"} 00:00:00"
            end_dt = f"{year}-{month}-{int(date) if int(date) > 10 else f"0{date}"} 17:30:00"
            epoch_start = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S")
                                .replace(tzinfo=timezone.utc)
                                .timestamp())
            epoch_end = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S")
                                .replace(tzinfo=timezone.utc)
                                .timestamp())
            
            if current_month_only:
                import calendar
                month_abbr = calendar.month_abbr[int(month)].upper()
                query = {
                            "sym": {"$regex": f"^{sym}.*{month_abbr}"}, "ti": {"$gte": epoch_start, "$lte": epoch_end}
                        }
            else:
                query = {
                            "sym": {"$regex": f"^{sym}"}, "ti": {"$gte": epoch_start, "$lte": epoch_end}
                        }
                
            print(db, epoch_end, epoch_start)
            payload = {
                    "db": db,
                    "collection": f"{year}",
                    "query": query 
                    }
            res = Utils.find_request_orb(ORB_URL, payload, accesstoken)
            rows = []
            for row in res:
                rows.append(row)
            output[f"{sym}-O"] =rows  
        return output
    
    
    def get_options_monthly_data(ORB_URL, accesstoken, syms, month, year, current_month_only=False):
        output = {}
        # print(month_abbr)  # JAN
        for sym in syms:
            if sym in ["NIFTY", "BANKNIFTY"]:
                db = "index_options_db"
            else:
                db = "stock_options_db"
            from datetime import datetime, timezone
            start_dt = f"{year}-{month}-01 18:30:00"
            end_dt = f"{year}-{month}-31 18:30:00"
            epoch_start = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S")
                                .replace(tzinfo=timezone.utc)
                                .timestamp())
            epoch_end = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S")
                                .replace(tzinfo=timezone.utc)
                                .timestamp())
            
            if current_month_only:
                import calendar
                month_abbr = calendar.month_abbr[int(month)].upper()
                query = {
                            "sym": {"$regex": f"^{sym}.*{month_abbr}"}, "ti": {"$gte": epoch_start, "$lte": epoch_end}
                        }
            else:
                query = {
                            "sym": {"$regex": f"^{sym}"}, "ti": {"$gte": epoch_start, "$lte": epoch_end}
                        }
                
            print(db, epoch_end, epoch_start)
            payload = {
                    "db": db,
                    "collection": f"{year}",
                    "query": query 
                    }
            res = Utils.find_request_orb(ORB_URL, payload, accesstoken)
            rows = []
            i = 0 
            for row in res:
                if (i % 1000) == 0:
                    print(i)
                elif i == 0:
                    print(i)
                rows.append(row)
                i += 1
            output[f"{sym}-O"] =rows  
        return output
                
        
    @staticmethod
    def for_sym_and_ti(accesstoken, syms, epochs, ORB_URL):
        collections, dbs = Utils.get_collections_and_dbs(syms, epochs)
        output = {}
        for i, sym in enumerate(syms):
            # print(sym, collections)
            # print(dbs[sym])
            rows = []
            db = dbs[sym]
            if len(epochs) == 1:
                # print("here")
                e = epochs[0]
                collection = collections[0]
                payload = {
                    "db": db,
                    "collection": collection,
                        "query": {
                        'ti': e,
                        'sym': sym 
                        }
                    }
                res = Utils.find_request_orb(ORB_URL, payload, accesstoken)
                for r in res:
                    rows.append(r)
                output[sym] = rows
                    
                        
            elif len(epochs) == 2:
                if len(set(collections)) == 1:
                    # print("here123")
                    collection = collections[0]
                    payload = {
                    "db": db,
                    "collection": collection,
                        "query": {
                        'ti': {
                            "$gte": epochs[0],
                            "$lte": epochs[1]
                        },
                        "sym": sym
                    }
                        }
                    res = Utils.find_request_orb(ORB_URL, payload, accesstoken)
                    if res:
                        for r in res:
                            rows.append(r)
                        output[sym] = rows
                        
                else: 
                    collections = list(set(collections))
                    for i, col in enumerate(collections):
                        # print("..: ", i, col, dbs[sym])
                        if i == 0:
                            payload = {
                                "db": db,
                                "collection": col,
                                    "query": {
                                "ti": {
                                    "$gte": epochs[0]
                                    },
                                "sym": sym
                                        }
                                    }
                        elif i == len(collections) - 1:
                            payload = {
                                "db": db,
                                "collection": col,
                                    "query": {
                                        "ti": {
                                            "$lte": epochs[1]
                                        },
                                        "sym": sym
                                        }
                                    }
                        else:
                            payload = {
                                "db": db,
                                "collection": col,
                                    "query": {
                                        "sym": sym
                                        }
                                    }
                        # print(payload)
                        res = Utils.find_request_orb(ORB_URL, payload, accesstoken)
                        if res:
                            for r in res:
                                rows.append(r)
                            output[sym] = rows
                        
                    
        return output
        