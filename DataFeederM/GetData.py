prod=True 
if prod:
    from .Utils import Utils
else:
    from Utils import Utils
    
    
class GetData:
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
        