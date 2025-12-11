from pymongo import MongoClient
# from Utils import Utils
from .Utils import Utils
import pandas as pd
import warnings
from dotenv import load_dotenv
import os

load_dotenv()  # loads variables from .env into environment

warnings.filterwarnings("ignore", category=FutureWarning)


# print(Utils.get_db_name("CIPLA"))

def get_collections_and_dbs(syms:list, epochs:list):
    collections = Utils.get_collections(epochs)
    dbs = Utils.get_dbs(syms)
    return collections, dbs

def main(mongo_uri, syms: list, epochs: list):
    client = MongoClient(mongo_uri)

    collections, dbs = get_collections_and_dbs(syms, epochs)
    output = {}
    for i, sym in enumerate(syms):
        # print(sym, collections)
        # print(dbs[sym])
        rows = []
        db = client[dbs[sym]]
        if len(epochs) == 1:
            # print("here")
            e = epochs[0]
            collection = db[collections[0]]
            res = collection.find({
                'ti': e,
                'sym': sym 
            })
            if res:
                    for r in res:
                        rows.append(r)
                    output[sym] = rows
                    
                    
        elif len(epochs) == 2:
            if len(set(collections)) == 1:
                # print("here123")
                collection = db[collections[0]]
                res = collection.find({
                    'ti': {
                        "$gte": epochs[0],
                        "$lte": epochs[1]
                    },
                    "sym": sym
                })
                if res:
                    for r in res:
                        rows.append(r)
                    output[sym] = rows
                    
            else: 
                ## NOT TESTED
                collections = list(set(collections))
                for i, col in enumerate(collections):
                    # print("..: ", i, col, dbs[sym])
                    collection = db[col] 
                    if i == 0:
                        res = collection.find({
                        "ti": {
                            "$gte": epochs[0]
                        },
                        "sym": sym
                            })
                    elif i == len(collections) - 1:
                        res = collection.find({
                        "ti": {
                            "$lte": epochs[1]
                        },
                        "sym": sym
                        })
                    else:
                        res = collection.find({
                            "sym": sym
                        })
                    if res:
                        for r in res:
                            rows.append(r)
                        output[sym] = rows
                    
                
    return output
        
        
if __name__ == "__main__":
    import random
    tis_df = pd.read_csv('ti.csv', index_col=0)
    tis_df.rename(columns={0: "ti"}, inplace=True)
    x = random.randint(0, len(tis_df))
    y = random.randint(x+1, len(tis_df))
    e_x = int(tis_df.loc[x][0])
    e_y = int(tis_df.loc[y][0])
    epoch_2023 = 1687854060 
    epochs = [e_x, e_y]
    syms = ['NIFTY BANK', 'NIFTY 50', 'CIPLA']
    output = main(mongo_uri=os.getenv('MONGO_URI'), syms=syms, epochs=epochs)
    if output:
        print(output)
                
                    
        
        