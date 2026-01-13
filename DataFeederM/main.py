prod = True
if prod:
    from .Utils import Utils
    from .GetData import GetData
else:
    from Utils import Utils
    from GetData import GetData


import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


def main(ORB_URL, ORB_USERNAME, ORB_PASSWORD, syms: list, epochs: list, symbol_type="SPOT", year=None, month=None, date=None, daily=True, monthly=False):
    accesstoken = Utils.login_orb(ORB_URL=ORB_URL, ORB_USERNAME=ORB_USERNAME, ORB_PASSWORD=ORB_PASSWORD)
    if symbol_type == "SPOT" or symbol_type == "FUTURE":
        output = GetData.for_sym_and_ti(accesstoken, syms, epochs, ORB_URL)
    elif symbol_type == "OPTION" or symbol_type == "OPTIONS":
        if monthly:
            output = GetData.get_options_monthly_data(ORB_URL=ORB_URL, syms=syms, year=year, month=month, accesstoken=accesstoken)
        elif daily: 
            output = GetData.get_options_daily_data(ORB_URL=ORB_URL, syms=syms, year=year, month=month, date=date, accesstoken=accesstoken)
    return output
        
        
if __name__ == "__main__":
    import random
    from dotenv import load_dotenv
    import os
    import pandas as pd

    load_dotenv()  # loads variables from .env into environment
    # main

    tis_df = pd.read_csv('ti.csv', index_col=0)
    tis_df.rename(columns={0: "ti"}, inplace=True)
    x = random.randint(0, len(tis_df))
    y = random.randint(x+1, len(tis_df))
    e_x = int(tis_df.loc[x][0])
    e_y = int(tis_df.loc[y][0])
    epoch_2023 = 1687854060
    epoch_2022 = 1640995200 
    epochs = [epoch_2023, e_y]
    # print(epochs)
    # epochs = [epoch_2023, 1735689600]
    syms = ['ADANIENT-I']
    # syms = ['CIPLA-I']
    output = main(ORB_URL=os.getenv('ORB_URL'), ORB_USERNAME=os.getenv('ORB_USERNAME'), ORB_PASSWORD=os.getenv('ORB_PASSWORD'), syms=["AXISBANK"], epochs=epochs, symbol_type="OPTIONS", date=3, month=1, year=2023, monthly=False, daily=True)
    # print(output)
    # # output = main(mongo_client=client, syms=syms, epochs=epochs)
    # if output:
    #     for k, v in output.items():
    #         # print(k)
    #         import pandas as pd
    #         df = pd.DataFrame(v)
    #         # print(df)
    #         # df.to_csv(f'{k}_OUTPUT.csv')
                
                    
        
        