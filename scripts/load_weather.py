from datetime import time
import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url=os.getenv("supabase_url")
key=os.getenv("supabase_key")
sb=create_client(url,key)

def load_to_supabase():
    csv_path="../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file{csv_path}")
    df=pd.read_csv(csv_path)

    #Convert timestamp to string
    df['time']=pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"]=pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    batch_size=20
    for i in range(0, len(df), batch_size):

            batch = df.iloc[i: i + batch_size].where(pd.notnull(df),None).to_dict("records")
            values = [f"('{r['time']}, {r.get('temperature_c''NULL')},{r.get('humidity_perecent','NULL')}"
                      f"({r.get('wind_speed''NULL')}, '{r.get('city' , 'Hyderbad')}','{r['extracted_at' ]}]' )"for r in batch]
            insert_sql=f"""insert into weather_data(time,temp,humi,wind,city,extracted_at) values {",".join(values)}"""
            # sb.rpc("execute_sql",{"query":insert_sql})
            sb.table("weather_data").insert(batch).execute()
            print(f"inserted rows{i+1}--{min(i+batch_size,len(df))}")
            # time.sleep(0.5)
    print("Finished loading of weather data.")


if __name__ == "__main__":
    load_to_supabase()