import json
from datetime import datetime
import requests
from pathlib import Path

Data_dir=Path(__file__).resolve().parents[1]/"data"/"raw"
Data_dir.mkdir(parents=True,exist_ok=True)

def extract_weather_data(lat=17.3850,lon=78.4867,days=1):
    url="https://api.open-meteo.com/v1/forecast"
    param={
        "latitude":lat,
        "longitude":lon,
        "hourly":"temperature_2m,relative_humidity_2m,wind_speed_10m",
        "forecast_days":days,
        "timezone":"auto",

    }
    resp=requests.get(url,params=param)
    resp.raise_for_status()
    data=resp.json()

    filename=Data_dir/f"weather_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json"
    filename.write_text(json.dumps(data,indent=2))

    print(f"Extracted weather data and saved to :{filename}")

    return data

if __name__=="__main__":
    extract_weather_data()
    