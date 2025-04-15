import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'environment.env'))

# Configuración
API_KEY = os.getenv('API_KEY')
BASE_URL = os.getenv('BASE_URL')
print(f"BASE_URL: {BASE_URL}")

def extract_data_API(location, start_date, end_date):
    url = f"{BASE_URL}/{location}/{start_date}/{end_date}"
    params = {
        'unitGroup': 'metric',  
        'include': 'days',      
        'key': API_KEY,
        'contentType': 'csv',
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Error: {response.status_code} in {location}")
        return None

def save_data(data, filename):
    if data:
        df = pd.read_csv(StringIO(data), encoding='utf-8')
        df.to_parquet(filename)
        print(f"Data saved in {filename}")

if __name__ == "__main__":

    #this_month = datetime.now().strftime('%Y-%m')
    year = 2024
    start_date = f'{year}-01-01'
    end_date = f'{year}-12-31'
    
    locations = {
        'Zurich, Switzerland': 'zurich_data.csv',
        'Andalucia, Spain': 'andalucia_data.csv',
        'Munich, Germany': 'munich_data.csv',
        'Amsterdam, Netherlands': 'amsterdam_data.csv',
    }

    #locations = {'Amsterdam, Netherlands': 'amsterdam_data.csv'}

    for location in locations.keys():
        csv_data = extract_data_API(location, start_date, end_date)
        if csv_data:
            country = location.split(",")[1].strip()
            save_data(csv_data, f'data/{country}/{country}_{start_date.split("-")[0]}.parquet')