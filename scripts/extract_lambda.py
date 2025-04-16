import boto3
import requests
import pandas as pd
from io import StringIO,BytesIO
from datetime import datetime

def get_API_key():
    ssm = boto3.client('ssm')
    response = ssm.get_parameter(Name='weatherAPI', WithDecryption=True)
    return response['Parameter']['Value']

def extract_data_API(api_key, base_url, location, start_date, end_date):
    url = f"{base_url}/{location}/{start_date}/{end_date}"
    params = {
        'unitGroup': 'metric',
        'include': 'days',
        'key': api_key,
        'contentType': 'csv',
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Error: {response.status_code} in {location}")
        return None

def lambda_handler(event,context):
    year = int(event.get('year', datetime.now().year))
    start_date = f'{year}-01-01'
    end_date = f'{year}-12-31'

    # Config
    api_key = get_API_key()
    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    s3 = boto3.client('s3')
    bucket_name = 'raw-weather-app'


    locations = [
        'Zurich, Switzerland',
        'Andalucia, Spain',
        'Munich, Germany',
        'Amsterdam, Netherlands'
    ]
    replacements = {
            '.*Schweiz.*': 'Switzerland',
            '.*Deutschland.*': 'Germany',
            '.*Nederland.*': 'Netherlands',
            '.*Espa.*': 'Spain'
        }

    for location in locations:
        csv_data = extract_data_API(api_key, base_url, location, start_date, end_date)
        if csv_data:
            df = pd.read_csv(StringIO(csv_data), encoding='utf-8')
            df['name'] = df['name'].replace(replacements, regex=True)
            df['year'] = df['datetime'].str.split('-').str[0]
            country = location.split(', ')[1]

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            key = f'raw/year={year}/country={country}/data.parquet'
            s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

            print(f"Datos de {country} subidos correctamente a {key}")

    return {
        'statusCode': 200,
        'body': 'Todos los datos subidos a S3 correctamente.'
    }