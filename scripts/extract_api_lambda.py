import boto3
import urllib3 
from urllib.parse import urlencode
import pandas as pd
from io import StringIO,BytesIO
from datetime import datetime

ssm = boto3.client('ssm')

def get_API_key():
    response = ssm.get_parameter(Name='weatherAPI', WithDecryption=True)
    return response['Parameter']['Value']

def extract_data_API(api_key, base_url, location, start_date, end_date):
    http = urllib3.PoolManager()
    url = f"{base_url}/{location}/{start_date}/{end_date}"
    params = {
        'unitGroup': 'metric',
        'include': 'days',
        'key': api_key,
        'contentType': 'csv',
    }
    full_url = f"{url}?{urlencode(params)}"
    response = http.request('GET', full_url)
    
    if response.status == 200:
        return response.data.decode('utf-8')
    else:
        print(f"Error: {response.status} in {location}")
        return None

def get_updated_year():
    last_year_used = ssm.get_parameter(Name='weather_lambda_last_year_loaded')
    year_updated = int(last_year_used['Parameter']['Value'])-1
    return year_updated

def lambda_handler(event,context):
    #awswrangler
    year = get_updated_year()
    if not year:
        return {'statusCode': 200,'body': 'Proceso completado'}

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
            # forzamos las siguientes columnas a float
            double_cols = ['tempmax', 'tempmin', 'temp', 'feelslikemax', 'feelslikemin', 'feelslike', 'dew', 'humidity', 'precip', 'precipprob', 'precipcover', 'snow', 'snowdepth', 'windgust', 'windspeed', 'winddir', 'sealevelpressure', 'cloudcover', 'visibility', 'solarradiation', 'solarenergy', 'uvindex', 'severerisk']
            df[double_cols] = df[double_cols].astype(float)


            country = location.split(', ')[1]

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            key = f'raw/year={year}/country={country}/{year}_{country}.parquet'
            s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

            print(f"Datos de {country} subidos correctamente a {key}")
    
    #Se actualiza el parameto del SSM
    ssm.put_parameter(Name='weather_lambda_last_year_loaded', Value=str(year), Type='String', Overwrite=True)

    return {
        'statusCode': 200,
        'body': 'Todos los datos subidos a S3 correctamente.'
    }