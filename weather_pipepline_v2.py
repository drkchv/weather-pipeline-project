import requests
import json
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from time import sleep

load_dotenv()
API_KEY = os.getenv('WEATHER_API_KEY')

CITIES = [
    "Saint Petersburg",
    "Moscow", 
    "London",
    "Tokyo",
    "New York"
]

DB_USER = "admin"
DB_PASSWORD = "admin123"
DB_HOST = "host.docker.internal"  
DB_PORT = "5432"
DB_NAME = "weather_db"


def get_weather_data(city, days=1):
    """Получает данные о погоде для города"""
    # current.json - текущая погода
    # forecast.json - прогноз 
    url = f"https://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={city}&days={days}&aqi=no"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json() # сырые данные
    else:
        print(f"Ошибка для {city}: {response.status_code}")
        return None

def save_current_weather(data, engine):
    """Сохраняет текущую погоду"""
    if not data:
        return
    
    city = data['location']['name']
    current = data['current']
    
    record = {
        'city': city,
        'temperature_c': current['temp_c'],
        'feels_like_c': current['feelslike_c'],
        'humidity': current['humidity'],
        'wind_kph': current['wind_kph'],
        'condition_text': current['condition']['text'],
        'last_updated': current['last_updated']
    }
    
    df = pd.DataFrame([record])
    df.to_sql('weather_raw', engine, if_exists='append', index=False)
    print(f"Текущая погода: {record['temperature_c']}°C, {record['condition_text']}")

def save_forecast(data, engine):
    """Сохраняет прогноз погоды"""
    if not data or 'forecast' not in data:
        return
    
    city = data['location']['name']
    forecast_days = data['forecast']['forecastday']
    
    records = []
    for day in forecast_days:
        forecast_date = day['date']
        # почасовой прогноз
        for hour in day['hour']:
            record = {
                'city': city,
                'forecast_date': forecast_date,
                'forecast_hour': int(hour['time'].split(' ')[1].split(':')[0]),
                'temperature_c': hour['temp_c'],
                'condition_text': hour['condition']['text'],
                'chance_of_rain': hour.get('chance_of_rain', 0),
                'forecast_updated': datetime.now()
            }
            records.append(record)
    
    if records:
        df = pd.DataFrame(records)
        df.to_sql('weather_forecast', engine, if_exists='append', index=False)
        print(f"Прогноз: {len(records)} записей на {forecast_days[0]['date']}")

def main():
    print("="*60)
    print("Запуск погодного пайплайна ")
    print(f"Городов: {len(CITIES)}")
    print("="*60)
    
    # подключение к БД
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    
    for city in CITIES:
        print(f"\nОбрабатываю {city}")
        
        # получаем данные (текущая погода + прогноз на 3 дня)
        data = get_weather_data(city, days=3)
        
        if data:
            save_current_weather(data, engine)
            save_forecast(data, engine)
            # небольшая задержка, чтобы не забанили за частые запросы
            sleep(1)
        else:
            print(f"Пропускаю {city}")
    
    print("\nПайплайн завершен!")

if __name__ == "__main__":
    main()