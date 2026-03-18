import json
import pandas as pd
from sqlalchemy import create_engine, text # для подключения к бд и написания sql запросов
from datetime import datetime
import glob # для поиска файлов по шаблону
import os

# настройки подключения к PostgreSQL
DB_USER = "admin"
DB_PASSWORD = "admin123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "weather_db"

# Создаем строку подключения
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Функция для создания таблицы (если её нет)
def create_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_raw (
        id SERIAL PRIMARY KEY,
        city VARCHAR(100),
        temperature_c FLOAT,
        feels_like_c FLOAT,
        humidity INTEGER,
        wind_kph FLOAT,
        condition_text VARCHAR(100),
        last_updated TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        print("Таблица проверена/создана")

# функция для загрузки последнего json
def load_latest_weather():
    # находим все файлы в папке raw_data
    json_files = glob.glob("raw_data/*.json")
    
    if not json_files:
        print("Нет файлов для загрузки")
        return
    
    # берем самый последний файл (по дате в имени)
    latest_file = max(json_files, key=os.path.getctime)
    print(f"Загружаю файл: {latest_file}")
    
    # читаем JSON
    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # извлекаем нужные поля
    weather_record = {
        'city': data['location']['name'],
        'temperature_c': data['current']['temp_c'],
        'feels_like_c': data['current']['feelslike_c'],
        'humidity': data['current']['humidity'],
        'wind_kph': data['current']['wind_kph'],
        'condition_text': data['current']['condition']['text'],
        'last_updated': data['current']['last_updated']
    }
    
    # превращаем в dataFrame 
    df = pd.DataFrame([weather_record])
    
    # загружаем в PostgreSQL
    df.to_sql('weather_raw', engine, if_exists='append', index=False)
    
    print(f"Данные загружены в БД:")
    print(f"   Город: {weather_record['city']}")
    print(f"   Температура: {weather_record['temperature_c']}°C")
    print(f"   Ощущается как: {weather_record['feels_like_c']}°C")
    print(f"   Влажность: {weather_record['humidity']}%")
    print(f"   Ветер: {weather_record['wind_kph']} км/ч")
    print(f"   Погода: {weather_record['condition_text']}")

if __name__ == "__main__":
    print("Начинаем загрузку данных в PostgreSQL...")
    create_table()
    load_latest_weather()