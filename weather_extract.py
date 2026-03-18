import requests  # для http-запросов 
import json   
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# загружаем секреты из файла .env 
load_dotenv()

# получаем ключ и город из переменных окружения
API_KEY = os.getenv('WEATHER_API_KEY')
CITY = os.getenv('CITY')

# URL для запроса к WeatherAPI
url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"

print(f"Запрашиваю погоду для города: {CITY}")

# отправляем запрос к API
response = requests.get(url)

# проверяем, что запрос прошел успешно (код 200)
if response.status_code == 200:
    weather_data = response.json() # превращает json в словарь python
    
    # создаем имя файла с текущей датой и временем
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"raw_data/{CITY}_{current_time}.json"
    
    # сохраняем json в файл ('w' - запись, indent - отступы)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, indent=2, ensure_ascii=False)
    
    print(f"Данные сохранены в файл: {filename}")
    print(f"Температура: {weather_data['current']['temp_c']}°C")
    print(f"Влажность: {weather_data['current']['humidity']}%")
    
else:
    print(f"Ошибка при запросе к API. Код ошибки: {response.status_code}")
    print(f"Текст ошибки: {response.text}")