-- текущая температура во всех городах
SELECT DISTINCT ON (city)
    city,
    temperature_c,
    condition_text,
    last_updated
FROM weather_raw
ORDER BY city, last_updated DESC;

-- сравнение городов за последние 24 часа
SELECT 
    city,
    AVG(temperature_c) as avg_temp,
    MAX(temperature_c) as max_temp,
    MIN(temperature_c) as min_temp
FROM weather_raw
WHERE last_updated > NOW() - INTERVAL '24 hours'
GROUP BY city
ORDER BY avg_temp DESC;

-- прогноз на 3 дня
SELECT 
    city,
    forecast_date,
    AVG(temperature_c) as avg_temperature,
    MAX(chance_of_rain) as max_rain_chance
FROM weather_forecast
WHERE forecast_date >= CURRENT_DATE
GROUP BY city, forecast_date
ORDER BY city, forecast_date;