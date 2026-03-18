-- таблица для текущей погоды
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

-- таблица для прогноза
CREATE TABLE IF NOT EXISTS weather_forecast (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    forecast_date DATE,
    forecast_hour INTEGER,
    temperature_c FLOAT,
    condition_text VARCHAR(100),
    chance_of_rain INTEGER,
    forecast_updated TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);