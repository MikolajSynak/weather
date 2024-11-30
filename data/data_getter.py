import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from settings.cities import *
from datetime import date, timedelta


def get_hourly_forecast(city=Gdańsk, forecast_days=7):
	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	latitude = city[0]
	longitude = city[1]
	if 16 < forecast_days < 1:
		raise ValueError("Forecast days should be between 1 and 16.")
	params = {
		"latitude": latitude,
		"longitude": longitude,
		"hourly": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "pressure_msl", "surface_pressure", "wind_speed_10m", "wind_speed_80m", "wind_direction_10m", "wind_direction_80m"],
		"timezone": "auto",
		"forecast_days": forecast_days
	}

	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
	retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
	openmeteo = openmeteo_requests.Client(session=retry_session)
	url = "https://api.open-meteo.com/v1/forecast"

	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation {response.Elevation()} m asl")
	print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
	print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")



	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
	hourly_apparent_temperature = hourly.Variables(2).ValuesAsNumpy()
	hourly_precipitation_probability = hourly.Variables(3).ValuesAsNumpy()
	hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
	hourly_rain = hourly.Variables(5).ValuesAsNumpy()
	hourly_pressure_msl = hourly.Variables(6).ValuesAsNumpy()
	hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(8).ValuesAsNumpy()
	hourly_wind_speed_80m = hourly.Variables(9).ValuesAsNumpy()
	hourly_wind_direction_10m = hourly.Variables(10).ValuesAsNumpy()
	hourly_wind_direction_80m = hourly.Variables(11).ValuesAsNumpy()

	hourly_data = {"date": pd.date_range(
		start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
		end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
		freq=pd.Timedelta(seconds=hourly.Interval()),
		inclusive="left"
	), "temperature_2m": hourly_temperature_2m, "relative_humidity_2m": hourly_relative_humidity_2m,
		"apparent_temperature": hourly_apparent_temperature,
		"precipitation_probability": hourly_precipitation_probability, "precipitation": hourly_precipitation,
		"rain": hourly_rain, "pressure_msl": hourly_pressure_msl, "surface_pressure": hourly_surface_pressure,
		"wind_speed_10m": hourly_wind_speed_10m, "wind_speed_80m": hourly_wind_speed_80m,
		"wind_direction_10m": hourly_wind_direction_10m, "wind_direction_80m": hourly_wind_direction_80m}

	hourly_dataframe = pd.DataFrame(data = hourly_data)

	return hourly_dataframe


def get_historical_weather_data(start_date="2022-01-01", end_date=date.today() - timedelta(days=1), city=Gdańsk):
	if city:
		latitude = city[0]
		longitude = city[1]
	start_date, end_date = str(start_date), str(end_date)

	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
	retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
	openmeteo = openmeteo_requests.Client(session=retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": latitude,
		"longitude": longitude,
		"start_date": start_date,
		"end_date": end_date,
		"hourly": "temperature_2m",
		"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
				  "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean", "sunrise",
				  "sunset", "daylight_duration", "sunshine_duration", "precipitation_sum", "rain_sum", "snowfall_sum",
				  "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
				  "shortwave_radiation_sum", "et0_fao_evapotranspiration"]
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation {response.Elevation()} m asl")
	print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
	print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

	hourly_data = {"date": pd.date_range(
		start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
		end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
		freq=pd.Timedelta(seconds=hourly.Interval()),
		inclusive="left"
	), "temperature_2m": hourly_temperature_2m}

	hourly_dataframe = pd.DataFrame(data=hourly_data)
	print(hourly_dataframe)

	# Process daily data. The order of variables needs to be the same as requested.
	daily = response.Daily()
	daily_weather_code = daily.Variables(0).ValuesAsNumpy()
	daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
	daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
	daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
	daily_apparent_temperature_max = daily.Variables(4).ValuesAsNumpy()
	daily_apparent_temperature_min = daily.Variables(5).ValuesAsNumpy()
	daily_apparent_temperature_mean = daily.Variables(6).ValuesAsNumpy()
	daily_sunrise = daily.Variables(7).ValuesAsNumpy()
	daily_sunset = daily.Variables(8).ValuesAsNumpy()
	daily_daylight_duration = daily.Variables(9).ValuesAsNumpy()
	daily_sunshine_duration = daily.Variables(10).ValuesAsNumpy()
	daily_precipitation_sum = daily.Variables(11).ValuesAsNumpy()
	daily_rain_sum = daily.Variables(12).ValuesAsNumpy()
	daily_snowfall_sum = daily.Variables(13).ValuesAsNumpy()
	daily_precipitation_hours = daily.Variables(14).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(15).ValuesAsNumpy()
	daily_wind_gusts_10m_max = daily.Variables(16).ValuesAsNumpy()
	daily_wind_direction_10m_dominant = daily.Variables(17).ValuesAsNumpy()
	daily_shortwave_radiation_sum = daily.Variables(18).ValuesAsNumpy()
	daily_et0_fao_evapotranspiration = daily.Variables(19).ValuesAsNumpy()

	daily_data = {"date": pd.date_range(
		start=pd.to_datetime(daily.Time(), unit="s", utc=True),
		end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
		freq=pd.Timedelta(seconds=daily.Interval()),
		inclusive="left"
	), "weather_code": daily_weather_code, "temperature_2m_max": daily_temperature_2m_max,
		"temperature_2m_min": daily_temperature_2m_min, "temperature_2m_mean": daily_temperature_2m_mean,
		"apparent_temperature_max": daily_apparent_temperature_max,
		"apparent_temperature_min": daily_apparent_temperature_min,
		"apparent_temperature_mean": daily_apparent_temperature_mean, "sunrise": daily_sunrise, "sunset": daily_sunset,
		"daylight_duration": daily_daylight_duration, "sunshine_duration": daily_sunshine_duration,
		"precipitation_sum": daily_precipitation_sum, "rain_sum": daily_rain_sum, "snowfall_sum": daily_snowfall_sum,
		"precipitation_hours": daily_precipitation_hours, "wind_speed_10m_max": daily_wind_speed_10m_max,
		"wind_gusts_10m_max": daily_wind_gusts_10m_max,
		"wind_direction_10m_dominant": daily_wind_direction_10m_dominant,
		"shortwave_radiation_sum": daily_shortwave_radiation_sum,
		"et0_fao_evapotranspiration": daily_et0_fao_evapotranspiration}

	daily_dataframe = pd.DataFrame(data=daily_data)
	return daily_dataframe


# forecast = get_hourly_forecast(city=Borkowo)
# historical = get_historical_weather_data(start_date="1940-01-01", city=Borkowo)

