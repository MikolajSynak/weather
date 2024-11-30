from Mongo.database_handler import *
from data.data_getter import *

if __name__ == "__main__":
    client, db = setup_mongo_client()
    # db = setup_database()

    # historical_data = get_historical_weather_data(start_date="20023-11-27", city=Gdańsk)
    # forecast = get_hourly_forecast(city=Borkowo, forecast_days=7)

    try:

        forecast = get_hourly_forecast(city=Borkowo, forecast_days=7)
        insert_forecast_data(db, forecast, "prognoza_borkowo")
        dane_z_bazy = fetch_data('prognoza_borkowo')

        historical = get_historical_weather_data(start_date='2000-01-01', city=WZ)
        insert_historical_data(db, historical, 'dane_historyczne_WZ')
        dane_historyczne_z_bazy = fetch_data('dane_historyczne_WZ')

        # Wyświetl dostępne bazy danyc
        databases = display_databases()

    finally:
        client.close()  # Jawne zamknięcie połączenia (inaczej debugger się wykracza)

print(';123')


# TODO ogarnij ponowne wstawianie danych do bazy
# Sprawdzanie, czy baza na pewno jest pusta przed dodaniem danych

# TODO ogarnij odpalanie aplikacji z terminala
# TODO ogarnij funckje do wyświetlania dostępnych baz
