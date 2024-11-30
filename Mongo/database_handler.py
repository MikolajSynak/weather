from pymongo import MongoClient
import pandas as pd


def setup_mongo_client():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["weather_data"]
    return client, db


def insert_forecast_data(db, forecast_dataframe, db_name):
    # Wstaw dane prognostyczne z DataFrame
    print("-" * 50)

    forecast_collection = db[db_name]

    # Wyczyszczenie kolekcji przed dodaniem nowych danych
    print(f"Usuwam stare dane z kolekcji '{db_name}' przed wstawieniem nowych.")
    forecast_collection.delete_many({})  # Usunięcie wszystkich dokumentów w kolekcji

    forecast_data = forecast_dataframe.to_dict("records")

    # Sprawdzenie liczby rekordów do dodania
    print(f"Baza danych: {db_name}")
    print(f"Liczba rekordów do dodania: {len(forecast_data)}")

    # Wstawienie nowych danych
    forecast_collection.insert_many(forecast_data)
    print(f"Dane prognostyczne zostały dodane do bazy o nazwie {db_name}.")


def insert_historical_data(db, historical_dataframe, db_name):
    # Wstaw dane historyczne z DataFrame
    print("-" * 50)

    historical_collection = db[db_name]

    # Wyczyszczenie kolekcji przed dodaniem nowych danych
    print(f"Usuwam stare dane z kolekcji '{db_name}' przed wstawieniem nowych.")
    historical_collection.delete_many({})  # Usunięcie wszystkich dokumentów w kolekcji

    historical_data = historical_dataframe.to_dict("records")

    # Wstawienie nowych danych
    print(f"Baza danych: {db_name}")
    print(f"Liczba rekordów do dodania: {len(historical_data)}")

    # Wstawienie nowych danych
    historical_collection.insert_many(historical_data)
    print("Dane historyczne zostały dodane do bazy.")


def fetch_data(collection_name):
    client, db = setup_mongo_client()

    # Pobierz dane z kolekcji
    collection = db[collection_name]
    data = list(collection.find({}))

    # Konwersja na DataFrame
    dataframe = pd.DataFrame(data)
    return dataframe


def display_databases():
    # Połącz się z MongoDB
    client = MongoClient("mongodb://localhost:27017/")

    # Wyświetl dostępne bazy danych
    databases = client.list_database_names()
    print("Dostępne bazy danych:", databases)
    return databases

