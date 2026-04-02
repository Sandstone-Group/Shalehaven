import requests
import os

BASE_URL = "https://insight.novilabs.com/api/"

username = os.environ.get("NOVI_USERNAME")
password = os.environ.get("NOVI_PASSWORD")

session = requests.Session()

# Authenticate and get token
response = session.post(BASE_URL + "v2/sessions", json={"email": username, "password": password})
response.raise_for_status()

token = response.json()["authentication_token"]
session.headers.update({"Authorization": f"Bearer {token}"})

print("Authenticated successfully")
