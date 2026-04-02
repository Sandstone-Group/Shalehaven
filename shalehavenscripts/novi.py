## This file contains functions for authenticating with the Novi API and retrieving data.
## Developed by Michael Tanner

import requests
import os


BASE_URL = "https://insight.novilabs.com/api/"

## Novi API Authentication and Data Retrieval Functions
def authNovi():
    username = os.environ.get("NOVI_USERNAME")
    password = os.environ.get("NOVI_PASSWORD")

    session = requests.Session()

    response = session.post(BASE_URL + "v2/sessions", json={"email": username, "password": password})
    response.raise_for_status()

    token = response.json()["authentication_token"]

    return token

def getWells(token):
   x= 5
   
   return x