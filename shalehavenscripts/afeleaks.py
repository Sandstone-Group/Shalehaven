### This file contains functions for authenticating with the AFE Leaks API and retrieving data.
### Developed by Michael Tanner

import requests
import os

BASE_URL = "https://api.afeleaks.com"
apiKey = os.environ.get("AFE_LEAKS_API_KEY")


    
    