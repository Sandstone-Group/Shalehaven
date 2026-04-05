## Unit tests for Novi API functions
## Run with: python -m pytest tests/test_novi.py -v -s

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

import pytest
import shalehavenscripts.novi as novi


@pytest.fixture(scope="module")
def token():
    """Authenticate once for all tests in this module"""
    t = novi.authNovi()
    assert t is not None
    print(f"Authenticated with token: {t[:20]}...")
    return t


class TestAuth:
    def test_auth_returns_token(self, token):
        assert isinstance(token, str)
        assert len(token) > 0


class TestForecastWellMonths:
    """Tests to debug the forecast_well_months endpoint"""

    def test_no_filter_page_1(self, token):
        """Hit forecast_well_months with no filter, just page 1 to see what comes back"""
        import requests
        params = {
            "authentication_token": token,
            "scope": "us-horizontals",
            "page": 1,
        }
        response = requests.get(novi.BASE_URL + "v3/forecast_well_months.json", params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        print(f"\nReturned {len(data)} records")
        if data:
            print(f"Columns: {list(data[0].keys())}")
            print(f"First record: {data[0]}")
        assert data is not None

    def test_filter_by_api10(self, token):
        """Test filtering by API10 directly"""
        import requests
        params = {
            "authentication_token": token,
            "scope": "us-horizontals",
            "q[API10_eq]": "4210337560",
            "page": 1,
        }
        response = requests.get(novi.BASE_URL + "v3/forecast_well_months.json", params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        print(f"\nAPI10 filter returned {len(data)} records")
        if data:
            print(f"Columns: {list(data[0].keys())}")

    def test_filter_by_api10_with_join(self, token):
        """Test filtering by API10 with join_table=Wells"""
        import requests
        params = {
            "authentication_token": token,
            "scope": "us-horizontals",
            "q[API10_eq]": "4210337560",
            "join_table": "Wells",
            "page": 1,
        }
        response = requests.get(novi.BASE_URL + "v3/forecast_well_months.json", params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        print(f"\nAPI10 + join_table=Wells returned {len(data)} records")
        if data:
            print(f"Columns: {list(data[0].keys())}")

    def test_batch_api10_in_filter(self, token):
        """Test if forecast_well_months supports q[API10_in][] for batch queries"""
        import requests
        params = {
            "authentication_token": token,
            "scope": "us-horizontals",
            "q[API10_in][]": ["4210337560", "4246138025"],
            "page": 1,
        }
        response = requests.get(novi.BASE_URL + "v3/forecast_well_months.json", params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        print(f"\nBatch API10_in returned {len(data)} records")
        if data:
            unique_apis = set(r.get("API10") for r in data)
            print(f"Unique API10s returned: {unique_apis}")
        assert data is not None

    def test_batch_api10_in_yearly(self, token):
        """Test if forecast_well_years supports q[API10_in][] for batch queries"""
        import requests
        params = {
            "authentication_token": token,
            "scope": "us-horizontals",
            "q[API10_in][]": ["4210337560", "4246138025"],
        }
        response = requests.get(novi.BASE_URL + "v3/forecast_well_years.json", params=params, timeout=120)
        response.raise_for_status()
        data = response.json()

        print(f"\nBatch yearly returned {len(data)} records")
        if data:
            unique_apis = set(r.get("API10") for r in data)
            print(f"Unique API10s returned: {unique_apis}")
        assert len(data) > 0

    def test_compare_yearly_endpoint(self, token):
        """Confirm the yearly endpoint works for the same well as a baseline"""
        import requests
        params = {
            "authentication_token": token,
            "scope": "us-horizontals",
            "q[API10_eq]": "4210337560",
        }
        response = requests.get(novi.BASE_URL + "v3/forecast_well_years.json", params=params, timeout=120)
        response.raise_for_status()
        data = response.json()

        print(f"\nYearly endpoint returned {len(data)} records")
        if data:
            print(f"Columns: {list(data[0].keys())}")
        assert len(data) > 0
