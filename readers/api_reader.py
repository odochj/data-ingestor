# import polars as pl
# import requests
# from secrets.secret import SecretType, Secret

# endpoint = SecretType.URL
# token = SecretType.TOKEN

#TODO: fix scaffold

# class APIReader:
#     def __init__(self, secrets: dict = None):
#         # self.api_key = secrets.get_required_keys("MY_API_KEY") if secrets else None

#     def read(self, url: str) -> list[pl.DataFrame]:
#         headers = {"Authorization": f"Bearer {self.api_key}"}
#         response = requests.get(url, headers=headers)
#         data = response.json()
#         return [pl.DataFrame(data)]
