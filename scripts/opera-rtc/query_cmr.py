import datetime
import json

import requests


dates = [
    datetime.datetime(2022, 2, 1),
    datetime.datetime(2022, 8, 1),
    datetime.datetime(2023, 2, 1),
    datetime.datetime(2023, 8, 1),
    datetime.datetime(2024, 2, 1),
    datetime.datetime(2024, 8, 1),
]

url = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'

granules = []
for dt in dates:
    print(dt)
    start = dt.isoformat(timespec='seconds')
    end = (dt + datetime.timedelta(days=1)).isoformat(timespec='seconds')
    params = {
        'short_name': 'OPERA_L2_RTC-S1_V1',
        'temporal': f'{start},{end}',
        'page_size': '2000',
    }
    headers: dict = {}

    while True:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        granules.extend(response.json()['items'])
        print(len(granules))
        if 'CMR-Search-After' not in response.headers:
            break
        headers['CMR-Search-After'] = response.headers['CMR-Search-After']

with open('rtc_granules.json', 'w') as f:
    json.dump(granules, f, separators=(',', ':'))
