import json
import random

import requests
from shapely.geometry import Polygon, shape


session = requests.Session()


def get_corresponding_burst_granule_name(opera_granule: dict) -> str:
    start = opera_granule['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']
    end = opera_granule['umm']['TemporalExtent']['RangeDateTime']['EndingDateTime']
    burst_id = get_attribute_values(opera_granule, 'OPERA_BURST_ID')[0][1:]
    polarization = get_attribute_values(opera_granule, 'POLARIZATION')[0]
    params = {
        'short_name': 'SENTINEL-1_BURSTS',
        'temporal': f'{start},{end}',
        'attribute[]': [
            f'string,BURST_ID_FULL,{burst_id}',
            f'string,POLARIZATION,{polarization}',
        ],
    }
    response = session.get('https://cmr.earthdata.nasa.gov/search/granules.umm_json', params=params)
    response.raise_for_status()
    if not response.json()['hits'] == 1:
        raise ValueError(f'{response.json()["hits"]} burst results found for {opera_granule["meta"]["native-id"]}')
    return response.json()['items'][0]['meta']['native-id']


def get_attribute_values(granule, attribute_name: str) -> list[str]:
    for attribute in granule['umm']['AdditionalAttributes']:
        if attribute['Name'] == attribute_name:
            return attribute['Values']
    raise ValueError(f'Attribute {attribute_name} not found for granule {granule["meta"]["native-id"]}')


def choose_sample(candidates: list) -> None:
    random.seed(42)
    for granule in random.sample(candidates, 10):
        print(f'{granule["meta"]["native-id"]},{get_corresponding_burst_granule_name(granule)}')


def filter_extreme_terrain_sample(candidates: list) -> list:
    with open('extreme_terrain.geojson') as f:
        extreme_terrain = json.load(f)
    geometries = [shape(feature['geometry']) for feature in extreme_terrain['features']]
    filtered_candidates = []
    for granule in candidates:
        boundary = granule['umm']['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['GPolygons'][0]['Boundary']
        granule_geometry = Polygon([(pt['Longitude'], pt['Latitude']) for pt in boundary['Points']])
        if any(geom.intersects(granule_geometry) for geom in geometries):
            filtered_candidates.append(granule)
    return filtered_candidates


def over_antimeridian(granule: dict) -> bool:
    longitudes = [
        point['Longitude']
        for poly in granule['umm']['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['GPolygons']
        for point in poly['Boundary']['Points']
    ]
    return min(longitudes) < -160 and 160 < max(longitudes)


def over_prime_meridian(granule: dict) -> bool:
    longitudes = [
        point['Longitude']
        for poly in granule['umm']['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['GPolygons']
        for point in poly['Boundary']['Points']
    ]
    return min(longitudes) < 0 < max(longitudes)


with open('rtc_granules.json') as f:
    granules = json.load(f)


def main():
    print('S1A')
    choose_sample([g for g in granules if g['umm']['Platforms'][0]['ShortName'] == 'Sentinel-1A'])
    print('S1B')
    choose_sample([g for g in granules if g['umm']['Platforms'][0]['ShortName'] == 'Sentinel-1B'])
    print('IW1')
    choose_sample([g for g in granules if 'IW1' in get_attribute_values(g, 'SUBSWATH_NAME')])
    print('IW2')
    choose_sample([g for g in granules if 'IW2' in get_attribute_values(g, 'SUBSWATH_NAME')])
    print('IW3')
    choose_sample([g for g in granules if 'IW3' in get_attribute_values(g, 'SUBSWATH_NAME')])
    print('ASCENDING')
    choose_sample([g for g in granules if 'ASCENDING' in get_attribute_values(g, 'ASCENDING_DESCENDING')])
    print('DESCENDING')
    choose_sample([g for g in granules if 'DESCENDING' in get_attribute_values(g, 'ASCENDING_DESCENDING')])
    print('HH')
    choose_sample([g for g in granules if get_attribute_values(g, 'POLARIZATION') == ['HH']])
    print('HH+HV')
    choose_sample([g for g in granules if get_attribute_values(g, 'POLARIZATION') == ['HH', 'HV']])
    print('VV')
    choose_sample([g for g in granules if get_attribute_values(g, 'POLARIZATION') == ['VV']])
    print('VV+VH')
    choose_sample([g for g in granules if get_attribute_values(g, 'POLARIZATION') == ['VV', 'VH']])
    print('prime meridian')
    choose_sample([g for g in granules if over_prime_meridian(g)])
    print('antimeridian')
    choose_sample([g for g in granules if over_antimeridian(g)])
    print('extreme terrain')
    choose_sample(filter_extreme_terrain_sample(granules))


if __name__ == '__main__':
    main()
