import json

from random import sample


def get_burst_granule(opera_granule_name: str) -> str:
    # TODO implement me
    return ''


def get_attribute_values(granule, attribute_name: str) -> list[str]:
    for attribute in granule['umm']['AdditionalAttributes']:
        if attribute['Name'] == attribute_name:
            return attribute['Values']
    raise ValueError(f'Attribute {attribute_name} not found for granule {granule["meta"]["native-id"]}')


with open('rtc_granules.json') as f:
    granules = json.load(f)


test_granules = []

# S1A
test_granules.extend(sample([g['meta']['native-id'] for g in granules if g['umm']['Platforms'][0]['ShortName'] == 'Sentinel-1A'], 10))
# S1B
# test_granules.extend([g['meta']['native-id'] for g in granules if g['umm']['Platforms'][0]['ShortName'] == 'Sentinel-1B'], 10))
# IW1
test_granules.extend(sample([g['meta']['native-id'] for g in granules if 'IW1' in get_attribute_values(g, 'SUBSWATH_NAME')], 10))
# IW2
test_granules.extend(sample([g['meta']['native-id'] for g in granules if 'IW2' in get_attribute_values(g, 'SUBSWATH_NAME')], 10))
# IW3
test_granules.extend(sample([g['meta']['native-id'] for g in granules if 'IW3' in get_attribute_values(g, 'SUBSWATH_NAME')], 10))
# ASCENDING
test_granules.extend(sample([g['meta']['native-id'] for g in granules if 'ASCENDING' in get_attribute_values(g, 'ASCENDING_DESCENDING')], 10))
# DESCENDING
test_granules.extend(sample([g['meta']['native-id'] for g in granules if 'DESCENDING' in get_attribute_values(g, 'ASCENDING_DESCENDING')], 10))
# HH
test_granules.extend(sample([g['meta']['native-id'] for g in granules if get_attribute_values(g, 'POLARIZATION') == ['HH']], 10))
# HH+HV
test_granules.extend(sample([g['meta']['native-id'] for g in granules if get_attribute_values(g, 'POLARIZATION') == ['HH', 'HV']], 10))
# VV
test_granules.extend(sample([g['meta']['native-id'] for g in granules if get_attribute_values(g, 'POLARIZATION') == ['VV']], 10))
# VV+VH
test_granules.extend(sample([g['meta']['native-id'] for g in granules if get_attribute_values(g, 'POLARIZATION') == ['VV', 'VH']], 10))

for granule in test_granules:
    print(granule)
