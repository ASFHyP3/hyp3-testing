import json
import os

import hyp3_sdk
import pytest
import requests
from osgeo import gdal

from hyp3_testing import util
from hyp3_testing.helpers import archive_tifs, job_tifs
from hyp3_testing.opera_compare import (
    compare_rtc_browse,
    compare_rtc_hdf5_files,
    compare_rtc_iso_xmls,
    compare_rtc_s1_products,
)


gdal.UseExceptions()
CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'
pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    testing_parameters = util.render_template('opera_rtc_s1_golden.json.j2', name=job_name)
    submission_payload = [{k: item[k] for k in ['name', 'job_parameters', 'job_type']} for item in testing_parameters]

    dir_, api = comparison_environments[1]
    dir_.mkdir(parents=True, exist_ok=True)

    hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
    jobs = hyp3.submit_prepared_jobs(submission_payload)
    request_time = jobs.jobs[0].request_time.isoformat(timespec='seconds')
    print(f'{dir_.name} request time: {request_time}')

    submission_details = {'name': job_name, 'request_time': request_time}
    submission_report = dir_ / f'{dir_.name}_submission.json'
    submission_report.write_text(json.dumps(submission_details))


@pytest.mark.timeout(3600)  # 60 minutes as RTC jobs timeout after 1 hour
@pytest.mark.dependency()
def test_golden_wait(comparison_environments, job_name, user_id):
    dir_, api = comparison_environments[1]

    if job_name is None:
        submission_report = dir_ / f'{dir_.name}_submission.json'
        submission_details = json.loads(submission_report.read_text())
        job_name = submission_details['name']

    hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
    jobs = hyp3.find_jobs(name=job_name, user_id=user_id)

    assert len(jobs) > 0  # will throw if job_name not associated with user_id

    _ = hyp3.watch(jobs)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_job_succeeds(develop_jobs_info):
    assert all(value['develop']['succeeded'] for value in develop_jobs_info.values())


def get_opera_rtc_s1_info(granule_name: str) -> tuple[str, list[str]]:
    granule_prefix = '_'.join(granule_name.split('_')[:5])
    params = (
        ('short_name', 'OPERA_L2_RTC-S1_V1'),
        ('granule_ur', f'{granule_prefix}*'),
        ('options[granule_ur][pattern]', 'true'),
    )
    response = requests.get(CMR_URL, params=params)
    response.raise_for_status()
    response_dict = response.json()
    assert response_dict['hits'] != 0, 'No matching OPERA_L2_RTC-S1_V1 granule found'
    assert response_dict['hits'] == 1, 'More than one matching OPERA_L2_RTC-S1_V1 granule found'
    item = response_dict['items'][0]
    data_links = [str(x['URL']) for x in item['umm']['RelatedUrls'] if x['Type'] == 'GET DATA']
    iso_xml_link = [
        str(x['URL'])
        for x in item['umm']['RelatedUrls']
        if x['Type'] == 'EXTENDED METADATA' and x['Format'] == 'XML' and 'S3' not in x['Description']
    ]
    assert len(iso_xml_link) == 1, 'More than one matching ISO XML link found'
    data_links.append(iso_xml_link[0])
    browse_link = [
        str(x['URL'])
        for x in item['umm']['RelatedUrls']
        if x['Type'] == 'GET RELATED VISUALIZATION'
        and x['Format'] == 'PNG'
        and 'S3' not in x['Description']
        and str(x['URL']).endswith('BROWSE.png')
    ]
    assert len(browse_link) == 1, 'More than one matching browse link found'
    data_links.append(browse_link[0])
    return str(item['meta']['native-id']), data_links


KNOWN_FAIL = [
    # New
    'OPERA_L2_RTC-S1_T078-165731-IW1_20230801T005941Z_20250213T195257Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T081-172601-IW2_20230801T061531Z_20250210T213622Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T083-176837-IW1_20230801T093014Z_20250210T220308Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T136-290821-IW3_20230501T005010Z_20250201T235523Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T136-290836-IW3_20230501T005052Z_20250201T235741Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T137-292489-IW2_20230501T020650Z_20250202T000414Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T137-292529-IW1_20230501T020840Z_20250202T000432Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T139-297118-IW1_20230501T053937Z_20250202T001552Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T145-309683-IW3_20230501T151717Z_20250202T005520Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T146-312626-IW2_20230501T173234Z_20250202T010411Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T149-318190-IW2_20230501T214821Z_20250202T011927Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T168-359459-IW1_20220201T052524Z_20241220T204930Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T001-000684-IW2_20220201T183205Z_20241220T221908Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T008-015738-IW1_20220801T060415Z_20250215T122841Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T008-015800-IW3_20220801T060708Z_20250215T122605Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T066-140036-IW2_20230201T051822Z_20250121T120419Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T066-140037-IW2_20230201T051825Z_20250121T120419Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T066-141655-IW3_20230201T063249Z_20250121T120840Z_S1A_30_v1.0',
    'OPERA_L2_RTC-S1_T066-141657-IW3_20230201T063254Z_20250121T120840Z_S1A_30_v1.0',
]


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_opera_rtc_s1(comparison_environments, develop_jobs_info, keep):
    (main_dir, _), (develop_dir, develop_api) = comparison_environments
    for job_info in develop_jobs_info.values():
        product_id, urls = get_opera_rtc_s1_info(job_info['develop']['dir'])
        # Can uncomment for debugging
        # if product_id not in KNOWN_FAIL:
        #     continue
        with (
            archive_tifs(product_id, urls, main_dir, keep) as main_tifs,
            job_tifs(job_info['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs,
        ):
            print(f'Comparing {product_id}...')
            main_file_dir = main_dir / product_id
            develop_file_dir = develop_dir / job_info['develop']['dir']

            main_h5 = list(main_file_dir.glob('*h5'))[0]
            develop_h5 = list(develop_file_dir.glob('*h5'))[0]
            compare_rtc_hdf5_files(main_h5, develop_h5)

            main_xml = list(main_file_dir.glob('*iso.xml'))[0]
            develop_xml = list(develop_file_dir.glob('*iso.xml'))[0]
            compare_rtc_iso_xmls(main_xml, develop_xml)

            main_browse = list(main_file_dir.glob('*BROWSE.png'))[0]
            develop_browse = list(develop_file_dir.glob('*BROWSE.png'))[0]
            compare_rtc_browse(main_browse, develop_browse)

            for main_tif, develop_tif in zip(main_tifs, develop_tifs):
                compare_rtc_s1_products(main_tif, develop_tif)
