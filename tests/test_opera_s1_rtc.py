import json
import os

import hyp3_sdk
import pytest
import requests
from osgeo import gdal

from hyp3_testing import util
from hyp3_testing.helpers import archive_tifs, job_tifs
from hyp3_testing.opera_compare import compare_rtc_hdf5_files, compare_rtc_s1_products


gdal.UseExceptions()
CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'
pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    comparison_environments = [comparison_environments[1]]
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    testing_parameters = util.render_template('opera_s1_rtc_golden.json.j2', name=job_name)
    submission_payload = [{k: item[k] for k in ['name', 'job_parameters', 'job_type']} for item in testing_parameters]

    for dir_, api in comparison_environments:
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
    comparison_environments = [comparison_environments[1]]
    for dir_, api in comparison_environments:
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
    develop_succeeds = sum([value['develop']['succeeded'] for value in develop_jobs_info.values()])
    assert develop_succeeds != 0


def get_opera_s1_rtc_info(granule_name: str) -> list[str]:
    granule_prefix = '_'.join(granule_name.split('_')[:5])
    params = (
        ('short_name', 'OPERA_L2_RTC-S1_V1'),
        ('granule_ur', f'{granule_prefix}*'),
        ('options[granule_ur][pattern]', 'true'),
    )
    response = requests.get(CMR_URL, params=params)
    response.raise_for_status()
    response_dict = response.json()
    assert response_dict['hits'] == 1, 'More than one matching OPERA-S1-RTC granule found'
    item = response_dict['items'][0]
    data_links = [x['URL'] for x in item['umm']['RelatedUrls'] if x['Type'] == 'GET DATA']
    return item['meta']['native-id'], data_links


def test_opera_s1_rtc(comparison_environments, develop_jobs_info, keep):
    (main_dir, _), (develop_dir, develop_api) = comparison_environments
    for job_info in develop_jobs_info.values():
        product_id, urls = get_opera_s1_rtc_info(job_info['develop']['dir'])
        with (
            archive_tifs(product_id, urls, main_dir, keep) as main_tifs,
            job_tifs(job_info['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs,
        ):
            main_file_dir = main_dir / product_id
            develop_file_dir = develop_dir / job_info['develop']['dir']

            main_h5 = list(main_file_dir.glob('*h5'))[0]
            develop_h5 = list(develop_file_dir.glob('*h5'))[0]
            compare_rtc_hdf5_files(main_h5, develop_h5)
            for main_tif, develop_tif in zip(main_tifs, develop_tifs):
                compare_rtc_s1_products(main_tif, develop_tif)
