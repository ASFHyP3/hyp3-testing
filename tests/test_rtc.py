import json
import os
from pathlib import Path

import hyp3_sdk.util
import pytest
import rioxarray  # noqa: F401
import xarray as xr

from hyp3_testing import compare
from hyp3_testing import util
from hyp3_testing.helpers import job_tifs

pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    testing_parameters = util.render_template('rtc_gamma_golden.json.j2', name=job_name)
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


@pytest.mark.timeout(5400)  # 90 minutes as RTC jobs can take ~1.5 hr
@pytest.mark.dependency()
def test_golden_wait(comparison_environments, job_name, user_id):
    for dir_, api in comparison_environments:
        if job_name is None:
            submission_report = dir_ / f'{dir_.name}_submission.json'
            submission_details = json.loads(submission_report.read_text())
            job_name = submission_details['name']

        hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.find_jobs(name=job_name, user_id=user_id)
        _ = hyp3.watch(jobs)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_job_succeeds(jobs_info):
    main_succeeds = sum([value['main']['succeeded'] for value in jobs_info.values()])
    develop_succeeds = sum([value['develop']['succeeded'] for value in jobs_info.values()])
    assert main_succeeds == develop_succeeds


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_tif_names(jobs_info):
    for pair_information in jobs_info.values():
        main_normalized_files = pair_information['main']['normalized_files']
        develop_normalized_files = pair_information['develop']['normalized_files']
        assert main_normalized_files == develop_normalized_files


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_rtc(comparison_environments, jobs_info, rtc_tolerances, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments

    failure_count = 0
    messages = []
    for pair, pair_information in jobs_info.items():
        pair_tolerances = rtc_tolerances[pair]

        with job_tifs(pair_information['main']['job_id'], main_api, main_dir, keep) as main_tifs, \
                job_tifs(pair_information['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs:

            for main_tif, develop_tif in zip(main_tifs, develop_tifs):
                file_type = '_'.join(Path(main_tif).name.split('_')[8:])[:-4]

                file_tolerance = pair_tolerances[file_type]
                absolute_tolerance, relative_tolerance = file_tolerance['atol'], file_tolerance['rtol']

                comparison_header = '\n'.join(['-' * 80, str(main_tif), str(develop_tif), '-' * 80])

                main_ds = xr.open_dataset(main_tif, engine='rasterio')
                develop_ds = xr.open_dataset(develop_tif, engine='rasterio')
                try:
                    compare.compare_raster_info(main_tif, develop_tif)
                    compare.values_are_close(main_ds, develop_ds, rtol=relative_tolerance, atol=absolute_tolerance)
                except compare.ComparisonFailure as e:
                    messages.append(f'{comparison_header}\n{e}')
                    failure_count += 1

    if messages:
        messages.insert(0, f'{failure_count} differences found!!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
