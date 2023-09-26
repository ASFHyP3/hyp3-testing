import json
import os
from pathlib import Path
from pprint import pformat

import hyp3_sdk
import pytest
import xarray as xr

from hyp3_testing import compare
from hyp3_testing import helpers
from hyp3_testing import util

pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    submission_payload = util.render_template('autorift_golden.json.j2', name=job_name)

    for dir_, api in comparison_environments:
        dir_.mkdir(parents=True, exist_ok=True)

        hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.submit_prepared_jobs(submission_payload)
        request_time = jobs.jobs[0].request_time.isoformat(timespec='seconds')
        print(f'{dir_.name} request time: {request_time}')

        submission_details = {'name': job_name, 'request_time': request_time}
        submission_report = dir_ / f'{dir_.name}_submission.json'
        submission_report.write_text(json.dumps(submission_details))


@pytest.mark.timeout(10800)  # 3 hours
@pytest.mark.dependency()
def test_golden_wait(comparison_environments, job_name, user_id):
    for dir_, api in comparison_environments:
        products = helpers.find_products(dir_, pattern='*.nc')
        if products:
            continue

        if job_name is None:
            submission_report = dir_ / f'{dir_.name}_submission.json'
            submission_details = json.loads(submission_report.read_text())
            job_name = submission_details['name']

        hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.find_jobs(name=job_name, user_id=user_id)
        _ = hyp3.watch(jobs)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_products(comparison_environments, job_name, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments
    if job_name is None:
        submission_report = main_dir / f'{main_dir.name}_submission.json'
        submission_details = json.loads(submission_report.read_text())
        job_name = submission_details['name']

    failure_count = 0
    messages = []

    main_jobs = helpers.get_jobs_in_environment(job_name, main_api)
    develop_jobs = helpers.get_jobs_in_environment(job_name, develop_api)

    if main_jobs._count_statuses()['SUCCEEDED'] != develop_jobs._count_statuses()['SUCCEEDED']:
        failure_count += 1
        messages.append(f'Number of jobs that SUCCEEDED is different!\n'
                        f'    Main: {main_jobs}'
                        f'    Develop: {develop_jobs}')

    for main_job, develop_job in zip(main_jobs, develop_jobs):
        if main_job.failed() or develop_job.failed():
            continue

        main_nc = main_job.download_files(main_dir)[0]
        main_hash = main_nc.stem

        develop_nc = develop_job.download_files(develop_dir)[0]
        develop_hash = develop_nc.stem

        if main_hash != develop_hash:
            failure_count += 1
            messages.append(f'File names are different!\n'
                            f'    Main:\n{pformat(main_hash)}\n'
                            f'    develop:\n{pformat(develop_hash)}\n')

        comparison_header = '\n'.join(['-' * 80, str(main_nc), str(develop_nc), '-' * 80])

        try:
            compare.bit_for_bit(main_nc, develop_nc)
        except compare.ComparisonFailure as b4b_failure:
            main_ds = xr.load_dataset(main_nc)
            develop_ds = xr.load_dataset(develop_nc)
            try:
                xr.testing.assert_identical(main_ds, develop_ds)
            except AssertionError as identical_failure:
                xr_msg = helpers.clarify_xr_message(str(identical_failure))
                failure_count += 1
                messages.append(f'{comparison_header}\n{xr_msg}')

                try:
                    compare.values_are_close(main_ds, develop_ds)
                except compare.ComparisonFailure as value_failure:
                    messages.append(str(value_failure))

                try:
                    compare.compare_cf_spatial_reference(main_ds, develop_ds)
                except compare.ComparisonFailure as spatial_ref_failure:
                    messages.append(str(spatial_ref_failure))
                continue

            failure_count += 1
            messages.append(f'{comparison_header}\n{b4b_failure}')  # not b4b, but identical

        if not keep:
            for product_file in (main_nc, develop_nc):
                Path(product_file).unlink()

    if messages:
        messages.insert(0, f'{failure_count} differences found!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
