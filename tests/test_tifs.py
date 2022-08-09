import json
import os
from pathlib import Path
from pprint import pformat

import hyp3_sdk.util
import pytest
import rioxarray
import xarray as xr

from hyp3_testing import compare
from hyp3_testing import helpers
from hyp3_testing import util

pytestmark = pytest.mark.golden


def _get_tif_tolerances(file_name: str):
    """
    return the absolute and relative tolerances for the tif comparisons.
    Comparison will use `numpy.isclose` on the back end:
        https://numpy.org/doc/stable/reference/generated/numpy.isclose.html
    Comparison function:
         absolute(a - b) <= rtol * absolute(b) + atol

    returns: rtol, atol
    """
    rtol, atol = 0.0, 0.0

    # InSAR
    if file_name.endswith('amp.tif'):
        rtol, atol = 0.0, 1.5
    if file_name.endswith('corr.tif'):
        rtol, atol = 0.0, 1.0
    if file_name.endswith('vert_disp.tif'):
        rtol, atol = 0.0, 1.1
    if file_name.endswith('los_disp.tif'):
        rtol, atol = 0.0, 1.5e-01
    if file_name.endswith('unw_phase.tif'):
        rtol, atol = 0.0, 200.0

    # RTC
    backscatter_extensions = ['VV.tif', 'VH.tif', 'HH.tif', 'HV.tif']
    if any([file_name.endswith(ext) for ext in backscatter_extensions]):
        rtol, atol = 2e-05, 1e-05
    if file_name.endswith('area.tif'):
        rtol, atol = 2e-05, 0.0
    if file_name.endswith('rgb.tif'):
        rtol, atol = 0.0, 1.0

    return rtol, atol


@pytest.mark.nameskip
def test_golden_submission(comparison_environments, process):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    testing_parameters = util.render_template(f'{process}_gamma_golden.json.j2', name=job_name)
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


@pytest.mark.timeout(10800)  # 180 minutes as InSAR jobs can take ~2.5 hrs
@pytest.mark.dependency()
def test_golden_wait(comparison_environments, job_name):
    for dir_, api in comparison_environments:
        if job_name is None:
            submission_report = dir_ / f'{dir_.name}_submission.json'
            submission_details = json.loads(submission_report.read_text())
            job_name = submission_details['name']

        hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.find_jobs(name=job_name)
        _ = hyp3.watch(jobs)


def download_jobs(job_instance, directory):
    product_dir = directory / job_instance.to_dict()['files'][0]['filename'][:-4]
    if not product_dir.is_dir():
        product_archive = job_instance.download_files(directory)[0]
        product_dir = hyp3_sdk.util.extract_zipped_product(product_archive)
    hash_name = product_dir.name.split('_')[-1]
    files = sorted(product_dir.glob('*'))
    files_normalized = {f.name.replace(hash_name, 'HASH') for f in files}
    tif_paths = sorted(product_dir.glob('*.tif'))
    return product_dir, tif_paths, files_normalized


@pytest.fixture(scope='function')
def insar_tolerances(job_name):
    testing_parameters = util.render_template('insar_gamma_golden.json.j2', name=job_name)
    tolerance_names = ['_'.join(sorted(item['job_parameters']['granules'])) for item in testing_parameters]
    tolerance_parameters = [item['tolerance_parameters'] for item in testing_parameters]
    tolerance_dict = {k: v for k, v in zip(tolerance_names, tolerance_parameters)}
    return tolerance_dict


# @pytest.fixture(scope='function')
# def rtc_tolerances(job_name):
#     testing_parameters = util.render_template('insar_gamma_golden.json.j2', name=job_name)
#     tolerance_names = ['_'.join(sorted(item['job_parameters']['granules'])) for item in testing_parameters]
#     tolerance_parameters = [item['tolerance_parameters'] for item in testing_parameters]
#     tolerance_dict = {k: v for k, v in zip(tolerance_names, tolerance_parameters)}
#     return tolerance_dict


@pytest.fixture(scope='function')
def jobs_info(comparison_environments, insar_tolerances, job_name, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments
    if job_name is None:
        submission_report = main_dir / f'{main_dir.name}_submission.json'
        submission_details = json.loads(submission_report.read_text())
        job_name = submission_details['name']

    main_jobs = helpers.get_jobs_in_environment(job_name, main_api)
    main_n_succeed = main_jobs._count_statuses()['SUCCEEDED']

    develop_jobs = helpers.get_jobs_in_environment(job_name, develop_api)
    develop_n_succeed = develop_jobs._count_statuses()['SUCCEEDED']

    jobs_dict = {'status': {'main': {}, 'develop': {}}, 'pairs': {}}
    jobs_dict['status']['main']['n_succeed'] = main_n_succeed
    jobs_dict['status']['develop']['n_succeed'] = develop_n_succeed
    for main_job, develop_job in zip(main_jobs, develop_jobs):
        pair_name = '_'.join(sorted(main_job.to_dict()['job_parameters']['granules']))
        job_tolerances = insar_tolerances[pair_name]

        job_main_dir, main_tifs, main_normalized_files = download_jobs(main_job, main_dir)
        job_develop_dir, develop_tifs, develop_normalized_files = download_jobs(develop_job, develop_dir)
        jobs_dict['pairs'][pair_name] = {
            'main': {'tifs': main_tifs, 'normalized_files': main_normalized_files, 'job': main_job,
                     'dir': job_main_dir},
            'develop': {'tifs': develop_tifs,
                        'normalized_files': develop_normalized_files, 'job': develop_job, 'dir': job_develop_dir},
            'tolerances': job_tolerances}

    yield jobs_dict

    if not keep:
        all_files = [jobs_dict['pairs'][x][y]['tifs'] for y in ['main', 'develop'] for x in jobs_dict['pairs'].keys()]
        flat_files = [element for sublist in all_files for element in sublist]
        [Path(x).unlink() for x in flat_files]

        all_dirs = [jobs_dict['pairs'][x][y]['dir'] for y in ['main', 'develop'] for x in jobs_dict['pairs'].keys()]
        [Path(x).rmdir() for x in all_dirs]


# @pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_insar(jobs_info, keep):
    failure_count = 0
    messages = []

    # TODO: make own test?~~~~~~~#
    if jobs_info['status']['main']['n_succeed'] != jobs_info['status']['develop']['n_succeed']:
        failure_count += 1
        messages.append(f'Number of jobs that SUCCEEDED is different!\n'
                        f'    Main: {[jobs_info["pairs"][key]["main"]["job"] for key in jobs_info["pairs"].keys()]}'
                        f'    Develop: {[jobs_info["pairs"][key]["develop"]["job"] for key in jobs_info["pairs"].keys()]}')
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~#

    for pair in jobs_info['pairs']:
        pair_information = jobs_info['pairs'][pair]

        main_tifs = pair_information['main']['tifs']
        develop_tifs = pair_information['develop']['tifs']

        main_normalized_files = pair_information['main']['normalized_files']
        develop_normalized_files = pair_information['develop']['normalized_files']

        pair_tolerances = pair_information['tolerances']

        # TODO: make own test?~~~~~~~#
        if main_normalized_files != develop_normalized_files:
            failure_count += 1
            messages.append(f'File names are different!\n'
                            f'    Main:\n{pformat(main_normalized_files)}\n'
                            f'    develop:\n{pformat(develop_normalized_files)}\n')
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~#

        for main_tif, develop_tif in zip(main_tifs, develop_tifs):
            comparison_header = '\n'.join(['-' * 80, str(main_tif), str(develop_tif), '-' * 80])

            main_ds = xr.open_dataset(main_tif, engine='rasterio').band_data.data[0]
            develop_ds = xr.open_dataset(develop_tif, engine='rasterio').band_data.data[0]

            try:
                compare.compare_raster_info(main_tif, develop_tif)
                if pair_tolerances != {}:
                    file_type = '_'.join(Path(main_tif).name.split('_')[8:])[:-4]
                    file_tolerance = pair_tolerances[file_type]
                    threshold = file_tolerance['threshold']
                    n_allowable = file_tolerance['n_allowable']
                    compare.values_are_within_tolerance(main_ds, develop_ds, atol=threshold,
                                                        n_allowable=n_allowable)
            except compare.ComparisonFailure as e:
                messages.append(f'{comparison_header}\n{e}')
                failure_count += 1

    if messages:
        messages.insert(0, f'{failure_count} differences found!!')
        raise compare.ComparisonFailure('\n\n'.join(messages))


# @pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_rtc(comparison_environments, job_name, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments
    if job_name is None:
        submission_report = main_dir / f'{main_dir.name}_submission.json'
        submission_details = json.loads(submission_report.read_text())
        job_name = submission_details['name']

    failure_count = 0
    messages = []

    main_jobs = helpers.get_jobs_in_environment(job_name, main_api)
    develop_jobs = helpers.get_jobs_in_environment(job_name, develop_api)
    job_type = main_jobs[0].to_dict()['job_type'].lower()

    if main_jobs._count_statuses()['SUCCEEDED'] != develop_jobs._count_statuses()['SUCCEEDED']:
        failure_count += 1
        messages.append(f'Number of jobs that SUCCEEDED is different!\n'
                        f'    Main: {main_jobs}'
                        f'    Develop: {develop_jobs}')

    for main_job, develop_job in zip(main_jobs, develop_jobs):
        main_product_dir = main_dir / main_job.to_dict()['files'][0]['filename'][:-4]
        if not main_product_dir.is_dir():
            main_product_archive = main_job.download_files(main_dir)[0]
            main_product_dir = hyp3_sdk.util.extract_zipped_product(main_product_archive)
        main_hash = main_product_dir.name.split('_')[-1]

        develop_product_dir = develop_dir / develop_job.to_dict()['files'][0]['filename'][:-4]
        if not develop_product_dir.is_dir():
            develop_product_archive = develop_job.download_files(develop_dir)[0]
            develop_product_dir = hyp3_sdk.util.extract_zipped_product(develop_product_archive)
        develop_hash = develop_product_dir.name.split('_')[-1]

        main_files = sorted(main_product_dir.glob('*'))
        develop_files = sorted(develop_product_dir.glob('*'))

        main_files_normalized = {f.name.replace(main_hash, 'HASH') for f in main_files}
        develop_files_normalized = {f.name.replace(develop_hash, 'HASH') for f in develop_files}

        if main_files_normalized != develop_files_normalized:
            failure_count += 1
            messages.append(f'File names are different!\n'
                            f'    Main:\n{pformat(main_files_normalized)}\n'
                            f'    develop:\n{pformat(develop_files_normalized)}\n')

        main_tifs = sorted(main_product_dir.glob('*.tif'))
        develop_tifs = sorted(develop_product_dir.glob('*.tif'))

        for main_tif, develop_tif in zip(main_tifs, develop_tifs):
            comparison_header = '\n'.join(['-' * 80, str(main_tif), str(develop_tif), '-' * 80])

            main_ds = xr.open_dataset(main_tif, engine='rasterio').band_data.data[0]
            develop_ds = xr.open_dataset(develop_tif, engine='rasterio').band_data.data[0]

            try:
                compare.compare_raster_info(main_tif, develop_tif)
                relative_tolerance, absolute_tolerance = _get_tif_tolerances(str(main_tif))
                compare.values_are_close(main_ds, develop_ds, rtol=relative_tolerance, atol=absolute_tolerance)
            except compare.ComparisonFailure as e:
                messages.append(f'{comparison_header}\n{e}')
                failure_count += 1

        if not keep:
            for product_file in main_files + develop_files:
                Path(product_file).unlink()
            Path(main_product_dir).rmdir()
            Path(develop_product_dir).rmdir()

    if messages:
        messages.insert(0, f'{failure_count} differences found!!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
