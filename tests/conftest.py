import json
import shutil
from pathlib import Path

import hyp3_sdk
import pytest

from hyp3_testing import helpers
from hyp3_testing import util


def pytest_addoption(parser):
    parser.addoption(
        "--keep", action='store_true', help="Do not remove downloaded test products"
    )
    parser.addoption(
        "--name", nargs='?', help="Find jobs with this name to compare"
    )
    parser.addoption(
        "--golden-dirs", nargs=2, help="Main and develop directories to use for comparison"
    )
    parser.addoption(
        "--user-id", nargs='?', help="Find jobs submitted by this user to compare"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--name"):
        name_skip = pytest.mark.skip(reason="Provided name; no need to submit")
        for item in items:
            if "nameskip" in item.keywords:
                item.add_marker(name_skip)


@pytest.fixture(scope='session')
def comparison_dirs(tmp_path_factory, golden_dirs):
    if golden_dirs is None:
        comparison_dirs = [
            tmp_path_factory.mktemp('main', numbered=False),
            tmp_path_factory.mktemp('develop', numbered=False)
        ]
    else:
        comparison_dirs = []
        for dir_ in golden_dirs:
            path = Path(dir_)
            path.mkdir(exist_ok=True, parents=True)
            comparison_dirs.append(path)

    return comparison_dirs


@pytest.fixture(scope='session')
def comparison_environments(comparison_dirs):
    comparison_apis = [hyp3_sdk.PROD_API, hyp3_sdk.TEST_API]
    return list(zip(comparison_dirs, comparison_apis))


@pytest.fixture(scope='session')
def its_live_environments(comparison_dirs):
    comparison_apis = ['https://hyp3-its-live.asf.alaska.edu', 'https://hyp3-its-live-test.asf.alaska.edu']
    return list(zip(comparison_dirs, comparison_apis))


@pytest.fixture(scope='module')
def keep(request):
    return request.config.getoption("--keep")


@pytest.fixture(scope='module')
def job_name(request):
    return request.config.getoption("--name")


@pytest.fixture(scope='session')
def golden_dirs(request):
    return request.config.getoption("--golden-dirs")


@pytest.fixture(scope='session')
def user_id(request):
    return request.config.getoption("--user-id")


@pytest.fixture
def comparison_netcdfs(tmp_path_factory, test_data_dir):
    tmp_dir = tmp_path_factory.mktemp('data')
    nc106 = tmp_dir / 'autorift_106.nc'
    nc107 = tmp_dir / 'autorift_107.nc'

    shutil.copy(test_data_dir / nc106.name, nc106)
    shutil.copy(test_data_dir / nc107.name, nc107)

    return nc106, nc107


@pytest.fixture
def test_data_dir():
    data_dir = Path(__file__).resolve().parent / 'data'
    return data_dir


@pytest.fixture(scope='module')
def rtc_tolerances(job_name):
    testing_parameters = util.render_template('rtc_gamma_golden.json.j2', name=job_name)
    tolerance_names = ['_'.join(sorted(item['job_parameters']['granules'])) for item in testing_parameters]
    backscatter_types = ['VV', 'VH', 'HH', 'HV']
    other_types = ['inc_map', 'ls_map', 'dem']

    backscatter_tolerances = {x: {'rtol': 2e-05, 'atol': 1e-05} for x in backscatter_types}
    other_tolerances = {x: {'rtol': 0.0, 'atol': 0.0} for x in other_types}
    specific_tolerances = {'area': {'rtol': 2e-05, 'atol': 0.0}, 'rgb': {'rtol': 0.0, 'atol': 1.0}}

    tolerances = {**backscatter_tolerances, **specific_tolerances, **other_tolerances}
    tolerance_dict = {k: tolerances for k in tolerance_names}
    return tolerance_dict


@pytest.fixture(scope='module')
def jobs_info(comparison_environments, job_name, user_id):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments
    if job_name is None:
        submission_report = main_dir / f'{main_dir.name}_submission.json'
        submission_details = json.loads(submission_report.read_text())
        job_name = submission_details['name']

    main_jobs = helpers.get_jobs_in_environment(job_name, main_api, user_id=user_id)
    develop_jobs = helpers.get_jobs_in_environment(job_name, develop_api, user_id=user_id)

    jobs_dict = {}
    for main_job, develop_job in zip(main_jobs, develop_jobs):
        pair_name = '_'.join(sorted(main_job.job_parameters['granules']))

        job_main_dir, main_normalized_files = helpers.determine_product_files(main_job)
        job_develop_dir, develop_normalized_files = helpers.determine_product_files(develop_job)

        jobs_dict[pair_name] = {
            'main': {
                'job_id': main_job.job_id, 'succeeded': main_job.succeeded(),
                'dir': job_main_dir, 'normalized_files': main_normalized_files,
            },
            'develop': {
                'job_id': develop_job.job_id, 'succeeded': develop_job.succeeded(),
                'dir': job_develop_dir, 'normalized_files': develop_normalized_files,
            },
        }

    return jobs_dict
