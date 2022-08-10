import json
import shutil
from pathlib import Path

import hyp3_sdk
import pytest

from hyp3_testing import helpers
from hyp3_testing import util


def pytest_addoption(parser):
    parser.addoption(
        "--process", choices=["rtc", "insar"], help="Submit this processes payload"
    )
    parser.addoption(
        "--keep", action='store_true', help="Do not remove downloaded test products"
    )
    parser.addoption(
        "--name", nargs='?', help="Find jobs by this name to compare"
    )
    parser.addoption(
        "--golden-dirs", nargs=2, help="Main and develop directories to use for comparison"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--name"):
        name_skip = pytest.mark.skip(reason="Provided name; no need to submit")
        for item in items:
            if "nameskip" in item.keywords:
                item.add_marker(name_skip)


@pytest.fixture(scope='session')
def comparison_environments(tmp_path_factory, golden_dirs):
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

    comparison_apis = [hyp3_sdk.PROD_API, hyp3_sdk.TEST_API]
    return list(zip(comparison_dirs, comparison_apis))


@pytest.fixture
def process(request):
    return request.config.getoption("--process")


@pytest.fixture
def keep(request):
    return request.config.getoption("--keep")


@pytest.fixture
def job_name(request):
    return request.config.getoption("--name")


@pytest.fixture(scope='session')
def golden_dirs(request):
    return request.config.getoption("--golden-dirs")


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


@pytest.fixture(scope='function')
def insar_tolerances(job_name):
    testing_parameters = util.render_template('insar_gamma_golden.json.j2', name=job_name)
    tolerance_names = ['_'.join(sorted(item['job_parameters']['granules'])) for item in testing_parameters]
    tolerances = [item['tolerance_parameters'] for item in testing_parameters]
    tolerance_dict = {k: v for k, v in zip(tolerance_names, tolerances)}
    return tolerance_dict


@pytest.fixture(scope='function')
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


@pytest.fixture(scope='function')
def jobs_info(comparison_environments, job_name, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments
    if job_name is None:
        submission_report = main_dir / f'{main_dir.name}_submission.json'
        submission_details = json.loads(submission_report.read_text())
        job_name = submission_details['name']

    main_jobs = helpers.get_jobs_in_environment(job_name, main_api)
    develop_jobs = helpers.get_jobs_in_environment(job_name, develop_api)

    jobs_dict = {}
    for main_job, develop_job in zip(main_jobs, develop_jobs):
        pair_name = '_'.join(sorted(main_job.to_dict()['job_parameters']['granules']))
        main_succeed = main_job.to_dict()['status_code'] == 'SUCCEEDED'
        develop_succeed = develop_job.to_dict()['status_code'] == 'SUCCEEDED'

        job_main_dir, main_tifs, main_normalized_files = helpers.download_jobs(main_job, main_dir)
        job_develop_dir, develop_tifs, develop_normalized_files = helpers.download_jobs(develop_job, develop_dir)
        jobs_dict[pair_name] = {
            'main': {'tifs': main_tifs, 'normalized_files': main_normalized_files,
                     'dir': job_main_dir, 'succeeded': main_succeed},
            'develop': {'tifs': develop_tifs, 'normalized_files': develop_normalized_files,
                        'dir': job_develop_dir, 'succeeded': develop_succeed},
        }

    yield jobs_dict

    if not keep:
        all_files = [value[y]['tifs'] for y in ['main', 'develop'] for value in jobs_dict.values()]
        flat_files = [element for sublist in all_files for element in sublist]
        [Path(x).unlink() for x in flat_files]

        all_dirs = [value[y]['dir'] for y in ['main', 'develop'] for value in jobs_dict.values()]
        [Path(x).rmdir() for x in all_dirs]
