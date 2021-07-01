import shutil
from pathlib import Path

import hyp3_sdk
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--process", choices=["rtc", "insar"], help="Submit this processes payload"
    )
    parser.addoption(
        "--name", nargs='?', help="Find jobs by this name to compare"
    )
    parser.addoption(
        "--golden-dirs", nargs=2, help="Main and develop directories to use for comparison"
    )
    parser.addoption(
        "--delete-files", action='store_true', default=False
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--name"):
        name_skip = pytest.mark.skip(reason="Provided name; no need to submit")
        for item in items:
            if "nameskip" in item.keywords:
                item.add_marker(name_skip)


@pytest.fixture(scope='session')
def comparison_environments(tmp_path_factory, golden_dirs, delete_files):
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

    comparison_apis = [hyp3_sdk.HYP3_PROD, hyp3_sdk.HYP3_TEST]
    yield list(zip(comparison_dirs, comparison_apis))
    if delete_files:
        for _dir in comparison_dirs:
            shutil.rmtree(_dir.absolute())


@pytest.fixture
def job_name(request):
    return request.config.getoption("--name")


@pytest.fixture
def process(request):
    return request.config.getoption("--process")


@pytest.fixture(scope='session')
def golden_dirs(request):
    return request.config.getoption("--golden-dirs")


@pytest.fixture(scope='session')
def delete_files(request):
    return request.config.getoption("--delete-files")


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
