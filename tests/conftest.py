import shutil
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--process",  choices=["rtc", "insar"], help="Submit this processes payload"
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
def comparison_dirs(tmp_path_factory, golden_dirs):
    if golden_dirs is not None:
        comparison_dirs = []
        for dir_ in golden_dirs:
            path = Path(dir_)
            if not path.exists():
                path.mkdir()
            comparison_dirs.append(path)
        return comparison_dirs

    main_dir = tmp_path_factory.mktemp('main', numbered=False)
    develop_dir = tmp_path_factory .mktemp('develop', numbered=False)
    return main_dir, develop_dir


@pytest.fixture
def job_name(request):
    return request.config.getoption("--name")


@pytest.fixture
def process(request):
    return request.config.getoption("--process")


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
