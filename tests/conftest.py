import pytest


@pytest.fixture(scope='session')
def comparison_dirs(tmp_path_factory):
    main_dir = tmp_path_factory.mktemp('main', numbered=False)
    develop_dir = tmp_path_factory .mktemp('develop', numbered=False)
    return main_dir, develop_dir
