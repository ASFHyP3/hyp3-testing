import pytest

from hyp3_testing import helpers


def test_find_products(tmp_path):
    product_zips = [
        tmp_path / 'p1_h1.zip',
        tmp_path / 'p2_h2.zip',
        tmp_path / 'p3_h3.zip',
    ]
    for z in product_zips:
        z.touch()

    found_products = helpers.find_products(tmp_path)
    assert found_products == {
        'p1': 'h1',
        'p2': 'h2',
        'p3': 'h3',
    }


def test_find_files_in_products(tmp_path):
    main_dir = tmp_path / 'main' / 'product_MAIN'
    main_dir.mkdir(parents=True)

    develop_dir = tmp_path / 'develop' / 'product_DEV'
    develop_dir.mkdir(parents=True)

    product_tifs = ['a.tif', 'b.tif', 'c.tif']
    for f in product_tifs:
        (main_dir / f).touch()
        (develop_dir / f).touch()

    found_files = helpers.find_files_in_products(main_dir, develop_dir)
    assert found_files == [
        (main_dir / 'a.tif', develop_dir / 'a.tif'),
        (main_dir / 'b.tif', develop_dir / 'b.tif'),
        (main_dir / 'c.tif', develop_dir / 'c.tif'),
    ]


def test_golden_job_succeeds():
    jobs_info = {'pair1': {'main': {'succeeded': 1}, 'develop': {'succeeded': 1}}}
    helpers.golden_job_succeeds(jobs_info)

    jobs_info = {'pair1': {'main': {'succeeded': 0}, 'develop': {'succeeded': 1}}}
    with pytest.raises(AssertionError, match='Main jobs did not succeed'):
        helpers.golden_job_succeeds(jobs_info)

    jobs_info = {'pair1': {'main': {'succeeded': 1}, 'develop': {'succeeded': 0}}}
    with pytest.raises(AssertionError, match='Develop jobs did not succeed'):
        helpers.golden_job_succeeds(jobs_info)

    jobs_info = {'pair1': {'main': {'succeeded': 1}, 'develop': {'succeeded': 2}}}
    with pytest.raises(AssertionError, match='Main and develop job success counts do not match'):
        helpers.golden_job_succeeds(jobs_info)


def test_golden_tif_names():
    jobs_info = {
        'pair1': {
            'main': {'normalized_files': ['file1', 'file2']},
            'develop': {'normalized_files': ['file1', 'file2']},
        },
        'pair2': {
            'main': {'normalized_files': ['file3', 'file4']},
            'develop': {'normalized_files': ['file3', 'file4']},
        },
    }
    helpers.golden_tif_names(jobs_info)

    jobs_info['pair2']['develop']['normalized_files'] = ['file3', 'file5']
    with pytest.raises(AssertionError):
        helpers.golden_tif_names(jobs_info)
