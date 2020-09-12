from pathlib import Path

import pytest

from hyp3_testing import helpers


def test_get_submission_payload():
    template = Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json.j2'

    sp1 = helpers.get_submission_payload(template)
    sp2 = helpers.get_submission_payload(template)

    assert sp1["jobs"][0]["name"] != sp2["jobs"][0]["name"]


def test_get_jobs_update():
    # TODO: This.
    pass


def test_jobs_succeeded():
    jobs_list = [
        {"status_code": "SUCCEEDED"},
        {"status_code": "RUNNING"},
        {"status_code": "PENDING"},
    ]
    assert helpers.jobs_succeeded(jobs_list) is False

    jobs_list = [
        {"status_code": "SUCCEEDED"},
        {"status_code": "SUCCEEDED"},
        {"status_code": "SUCCEEDED"},
    ]
    assert helpers.jobs_succeeded(jobs_list) is True


def test_jobs_succeeded_failed():
    jobs_list = [
        {"status_code": "SUCCEEDED"},
        {"status_code": "RUNNING"},
        {"status_code": "PENDING"},
        {"status_code": "FAILED"},
    ]
    with pytest.raises(Exception) as exc:
        helpers.jobs_succeeded(jobs_list)
        assert 'Job failed' in str(exc)


def test_get_download_urls():
    urls = ['https://1', 'https://2', 'https://3', 'https://2']
    jobs_list = [
        {"files": [{"url": urls[0]}]},
        {"files": [{"url": urls[1]}]},
        {"files": [{"url": urls[2]}]},
        {"files": [{"url": urls[3]}]},
    ]
    assert urls == helpers.get_download_urls(jobs_list)


def test_download_products():
    # TODO: This.
    pass


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
