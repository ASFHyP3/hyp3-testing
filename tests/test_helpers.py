from pathlib import Path

import pytest

from hyp3_testing import helpers


def test_get_submission_payload():
    template = Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json'

    sp1 = helpers.get_submission_payload(template)
    sp2 = helpers.get_submission_payload(template)

    assert sp1["jobs"][0]["name"] != sp2["jobs"][0]["name"]


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
