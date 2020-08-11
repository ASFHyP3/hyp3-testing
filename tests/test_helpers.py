from pathlib import Path

import pytest
import requests
import responses

from hyp3_testing import API_URL
from hyp3_testing import helpers


def test_get_submission_payload():
    template = Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json'

    sp1 = helpers.get_submission_payload(template)
    sp2 = helpers.get_submission_payload(template)

    assert sp1["jobs"][0]["name"] != sp2["jobs"][0]["name"]
    assert len(sp1["jobs"][0]["name"]) == 7
    assert len(sp2["jobs"][0]["name"]) == 7


@responses.activate
def test_jobs_succeeded():
    jsn = {"jobs": [
        {"status_code": "SUCCEEDED"},
        {"status_code": "RUNNING"},
        {"status_code": "PENDING"},
    ]}
    responses.add(responses.GET, API_URL,
                  json=jsn, status=200)

    jsn = {"jobs": [
        {"status_code": "SUCCEEDED"},
        {"status_code": "SUCCEEDED"},
        {"status_code": "SUCCEEDED"},
    ]}
    responses.add(responses.GET, API_URL,
                  json=jsn, status=200)

    update = requests.get(API_URL)
    assert helpers.jobs_succeeded(update) is False

    update = requests.get(API_URL)
    assert helpers.jobs_succeeded(update) is True


@responses.activate
def test_jobs_succeeded_failed():
    jsn = {"jobs": [
        {"status_code": "SUCCEEDED"},
        {"status_code": "RUNNING"},
        {"status_code": "PENDING"},
        {"status_code": "FAILED"},
    ]}

    responses.add(responses.GET, API_URL,
                  json=jsn, status=200)

    update = requests.get(API_URL)

    with pytest.raises(Exception) as exc:
        helpers.jobs_succeeded(update)

        assert 'Job failed' in str(exc)


@responses.activate
def test_get_download_urls():
    urls = ['https://1', 'https://2', 'https://3', 'https://2']
    jsn = {"jobs": [
        {"files": [{"url": urls[0]}]},
        {"files": [{"url": urls[1]}]},
        {"files": [{"url": urls[2]}]},
        {"files": [{"url": urls[3]}]},
    ]}

    responses.add(responses.GET, API_URL,
                  json=jsn, status=200)

    update = requests.get(API_URL)
    assert set(urls) == helpers.get_download_urls(update)
