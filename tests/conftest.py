import json
import random
import string

import pytest
import requests
from jinja2 import Template

_AUTH_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code' \
            '&client_id=BO_n7nTIlMljdvU6kRRB3g' \
            '&redirect_uri=https://auth.asf.alaska.edu/login'


class HelperFunctions:
    @staticmethod
    def get_submission_payload(template):
        with open(template) as f:
            body = f.read()

        name = ''.join(random.choices(string.ascii_letters + string.digits, k=7))
        jobs = Template(body).render(name=name)

        return json.loads(jobs)

    @staticmethod
    def get_jobs_update(post_response, hyp3_session):
        pr = post_response.json()
        requests_time = pr['jobs'][0]['request_time']
        params = {
            'start': requests_time,
            'end': requests_time,
            'name': pr['jobs'][0]['name']
        }
        update = hyp3_session.get(post_response.url, json=params)
        update.raise_for_status()
        return update

    @staticmethod
    def jobs_succeeded(update):
        status = {job['status_code'] for job in update.json()['jobs']}
        if 'FAILED' in status:
            raise Exception('Job failed')

        return {'SUCCEEDED'}.issuperset(status)

    @staticmethod
    def get_download_urls(update):


        return urls

@pytest.fixture()
def helpers():
    return HelperFunctions

@pytest.fixture()
def hyp3_session():
    session = requests.Session()
    resp = session.get(_AUTH_URL)
    resp.raise_for_status()
    return session
