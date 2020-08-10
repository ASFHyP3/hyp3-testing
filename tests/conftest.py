import json
import random
import string
from pathlib import Path
from zipfile import ZipFile

import pytest
import requests
from hyp3lib.fetch import download_file
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

    # TODO: don't take response -- take params instead
    @staticmethod
    def get_jobs_update(post_response, hyp3_session):
        pr = post_response.json()
        requests_time = pr['jobs'][0]['request_time']
        params = {
            'start': requests_time,
            'end': requests_time,
            'name': pr['jobs'][0]['name']
        }
        update = hyp3_session.get(post_response.url, params=params)
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
        file_blocks = [file for job in update.json()['jobs'] for file in job['files']]
        file_urls = {file['url'] for file in file_blocks}
        return file_urls

    @staticmethod
    def download_products(update, directory):
        urls = HelperFunctions.get_download_urls(update)
        products = {}
        for url in urls:
            zip_file = download_file(url, directory=str(directory))
            with ZipFile(zip_file) as zip_:
                zip_.extractall(path=directory)

            file_split = Path(zip_file).stem.split('_')
            file_base = '_'.join(file_split[:-1])
            products[file_base] = file_split[-1]

        return products


@pytest.fixture()
def helpers():
    return HelperFunctions


@pytest.fixture()
def hyp3_session():
    session = requests.Session()
    resp = session.get(_AUTH_URL)
    resp.raise_for_status()
    return session
