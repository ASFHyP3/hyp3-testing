import json
import os
import random
import string
from glob import glob
from pathlib import Path
from zipfile import ZipFile

import requests
from hyp3lib.fetch import download_file
from jinja2 import Template

from hyp3_testing import AUTH_URL


def hyp3_session(username: str = None, password: str = None):
    username = username if username is not None else os.environ.get('EARTHDATA_LOGIN_USER')
    password = password if password is not None else os.environ.get('EARTHDATA_LOGIN_PASSWORD')
    if username is None and password is None:
        auth = None
    else:
        auth = (username, password)

    session = requests.Session()
    response = session.get(AUTH_URL, auth=auth)
    response.raise_for_status()

    return session


def get_submission_payload(template):
    with open(template) as f:
        body = f.read()

    name = ''.join(random.choices(string.ascii_letters + string.digits, k=7))
    jobs = Template(body).render(name=name)

    return json.loads(jobs)


def get_jobs_update(name, url, session, request_time=None):
    params = {'name': name}
    if request_time is not None:
        params.update({'start': request_time, 'end': request_time})

    update = session.get(url, params=params)
    update.raise_for_status()
    return update.json()


def jobs_succeeded(jobs):
    status = {job['status_code'] for job in jobs}
    if 'FAILED' in status:
        raise Exception('Job failed')

    return {'SUCCEEDED'} == status


def get_download_urls(jobs):
    file_blocks = [file for job in jobs for file in job.get('files', [])]
    file_urls = [file['url'] for file in file_blocks]
    return file_urls


def download_products(jobs, directory):
    urls = get_download_urls(jobs)
    for url in urls:
        zip_file = download_file(url, directory=str(directory))
        with ZipFile(zip_file) as zip_:
            zip_.extractall(path=directory)


def find_products(directory: Path, pattern: str = '*.zip'):
    products = {}
    for file in glob(str(directory / pattern)):
        file_split = Path(file).stem.split('_')
        file_base = '_'.join(file_split[:-1])
        products[file_base] = file_split[-1]
    return products


def find_files_in_products(main_dir: Path, develop_dir: Path, pattern: str = '*.tif'):
    main_hash = main_dir.name.split('_')[-1]
    develop_hash = main_dir.name.split('_')[-1]

    main_set = {f.replace(main_hash, 'HASH') for f in glob(str(str(main_dir / pattern)))}
    develop_set = {f.replace(develop_hash, 'HASH') for f in glob(str(str(develop_dir / pattern)))}

    comparison_set = main_set & develop_set

    comparison_files = [(f.replace('HASH', main_hash), f.replace('HASH', develop_hash)) for f in comparison_set]

    return comparison_files
