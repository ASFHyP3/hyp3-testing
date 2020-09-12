import json
import os
import random
import string
from glob import glob
from pathlib import Path
from typing import List, Tuple
from zipfile import ZipFile

import requests
from hyp3lib.fetch import download_file
from jinja2 import Template

from hyp3_testing import AUTH_URL


def hyp3_session(username: str = None, password: str = None) -> requests.Session:
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


def get_submission_payload(template: Path) -> dict:
    with open(template) as f:
        body = f.read()

    hash_ = ''.join(random.choices(string.ascii_letters + string.digits, k=7))
    jobs = Template(body).render(hash=hash_)

    return json.loads(jobs)


def get_jobs_update(name: str, url: str, session: requests.Session, request_time: str = None) -> dict:
    params = {'name': name}
    if request_time is not None:
        params.update({'start': request_time, 'end': request_time})

    update = session.get(url, params=params)
    update.raise_for_status()
    return update.json()


def jobs_succeeded(jobs: List[dict]) -> bool:
    status = {job['status_code'] for job in jobs}
    if 'FAILED' in status:
        raise Exception('Job failed')

    return {'SUCCEEDED'} == status


def get_download_urls(jobs: List[dict]) -> List[str]:
    file_blocks = [file for job in jobs for file in job.get('files', [])]
    file_urls = [file['url'] for file in file_blocks]
    return file_urls


def download_products(jobs: List[dict], directory: Path):
    urls = get_download_urls(jobs)
    for url in urls:
        zip_file = download_file(url, directory=str(directory))
        with ZipFile(zip_file) as zip_:
            zip_.extractall(path=directory)


def find_products(directory: Path, pattern: str = '*.zip') -> dict:
    products = {}
    for file in glob(str(directory / pattern)):
        file_split = Path(file).stem.split('_')
        file_base = '_'.join(file_split[:-1])
        products[file_base] = file_split[-1]
    return products


def find_files_in_products(main_dir: Path, develop_dir: Path, pattern: str = '*.tif') -> List[Tuple[Path, Path]]:
    main_base_path = main_dir.parent
    main_hash = main_dir.name.split('_')[-1]

    develop_base_path = develop_dir.parent
    develop_hash = develop_dir.name.split('_')[-1]

    main_set = {
        Path(f.replace(main_hash, 'HASH')).relative_to(main_base_path) for f in glob(str(main_dir / pattern))
    }
    develop_set = {
        Path(f.replace(develop_hash, 'HASH')).relative_to(develop_base_path) for f in glob(str(develop_dir / pattern))
    }

    comparison_set = main_set & develop_set

    comparison_files = [
        (main_base_path / str(f).replace('HASH', main_hash), develop_base_path / str(f).replace('HASH', develop_hash))
        for f in sorted(comparison_set)
    ]

    return comparison_files
