import json
import subprocess
from glob import glob
from pathlib import Path

import pytest

from hyp3_testing import API_TEST_URL, API_URL
from hyp3_testing import helpers

_API = {'main': API_URL, 'develop': API_TEST_URL}

# TODO: wait_and_download test
#          looks for --name argument,
#          or use main_response.json, develop_response.json if they exist?


@pytest.mark.nameskip
def test_golden_submission(comparison_dirs):
    hyp3_session = helpers.hyp3_session()

    submission_payload = helpers.get_submission_payload(
        Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json')
    print(f'Job name: {submission_payload["jobs"][0]["name"]}')

    for dir_ in comparison_dirs:
        response = hyp3_session.post(url=_API[dir_.name], json=submission_payload)
        response.raise_for_status()
        print(f'{dir_.name} request time: {response.json()["jobs"][0]["request_time"]}')

        with open(dir_ / f'{dir_.name}_response.json', 'w') as f:
            json.dump(response.json(), f)


@pytest.mark.timeout(5400)  # 90 minutes as golden 10m RTC jobs take ~1 hr
@pytest.mark.dependency()
def test_golden_wait_and_download(comparison_dirs, job_name):
    hyp3_session = helpers.hyp3_session()
    for dir_ in comparison_dirs:
        products = helpers.find_products(dir_, pattern='*.zip')
        if products:
            continue

        if job_name is None:
            with open(dir_ / f'{dir_.name}_response.json') as f:
                resp = json.load(f)
            job_name = resp['jobs'][0]['name']
            request_time = resp['jobs'][0]['request_time']
        else:
            request_time = None

        while True:
            update = helpers.get_jobs_update(job_name, _API[dir_.name], hyp3_session, request_time=request_time)
            if helpers.jobs_succeeded(update['jobs']):
                break

        helpers.download_products(update['jobs'], dir_)


@pytest.mark.dependency(depends=['test_golden_wait_and_download'])
def test_golden_product_files(comparison_dirs):
    main_dir, develop_dir = comparison_dirs
    main_products = helpers.find_products(main_dir, pattern='*.zip')
    develop_products = helpers.find_products(develop_dir, pattern='*.zip')

    assert sorted(main_products) == sorted(develop_products)

    for product_base, main_hash in main_products.items():
        develop_hash = develop_products[product_base]
        main_files = {Path(f).name.replace(main_hash, 'HASH')
                      for f in glob(str(main_dir / '_'.join([product_base, main_hash]) / '*'))}
        develop_files = {Path(f).name.replace(develop_hash, 'HASH')
                         for f in glob(str(develop_dir / '_'.join([product_base, develop_hash]) / '*'))}

        assert main_files == develop_files


@pytest.mark.dependency(depends=['test_golden_wait_and_download'])
def test_golden_tifs(comparison_dirs):
    main_dir, develop_dir = comparison_dirs
    main_products = helpers.find_products(main_dir, pattern='*.zip')
    develop_products = helpers.find_products(develop_dir, pattern='*.zip')

    products = set(main_products.keys()) & set(develop_products.keys())

    for product_base in products:
        main_hash = main_products[product_base]
        develop_hash = develop_products[product_base]

        comparison_files = helpers.find_files_in_products(
            main_dir / '_'.join([product_base, main_hash]),
            develop_dir / '_'.join([product_base, develop_hash]),
            pattern='*.tif'
        )

        for main_file, develop_file in comparison_files:
            ret = 0
            cmd = f'gdalcompare.py {main_file} {develop_file}'
            try:
                stdout = subprocess.check_output(cmd, shell=True, text=True)
            except subprocess.CalledProcessError as e:
                stdout = e.output
                ret = e.returncode
            print(f'{cmd}\n{stdout}')

            # ret == 0 --> bit-for-bit
            # ret == 1 --> only binary level differences
            # ret > 1 -->  "visible data" is not identical
            # See: https://gdal.org/programs/gdalcompare.html
            assert ret <= 1
