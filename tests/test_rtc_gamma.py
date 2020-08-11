import json
import subprocess
import time
from glob import glob
from pathlib import Path

import pytest

from hyp3_testing import API_URL, API_TEST_URL
from hyp3_testing import helpers

_API = {'main': API_URL, 'develop': API_TEST_URL}

# TODO: submit test; doesn't run if --name argument passed
# TODO: wait_and_download test
#          looks for --name argument,
#          or use main_response.json, develop_response.json if they exist?


@pytest.mark.dependency()
def test_golden_submission(comparison_dirs):
    hyp3_session = helpers.hyp3_session()

    submission_payload = helpers.get_submission_payload(
        Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json')
    print(f'Job name: {submission_payload["jobs"][0]["name"]}')

    for dir_ in comparison_dirs:
        response = hyp3_session.post(url=_API[dir_.name], json=submission_payload)
        response.raise_for_status()
        print(f'{dir_.name} request time: {response.json()["jobs"][0]["request_time"]}')

        with open(dir_, f'{dir_.name}_response.json', 'w') as f:
            json.dump(response.json(), f)



@pytest.mark.dependency(depends=['test_golden_submission'])
def test_golden_wait_and_download(comparison_dirs):
    main_dir, develop_dir = comparison_dirs
    hyp3_session = helpers.hyp3_session()

    assert (main_dir / 'main_response.json').exists()
    assert (develop_dir / 'develop_response.json').exists()

    with open(main_dir / 'main_response.json') as f:
        main_response = json.load(f)
    with open(develop_dir / 'develop_response.json') as f:
        develop_response = json.load(f)

    ii = 0
    main_succeeded = False
    develop_succeeded = False
    main_update = None
    develop_update = None
    while (ii := ii + 1) < 90:  # golden 10m RTC jobs take ~1 hr
        if not main_succeeded:
            main_update = helpers.get_jobs_update(
                main_response['jobs'][0]['name'], API_URL, hyp3_session,
                request_time=main_response['jobs'][0]['request_time']
            )
            main_succeeded = helpers.jobs_succeeded(main_update.json()['jobs'])
        if not develop_succeeded:
            develop_update = helpers.get_jobs_update(
                develop_response['jobs'][0]['name'], API_TEST_URL, hyp3_session,
                request_time=develop_response['jobs'][0]['request_time']
            )
            develop_succeeded = helpers.jobs_succeeded(develop_update.json()['jobs'])

        if main_succeeded and develop_succeeded:
            break
        time.sleep(60)

    helpers.download_products(main_update.json()['jobs'], main_dir)
    helpers.download_products(develop_update.json()['jobs'], develop_dir)


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
