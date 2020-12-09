import json
from glob import glob
from pathlib import Path
from time import sleep

import pytest
import xarray as xr

from hyp3_testing import API_TEST_URL, API_URL
from hyp3_testing import compare
from hyp3_testing import helpers

pytestmark = pytest.mark.golden
_API = {'main': API_URL, 'develop': API_TEST_URL}


def _get_tif_tolerances(file_name):
    tif_type = file_name.name.split('_')[-1]
    if tif_type == 'area.tif':
        return 2e-05, 0.0
    if tif_type in ['VV.tif', 'VH.tif', 'HH.tif', 'HV.tif']:
        return 2e-05, 1e-05
    return 0.0, 0.0


@pytest.mark.nameskip
def test_golden_submission(comparison_dirs):
    hyp3_session = helpers.hyp3_session()

    submission_payload = helpers.get_submission_payload(
        Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json.j2')
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
            sleep(60)

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

    failure_count = 0
    total_count = 0
    messages = []
    for product_base in products:
        main_hash = main_products[product_base]
        develop_hash = develop_products[product_base]

        comparison_files = helpers.find_files_in_products(
            main_dir / '_'.join([product_base, main_hash]),
            develop_dir / '_'.join([product_base, develop_hash]),
            pattern='*.tif'
        )
        total_count += len(comparison_files)

        for main_file, develop_file in comparison_files:
            comparison_header = '\n'.join(['-'*80, main_file.name, develop_file.name, '-'*80])

            with xr.open_rasterio(main_file) as f:
                main_ds = f.load()
            with xr.open_rasterio(develop_file) as f:
                develop_ds = f.load()

            try:
                compare.compare_raster_info(main_file, develop_file)
                relative_tolerance, absolute_tolerance = _get_tif_tolerances(main_file)
                compare.values_are_close(main_ds, develop_ds, rtol=relative_tolerance, atol=absolute_tolerance)
            except compare.ComparisonFailure as e:
                messages.append(f'{comparison_header}\n{e}')
                failure_count += 1

    if messages:
        messages.insert(0, f'{failure_count} of {total_count} GeoTIFFs are different!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
