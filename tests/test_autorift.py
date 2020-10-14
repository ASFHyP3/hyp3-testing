import json
from glob import glob
from pathlib import Path

import pytest
import xarray as xr

from hyp3_testing import API_TEST_URL, API_URL
from hyp3_testing import helpers
from hyp3_testing import compare

pytestmark = pytest.mark.golden
_API = {'main': API_URL, 'develop': API_TEST_URL}


@pytest.mark.nameskip
def test_golden_submission(comparison_dirs):
    hyp3_session = helpers.hyp3_session()

    submission_payload = helpers.get_submission_payload(
        Path(__file__).resolve().parent / 'data' / 'autorift_golden.json.j2')
    print(f'Job name: {submission_payload["jobs"][0]["name"]}')

    for dir_ in comparison_dirs:
        response = hyp3_session.post(url=_API[dir_.name], json=submission_payload)
        response.raise_for_status()
        print(f'{dir_.name} request time: {response.json()["jobs"][0]["request_time"]}')

        with open(dir_ / f'{dir_.name}_response.json', 'w') as f:
            json.dump(response.json(), f)


@pytest.mark.timeout(10800)  # 3 hours
@pytest.mark.dependency()
def test_golden_wait_and_download(comparison_dirs, job_name):
    hyp3_session = helpers.hyp3_session()
    for dir_ in comparison_dirs:
        products = helpers.find_products(dir_, pattern='*.nc')
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
    main_products = helpers.find_products(main_dir, pattern='*.nc')
    develop_products = helpers.find_products(develop_dir, pattern='*.nc')

    assert sorted(main_products) == sorted(develop_products)

    for product_base, main_hash in main_products.items():
        develop_hash = develop_products[product_base]
        main_files = {Path(f).name.replace(main_hash, 'HASH')
                      for f in glob(str(main_dir / '_'.join([product_base, main_hash]) / '*'))}
        develop_files = {Path(f).name.replace(develop_hash, 'HASH')
                         for f in glob(str(develop_dir / '_'.join([product_base, develop_hash]) / '*'))}

        assert main_files == develop_files


@pytest.mark.dependency(depends=['test_golden_wait_and_download'])
def test_golden_products(comparison_dirs):
    main_dir, develop_dir = comparison_dirs
    main_products = helpers.find_products(main_dir, pattern='*.nc')
    develop_products = helpers.find_products(develop_dir, pattern='*.nc')

    products = set(main_products.keys()) & set(develop_products.keys())

    failure_count = 0
    messages = []
    for product_base in products:
        main_hash = main_products[product_base]
        develop_hash = develop_products[product_base]

        main_file = main_dir / '_'.join([product_base, f'{main_hash}.nc'])
        develop_file = develop_dir / '_'.join([product_base, f'{develop_hash}.nc'])

        comparison_header = '\n'.join(
            ['-'*80, f'{product_base}_{{{main_hash},{develop_hash}}}', '-'*80]
        )
        try:
            compare.bit_for_bit(main_file, develop_file)
        except compare.ComparisonFailure as b4b_failure:
            main_ds = xr.load_dataset(main_file)
            develop_ds = xr.load_dataset(develop_file)
            try:
                xr.testing.assert_identical(main_ds, develop_ds)
            except AssertionError as identical_failure:
                xr_msg = helpers.clarify_xr_message(str(identical_failure))
                failure_count += 1
                messages.append(f'{comparison_header}\n{xr_msg}')

                try:
                    compare.values_are_close(main_ds, develop_ds)
                except compare.ComparisonFailure as value_failure:
                    messages.append(str(value_failure))

                try:
                    compare.compare_cf_spatial_reference(main_ds, develop_ds)
                except compare.ComparisonFailure as spatial_ref_failure:
                    messages.append(str(spatial_ref_failure))
                continue

            failure_count += 1
            messages.append(f'{comparison_header}\n{b4b_failure}')  # not b4b, but identical

    if messages:
        messages.insert(0, f'{failure_count} of {len(products)} products are different!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
