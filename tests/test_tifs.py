import json
from glob import glob
from pathlib import Path

import pytest
import xarray as xr

from hyp3_testing import compare
from hyp3_testing import helpers
from hyp3_testing import util

pytestmark = pytest.mark.golden


def _get_tif_tolerances(file_name: str):
    """
    return the absolute and relative tolerances for the tif comparisons.
    Comparison will use `numpy.isclose` on the back end:
        https://numpy.org/doc/stable/reference/generated/numpy.isclose.html
    Comparison function:
         absolute(a - b) <= rtol * absolute(b) + atol

    returns: rtol, atol
    """
    rtol, atol = 0.0, 0.0

    # InSAR
    if file_name.endswith('amp.tif'):
        rtol, atol = 0.0, 1.5
    if file_name.endswith('corr.tif'):
        rtol, atol = 0.0, 1.0
    if file_name.endswith('vert_disp.tif'):
        rtol, atol = 0.0, 1.1
    if file_name.endswith('los_disp.tif'):
        rtol, atol = 0.0, 1.5e-01
    if file_name.endswith('unw_phase.tif'):
        rtol, atol = 0.0, 200.0

    # RTC
    backscatter_extensions = ['VV.tif', 'VH.tif', 'HH.tif', 'HV.tif']
    if any([file_name.endswith(ext) for ext in backscatter_extensions]):
        rtol, atol = 2e-05, 1e-05
    if file_name.endswith('area.tif'):
        rtol, atol = 2e-05, 0.0
    if file_name.endswith('rgb.tif'):
        rtol, atol = 0.0, 1.0

    return rtol, atol


@pytest.mark.nameskip
def test_golden_submission(comparison_environments, process):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    submission_payload = util.render_template(f'{process}_gamma_golden.json.j2', name=job_name)

    for dir_, hyp3 in comparison_environments:
        dir_.mkdir(parents=True, exist_ok=True)

        jobs = hyp3.submit_prepared_jobs(submission_payload)
        request_time = jobs.jobs[0].request_time.isoformat(timespec='seconds')
        print(f'{dir_.name} request time: {request_time}')

        submission_details = {'name': job_name, 'request_time': request_time}
        submission_report = dir_ / f'{dir_.name}_submission.json'
        submission_report.write_text(json.dumps(submission_details))


@pytest.mark.timeout(7200)  # 120 minutes as InSAR jobs can take ~1.5 hrs
@pytest.mark.dependency()
def test_golden_wait_and_download(comparison_environments, job_name):
    for dir_, hyp3 in comparison_environments:
        products = helpers.find_products(dir_, pattern='*.zip')
        if products:
            continue

        if job_name is None:
            submission_report = dir_ / f'{dir_.name}_submission.json'
            submission_details = json.loads(submission_report.read_text())
            job_name = submission_details['name']

        jobs = hyp3.find_jobs(name=job_name)
        jobs = hyp3.watch(jobs)
        products = jobs.download_files(dir_)
        helpers.extract_zip_files(products)


@pytest.mark.dependency(depends=['test_golden_wait_and_download'])
def test_golden_product_files(comparison_environments):
    (main_dir, _), (develop_dir, _) = comparison_environments
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
def test_golden_tifs(comparison_environments):
    (main_dir, _), (develop_dir, _) = comparison_environments
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
                relative_tolerance, absolute_tolerance = _get_tif_tolerances(str(main_file))
                compare.values_are_close(main_ds, develop_ds, rtol=relative_tolerance, atol=absolute_tolerance)
            except compare.ComparisonFailure as e:
                messages.append(f'{comparison_header}\n{e}')
                failure_count += 1

    if messages:
        messages.insert(0, f'{failure_count} of {total_count} GeoTIFFs are different!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
