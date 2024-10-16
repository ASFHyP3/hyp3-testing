import json
import os

import hyp3_sdk.util
import pytest
import rioxarray  # noqa: F401
import xarray as xr
from osgeo import gdal

from hyp3_testing import compare
from hyp3_testing import util
from hyp3_testing.helpers import job_tifs

gdal.UseExceptions()
pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    testing_parameters = util.render_template('insar_isce_burst_golden.json.j2', name=job_name)
    submission_payload = [{k: item[k] for k in ['name', 'job_parameters', 'job_type']} for item in testing_parameters]

    for dir_, api in comparison_environments:
        dir_.mkdir(parents=True, exist_ok=True)

        hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.submit_prepared_jobs(submission_payload)
        request_time = jobs.jobs[0].request_time.isoformat(timespec='seconds')
        print(f'{dir_.name} request time: {request_time}')

        submission_details = {'name': job_name, 'request_time': request_time}
        submission_report = dir_ / f'{dir_.name}_submission.json'
        submission_report.write_text(json.dumps(submission_details))


@pytest.mark.timeout(10800)  # 180 minutes as InSAR jobs can take ~2.5 hrs
@pytest.mark.dependency()
def test_golden_wait(comparison_environments, job_name, user_id):
    for dir_, api in comparison_environments:
        if job_name is None:
            submission_report = dir_ / f'{dir_.name}_submission.json'
            submission_details = json.loads(submission_report.read_text())
            job_name = submission_details['name']

        hyp3 = hyp3_sdk.HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.find_jobs(name=job_name, user_id=user_id)

        assert len(jobs) > 0  # will throw if job_name not associated with user_id

        _ = hyp3.watch(jobs)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_job_succeeds(jobs_info):
    main_succeeds = sum([value['main']['succeeded'] for value in jobs_info.values()])
    develop_succeeds = sum([value['develop']['succeeded'] for value in jobs_info.values()])
    assert main_succeeds != 0
    assert develop_succeeds != 0
    assert main_succeeds == develop_succeeds


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_tif_names(jobs_info):
    for pair_information in jobs_info.values():
        main_normalized_files = pair_information['main']['normalized_files']
        develop_normalized_files = pair_information['develop']['normalized_files']
        assert main_normalized_files == develop_normalized_files


def _comparisons(main_ds, develop_ds, pixel_size):
    compare.images_are_within_offset_threshold(main_ds, develop_ds, pixel_size=pixel_size,
                                               offset_threshold=5.0)
    compare.maskes_are_within_similarity_threshold(main_ds, develop_ds, mask_rate=0.98)
    compare.values_are_within_statistic(main_ds, develop_ds, confidence_level=0.99)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_burst_insar(comparison_environments, jobs_info, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments

    failure_count = 0
    messages = []
    for pair, pair_information in jobs_info.items():
        with job_tifs(pair_information['main']['job_id'], main_api, main_dir, keep) as main_tifs, \
                job_tifs(pair_information['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs:

            main_file_dir = main_dir / (main_product_name := pair_information['main']['dir'])
            develop_file_dir = develop_dir / (develop_product_name := pair_information['develop']['dir'])

            compare.compare_product_files(main_file_dir, develop_file_dir)

            main_parameter_file = (main_file_dir / main_product_name).with_suffix('.txt')
            develop_parameter_file = (develop_file_dir / develop_product_name).with_suffix('.txt')

            compare.compare_parameter_files(str(main_parameter_file), str(develop_parameter_file))

            for main_tif, develop_tif in zip(main_tifs, develop_tifs):
                comparison_header = '\n'.join(['-' * 80, str(main_tif), str(develop_tif), '-' * 80])

                main_ds = xr.open_dataset(main_tif, engine='rasterio').band_data.data[0]
                develop_ds = xr.open_dataset(develop_tif, engine='rasterio').band_data.data[0]

                try:
                    compare.compare_raster_info(main_tif, develop_tif)

                    pixel_size = gdal.Info(str(main_tif), format='json')['geoTransform'][1]
                    # OpenCV does not support complex data, so we must compare each component as real values.
                    if main_ds.dtype in ('complex32', 'complex64'):
                        _comparisons(main_ds.real, develop_ds.real, pixel_size)
                        _comparisons(main_ds.imag, develop_ds.imag, pixel_size)
                    else:
                        _comparisons(main_ds, develop_ds, pixel_size)

                    if '_unw_phase.tif' in str(main_tif):
                        compare.nodata_count_change_are_within_threshold(main_ds, develop_ds, threshold=0.01)

                    if '_corr.tif' in str(main_tif):
                        compare.corr_average_decrease_within_threshold(main_ds, develop_ds, threshold=0.05)

                except compare.ComparisonFailure as e:
                    messages.append(f'{comparison_header}\n{e}')
                    failure_count += 1

    if messages:
        messages.insert(0, f'{failure_count} differences found!!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
