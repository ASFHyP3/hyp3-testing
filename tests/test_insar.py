import pytest
import rioxarray  # noqa: F401
import xarray as xr
from osgeo import gdal

from hyp3_testing import compare, helpers
from hyp3_testing.helpers import job_tifs


gdal.UseExceptions()
pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    helpers.golden_submission(comparison_environments, 'insar_gamma_golden.json.j2')


@pytest.mark.timeout(10800)  # 180 minutes as InSAR jobs can take ~2.5 hrs
@pytest.mark.dependency()
def test_golden_wait(comparison_environments, job_name, user_id):
    helpers.golden_wait(comparison_environments, job_name, user_id)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_job_succeeds(jobs_info):
    helpers.golden_job_succeeds(jobs_info)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_tif_names(jobs_info):
    helpers.golden_tif_names(jobs_info)


@pytest.mark.dependency(depends=['test_golden_wait'])
def test_golden_insar(comparison_environments, jobs_info, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments

    failure_count = 0
    messages = []
    for pair, pair_information in jobs_info.items():
        with (
            job_tifs(pair_information['main']['job_id'], main_api, main_dir, keep) as main_tifs,
            job_tifs(pair_information['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs,
        ):
            for main_tif, develop_tif in zip(main_tifs, develop_tifs):
                comparison_header = '\n'.join(['-' * 80, str(main_tif), str(develop_tif), '-' * 80])

                main_ds = xr.open_dataset(main_tif, engine='rasterio').band_data.data[0]
                develop_ds = xr.open_dataset(develop_tif, engine='rasterio').band_data.data[0]

                try:
                    compare.compare_raster_info(main_tif, develop_tif)

                    pixel_size = gdal.Info(str(main_tif), format='json')['geoTransform'][1]
                    compare.images_are_within_offset_threshold(
                        main_ds, develop_ds, pixel_size=pixel_size, offset_threshold=5.0
                    )

                    compare.maskes_are_within_similarity_threshold(main_ds, develop_ds, mask_rate=0.98)

                    compare.values_are_within_statistic(main_ds, develop_ds, confidence_level=0.99)

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
