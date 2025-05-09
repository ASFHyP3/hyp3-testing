import rioxarray  # noqa: F401
import xarray as xr
from osgeo import gdal

from hyp3_testing import compare
from hyp3_testing.helpers import job_tifs


gdal.UseExceptions()


def _comparisons(main_ds, develop_ds, pixel_size):
    compare.images_are_within_offset_threshold(main_ds, develop_ds, pixel_size=pixel_size, offset_threshold=5.0)
    compare.maskes_are_within_similarity_threshold(main_ds, develop_ds, mask_rate=0.98)
    compare.values_are_within_statistic(main_ds, develop_ds, confidence_level=0.99)


def compare_isce2_derived_products(comparison_environments, jobs_info, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments

    failure_count = 0
    messages = []
    for pair, pair_information in jobs_info.items():
        with (
            job_tifs(pair_information['main']['job_id'], main_api, main_dir, keep) as main_tifs,
            job_tifs(pair_information['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs,
        ):
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
