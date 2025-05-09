from pathlib import Path

import pytest
import rioxarray  # noqa: F401
import xarray as xr

from hyp3_testing import compare, helpers
from hyp3_testing.helpers import job_tifs


pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    helpers.golden_submission(comparison_environments, 'rtc_gamma_golden.json.j2')


@pytest.mark.timeout(5400)  # 90 minutes as RTC jobs can take ~1.5 hr
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
def test_golden_rtc(comparison_environments, jobs_info, rtc_tolerances, keep):
    (main_dir, main_api), (develop_dir, develop_api) = comparison_environments

    failure_count = 0
    messages = []
    for pair, pair_information in jobs_info.items():
        pair_tolerances = rtc_tolerances[pair]

        with (
            job_tifs(pair_information['main']['job_id'], main_api, main_dir, keep) as main_tifs,
            job_tifs(pair_information['develop']['job_id'], develop_api, develop_dir, keep) as develop_tifs,
        ):
            for main_tif, develop_tif in zip(main_tifs, develop_tifs):
                file_type = '_'.join(Path(main_tif).name.split('_')[8:])[:-4]

                file_tolerance = pair_tolerances[file_type]
                absolute_tolerance, relative_tolerance = file_tolerance['atol'], file_tolerance['rtol']

                comparison_header = '\n'.join(['-' * 80, str(main_tif), str(develop_tif), '-' * 80])

                main_ds = xr.open_dataset(main_tif, engine='rasterio')
                develop_ds = xr.open_dataset(develop_tif, engine='rasterio')
                try:
                    compare.compare_raster_info(main_tif, develop_tif)
                    compare.values_are_close(main_ds, develop_ds, rtol=relative_tolerance, atol=absolute_tolerance)
                except compare.ComparisonFailure as e:
                    messages.append(f'{comparison_header}\n{e}')
                    failure_count += 1

    if messages:
        messages.insert(0, f'{failure_count} differences found!!')
        raise compare.ComparisonFailure('\n\n'.join(messages))
