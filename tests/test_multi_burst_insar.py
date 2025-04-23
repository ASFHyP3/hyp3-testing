import pytest

import hyp3_testing.helpers as helpers
from hyp3_testing.isce2_compare import compare_isce2_derived_products


pytestmark = pytest.mark.golden


@pytest.mark.nameskip
def test_golden_submission(comparison_environments):
    helpers.golden_submission(comparison_environments, 'insar_isce_multi_burst_golden.json.j2')


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
def test_golden_multi_burst_insar(comparison_environments, jobs_info, keep):
    compare_isce2_derived_products(comparison_environments, jobs_info, keep)
