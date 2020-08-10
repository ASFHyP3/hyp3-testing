import subprocess
import time
from glob import glob
from pathlib import Path

_API_URL = 'https://hyp3-api.asf.alaska.edu/jobs'
_API_TEST_URL = 'https://hyp3-test-api.asf.alaska.edu/jobs'

# TODO: submit test; doesn't run if --name argument passed
# TODO: wait_and_download test; looks for --name argument, or use main_response.json, develop_response.json if they exist?
#          will mean can't use URL from submit...


def test_golden(tmp_path, helpers, hyp3_session):
    submission_payload = helpers.get_submission_payload(Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json')

    main_response = hyp3_session.post(url=_API_URL, json=submission_payload)
    main_response.raise_for_status()

    develop_response = hyp3_session.post(url=_API_TEST_URL, json=submission_payload)
    develop_response.raise_for_status()

    ii = 0
    main_succeeded = develop_succeeded = False
    main_update = develop_update = None
    while (ii := ii + 1) < 90:  # golden 10m RTC jobs take ~1 hr
        if not main_succeeded:
            main_update = helpers.get_jobs_update(main_response, hyp3_session)
            main_succeeded = helpers.jobs_succeeded(main_update)
        if not develop_succeeded:
            develop_update = helpers.get_jobs_update(develop_response, hyp3_session)
            develop_succeeded = helpers.jobs_succeeded(develop_update)

        if main_succeeded and develop_succeeded:
            break
        time.sleep(60)

    main_dir = tmp_path / 'main'
    main_dir.mkdir()
    main_products = helpers.download_products(main_update, main_dir)

    develop_dir = tmp_path / 'develop'
    develop_dir.mkdir()
    develop_products = helpers.download_products(develop_update, develop_dir)

    # TODO: log asserts instead and continue with set unions to do most possible comparisons
    #   OR: all of the above as a session scoped fixture...
    # produced the same set of product base names for both
    assert sorted(main_products) == sorted(develop_products)

    for product_base, main_hash in main_products.items():
        develop_hash = develop_products[product_base]
        main_files = {Path(f).name.replace(main_hash, 'HASH')
                      for f in glob(str(main_dir / '_'.join([product_base, main_hash]) / '*'))}
        develop_files = {Path(f).name.replace(develop_hash, 'HASH')
                         for f in glob(str(develop_dir / '_'.join([product_base, develop_hash]) / '*'))}

        # produced the same set of files in both products
        assert main_files == develop_files

        for dm, df in zip(main_files, develop_files):
            # TODO: compare *kmz
            # TODO: compare *png
            # TODO: compare *shp *shx *prj *dbf
            # TODO: compare *xml
            # ~~TODO: compare log~~
            # TODO: compare README.txt
            if df.endswith('.tif'):
                subprocess.check_output(
                    f'gdalcompare.py {dm.replace("HASH", main_hash)} {df.replace("HASH", develop_hash)}'
                )
            else:
                print(f'Comparisons not implemented for this file type: {dm}')
