import subprocess
import time
from glob import glob
from pathlib import Path

from hyp3_testing import API_URL, API_TEST_URL
from hyp3_testing import helpers


# TODO: submit test; doesn't run if --name argument passed
# TODO: wait_and_download test
#          looks for --name argument,
#          or use main_response.json, develop_response.json if they exist?


def test_golden(tmp_path):
    hyp3_session = helpers.hyp3_session()

    submission_payload = helpers.get_submission_payload(Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json')

    main_response = hyp3_session.post(url=API_URL, json=submission_payload)
    main_response.raise_for_status()

    develop_response = hyp3_session.post(url=API_TEST_URL, json=submission_payload)
    develop_response.raise_for_status()

    ii = 0
    main_succeeded = False
    develop_succeeded = False
    main_update = None
    develop_update = None
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
    assert sorted(main_products) == sorted(develop_products)

    for product_base, main_hash in main_products.items():
        develop_hash = develop_products[product_base]
        main_files = {Path(f).name.replace(main_hash, 'HASH')
                      for f in glob(str(main_dir / '_'.join([product_base, main_hash]) / '*'))}
        develop_files = {Path(f).name.replace(develop_hash, 'HASH')
                         for f in glob(str(develop_dir / '_'.join([product_base, develop_hash]) / '*'))}

        assert main_files == develop_files

        for file_ in main_files & develop_files:
            if file_.endswith('.tif'):
                main_file = main_dir / "_".join([product_base, main_hash]) / file_.replace("HASH", main_hash)
                develop_file = develop_dir / "_".join([product_base, develop_hash]) / file_.replace("HASH", develop_hash)

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

            else:
                print(f'Content comparisons not implemented for this file type: {file_}')
