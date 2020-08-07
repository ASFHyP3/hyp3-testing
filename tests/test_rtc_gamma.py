import subprocess
import time
from glob import glob
from pathlib import Path
from zipfile import ZipFile

from hyp3lib.fetch import download_file


def test_golden(tmp_path, helpers, hyp3_session):
    submission_payload = helpers.get_submission_payload(Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json')

    main_response = hyp3_session.post(url='https://hyp3-api.asf.alaska.edu/jobs', json=submission_payload)
    main_response.raise_for_status()

    develop_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=submission_payload)
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
    main_products = set()
    for url in helpers.get_download_urls(main_update):
        zip_file = download_file(url, directory=str(main_dir))
        with ZipFile(zip_file) as zip_:
            zip_.extractall(path=main_dir)
        main_products.add(Path(zip_file).stem)

    develop_dir = tmp_path / 'develop'
    develop_dir.mkdir()
    develop_products = set()
    for url in helpers.get_download_urls(develop_update):
        zip_file = download_file(url, directory=str(develop_dir))
        with ZipFile(zip_file) as zip_:
            zip_.extractall(path=develop_dir)
        develop_products.add(Path(zip_file).stem)

    # TODO: log asserts instead and continue with set unions to do most possible comparisons
    # produced the same set of products names for both
    assert main_products == develop_products

    for product in develop_products:
        main_files = {Path(f).name for f in glob((main_dir / product / '*'))}
        develop_files = {Path(f).name for f in glob((develop_dir / product / '*'))}

        # produced the same set of files in both products
        assert main_files == develop_files

        for file in develop_files:
            # TODO: compare *kmz
            # TODO: compare *png
            # TODO: compare *shp *shx *prj *dbf
            # TODO: compare *xml
            # TODO: compare log
            # TODO: compare README.txt
            if file.endswith('.tif'):
                # TODO subprocess gdal comapre
                pass
            else:
                print(f'Comparisons not implemented for this file type: {file}')