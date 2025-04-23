import json
import os
from contextlib import contextmanager
from glob import glob
from pathlib import Path
from zipfile import ZipFile

from hyp3_sdk import Batch, HyP3, Job
from hyp3_sdk.util import extract_zipped_product
from remotezip import RemoteZip

from hyp3_testing import util


def freeze_job_parameters(job: Job) -> tuple:
    job_parameters = job.job_parameters
    return tuple((key, job_parameters[key]) for key in sorted(job_parameters.keys()))


def sort_jobs_by_parameters(jobs: Batch) -> Batch:
    sorted_jobs = sorted(jobs, key=freeze_job_parameters)
    return Batch(sorted_jobs)


def get_jobs_in_environment(job_name: str, api: str, user_id: str | None = None) -> Batch:
    hyp3 = HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
    jobs = hyp3.find_jobs(name=job_name, user_id=user_id)
    return sort_jobs_by_parameters(jobs)


def extract_zip_files(zip_files: list[Path]):
    for product_file in zip_files:
        with ZipFile(product_file) as zip_:
            zip_.extractall(path=product_file.parent)


def find_products(directory: Path, pattern: str = '*.zip') -> dict:
    products = {}
    for file in glob(str(directory / pattern)):
        file_split = Path(file).stem.split('_')
        file_base = '_'.join(file_split[:-1])
        products[file_base] = file_split[-1]
    return products


def find_files_in_products(main_dir: Path, develop_dir: Path, pattern: str = '*.tif') -> list[tuple[Path, Path]]:
    main_base_path = main_dir.parent
    main_hash = main_dir.name.split('_')[-1]

    develop_base_path = develop_dir.parent
    develop_hash = develop_dir.name.split('_')[-1]

    main_set = {Path(f.replace(main_hash, 'HASH')).relative_to(main_base_path) for f in glob(str(main_dir / pattern))}
    develop_set = {
        Path(f.replace(develop_hash, 'HASH')).relative_to(develop_base_path) for f in glob(str(develop_dir / pattern))
    }

    comparison_set = main_set & develop_set

    comparison_files = [
        (main_base_path / str(f).replace('HASH', main_hash), develop_base_path / str(f).replace('HASH', develop_hash))
        for f in sorted(comparison_set)
    ]

    return comparison_files


def clarify_xr_message(message: str, left: str = 'reference', right: str = 'secondary'):
    # Note: xarray refers to the left (L) and right (R) datasets, which we
    #       typically call reference (R) and secondary (S) datasets
    message = message.replace('Left', left.title())
    message = message.replace('left', left.lower())
    message = message.replace('Right', right.title())
    message = message.replace('right', right.lower())
    message = message.replace('\nR ', f'\n{right[0].upper()} ')
    message = message.replace('\nL ', f'\n\n{left[0].upper()} ')
    message = message.replace('\n\n\n', '\n\n')
    return message


def determine_product_files(job_instance):
    product_archive = job_instance.files[0]['url']

    with RemoteZip(product_archive) as z:
        files = z.infolist()

    product_name = files[0].filename.rstrip('/')

    hash_name = product_name.split('_')[-1]
    files_normalized = {f.filename.replace(hash_name, 'HASH') for f in files if not f.is_dir()}

    return product_name, files_normalized


@contextmanager
def job_tifs(job_id, api, directory, keep=False):
    hyp3 = HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
    job = hyp3.get_job_by_id(job_id)

    product_dir = directory / job.files[0]['filename'].replace('.zip', '')
    if not product_dir.exists():
        product_archive = job.download_files(directory)[0]
        product_dir = extract_zipped_product(product_archive)

    tif_paths = sorted(product_dir.glob('*.tif'))
    try:
        yield tif_paths
    finally:
        if not keep:
            for ff in product_dir.rglob('*'):
                ff.unlink()
            product_dir.rmdir()


def golden_submission(comparison_environments, job_template):
    job_name = util.generate_job_name()
    print(f'Job name: {job_name}')

    testing_parameters = util.render_template(job_template, name=job_name)
    submission_payload = [{k: item[k] for k in ['name', 'job_parameters', 'job_type']} for item in testing_parameters]

    for dir_, api in comparison_environments:
        dir_.mkdir(parents=True, exist_ok=True)

        hyp3 = HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.submit_prepared_jobs(submission_payload)
        request_time = jobs.jobs[0].request_time.isoformat(timespec='seconds')
        print(f'{dir_.name} request time: {request_time}')

        submission_details = {'name': job_name, 'request_time': request_time}
        submission_report = dir_ / f'{dir_.name}_submission.json'
        submission_report.write_text(json.dumps(submission_details))


def golden_wait(comparison_environments, job_name, user_id):
    for dir_, api in comparison_environments:
        if job_name is None:
            submission_report = dir_ / f'{dir_.name}_submission.json'
            submission_details = json.loads(submission_report.read_text())
            job_name = submission_details['name']

        hyp3 = HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
        jobs = hyp3.find_jobs(name=job_name, user_id=user_id)

        assert len(jobs) > 0  # will throw if job_name not associated with user_id

        _ = hyp3.watch(jobs)


def golden_job_succeeds(jobs_info):
    main_succeeds = sum([value['main']['succeeded'] for value in jobs_info.values()])
    develop_succeeds = sum([value['develop']['succeeded'] for value in jobs_info.values()])
    assert main_succeeds != 0
    assert develop_succeeds != 0
    assert main_succeeds == develop_succeeds


def golden_tif_names(jobs_info):
    for pair_information in jobs_info.values():
        main_normalized_files = pair_information['main']['normalized_files']
        develop_normalized_files = pair_information['develop']['normalized_files']
        assert main_normalized_files == develop_normalized_files
