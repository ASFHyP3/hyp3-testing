import os
from contextlib import contextmanager
from glob import glob
from pathlib import Path
from typing import List, Tuple, Optional
from zipfile import ZipFile

from hyp3_sdk import Batch, HyP3, Job
from hyp3_sdk.util import extract_zipped_product
from remotezip import RemoteZip


def freeze_job_parameters(job: Job) -> tuple:
    job_parameters = job.job_parameters
    return tuple((key, job_parameters[key]) for key in sorted(job_parameters.keys()))


def sort_jobs_by_parameters(jobs: Batch) -> Batch:
    sorted_jobs = sorted(jobs, key=freeze_job_parameters)
    return Batch(sorted_jobs)


def get_jobs_in_environment(job_name: str, api: str, user_id: Optional[str]) -> Batch:
    hyp3 = HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
    jobs = hyp3.find_jobs(name=job_name, user_id=user_id)
    return sort_jobs_by_parameters(jobs)


def extract_zip_files(zip_files: List[Path]):
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


def find_files_in_products(main_dir: Path, develop_dir: Path, pattern: str = '*.tif') -> List[Tuple[Path, Path]]:
    main_base_path = main_dir.parent
    main_hash = main_dir.name.split('_')[-1]

    develop_base_path = develop_dir.parent
    develop_hash = develop_dir.name.split('_')[-1]

    main_set = {
        Path(f.replace(main_hash, 'HASH')).relative_to(main_base_path) for f in glob(str(main_dir / pattern))
    }
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
