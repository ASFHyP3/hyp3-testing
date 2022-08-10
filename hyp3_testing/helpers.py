import os
from glob import glob
from pathlib import Path
from typing import List, Tuple
from zipfile import ZipFile

from hyp3_sdk import Batch, HyP3, Job


def freeze_job_parameters(job: Job) -> tuple:
    job_parameters = job.job_parameters
    return tuple((key, job_parameters[key]) for key in sorted(job_parameters.keys()))


def sort_jobs_by_parameters(jobs: Batch) -> Batch:
    sorted_jobs = sorted(jobs, key=freeze_job_parameters)
    return Batch(sorted_jobs)


def get_jobs_in_environment(job_name, api):
    hyp3 = HyP3(api, os.environ.get('EARTHDATA_LOGIN_USER'), os.environ.get('EARTHDATA_LOGIN_PASSWORD'))
    jobs = hyp3.find_jobs(name=job_name)
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


def download_jobs(job_instance, directory):
    product_dir = directory / job_instance.to_dict()['files'][0]['filename'][:-4]
    if not product_dir.is_dir():
        product_archive = job_instance.download_files(directory)[0]
        product_dir = hyp3_sdk.util.extract_zipped_product(product_archive)
    hash_name = product_dir.name.split('_')[-1]
    files = sorted(product_dir.glob('*'))
    files_normalized = {f.name.replace(hash_name, 'HASH') for f in files}
    tif_paths = sorted(product_dir.glob('*.tif'))
    return product_dir, tif_paths, files_normalized

