import os

from setuptools import find_packages, setup

_HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(_HERE, 'README.md'), 'r') as f:
    long_desc = f.read()

setup(
    name='hyp3_testing',
    use_scm_version=True,
    description='HyP3 plugin for testing processing',
    long_description=long_desc,
    long_description_content_type='text/markdown',

    url='https://github.com/ASFHyP3/hyp3-testing',

    author='ASF APD/Tools Team',
    author_email='uaf-asf-apd@alaska.edu',

    license='BSD',
    include_package_data=True,

    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        ],

    python_requires='>=3.10',

    install_requires=[
        'hyp3_sdk>=2.1.1',
        'jinja2',
        'numpy',
        'netCDF4',  # provides xarray netCDF IO backend
        'rasterio',
        'xarray',
    ],

    extras_require={
        'develop': [
            'pytest',
            'pytest-cov',
            'pytest-dependency',
            'pytest-timeout',
        ]
    },

    packages=find_packages(),

    zip_safe=False,
)
