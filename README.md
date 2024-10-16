# HyP3 Testing

A package for automated system testing of HyP3 processes

## Available system tests

System test post a set of jobs to both HyP3v2 production 
(`https://hyp3-api.asf.alaska.edu/jobs`) and test (`https://hyp3-test-api.asf.alaska.edu/jobs`)
and compares them. It uses production HyP3v2 as the "golden" (aka baseline, reference) set for the comparison.
All system test will check:
* that the same products were produced with the same product names
* for multi-file products, that the same set of product files were produced inside each product


### Golden TIF comparison

The Golden TIF comparison is used for HyP3 processes that produce GeoTIFFs in a zip-ed product directory.
It takes a process name (e.g., `rtc`, `insar`) and posts a set jobs covering the range of available user options.

Currently, it additionally checks:
* The raster information is the same as provided by `gdal.Info`
* The raster data is "close" -- within a specified tolerance

### Golden autoRIFT comparison

The Golden autoRIFT comparison posts a set of four jobs covering both Greenland and Antarctica.

Currently, it additionally checks for each product:
* that the netCDF file is identical (data and attributes)
* that each data variable is close
* that there is a spatial variable following [CF Conventions](https://cfconventions.org/)
  with WKT defining the reference system

## Quickstart -- Using the manual GitHub actions

Navigate to the [Actions](https://github.com/ASFHyP3/hyp3-testing/actions) panel on GitHub

![Actions menu item](docs/imgs/actions-tab.png?raw=true)

Select the workflow for any of the available system tests. For example, the "Golden TIFs" test

![Golden RTC workflow](docs/imgs/golden-tif-workflow.png?raw=true)

Open the "Run workflow" dropdown, provide any required inputs, and click run workflow

![Golden RTC workflow](docs/imgs/golden-tif-run.png?raw=true)

You will see a new workflow start in that workflows list, and you can watch its progress by selecting it.
For Golden TIFs, it will take ~0.5 hours for an RTC comparison, and ~1.5 hours for an InSAR comparison.
Tests that pass will be marked with a green check, and tests that fail will be marked with a red x.

When viewing details of the test, look at "Pytest in conda environment" step in the "golden" job

![Golden RTC workflow](docs/imgs/golden-tif-details.png?raw=true)

## Local testing and development

To do local testing and development, clone this repository to your system and navigate to the repository

```
git clone https://github.com/ASFHyP3/hyp3-testing.git
cd hyp3-testing
```

### Setup a test environment

A HyP3 Testing environment can be setup via 
[Anaconda/Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/download.html#) 
using the provided `environment.yml`:

```
conda env create -f environment.yml
```

which will create a `hyp3-testing` conda environment. Once created, you can activate it like:

```
conda activate hyp3-testing
```

Then, install the development version of the `hyp3_testing` package into the environment

`python -m pip install -e .[develop]`

### Running the unit tests

To ensure your environment is setup correctly, run the unit tests for `hyp3_testing`

```
pytest -m "not golden"
```

*Note: system tests are marked "golden" and are extremely long running. This command skips the system tests*


### manually running the system tests

Because the system tests are particularly lon running, it is preferred to not run them all at once.
you can run the individual test you want by pointing directly to the test file. For example,
run the Golden RTC test like:

```
pytest tests/test_rtc.py
```

You also can, if you really want to, run all the golden tests like

```
pytest -m golden
```

#### Customizing the system tests

Sometimes you might not to run through the entire workflow, or re-use and old submission. We provide
a couple options to pytest to help.

* You can skip submitting a new set of jobs by specifying a name
  ```
  pytest --name [NAME] ...
  ```

  which will download the products from the jobs with that name (from both `hyp3-api` and `hyp3-test-api`)
  and run the comparisons on them.

* You can specify specific product directories to use (instead of temporary ones)
  ```
  pytest --golden-dirs [DIR1] [DIR2]
  ```
  which will use the products found inside those directories (with `DIR1` being considered the golden set)
  or, if no products found, download the appropriate products to that directory (either from the 
  submission or the specified name)

* You can also specify an alternate `user_id`
  ```
  pytest --user-id [USER_ID]
  ```
  which will find jobs submitted by `USER_ID` instead of defaulting to jobs submitted by the authorized user. This is
  particularly useful when re-running a comparison run by someone else, or by the GitHub Actions user.
