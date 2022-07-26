"""Tools for comparing datasets"""

import filecmp
import warnings
from functools import singledispatch
from pathlib import Path
from typing import Hashable, Optional, Union

import numpy as np
import xarray as xr
from osgeo import gdal
from rasterio.crs import CRS
from rasterio.errors import CRSError

from hyp3_testing.helpers import clarify_xr_message

XR = Union[xr.Dataset, xr.DataArray, xr.Variable]


class ComparisonFailure(Exception):
    """Exception to raise when a comparison fails"""


class ToleranceFailure(Exception):
    """Exception to raise when a tolerance fails"""


def bit_for_bit(reference: Path, secondary: Path):
    filecmp.clear_cache()
    if not filecmp.cmp(reference, secondary, shallow=False):
        raise ComparisonFailure('Files differ at the binary level')


def values_are_within_tolerance(reference: XR, secondary: XR, atol: float, n_allowable: int):
    try:
        diff = np.ma.masked_invalid(reference - secondary)
        n_exceed = (~np.isclose(diff.filled(0.0), 0.0, rtol=0.0, atol=atol)).sum()
        if n_exceed > n_allowable:
            raise ToleranceFailure('Too many values are outside of the tolerance')
    except AssertionError as e:
        detailed_failure_message = _compare_values_message(reference, secondary, rtol=0.0, atol=atol)
        raise ComparisonFailure(
            '\n'.join(['Values are different.', detailed_failure_message, '', clarify_xr_message(str(e))])
        )


def values_are_close(reference: XR, secondary: XR, rtol: float = 1e-05, atol: float = 1e-08):
    try:
        xr.testing.assert_allclose(reference, secondary, rtol=rtol, atol=atol)
    except AssertionError as e:
        detailed_failure_message = _compare_values_message(reference, secondary, rtol=rtol, atol=atol)
        raise ComparisonFailure(
            '\n'.join(['Values are different.', detailed_failure_message, '', clarify_xr_message(str(e))])
        )


@singledispatch
def _compare_values_message(reference, secondary, rtol=1e-05, atol=1e-08):
    raise NotImplementedError


# Note: functools can't handle non-class types, so typing.Union is a no-go.
@_compare_values_message.register(xr.Variable)
@_compare_values_message.register(xr.DataArray)
def _array_message(reference, secondary, rtol=1e-05, atol=1e-08):
    # https://numpy.org/doc/stable/reference/generated/numpy.dtype.kind.html#numpy.dtype.kind
    exact_dtypes = ["M", "m", "O", "S", "U"]
    if reference.dtype.kind in exact_dtypes or secondary.dtype.kind in exact_dtypes:
        if reference.values != secondary.values:
            return f'Values are different.\n    Reference: {reference.values}\n    Secondary: {secondary.values}'
        return

    if reference.shape != secondary.shape:
        raise ComparisonFailure(
            f'Data arrays are different shapes. Reference: {reference.shape}; secondary: {secondary.shape}'
        )

    diff = np.ma.masked_invalid(reference - secondary)
    n_close = np.isclose(diff.filled(0.0), 0.0, rtol=rtol, atol=atol).sum()

    n_different = diff.size - n_close
    if n_different == 0:
        return None

    messages = [
        f'{n_different:,}/{diff.size:,} ({n_different / diff.size:.2%}) values are different.',
        'Reference - secondary:',
        f'    max {diff.max()}; min {diff.min()}; mean {diff.mean()};',
        f'    std {diff.std()}; var {diff.var()}',
    ]
    return '\n'.join(messages)


@_compare_values_message.register
def _dataset_message(reference: xr.Dataset, secondary, rtol=1e-05, atol=1e-08):
    ref_vars = set(reference.keys())
    sec_vars = set(secondary.keys())

    messages = []
    for var in ref_vars & sec_vars:
        msg = _compare_values_message(reference.variables[var], secondary.variables[var], rtol=rtol, atol=atol)
        if msg is not None:
            messages.append(f'\nDataset variable: {var}\n{msg}')

    return '\n'.join(messages)


def compare_cf_spatial_reference(reference: xr.Dataset, secondary: xr.Dataset):
    if (ref_conventions := reference.attrs.get('Conventions')) is None:
        raise ComparisonFailure('Reference dataset does follow CF Conventions')

    if (sec_conventions := secondary.attrs.get('Conventions')) is None:
        raise ComparisonFailure('Secondary dataset does follow CF Conventions')

    if ref_conventions != sec_conventions:
        warnings.warn(f'CF Conventions differ. Reference: {ref_conventions}; secondary {sec_conventions}')

    ref_grid_map_var = _find_grid_mapping_variable_name(reference)
    sec_grid_map_var = _find_grid_mapping_variable_name(secondary)

    if ref_grid_map_var is None or sec_grid_map_var is None:
        raise ComparisonFailure(
            f'Could not find a grid_mapping variable! Reference: {ref_grid_map_var}; secondary {sec_grid_map_var}'
        )

    ref_wkt = _find_wkt(reference.variables[ref_grid_map_var])
    sec_wkt = _find_wkt(secondary.variables[sec_grid_map_var])
    if ref_wkt is None or sec_wkt is None:
        raise ComparisonFailure(
            f'Could not find WKT describing spatial reference.\n  Reference: {ref_wkt}\n  secondary {sec_wkt}'
        )

    try:
        ref_crs = CRS.from_wkt(ref_wkt)
        sec_crs = CRS.from_wkt(sec_wkt)
    except CRSError:
        raise ComparisonFailure(f'WKT could not be parsed:\n  Reference: {ref_wkt}\n  secondary {sec_wkt}')

    if not ref_crs == sec_crs:
        raise ComparisonFailure(
            f'Spatial references are not the same.\n  Reference: {ref_wkt}\n  secondary {sec_wkt}'
        )


def compare_raster_info(reference: Path, secondary: Path):
    ref_info = gdal.Info(str(reference), format='json')
    sec_info = gdal.Info(str(secondary), format='json')
    for key in ('description', 'files'):
        ref_info.pop(key, None)
        sec_info.pop(key, None)
    ref_info['metadata'][''].pop('TIFFTAG_DATETIME', None)
    sec_info['metadata'][''].pop('TIFFTAG_DATETIME', None)
    if not ref_info == sec_info:
        raise ComparisonFailure(
            f'Raster info are not the same.\n  Reference: {ref_info}\n  Secondary: {sec_info}'
        )


def _find_grid_mapping_variable_name(dataset: xr.Dataset) -> Optional[Hashable]:
    for var in dataset.variables:
        if dataset.variables[var].attrs.get('grid_mapping_name') is not None:
            return var

    return None


def _find_wkt(variable: xr.Variable) -> Optional[str]:
    wkt = variable.attrs.get('crs_wkt')
    if wkt is None:
        wkt = variable.attrs.get('spatial_ref')
    return wkt
