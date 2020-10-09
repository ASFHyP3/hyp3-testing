"""Tools for comparing datasets"""

import filecmp
from functools import singledispatch
from pathlib import Path
from typing import Hashable, Optional, Union

import numpy as np
import xarray as xr
from rasterio.crs import CRS

from hyp3_testing.helpers import clarify_xr_message

XR = Union[xr.Dataset, xr.DataArray, xr.Variable]


class ComparisonFailure(Exception):
    """Exception to raise when a comparison fails"""


def bit_for_bit(reference: Path, secondary: Path):
    if (ref_size := reference.stat().st_size) != (sec_size := secondary.stat().st_size):
        raise ComparisonFailure(f'File sizes differ. Reference: {ref_size}; secondary: {sec_size}')

    if not filecmp.cmp(reference, secondary):
        raise ComparisonFailure('Files differ at the binary level')


def compare_values(reference: XR, secondary: XR, rtol: float = 1e-05, atol: float = 1e-08):
    xr_not_equal_message = None
    xr_not_close_message = None

    try:
        xr.testing.assert_equal(reference, secondary)
    except AssertionError as e:
        xr_not_equal_message = str(e)

    if xr_not_equal_message is None:
        return
    else:
        xr_not_equal_message = clarify_xr_message(xr_not_equal_message)

    try:
        xr.testing.assert_allclose(reference, secondary, rtol=rtol, atol=atol)
    except AssertionError as e:
        xr_not_close_message = str(e)

    if xr_not_close_message is None:
        raise ComparisonFailure('Values are close, but not equal.\n' + xr_not_equal_message)

    detailed_failure_message = _compare_values_message(reference, secondary, rtol=rtol, atol=atol)
    raise ComparisonFailure(
        '\n'.join(['Values are different.', detailed_failure_message, '', xr_not_equal_message])
    )


@singledispatch
def _compare_values_message(reference, secondary, rtol=1e-05, atol=1e-08):
    raise NotImplementedError


# Note: functools can't handle non-class types, so typing.Union is a no-go.
@_compare_values_message.register(xr.Variable)
@_compare_values_message.register(xr.DataArray)
def _array_message(reference, secondary, rtol=1e-05, atol=1e-08):
    if reference.dtype.kind == 'S' or secondary.dtype.kind == 'S':
        if reference.values != secondary.values:
            raise ComparisonFailure(
                f'Strings are different.\n    {reference.values}\n    {secondary.values}'
            )
        else:
            return

    if reference.shape != secondary.shape:
        raise ComparisonFailure(
            f'DataArrays are different shapes. Reference: {reference.shape}; secondary: {secondary.shape}'
        )

    diff = np.ma.masked_invalid(reference - secondary)
    n_close = np.isclose(diff.filled(0.0), 0.0, rtol=rtol, atol=atol).sum()

    n_different = diff.size - n_close
    if n_different == 0:
        return None

    messages = [
        f'{n_different:,}/{diff.size:,} ({n_different / diff.size:.2%}) values are different.',
        f'Reference - secondary:',
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

    messages = []
    if ref_conventions != sec_conventions:
        messages.append(f'CF Conventions differ. Reference: {ref_conventions}; secondary {sec_conventions}')

    ref_grid_map_var = _find_grid_mapping_variable_name(reference)
    sec_grid_map_var = _find_grid_mapping_variable_name(secondary)

    if ref_grid_map_var is None or sec_grid_map_var is None:
        ComparisonFailure(
            f'Could not find a grid_mapping variable! Reference: {ref_grid_map_var}; secondary {sec_grid_map_var}'
        )

    ref_wkt = _find_wkt(reference.variables[ref_grid_map_var])
    sec_wkt = _find_wkt(reference.variables[sec_grid_map_var])
    if ref_wkt is None or sec_wkt is None:
        ComparisonFailure(
            f'Could not find WKT describing spatial reference.\n  Reference: {ref_wkt}\n  secondary {sec_wkt}'
        )

    if not CRS.from_wkt(ref_wkt) == CRS.from_wkt(sec_wkt):
        raise ComparisonFailure(
            f'Spatial references are not the same.\n  Reference: {ref_wkt}\n  secondary {sec_wkt}'
        )


def _find_grid_mapping_variable_name(dataset: xr.Dataset) -> Optional[Hashable]:
    grid_map_var = None
    for var in dataset.variables:
        if dataset.variables[var].attrs.get('grid_mapping_name') is not None:
            grid_map_var = var
            break

    return grid_map_var


def _find_wkt(variable: xr.Variable) -> Optional[str]:
    wkt = variable.attrs.get('crs_wkt')
    if wkt is None:
        wkt = variable.attrs.get('spatial_ref')
    return wkt
