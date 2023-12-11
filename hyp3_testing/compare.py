"""Tools for comparing datasets"""

import filecmp
import warnings
from functools import singledispatch
from os import listdir
from pathlib import Path
from typing import Hashable, Optional, Union

import cv2
import numpy as np
import scipy
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


def _assert_mask_similarity(reference: np.array, secondary: np.array, mask_rate: float = 0.95):
    data_main = np.ma.masked_invalid(reference)
    data_deve = np.ma.masked_invalid(secondary)
    # compare mask
    valid_mask_and = np.bitwise_and(~data_main.mask, ~data_deve.mask)
    valid_mask_or = np.bitwise_or(~data_main.mask, ~data_deve.mask)
    msk_rate = valid_mask_and.sum()/valid_mask_or.sum()
    if msk_rate <= mask_rate:
        raise AssertionError(
            f'Two masks match with less than {mask_rate}')


def maskes_are_within_similarity_threshold(reference: np.array, secondary: np.array, mask_rate: float = 0.95):
    try:
        _assert_mask_similarity(reference=reference, secondary=secondary, mask_rate=mask_rate)
    except AssertionError as e:
        raise ComparisonFailure(
            '\n'.join(['Values are different.', '', clarify_xr_message(str(e))])
        )


def _assert_within_statistic(reference: np.array, secondary: np.array, confidence_level: float = 0.99):
    data_main = np.ma.masked_invalid(reference)
    data_deve = np.ma.masked_invalid(secondary)

    valid_mask = np.bitwise_and(~data_main.mask, ~data_deve.mask)

    results = scipy.stats.ks_2samp(data_main.data[valid_mask], data_deve.data[valid_mask],
                                   alternative='two-sided', method='auto')

    if results.pvalue < confidence_level:
        raise AssertionError(f'Two data are not similar with confidence level {confidence_level*100} %')


def values_are_within_statistic(reference: np.array, secondary: np.array, confidence_level: float = 0.95):
    try:
        _assert_within_statistic(reference=reference, secondary=secondary, confidence_level=confidence_level)
    except AssertionError as e:
        raise ComparisonFailure(
            '\n'.join(['Values are different.', '', clarify_xr_message(str(e))])
        )


def _assert_within_offset_distance(reference: np.array, secondary: np.array, pixel_size: int,
                                   offset_threshold: float = 5.0):
    data_main = np.ma.masked_invalid(reference)
    data_deve = np.ma.masked_invalid(secondary)
    mask = np.bitwise_or(data_main.mask, data_deve.mask)
    data_main.data[mask] = 0
    data_deve.data[mask] = 0
    mgs_obj = cv2.reg_MapperGradShift()
    result = mgs_obj.calculate(data_main.data, data_deve.data)
    x_shift, y_shift = cv2.reg.MapTypeCaster.toShift(result).getShift().flatten()
    distance_pixels = np.sqrt((x_shift**2) + (y_shift**2))
    distance = distance_pixels * pixel_size
    if distance >= offset_threshold:
        raise AssertionError(
            f'Calculated offset distance ({distance:.2f} m) is greater than the {offset_threshold} m threshold'
        )


def images_are_within_offset_threshold(reference: np.array, secondary: np.array, pixel_size: int = 80,
                                       offset_threshold: float = 5.0):
    try:
        _assert_within_offset_distance(reference=reference, secondary=secondary, pixel_size=pixel_size,
                                       offset_threshold=offset_threshold)
    except AssertionError as e:
        raise ComparisonFailure(
            '\n'.join(['Images are not coregistered.', '', clarify_xr_message(str(e))])
        )


def _nodata_count_change(reference: np.array, secondary: np.array, threshold: float = 0.01):
    data_main = np.ma.masked_invalid(reference)
    data_deve = np.ma.masked_invalid(secondary)

    if (data_deve.mask.sum() - data_main.mask.sum())/data_main.mask.sum() > threshold:
        raise AssertionError(
            f'Number of nodata pixels in develop data is {threshold*100} % larger than those in main data'
        )


def nodata_count_change_are_within_threshold(reference: np.array, secondary: np.array, threshold: float = 0.01):
    try:
        _nodata_count_change(reference=reference, secondary=secondary, threshold=threshold)
    except AssertionError as e:
        raise ComparisonFailure(
            '\n'.join(['Images have differnt nodata pixles.', '', clarify_xr_message(str(e))])
        )


def _corr_average_decrease(reference: np.array, secondary: np.array, threshold: float = 0.05):
    data_main = np.ma.masked_invalid(reference)
    data_deve = np.ma.masked_invalid(secondary)

    if (data_main.mean() - data_deve.mean())/data_main.mean() > threshold:
        raise AssertionError(
            f'Average spatial coherence has decreased by more than {threshold * 100} %'
        )


def corr_average_decrease_within_threshold(reference: np.array, secondary: np.array, threshold: float = 0.05):
    try:
        _corr_average_decrease(reference=reference, secondary=secondary, threshold=threshold)
    except AssertionError as e:
        raise ComparisonFailure(
            '\n'.join(['Average correlation decreases.', '', clarify_xr_message(str(e))])
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


def compare_product_files(main_dir: str, develop_dir: str):
    main_files = listdir(main_dir)
    develop_files = listdir(develop_dir)

    for i in range(len(main_files)):
        main_files[i] = main_files[i].split('_')
        develop_files[i] = develop_files[i].split('_')
        # remove the unique ids before comparison
        del main_files[i][7]
        del develop_files[i][7]

    if main_files.sort() != develop_files.sort():
        raise ValueError(
            f'Product files are not the same.\n  Reference: {main_files}\n  Secondary: {develop_files}'
        )


def compare_parameter_files(main_parameter_file: str, develop_parameter_file: str):
    with open(str(main_parameter_file), 'r') as main_parameters:
        main_parameters = main_parameters.read()
        with open(str(develop_parameter_file), 'r') as develop_parameters:
            develop_parameters = develop_parameters.read()
            if main_parameters != develop_parameters:
                err = f'Parameter files are not the same.\n'
                raise ComparisonFailure(
                    err + f'  Reference: {main_parameters}\n  Secondary: {develop_parameters}'
                )
