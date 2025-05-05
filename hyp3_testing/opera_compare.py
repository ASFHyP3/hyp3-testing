"""Modified from the OPERA RTC comparison script created by JPL OPERA RTC team.
Source: https://github.com/opera-adt/RTC/blob/main/app/rtc_compare.py
"""

import argparse
import itertools
from pathlib import Path

import h5py
import numpy as np
from osgeo import gdal


gdal.UseExceptions()


RTC_S1_PRODUCTS_ERROR_REL_TOLERANCE = 1e-03
RTC_S1_PRODUCTS_ERROR_ABS_TOLERANCE = 1e-04
ALL_CLOSE_ARGS = dict(
    rtol=RTC_S1_PRODUCTS_ERROR_REL_TOLERANCE,
    atol=RTC_S1_PRODUCTS_ERROR_ABS_TOLERANCE,
    equal_nan=True,
)
LIST_EXCLUDE_COMPARISON = [
    '//identification/productID',
    '//metadata/processingInformation/inputs/annotationFiles',
    '//identification/processingDateTime',
    '//metadata/processingInformation/inputs/l1SlcGranules',
]
LIST_EXCLUDE_COMPARISON_PRODUCT = [
    'FILENAME',
    'PRODUCT_ID',
    'INPUTS_ANNOTATION_FILES',
    'INPUTS_CONFIG_FILES',
    'PROCESSING_DATETIME',
    'INPUT_ANNOTATION_FILES',
    'INPUT_L1_SLC_GRANULES',
]


def _get_parser():
    parser = argparse.ArgumentParser(
        description='Compare two RTC products', formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Inputs
    parser.add_argument('input_dirs', type=str, nargs=2, help='Input RTC products` directories')

    return parser


def _unpack_array(val_in, hdf5_obj_in):
    """
    Unpack the array of array into ordinary numpy array.
    Convert an HDF5 object reference into the path it is pointing to.

    For internal use in this script.

    Args:
        val_in: numpy array to unpack
        hdf5_obj_in: Source HDF5 object of `val_in`

    Returns:
        unpacked numpy array
    """
    list_val_in = list(itertools.chain.from_iterable(val_in))

    list_val_out = [None] * len(list_val_in)
    for i_val, element_in in enumerate(list_val_in):
        if isinstance(element_in, h5py.h5r.Reference):
            list_val_out[i_val] = np.str_(hdf5_obj_in[element_in].name)
        else:
            list_val_out[i_val] = element_in
    val_out = np.array(list_val_out)

    return val_out


def get_list_dataset_attrs_keys(
    hdf_obj_1: h5py.Group, key_in: str = '/', list_dataset_so_far: list = None, list_attrs_so_far: list = None
):
    """
    Recursively traverse the datasets and attributes within the input HDF5 group.
    Returns the list of keys for datasets and attributes.

    NOTE:
    In case of attributes, the path and the attribute keys are
    separated by newline character ('\n')

    Args:
        hdf_obj_1: HDF5 object to retrieve the dataset and the attribute list
        key_in: path in the HDF5 object
        list_dataset_so_far: list of the dataset keys that have found so far
        list_attrs_so_far: list of the attribute path/keys that have found so far

    Returns:
        Lists of dataset keys and attribute path/keys
    """
    # default values for the lists
    if list_dataset_so_far is None:
        list_dataset_so_far = []
    if list_attrs_so_far is None:
        list_attrs_so_far = []

    if isinstance(hdf_obj_1[key_in], h5py.Group):
        # Append the attributes keys if there are any
        for key_attr_1 in hdf_obj_1[key_in].attrs:
            list_attrs_so_far.append('\n'.join([key_in, key_attr_1]))

        for key_1, _ in hdf_obj_1[key_in].items():
            get_list_dataset_attrs_keys(hdf_obj_1, f'{key_in}/{key_1}', list_dataset_so_far, list_attrs_so_far)

    else:
        list_dataset_so_far.append(key_in)
        for key_attr_1 in hdf_obj_1[key_in].attrs:
            # Append the attributes keys if there are any
            list_attrs_so_far.append('\n'.join([key_in, key_attr_1]))
    return list_dataset_so_far, list_attrs_so_far


def compare_hdf5_elements(
    hdf5_obj_1,
    hdf5_obj_2,
    str_key,
    is_attr=False,
    id_key=None,
    total_key=None,
    list_exclude: list = None,
):
    """
    Compare the dataset or attribute defined by `str_key`
    NOTE: For attributes, the path and the key are
    separated by newline character ('\n')

    Args:
        hdf5_obj_1: The 1st HDF5 object to compare
        hdf5_obj_2: The 2nd HDF5 object to compare
        str_key: Key to the dataset or attribute
        is_attr: Designate if `str_key` is for dataset or attribute
        id_key: index of the key in the list. Optional for printout purpose.
        total_key: total number of the list. Optional for printout purpose.
        list_exclude: Absolute paths of the elements to be excluded from the comparison
    """

    if id_key is None or total_key is None:
        str_order = ''
    else:
        str_order = f'{id_key + 1} of {total_key}'

    # Prepare to comapre the data in the HDF objects
    if is_attr:
        # str_key is for attribute
        path_attr, key_attr = str_key.split('\n')
        val_1 = hdf5_obj_1[path_attr].attrs[key_attr]
        val_2 = hdf5_obj_2[path_attr].attrs[key_attr]
        str_message_data_location = f'Attribute {str_order}. path: {path_attr} ; key: {key_attr}'
        # Force the types of the values to np.ndarray to utulize numpy features
        if not isinstance(val_1, np.ndarray):
            val_1 = np.array(val_1)
        if not isinstance(val_2, np.ndarray):
            val_2 = np.array(val_2)
    else:
        # str_key is for dataset
        str_message_data_location = f'Dataset {str_order}: {str_key}'
        val_1 = np.array(hdf5_obj_1[str_key])
        val_2 = np.array(hdf5_obj_2[str_key])

    # convert object reference to the path to which it is pointing
    # Example:
    # attribute `REFERENCE_LIST` in
    # /data/xCoordinates'
    # attribute `DIMENSION_LIST` in
    # /data/VH
    if (len(val_1.shape) >= 1) and ('shape' in dir(val_1[0])):
        is_void = isinstance(val_1[0], np.void)
        is_reference = (len(val_1[0].shape) == 1) and isinstance(val_1[0][0], h5py.h5r.Reference)
        if is_void or is_reference:
            val_1 = _unpack_array(val_1, hdf5_obj_1)

    # Repeat the same process for val_2
    if (len(val_2.shape) >= 1) and ('shape' in dir(val_2[0])):
        is_void = isinstance(val_2[0], np.void)
        is_reference = (len(val_2[0].shape) == 1) and isinstance(val_2[0][0], h5py.h5r.Reference)
        if is_void or is_reference:
            val_2 = _unpack_array(val_2, hdf5_obj_2)

    if list_exclude is not None and str_key in list_exclude:
        return

    shape_val_1 = val_1.shape
    shape_val_2 = val_2.shape
    assert shape_val_1 == shape_val_2
    assert val_1.dtype == val_2.dtype

    if len(shape_val_1) == 0:
        if issubclass(val_1.dtype.type, np.number):
            assert np.allclose(val_1, val_2, **ALL_CLOSE_ARGS)
            return
        # All other non-numerical cases, including the npy array with bytes
        assert np.array_equal(val_1, val_2)
        return

    if len(shape_val_1) == 1:
        if issubclass(val_1.dtype.type, np.number):
            assert np.allclose(val_1, val_2, **ALL_CLOSE_ARGS)
            return
        assert np.array_equal(val_1, val_2)
        return

    if len(shape_val_1) >= 2:
        assert np.allclose(val_1, val_2, **ALL_CLOSE_ARGS)
        return

    # Unexpected failure to compare `val_1` and `val_2`
    raise ValueError(
        f'Failed to compare the element: {str_message_data_location}'
        f'dataset shape in the 1st HDF5: {shape_val_1}'
        f'dataset shape in the 2nd HDF5: {shape_val_2}'
    )


def compare_rtc_hdf5_files(file_1: str, file_2: str, list_elements_to_exclude: list = None):
    """
    Compare the two RTC products (in HDF5) if they are equivalent
    within acceptable difference

    Parameters
    -----------
    file_1, file_2: str
        Path to the RTC products (in HDF5)
    list_elements_to_exclude: list(str)
        Absolute paths to the elements to be excluded from the comparison

    Return:
    -------
    _: bool
        `True` if the two products are equivalent; `False` otherwise

    """

    with h5py.File(file_1, 'r') as hdf5_in_1, h5py.File(file_2, 'r') as hdf5_in_2:
        list_dataset_1, list_attrs_1 = get_list_dataset_attrs_keys(hdf5_in_1)
        set_dataset_1 = set(list_dataset_1)
        set_attrs_1 = set(list_attrs_1)

        list_dataset_2, list_attrs_2 = get_list_dataset_attrs_keys(hdf5_in_2)
        set_dataset_2 = set(list_dataset_2)
        set_attrs_2 = set(list_attrs_2)

        intersection_set_dataset = set_dataset_1.intersection(set_dataset_2)
        assert len(intersection_set_dataset) == len(set_dataset_1)
        assert len(intersection_set_dataset) == len(set_dataset_2)

        intersection_set_attrs = set_attrs_1.intersection(set_attrs_2)
        assert len(intersection_set_attrs) == len(set_attrs_1)
        assert len(intersection_set_attrs) == len(set_attrs_2)

        # Proceed with checking the values in dataset,
        # regardless of the agreement of their structure.
        list_flag_identical_dataset = [None] * len(intersection_set_dataset)
        for id_flag, key_dataset in enumerate(intersection_set_dataset):
            list_flag_identical_dataset[id_flag] = compare_hdf5_elements(
                hdf5_in_1,
                hdf5_in_2,
                key_dataset,
                is_attr=False,
                id_key=id_flag,
                total_key=len(intersection_set_dataset),
                list_exclude=list_elements_to_exclude,
            )

        # Proceed with checking the values in attributes,
        # regardless of the agreement of their structure.
        list_flag_identical_attrs = [None] * len(intersection_set_attrs)
        for id_flag, key_attr in enumerate(intersection_set_attrs):
            list_flag_identical_attrs[id_flag] = compare_hdf5_elements(
                hdf5_in_1,
                hdf5_in_2,
                key_attr,
                is_attr=True,
                id_key=id_flag,
                total_key=len(intersection_set_attrs),
                list_exclude=list_elements_to_exclude,
            )


def compare_rtc_s1_products(file_1, file_2):
    assert file_1.exists()
    assert file_2.exists()

    # TODO: compare projections ds.GetProjection()
    layer_gdal_dataset_1 = gdal.Open(file_1, gdal.GA_ReadOnly)
    geotransform_1 = layer_gdal_dataset_1.GetGeoTransform()
    metadata_1 = layer_gdal_dataset_1.GetMetadata()
    nbands_1 = layer_gdal_dataset_1.RasterCount

    layer_gdal_dataset_2 = gdal.Open(file_2, gdal.GA_ReadOnly)
    geotransform_2 = layer_gdal_dataset_2.GetGeoTransform()
    metadata_2 = layer_gdal_dataset_2.GetMetadata()
    nbands_2 = layer_gdal_dataset_2.RasterCount

    assert nbands_1 == nbands_2
    assert np.array_equal(geotransform_1, geotransform_2)

    for band_index in range(1, nbands_1 + 1):
        gdal_band_1 = layer_gdal_dataset_1.GetRasterBand(band_index)
        gdal_band_2 = layer_gdal_dataset_2.GetRasterBand(band_index)
        image_1 = gdal_band_1.ReadAsArray()
        image_2 = gdal_band_2.ReadAsArray()
        assert image_1.shape == image_2.shape
        assert image_1.dtype == image_2.dtype
        assert np.allclose(image_1, image_2, **ALL_CLOSE_ARGS)

    _compare_rtc_s1_metadata(metadata_1, metadata_2)


def _compare_rtc_s1_metadata(metadata_1, metadata_2):
    set_1_m_2 = set(metadata_1.keys()) - set(metadata_2.keys())
    assert set_1_m_2 == set()
    set_2_m_1 = set(metadata_2.keys()) - set(metadata_1.keys())
    assert set_2_m_1 == set()
    for k1, v1 in metadata_1.items():
        exclude_keys = LIST_EXCLUDE_COMPARISON_PRODUCT + ['PROCESSING_DATE_TIME']
        if k1 in exclude_keys:
            continue
        assert metadata_2[k1] == v1


def check_file_types(file_list_1, file_list_2):
    assert len(file_list_1) == len(file_list_2)
    suffixes_1 = sorted([s.name.split('.')[-1] for s in file_list_1])
    suffixes_2 = sorted([s.name.split('.')[-1] for s in file_list_2])
    assert suffixes_1 == suffixes_2
    assert sorted(list(set(suffixes_1))) == ['h5', 'tif']


def main():
    """
    main function of the RTC product comparison script
    """
    parser = _get_parser()

    args = parser.parse_args()

    file_list_1 = list(Path(args.input_dirs[0]).glob('*tif'))
    file_list_1 += list(Path(args.input_dirs[0]).glob('*h5'))

    file_list_2 = list(Path(args.input_dirs[1]).glob('*tif'))
    file_list_2 += list(Path(args.input_dirs[1]).glob('*h5'))
    check_file_types(file_list_1, file_list_2)

    for file_1 in file_list_1:
        layer_suffix = file_1.name.split('_')[-1]
        file_2 = [s for s in file_list_2 if s.name.endswith(layer_suffix)][0]
        if file_1.name.endswith('h5'):
            compare_rtc_hdf5_files(file_1, file_2, LIST_EXCLUDE_COMPARISON)
        elif file_1.name.endswith('tif'):
            compare_rtc_s1_products(file_1, file_2)


if __name__ == '__main__':
    main()
