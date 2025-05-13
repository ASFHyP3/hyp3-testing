"""Adapted from the OPERA RTC comparison script created by JPL OPERA RTC team:
https://github.com/opera-adt/RTC/blob/main/app/rtc_compare.py
"""

import itertools
from pathlib import Path
from typing import TypedDict

import h5py
import numpy as np
from lxml import etree
from osgeo import gdal


gdal.UseExceptions()


RTC_S1_PRODUCTS_ERROR_REL_TOLERANCE = 1e-03
RTC_S1_PRODUCTS_ERROR_ABS_TOLERANCE = 1e-04
LIST_EXCLUDE_COMPARISON_HDF5 = [
    '//identification/productID',
    '//identification/processingDateTime',
]
LIST_EXCLUDE_COMPARISON_XML = [
    '/gmi:MI_Metadata/gmd:fileIdentifier/gco:CharacterString',
    '/gmi:MI_Metadata/gmd:dateStamp/gco:DateTime',
    '/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gmx:FileName',
    '/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:DateTime',
    '/gmi:MI_Metadata/gmd:contentInfo/gmd:MD_CoverageDescription/gmd:dimension/gmd:MD_Band/gmd:otherProperty/gco:Record/eos:AdditionalAttributes/eos:AdditionalAttribute[24]/eos:value/gco:CharacterString',
    '/gmi:MI_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier[1]/gmd:MD_Identifier/gmd:code/gco:CharacterString',
    '/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep[2]/gmi:LE_ProcessStep/gmd:dateTime/gco:DateTime',
    '/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:source[2]/gmd:LI_Source/gmd:description/gco:CharacterString',
    '/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:source[4]/gmd:LI_Source/gmd:description/gco:CharacterString'
    '/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep[1]/gmi:LE_ProcessStep/gmi:output/gmd:LI_Source/gmd:description/gco:CharacterString',
    '/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep[1]/gmi:LE_ProcessStep/gmi:output/gmd:LI_Source/gmd:description/gco:CharacterString',
    '/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:source[4]/gmd:LI_Source/gmd:description/gco:CharacterString',
]
LIST_EXCLUDE_COMPARISON_IMAGE = [
    'FILENAME',
    'PRODUCT_ID',
    'INPUTS_CONFIG_FILES',
    'PROCESSING_DATETIME',
]


class AllCloseArgs(TypedDict):
    rtol: float
    atol: float
    equal_nan: bool


ALL_CLOSE_ARGS: AllCloseArgs = dict(
    rtol=RTC_S1_PRODUCTS_ERROR_REL_TOLERANCE, atol=RTC_S1_PRODUCTS_ERROR_ABS_TOLERANCE, equal_nan=True
)


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

    list_val_out = ['placeholder'] * len(list_val_in)
    for i_val, element_in in enumerate(list_val_in):
        if isinstance(element_in, h5py.h5r.Reference):
            list_val_out[i_val] = str(hdf5_obj_in[element_in].name)
        else:
            list_val_out[i_val] = element_in

    assert 'placeholder' not in list_val_out, 'Unpacking failed'
    val_out = np.array(list_val_out)
    return val_out


def get_list_dataset_attrs_keys(
    hdf_obj_1: h5py.Group, key_in: str = '/', list_dataset_so_far: list = [], list_attrs_so_far: list = []
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
    """
    # Prepare to comapre the data in the HDF objects
    if is_attr:
        # str_key is for attribute
        path_attr, key_attr = str_key.split('\n')
        val_1 = hdf5_obj_1[path_attr].attrs[key_attr]
        val_2 = hdf5_obj_2[path_attr].attrs[key_attr]
        str_message_data_location = f'Attribute path: {path_attr} ; key: {key_attr}'
        # Force the types of the values to np.ndarray to utulize numpy features
        if not isinstance(val_1, np.ndarray):
            val_1 = np.array(val_1)
        if not isinstance(val_2, np.ndarray):
            val_2 = np.array(val_2)
    else:
        # str_key is for dataset
        str_message_data_location = f'Dataset: {str_key}'
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

    if str_key in LIST_EXCLUDE_COMPARISON_HDF5:
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
        assert np.array_equal(val_1, val_2), f'Values for key {str_key} do not match ({val_1} | {val_2})'
        return

    if len(shape_val_1) == 1:
        if issubclass(val_1.dtype.type, np.number):
            assert np.allclose(val_1, val_2, **ALL_CLOSE_ARGS), (
                f'Values for key {str_key} do not match ({val_1} | {val_2})'
            )
            return
        assert np.array_equal(val_1, val_2)
        return

    if len(shape_val_1) >= 2:
        assert np.allclose(val_1, val_2, **ALL_CLOSE_ARGS), f'Values for key {str_key} do not match ({val_1} | {val_2})'
        return

    # Unexpected failure to compare `val_1` and `val_2`
    raise ValueError(
        f'Failed to compare the element: {str_message_data_location}'
        f'dataset shape in the 1st HDF5: {shape_val_1}'
        f'dataset shape in the 2nd HDF5: {shape_val_2}'
    )


def compare_rtc_hdf5_files(file_1: Path, file_2: Path):
    """
    Compare the two RTC products (in HDF5) if they are equivalent
    within acceptable difference

    Args:
        file_1: Path to the first HDF5 file
        file_2: Path to the second HDF5 file
    """
    assert file_1.exists()
    assert file_2.exists()
    with h5py.File(str(file_1), 'r') as hdf5_in_1, h5py.File(str(file_2), 'r') as hdf5_in_2:
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
        for key_dataset in intersection_set_dataset:
            compare_hdf5_elements(hdf5_in_1, hdf5_in_2, key_dataset, is_attr=False)

        # Proceed with checking the values in attributes,
        # regardless of the agreement of their structure.
        for key_attr in intersection_set_attrs:
            compare_hdf5_elements(hdf5_in_1, hdf5_in_2, key_attr, is_attr=True)


def elements_equal(root, e1, e2):
    nested = ['{http://earthdata.nasa.gov/schema/eos}value']
    assert e1.tag == e2.tag, f'Tag mismatch at {e1.tag}: {e1.tag} != {e2.tag}'
    assert len(e1) == len(e2), f'Children count mismatch at {e1.tag}: {len(e1)} != {len(e2)}'
    assert e1.attrib == e2.attrib, f'Attribute mismatch at {e1.tag}: {e1.attrib} != {e2.attrib}'
    assert e1.nsmap == e2.nsmap, f'Namespace mismatch at {e1.tag}: {e1.nsmap} != {e2.nsmap}'

    full_path = root.getroottree().getpath(e1)
    if full_path not in LIST_EXCLUDE_COMPARISON_XML:
        # if not (e1.text or '').strip() == (e2.text or '').strip():
        assert (e1.text or '').strip() == (e2.text or '').strip(), (
            f'Text mismatch at {full_path}: {e1.text} != {e2.text}'
        )

    # Recursively compare children
    for c1, c2 in zip(e1, e2):
        elements_equal(root, c1, c2)


def compare_rtc_iso_xmls(file1, file2):
    root1 = etree.parse(file1).getroot()
    root2 = etree.parse(file2).getroot()
    elements_equal(root1, root1, root2)


def _compare_rtc_s1_metadata(metadata_1, metadata_2):
    set_1_m_2 = set(metadata_1.keys()) - set(metadata_2.keys())
    assert set_1_m_2 == set()
    set_2_m_1 = set(metadata_2.keys()) - set(metadata_1.keys())
    assert set_2_m_1 == set()
    for k1, v1 in metadata_1.items():
        if k1 in LIST_EXCLUDE_COMPARISON_IMAGE:
            continue
        v2 = metadata_2[k1]
        assert v2 == v1, f'Values for key {k1} do not match ({v1} | {v2})'


def compare_rtc_s1_products(file_1: Path, file_2: Path):
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

    _compare_rtc_s1_metadata(metadata_1, metadata_2)

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


if __name__ == '__main__':
    file1 = Path(
        'main2/OPERA_L2_RTC-S1_T035-073251-IW2_20220111T020806Z_20241218T153135Z_S1A_30_v1.0/OPERA_L2_RTC-S1_T035-073251-IW2_20220111T020806Z_20241218T153135Z_S1A_30_v1.0.iso.xml'
    )
    file2 = Path(
        'dev2/OPERA_L2_RTC-S1_T035-073251-IW2_20220111T020806Z_20250513T175138Z_S1A_30_v1.0/OPERA_L2_RTC-S1_T035-073251-IW2_20220111T020806Z_20250513T175138Z_S1A_30_v1.0.iso.xml'
    )
    compare_rtc_iso_xmls(file1, file2)
