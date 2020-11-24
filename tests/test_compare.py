import pytest
import xarray as xr

from hyp3_testing import compare

ALAKSA_ALBERS_WKT = 'PROJCS["NAD83 / Alaska Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",55],PARAMETER["standard_parallel_2",65],PARAMETER["latitude_of_center",50],PARAMETER["longitude_of_center",-154],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","3338"]]'  # noqa: E501


def test_bit_for_bit(tmp_path):
    ref_file = tmp_path / 'ref.txt'
    ref_file.write_text('hello')

    sec_file = tmp_path / 'sec.txt'
    sec_file.write_text('hello')

    compare.bit_for_bit(ref_file, sec_file)

    with pytest.raises(compare.ComparisonFailure):
        sec_file.write_text('hell0')
        compare.bit_for_bit(ref_file, sec_file)

    with pytest.raises(compare.ComparisonFailure):
        sec_file.write_text('Goodbye!')
        compare.bit_for_bit(ref_file, sec_file)


def test_values_are_close(comparison_netcdfs):
    reference, secondary = comparison_netcdfs

    ref_ds = xr.load_dataset(reference)
    sec_ds = xr.load_dataset(secondary)

    compare.values_are_close(ref_ds, ref_ds)
    compare.values_are_close(sec_ds, sec_ds)

    with pytest.raises(compare.ComparisonFailure):
        compare.values_are_close(ref_ds, sec_ds)

    # https://numpy.org/doc/stable/reference/generated/numpy.isclose.html
    compare.values_are_close(ref_ds, sec_ds, atol=1000.0)  # coordinates and variables

    with pytest.raises(compare.ComparisonFailure):
        compare.values_are_close(ref_ds.variables['v'], sec_ds.variables['v'])

    compare.values_are_close(ref_ds.variables['v'], sec_ds.variables['v'], atol=5.0)


def test_compare_values_message(comparison_netcdfs):
    reference, secondary = comparison_netcdfs

    ref_ds = xr.load_dataset(reference)
    sec_ds = xr.load_dataset(secondary)

    message = compare._compare_values_message(ref_ds, sec_ds)
    assert 'Dataset variable:' in message

    message = compare._compare_values_message(ref_ds.variables['v'], sec_ds.variables['v'])
    assert 'values are different' in message

    ref_ds.update({'stringy': 'McStringer'})
    sec_ds.update({'stringy': 'McStringerFace'})
    message = compare._compare_values_message(ref_ds.variables['stringy'], sec_ds.variables['stringy'])
    assert 'Values are different' in message

    with pytest.raises(compare.ComparisonFailure) as execinfo:
        compare._compare_values_message(
            ref_ds.variables['v'], ref_ds.sel({'x': ref_ds.x[:-2], 'y': ref_ds.y[:-2]}).variables['v']
        )
    assert 'Data arrays are different shapes' in str(execinfo.value)

    with pytest.raises(NotImplementedError):
        compare._compare_values_message('a', 'a')


def test_compare_cf_spatial_reference(comparison_netcdfs):
    reference, secondary = comparison_netcdfs

    ref_ds = xr.load_dataset(reference)
    sec_ds = xr.load_dataset(secondary)

    compare.compare_cf_spatial_reference(ref_ds, sec_ds)

    sec_ds.attrs.pop('Conventions')
    with pytest.raises(compare.ComparisonFailure) as execinfo:
        compare.compare_cf_spatial_reference(ref_ds, sec_ds)
    assert 'does follow CF Conventions' in str(execinfo.value)

    sec_ds.attrs['Conventions'] = 'CF-1.7'
    with pytest.warns(UserWarning):
        compare.compare_cf_spatial_reference(ref_ds, sec_ds)

    with pytest.raises(compare.ComparisonFailure) as execinfo:
        compare.compare_cf_spatial_reference(ref_ds, sec_ds.drop_vars(['Polar_Stereographic']))
    assert 'Could not find a grid_mapping variable' in str(execinfo.value)

    sec_ds.variables['Polar_Stereographic'].attrs.pop('spatial_ref')
    with pytest.raises(compare.ComparisonFailure) as execinfo:
        compare.compare_cf_spatial_reference(ref_ds, sec_ds)
    assert 'Could not find WKT' in str(execinfo.value)

    sec_ds.variables['Polar_Stereographic'].attrs['spatial_ref'] = ALAKSA_ALBERS_WKT
    with pytest.raises(compare.ComparisonFailure) as execinfo:
        compare.compare_cf_spatial_reference(ref_ds, sec_ds)
    assert 'Spatial references are not the same' in str(execinfo.value)


def test_compare_raster_info(test_data_dir):
    a = test_data_dir / 'dem_nodata_0.tif'
    b = test_data_dir / 'dem_nodata_1.tif'
    compare.compare_raster_info(a, a)
    compare.compare_raster_info(b, b)
    with pytest.raises(compare.ComparisonFailure):
        compare.compare_raster_info(a, b)


def test_find_grid_mapping_variable_name(comparison_netcdfs):
    reference, _ = comparison_netcdfs
    ref_ds = xr.load_dataset(reference)

    assert 'Polar_Stereographic' == compare._find_grid_mapping_variable_name(ref_ds)

    ref_ds = ref_ds.drop_vars(['Polar_Stereographic'])
    assert compare._find_grid_mapping_variable_name(ref_ds) is None


def test_find_wkt(comparison_netcdfs):
    reference, _ = comparison_netcdfs
    ref_ds = xr.load_dataset(reference)

    ref_ds.variables['Polar_Stereographic'].attrs['spatial_ref'] = ALAKSA_ALBERS_WKT
    assert ALAKSA_ALBERS_WKT == compare._find_wkt(ref_ds.variables['Polar_Stereographic'])

    ref_ds.variables['Polar_Stereographic'].attrs.pop('spatial_ref')
    assert compare._find_wkt(ref_ds.variables['Polar_Stereographic']) is None

    ref_ds.variables['Polar_Stereographic'].attrs['crs_wkt'] = ALAKSA_ALBERS_WKT
    assert ALAKSA_ALBERS_WKT == compare._find_wkt(ref_ds.variables['Polar_Stereographic'])
