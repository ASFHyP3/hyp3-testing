from pathlib import Path

import pytest

from hyp3_testing.opera_compare import compare_rtc_hdf5_files, compare_rtc_s1_products


main = Path('hyp3_testing/main2023')
file_list_1 = list(main.glob('*tif'))
file_list_1 += list(main.glob('*h5'))
file_list_1.sort()

dev = Path('hyp3_testing/dev2023')
file_list_2 = list(dev.glob('*tif'))
file_list_2 += list(dev.glob('*h5'))
file_list_2.sort()


def test_check_file_types():
    assert len(file_list_1) == len(file_list_2)
    suffixes_1 = sorted([s.name.split('.')[-1] for s in file_list_1])
    suffixes_2 = sorted([s.name.split('.')[-1] for s in file_list_2])
    assert suffixes_1 == suffixes_2
    assert sorted(list(set(suffixes_1))) == ['h5', 'tif']


@pytest.mark.parametrize('file_1, file_2', zip(file_list_1, file_list_2))
def test_compare_files(file_1, file_2):
    layer_suffix = file_1.name.split('_')[-1]
    file_2 = [s for s in file_list_2 if s.name.endswith(layer_suffix)][0]
    if file_1.name.endswith('h5'):
        compare_rtc_hdf5_files(file_1, file_2)
    elif file_1.name.endswith('tif'):
        compare_rtc_s1_products(file_1, file_2)
