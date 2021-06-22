from hyp3_testing import helpers


def test_find_products(tmp_path):
    product_zips = [
        tmp_path / 'p1_h1.zip',
        tmp_path / 'p2_h2.zip',
        tmp_path / 'p3_h3.zip',
    ]
    for z in product_zips:
        z.touch()

    found_products = helpers.find_products(tmp_path)
    assert found_products == {
        'p1': 'h1',
        'p2': 'h2',
        'p3': 'h3',
    }


def test_find_files_in_products(tmp_path):
    main_dir = tmp_path / 'main' / 'product_MAIN'
    main_dir.mkdir(parents=True)

    develop_dir = tmp_path / 'develop' / 'product_DEV'
    develop_dir.mkdir(parents=True)

    product_tifs = ['a.tif', 'b.tif', 'c.tif']
    for f in product_tifs:
        (main_dir / f).touch()
        (develop_dir / f).touch()

    found_files = helpers.find_files_in_products(main_dir, develop_dir)
    assert found_files == [
        (main_dir / 'a.tif', develop_dir / 'a.tif'),
        (main_dir / 'b.tif', develop_dir / 'b.tif'),
        (main_dir / 'c.tif', develop_dir / 'c.tif'),
    ]
