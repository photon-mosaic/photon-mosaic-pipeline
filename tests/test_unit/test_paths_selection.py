from photon_mosaic.paths_selection import find_raw_data_paths


def test_find_raw_data_paths(tmp_path, data_factory):
    raw_data = data_factory.create_neuroblueprint_dataset(tmp_path)
    print(raw_data)
    paths = find_raw_data_paths(tmp_path)

    assert paths
    assert len(paths) == 2

    subjects = ["sub-001_strain-C57BL6_sex-M", "sub-002_strain-C57BL6_sex-F"]

    sessions = [
        "ses-001_date-20250225_protocol-training",
        "ses-001_date-20250226_protocol-testing",
    ]

    expected_paths = [
        raw_data / subjects[0] / sessions[0] / "funcimg" / "recording.tif",
        raw_data / subjects[1] / sessions[1] / "funcimg" / "recording.tif",
    ]

    assert set(paths) == set(expected_paths)
