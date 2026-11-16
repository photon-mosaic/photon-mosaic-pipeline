from pathlib import Path

from photon_mosaic_pipeline.paths_selection import (
    _RULE_OUTPUTS,
    find_raw_data_paths,
    set_up_session_targets,
)


def test_find_raw_data_paths(tmp_path, data_factory):
    raw_data = data_factory.create_neuroblueprint_dataset(tmp_path)

    paths = find_raw_data_paths(tmp_path)

    assert paths
    assert len(paths) == 11

    expected_paths = [
        raw_data
        / "sub-001_strain-C57BL6_sex-M"
        / "ses-001_date-20250225_protocol-training"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-002_strain-C57BL6_sex-F"
        / "ses-001_date-20250226_protocol-testing"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-005_strain-BALBC_sex-M"
        / "ses-001_date-20250221_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-005_strain-BALBC_sex-M"
        / "ses-003_date-20250223_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-005_strain-BALBC_sex-M"
        / "ses-007_date-20250227_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-010_strain-BALBC_sex-F"
        / "ses-002_date-20250222_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-010_strain-BALBC_sex-F"
        / "ses-005_date-20250225_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-025_strain-BALBC_sex-M"
        / "ses-001_date-20250221_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-025_strain-BALBC_sex-M"
        / "ses-004_date-20250224_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-025_strain-BALBC_sex-M"
        / "ses-008_date-20250228_protocol-test"
        / "funcimg"
        / "recording.tif",
        raw_data
        / "sub-025_strain-BALBC_sex-M"
        / "ses-009_date-20250228_protocol-test"
        / "funcimg"
        / "recording.tif",
    ]

    assert set(paths) == set(expected_paths)


def test_set_up_session_targets_once_per_session():
    """A session with several TIFFs gets each rule output once, and every
    session gets all of them.
    """
    one = Path("derivatives/sub-001/ses-001/funcimg")
    two = Path("derivatives/sub-001/ses-002/funcimg")
    preproc_targets = [
        str(one / "a.tif"),
        str(one / "b.tif"),
        str(two / "a.tif"),
    ]

    targets = set_up_session_targets(preproc_targets)

    assert sorted(targets) == sorted(
        str(folder / output)
        for folder in (one, two)
        for output in _RULE_OUTPUTS.values()
    )
