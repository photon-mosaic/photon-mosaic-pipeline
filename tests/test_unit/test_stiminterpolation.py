"""
Unit tests for the stiminterpolation preprocessing step.

The tests mock the external `run_stiminterp` and ScanImage metadata parser
so they exercise the orchestration layer (file discovery, kwargs threading,
save_metadata gating, error paths) without needing a real TIFF or HDF5
file with photostimulation artefacts.
"""

import json
from unittest.mock import patch

import pytest

from photon_mosaic_pipeline.preprocessing import stiminterpolation


def _create_dummy_tiff(path):
    """Write a minimal placeholder file at `path` so .exists() is True.

    The contents don't matter — every test that touches them mocks
    parse_si_metadata or run_stiminterp.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_run_calls_run_stiminterp_with_expected_args(
    mock_run, mock_parse, tmp_path
):
    """Happy path: run_stiminterp is called with the resolved input/output
    paths and the configured h5 path."""
    dataset_folder = tmp_path / "rawdata"
    output_folder = tmp_path / "out"
    tiff_name = "recording.tif"
    _create_dummy_tiff(dataset_folder / tiff_name)
    mock_parse.return_value = {"FrameRate": 30.0}

    stiminterpolation.run(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        tiff_name=tiff_name,
        path_to_stim_h5="/path/to/ttl.h5",
        save_metadata=True,
    )

    mock_run.assert_called_once_with(
        input_tif=str(dataset_folder / tiff_name),
        input_h5="/path/to/ttl.h5",
        output_tif=str(output_folder / "stiminterpolated_recording.tif"),
    )


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_run_threads_kwargs_through_to_stiminterp(
    mock_run, mock_parse, tmp_path
):
    """Extra kwargs in the user config must reach run_stiminterp.

    Regression guard: the original implementation accepted **kwargs but
    never passed them on, so user-provided stiminterp parameters were
    silently dropped.
    """
    dataset_folder = tmp_path / "rawdata"
    output_folder = tmp_path / "out"
    tiff_name = "recording.tif"
    _create_dummy_tiff(dataset_folder / tiff_name)
    mock_parse.return_value = None

    stiminterpolation.run(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        tiff_name=tiff_name,
        path_to_stim_h5=None,
        save_metadata=False,
        threshold=0.5,
        n_pre_frames=3,
    )

    _, kwargs = mock_run.call_args
    assert kwargs["threshold"] == 0.5
    assert kwargs["n_pre_frames"] == 3


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_save_metadata_true_writes_json(mock_run, mock_parse, tmp_path):
    """save_metadata=True writes the metadata JSON beside the output."""
    dataset_folder = tmp_path / "rawdata"
    output_folder = tmp_path / "out"
    tiff_name = "recording.tif"
    _create_dummy_tiff(dataset_folder / tiff_name)
    mock_parse.return_value = {"FrameRate": 30.0, "ZoomFactor": 2.0}

    stiminterpolation.run(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        tiff_name=tiff_name,
        save_metadata=True,
    )

    json_path = output_folder / "stiminterpolated_recording_metadata.json"
    assert json_path.exists()
    assert json.loads(json_path.read_text()) == {
        "FrameRate": 30.0,
        "ZoomFactor": 2.0,
    }


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_save_metadata_false_skips_json(mock_run, mock_parse, tmp_path):
    """save_metadata=False suppresses the JSON write entirely.

    Regression guard: the original implementation always saved metadata
    regardless of this flag.
    """
    dataset_folder = tmp_path / "rawdata"
    output_folder = tmp_path / "out"
    tiff_name = "recording.tif"
    _create_dummy_tiff(dataset_folder / tiff_name)

    stiminterpolation.run(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        tiff_name=tiff_name,
        save_metadata=False,
    )

    # parse_si_metadata must not even be called when save_metadata is False
    mock_parse.assert_not_called()
    json_path = output_folder / "stiminterpolated_recording_metadata.json"
    assert not json_path.exists()


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_run_finds_tiff_via_recursive_search(mock_run, mock_parse, tmp_path):
    """When the TIFF isn't directly under dataset_folder, rglob finds it."""
    dataset_folder = tmp_path / "rawdata"
    nested = dataset_folder / "sub-001" / "ses-001" / "funcimg"
    tiff_name = "recording.tif"
    _create_dummy_tiff(nested / tiff_name)
    output_folder = tmp_path / "out"
    mock_parse.return_value = None

    stiminterpolation.run(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        tiff_name=tiff_name,
        save_metadata=False,
    )

    _, kwargs = mock_run.call_args
    assert kwargs["input_tif"] == str(nested / tiff_name)


def test_run_raises_when_tiff_missing(tmp_path):
    """Missing TIFF (top-level + nested) raises FileNotFoundError."""
    dataset_folder = tmp_path / "rawdata"
    dataset_folder.mkdir()
    output_folder = tmp_path / "out"

    with pytest.raises(FileNotFoundError, match="missing.tif"):
        stiminterpolation.run(
            dataset_folder=dataset_folder,
            output_folder=output_folder,
            tiff_name="missing.tif",
            save_metadata=False,
        )


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_empty_string_h5_is_passed_as_none(mock_run, mock_parse, tmp_path):
    """An empty `path_to_stim_h5` in YAML is treated as None.

    A YAML `path_to_stim_h5: ""` should not reach run_stiminterp as a
    literal empty string — it would fail the file-open downstream.
    """
    dataset_folder = tmp_path / "rawdata"
    output_folder = tmp_path / "out"
    tiff_name = "recording.tif"
    _create_dummy_tiff(dataset_folder / tiff_name)
    mock_parse.return_value = None

    stiminterpolation.run(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        tiff_name=tiff_name,
        path_to_stim_h5="",
        save_metadata=False,
    )

    _, kwargs = mock_run.call_args
    assert kwargs["input_h5"] is None


@patch(
    "photon_mosaic_pipeline.preprocessing.stiminterpolation.parse_si_metadata"
)
@patch("photon_mosaic_pipeline.preprocessing.stiminterpolation.run_stiminterp")
def test_string_path_inputs_are_coerced_to_path(
    mock_run, mock_parse, tmp_path
):
    """str inputs for dataset_folder / output_folder must be accepted."""
    dataset_folder = tmp_path / "rawdata"
    output_folder = tmp_path / "out"
    tiff_name = "recording.tif"
    _create_dummy_tiff(dataset_folder / tiff_name)
    mock_parse.return_value = None

    stiminterpolation.run(
        dataset_folder=str(dataset_folder),
        output_folder=str(output_folder),
        tiff_name=tiff_name,
        save_metadata=False,
    )

    assert output_folder.is_dir()
    _, kwargs = mock_run.call_args
    assert kwargs["output_tif"] == str(
        output_folder / "stiminterpolated_recording.tif"
    )
