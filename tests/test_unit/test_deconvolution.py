"""
Unit tests for the spike deconvolution (Cascade) step.

Model download and inference are monkeypatched so these tests run offline
and only exercise the pipeline glue: target paths, frame-rate lookup,
model selection and the per-dataset split handling.
"""

import json
import shutil
from pathlib import Path

import numpy as np
import pytest

from photon_mosaic_pipeline.deconvolution import cascade, cascade_run
from photon_mosaic_pipeline.paths_selection import set_up_cascade_targets
from photon_mosaic_pipeline.scanimage_utils import (
    get_frame_rate_from_scanimage_tiff,
)

MASTER_TIFF = Path(__file__).parents[1] / "data" / "master.tif"
FRAME_RATE = 29.76


@pytest.fixture
def raw_tiff(tmp_path):
    """A TIFF without embedded ScanImage metadata."""
    dst = tmp_path / "rawdata" / "recording.tif"
    dst.parent.mkdir(parents=True)
    shutil.copy2(MASTER_TIFF, dst)
    return dst


def _write_metadata_json(path, frame_rate=FRAME_RATE, nested=False):
    frame_data = {"SI.hRoiManager.scanVolumeRate": frame_rate}
    metadata = {"FrameData": frame_data} if nested else frame_data
    path.write_text(json.dumps(metadata))
    return path


@pytest.fixture
def fake_cascade(tmp_path, monkeypatch):
    """Patch model storage, download and predict; record predict calls."""
    model_dir = tmp_path / "models"
    monkeypatch.setattr(cascade_run, "_CASCADE_MODEL_DIR", model_dir)

    calls = {"predict": [], "download": []}

    def fake_download(model_name, model_folder, verbose=1):
        calls["download"].append(model_name)
        (Path(model_folder) / model_name).mkdir(parents=True)
        (Path(model_folder) / model_name / "model.pth").touch()

    def fake_predict(model_name, traces, model_folder, device):
        calls["predict"].append(model_name)
        return traces * 2.0

    monkeypatch.setattr(cascade, "download_model", fake_download)
    monkeypatch.setattr(cascade, "predict", fake_predict)
    return calls


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------


def test_set_up_cascade_targets():
    preproc = [
        str(Path("derivatives/sub-001/ses-001/funcimg/rec.tif")),
        str(Path("derivatives/sub-001/ses-002/funcimg/rec.tif")),
    ]

    targets = set_up_cascade_targets(preproc)

    assert targets == [
        str(
            Path(
                "derivatives/sub-001/ses-001/funcimg/cascade/plane0/"
                "spike_prob.npy"
            )
        ),
        str(
            Path(
                "derivatives/sub-001/ses-002/funcimg/cascade/plane0/"
                "spike_prob.npy"
            )
        ),
    ]


# ---------------------------------------------------------------------------
# Frame rate
# ---------------------------------------------------------------------------


def test_frame_rate_without_metadata_or_fallback_raises(raw_tiff):
    with pytest.raises(ValueError, match="No ScanImage metadata"):
        get_frame_rate_from_scanimage_tiff(raw_tiff)


def test_frame_rate_missing_fallback_file_raises(raw_tiff, tmp_path):
    with pytest.raises(ValueError, match="does not exist"):
        get_frame_rate_from_scanimage_tiff(
            raw_tiff, fallback=tmp_path / "missing.json"
        )


@pytest.mark.parametrize("nested", [False, True])
def test_frame_rate_from_json_fallback(raw_tiff, tmp_path, nested):
    fallback = _write_metadata_json(tmp_path / "meta.json", nested=nested)

    assert get_frame_rate_from_scanimage_tiff(
        raw_tiff, fallback=fallback
    ) == pytest.approx(FRAME_RATE)


def test_frame_rate_unrecognised_layout_raises(raw_tiff, tmp_path):
    fallback = tmp_path / "meta.json"
    fallback.write_text(json.dumps({"something": 1}))

    with pytest.raises(ValueError, match="Unrecognised ScanImage metadata"):
        get_frame_rate_from_scanimage_tiff(raw_tiff, fallback=fallback)


def test_frame_rate_missing_key_raises(raw_tiff, tmp_path):
    fallback = tmp_path / "meta.json"
    fallback.write_text(json.dumps({"SI.other": 1}))

    with pytest.raises(ValueError, match="scanVolumeRate"):
        get_frame_rate_from_scanimage_tiff(raw_tiff, fallback=fallback)


# ---------------------------------------------------------------------------
# run_cascade
# ---------------------------------------------------------------------------


def _write_dff(tmp_path, traces):
    dff_path = tmp_path / "dff" / "plane0" / "dFF.npy"
    dff_path.parent.mkdir(parents=True)
    np.save(dff_path, traces)
    return dff_path


def test_run_cascade_uses_rawdata_metadata_json(
    tmp_path, raw_tiff, fake_cascade
):
    _write_metadata_json(raw_tiff.with_name("recording_metadata.json"))
    traces = np.arange(12, dtype=float).reshape(3, 4)
    dff_path = _write_dff(tmp_path, traces)
    out_path = tmp_path / "cascade" / "plane0" / "spike_prob.npy"

    cascade_run.run_cascade(
        str(dff_path), str(raw_tiff), str(out_path), {"method": "cascade"}
    )

    np.testing.assert_array_equal(np.load(out_path), traces * 2.0)
    # Default model is downloaded once and used for prediction
    assert fake_cascade["download"] == [cascade_run._DEFAULT_MODEL_NAME]
    assert fake_cascade["predict"] == [cascade_run._DEFAULT_MODEL_NAME]


def test_run_cascade_uses_derivatives_metadata_fallback(
    tmp_path, raw_tiff, fake_cascade
):
    derivatives_meta = _write_metadata_json(
        tmp_path / "stiminterpolated_recording_metadata.json"
    )
    dff_path = _write_dff(tmp_path, np.ones((2, 5)))
    out_path = tmp_path / "cascade" / "plane0" / "spike_prob.npy"

    cascade_run.run_cascade(
        str(dff_path),
        str(raw_tiff),
        str(out_path),
        {"model_name": "Global_EXC_30Hz_smoothing50ms"},
        metadata_fallbacks=[derivatives_meta],
    )

    assert out_path.exists()
    assert fake_cascade["predict"] == ["Global_EXC_30Hz_smoothing50ms"]


def test_run_cascade_skips_download_if_model_present(
    tmp_path, raw_tiff, fake_cascade
):
    _write_metadata_json(raw_tiff.with_name("recording_metadata.json"))
    model = cascade_run._CASCADE_MODEL_DIR / cascade_run._DEFAULT_MODEL_NAME
    model.mkdir(parents=True)
    (model / "model.pth").touch()
    dff_path = _write_dff(tmp_path, np.ones((2, 5)))

    cascade_run.run_cascade(
        str(dff_path),
        str(raw_tiff),
        str(tmp_path / "out" / "spike_prob.npy"),
        {},
    )

    assert fake_cascade["download"] == []


def test_run_cascade_zero_rois(tmp_path, raw_tiff, fake_cascade):
    _write_metadata_json(raw_tiff.with_name("recording_metadata.json"))
    dff_path = _write_dff(tmp_path, np.empty((0, 7)))
    out_path = tmp_path / "out" / "spike_prob.npy"

    cascade_run.run_cascade(str(dff_path), str(raw_tiff), str(out_path), {})

    assert np.load(out_path).shape == (0, 7)
    assert fake_cascade["predict"] == []


def test_run_cascade_split_datasets(tmp_path, raw_tiff, fake_cascade):
    _write_metadata_json(raw_tiff.with_name("recording_metadata.json"))
    dff_path = _write_dff(tmp_path, np.ones((2, 7)))
    dset_dir = dff_path.parent / "dset_separated"
    dset_dir.mkdir()
    # dset10 sorts before dset2 lexically -- order must be numeric
    parts = {0: np.full((2, 3), 1.0), 2: np.full((2, 2), 3.0)}
    parts[10] = np.full((2, 2), 5.0)
    for idx, arr in parts.items():
        np.save(dset_dir / f"dFF_dset{idx}.npy", arr)
    out_path = tmp_path / "cascade" / "plane0" / "spike_prob.npy"

    cascade_run.run_cascade(str(dff_path), str(raw_tiff), str(out_path), {})

    out_dset_dir = out_path.parent / "dset_separated"
    for idx, arr in parts.items():
        np.testing.assert_array_equal(
            np.load(out_dset_dir / f"spike_prob_dset{idx}.npy"), arr * 2.0
        )
    np.testing.assert_array_equal(
        np.load(out_path),
        np.concatenate([parts[0], parts[2], parts[10]], axis=1) * 2.0,
    )
