import numpy as np
import photon_mosaic as pm
import pytest

from photon_mosaic_pipeline.rules.roi_analyzer_run import compute_extension


# photon-mosaic closes the files of a binary movie when the object is
# garbage collected, which pytest reports as an unraisable ResourceWarning
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_split_multitiff_cuts_the_traces_at_the_epoch_boundaries(tmp_path):
    """Each TIFF is one epoch: with ``split_multitiff`` every saved part
    holds the frames of its own TIFF, in order, and nothing else.

    The movie has two epochs of 3 and 5 frames, so parts of equal length
    or cut anywhere else do not match.
    """
    rois, imaging, _ = pm.generate_imaging_with_rois(
        num_frames=8, height=32, width=32, num_rois=3, radius_range=(3, 5)
    )
    movie = pm.split_epoch_at_frames(imaging, 0, [3]).save(
        format="binary", folder=tmp_path / "movie"
    )
    pm.create_roi_analyzer(
        rois, movie, format="binary_folder", folder=tmp_path / "analyzer"
    )

    compute_extension(
        tmp_path / "analyzer",
        "fluorescence",
        {"use_neuropil": False},
        split_multitiff=True,
    )

    traces = np.load(
        tmp_path
        / "analyzer"
        / "extensions"
        / "fluorescence"
        / "fluorescence.npy"
    )
    folder = tmp_path / "split_multitiff"
    np.testing.assert_array_equal(
        np.load(folder / "fluorescence_tiff0.npy"), traces[:3]
    )
    np.testing.assert_array_equal(
        np.load(folder / "fluorescence_tiff1.npy"), traces[3:]
    )
    assert len(list(folder.iterdir())) == 2
