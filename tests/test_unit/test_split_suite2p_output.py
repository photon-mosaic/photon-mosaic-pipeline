"""
Unit tests for splitting Suite2p's combined output back into one set of
files per raw TIFF (see suite2p_ops.split_multitiff).
"""

import numpy as np
import pytest

from photon_mosaic_pipeline.rules.dff_run import calculate_dFF
from photon_mosaic_pipeline.rules.neuropil_run import (
    calculate_neuropil_correction,
)
from photon_mosaic_pipeline.rules.split_suite2p_output import (
    split_suite2p_output,
)


def _write_suite2p_plane(plane_dir, frames_per_file, n_rois=2, seed=0):
    """Write a synthetic combined Suite2p plane output (as if `frames_per_file`
    raw TIFFs had been concatenated and processed), returning the raw arrays
    for tests to compare split output against."""
    plane_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    total = sum(frames_per_file)

    F = rng.standard_normal((n_rois, total))
    Fneu = rng.standard_normal((n_rois, total)) * 0.1
    spks = rng.standard_normal((n_rois, total))
    stat = np.array([{"roi": i} for i in range(n_rois)], dtype=object)
    iscell = np.column_stack([np.ones(n_rois), np.full(n_rois, 0.9)])
    ops = {"frames_per_file": np.array(frames_per_file), "fs": 10.0}

    np.save(plane_dir / "F.npy", F)
    np.save(plane_dir / "Fneu.npy", Fneu)
    np.save(plane_dir / "spks.npy", spks)
    np.save(plane_dir / "stat.npy", stat)
    np.save(plane_dir / "iscell.npy", iscell)
    np.save(plane_dir / "ops.npy", ops)

    return {
        "F": F,
        "Fneu": Fneu,
        "spks": spks,
        "stat": stat,
        "iscell": iscell,
        "ops": ops,
    }


def test_split_slices_per_frame_arrays_at_the_right_boundaries(tmp_path):
    """F/Fneu/spks are cut exactly at the cumulative frames_per_file
    boundaries -- each dset's slice must match the source array."""
    plane_dir = tmp_path / "suite2p" / "plane0"
    frames_per_file = [40, 60]
    source = _write_suite2p_plane(plane_dir, frames_per_file)

    split_suite2p_output(plane_dir.parent)

    out_dir = plane_dir / "dset_separated"
    boundaries = np.cumsum([0, *frames_per_file])
    for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:])):
        for name in ("F", "Fneu", "spks"):
            chunk = np.load(out_dir / f"{name}_dset{i}.npy")
            assert np.array_equal(chunk, source[name][:, start:end])


def test_split_keeps_stat_and_iscell_identical_across_every_chunk(tmp_path):
    """stat/iscell describe ROIs, not frames -- every chunk should get an
    exact, unsliced copy of the original, not a per-frame slice."""
    plane_dir = tmp_path / "suite2p" / "plane0"
    source = _write_suite2p_plane(plane_dir, frames_per_file=[10, 20, 5])

    split_suite2p_output(plane_dir.parent)

    out_dir = plane_dir / "dset_separated"
    for i in range(3):
        stat_i = np.load(out_dir / f"stat_dset{i}.npy", allow_pickle=True)
        iscell_i = np.load(out_dir / f"iscell_dset{i}.npy")
        assert np.array_equal(stat_i, source["stat"])
        assert np.array_equal(iscell_i, source["iscell"])


def test_split_ops_has_nframes_updated_but_other_keys_preserved(tmp_path):
    plane_dir = tmp_path / "suite2p" / "plane0"
    _write_suite2p_plane(plane_dir, frames_per_file=[40, 60])

    split_suite2p_output(plane_dir.parent)

    out_dir = plane_dir / "dset_separated"
    ops0 = np.load(out_dir / "ops_dset0.npy", allow_pickle=True).item()
    ops1 = np.load(out_dir / "ops_dset1.npy", allow_pickle=True).item()
    assert ops0["nframes"] == 40
    assert ops1["nframes"] == 60
    assert ops0["fs"] == 10.0
    assert ops1["fs"] == 10.0


def test_split_handles_three_uneven_tiffs(tmp_path):
    """A tiny first chunk and a large last chunk would expose an off-by-one
    in the boundary math that two similarly-sized chunks could hide."""
    plane_dir = tmp_path / "suite2p" / "plane0"
    frames_per_file = [1, 100, 5]
    source = _write_suite2p_plane(plane_dir, frames_per_file)

    split_suite2p_output(plane_dir.parent)

    out_dir = plane_dir / "dset_separated"
    boundaries = np.cumsum([0, *frames_per_file])
    for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:])):
        F_i = np.load(out_dir / f"F_dset{i}.npy")
        assert F_i.shape == (source["F"].shape[0], end - start)
        assert np.array_equal(F_i, source["F"][:, start:end])


def test_split_keeps_multiple_planes_independent(tmp_path):
    """plane0 and plane1 must each get their own correct split -- nothing
    should bleed between them."""
    save_folder = tmp_path / "suite2p"
    plane0 = save_folder / "plane0"
    plane1 = save_folder / "plane1"
    source0 = _write_suite2p_plane(plane0, frames_per_file=[10, 20], seed=1)
    source1 = _write_suite2p_plane(plane1, frames_per_file=[15, 5], seed=2)

    split_suite2p_output(save_folder)

    out0 = plane0 / "dset_separated"
    out1 = plane1 / "dset_separated"
    assert np.array_equal(np.load(out0 / "F_dset0.npy"), source0["F"][:, :10])
    assert np.array_equal(
        np.load(out0 / "F_dset1.npy"), source0["F"][:, 10:30]
    )
    assert np.array_equal(np.load(out1 / "F_dset0.npy"), source1["F"][:, :15])
    assert np.array_equal(
        np.load(out1 / "F_dset1.npy"), source1["F"][:, 15:20]
    )


def test_split_single_tiff_is_a_noop(tmp_path):
    plane_dir = tmp_path / "suite2p" / "plane0"
    _write_suite2p_plane(plane_dir, frames_per_file=[50])

    split_suite2p_output(plane_dir.parent)

    assert not (plane_dir / "dset_separated").exists()


def test_split_skips_plane_without_ops(tmp_path):
    """A plane directory that exists but has no ops.npy yet (e.g. Suite2p
    hasn't finished that plane) is skipped, not treated as an error."""
    save_folder = tmp_path / "suite2p"
    (save_folder / "plane0").mkdir(parents=True)

    split_suite2p_output(save_folder)  # must not raise


def test_split_raises_on_frame_count_mismatch(tmp_path):
    plane_dir = tmp_path / "suite2p" / "plane0"
    _write_suite2p_plane(plane_dir, frames_per_file=[40, 60])

    # Corrupt F.npy so its frame count no longer matches frames_per_file.
    np.save(plane_dir / "F.npy", np.zeros((2, 50)))

    with pytest.raises(ValueError, match="does not match"):
        split_suite2p_output(plane_dir.parent)


def test_split_clears_stale_files_from_a_previous_larger_run(tmp_path):
    """If a session previously had more raw TIFFs (e.g. 3) and now has
    fewer (e.g. 2), a stale dset2 from the old run must not survive --
    otherwise a downstream consumer would see a dataset that no longer
    exists."""
    plane_dir = tmp_path / "suite2p" / "plane0"
    _write_suite2p_plane(plane_dir, frames_per_file=[40, 60])

    out_dir = plane_dir / "dset_separated"
    out_dir.mkdir(parents=True)
    stale_file = out_dir / "F_dset2.npy"
    np.save(stale_file, np.zeros((2, 5)))

    split_suite2p_output(plane_dir.parent)

    assert not stale_file.exists()
    assert (out_dir / "F_dset0.npy").exists()
    assert (out_dir / "F_dset1.npy").exists()


def test_split_chain_suite2p_to_neuropil_to_dff(tmp_path):
    """End-to-end: Suite2p's split output feeds correctly into neuropil's
    split, which feeds correctly into dFF's split -- confirms the three
    modules actually compose, not just work in isolation."""
    funcimg = tmp_path / "sub-1" / "ses-1" / "funcimg"
    plane_dir = funcimg / "suite2p" / "plane0"
    frames_per_file = [40, 60]
    _write_suite2p_plane(plane_dir, frames_per_file, n_rois=3)

    split_suite2p_output(plane_dir.parent)
    s2p_dset_dir = plane_dir / "dset_separated"

    neuropil_fc = funcimg / "neuropil" / "plane0" / "Fc.npy"
    calculate_neuropil_correction(
        str(plane_dir / "F.npy"),
        str(plane_dir / "Fneu.npy"),
        str(neuropil_fc),
        {"neucoeff": 0.7},
    )
    neuropil_dset_dir = neuropil_fc.parent / "dset_separated"
    Fc0 = np.load(neuropil_dset_dir / "Fc_dset0.npy")
    Fc1 = np.load(neuropil_dset_dir / "Fc_dset1.npy")
    assert Fc0.shape == (3, 40)
    assert Fc1.shape == (3, 60)

    F0 = np.load(s2p_dset_dir / "F_dset0.npy")
    Fneu0 = np.load(s2p_dset_dir / "Fneu_dset0.npy")
    expected_Fc0 = F0 - 0.7 * (Fneu0 - np.median(Fneu0, axis=1)[:, None])
    assert np.allclose(Fc0, expected_Fc0)

    dff_path = funcimg / "dff" / "plane0" / "dFF.npy"
    calculate_dFF(str(neuropil_fc), str(dff_path), {"gmm_ncomponents": 2})
    dff_dset_dir = dff_path.parent / "dset_separated"
    dff0 = np.load(dff_dset_dir / "dFF_dset0.npy")
    dff1 = np.load(dff_dset_dir / "dFF_dset1.npy")
    f00 = np.load(dff_dset_dir / "F0_dset0.npy")
    assert dff0.shape == (3, 40)
    assert dff1.shape == (3, 60)
    assert f00.shape == (3, 1)
