"""
Snakemake rule that writes the results of a session in suite2p's output
format, so they can be opened in the suite2p GUI.
"""

import json
from pathlib import Path

import numpy as np
import sparse
from photon_mosaic import load
from scipy.stats import skew

# Frames read at a time when computing the mean image
_CHUNK_SIZE = 500


def export_to_suite2p(funcimg_folder: Path, output_folder: Path):
    """Write a session's ROIs and traces as a suite2p ``plane0`` folder.

    Reads the RoiAnalyzer in ``funcimg_folder/roi_analyzer`` and writes, in
    ``output_folder``, the files the suite2p GUI opens: ``stat.npy``,
    ``iscell.npy``, ``F.npy``, ``Fneu.npy``, ``spks.npy`` and ``ops.npy``.
    ``ops.npy`` is written last.

    Parameters
    ----------
    funcimg_folder : Path
        The ``funcimg`` folder of the session, holding the outputs of the
        motion correction, segmentation and RoiAnalyzer rules.
    output_folder : Path
        The folder where the suite2p files are saved.
    """
    funcimg_folder = Path(funcimg_folder)
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    analyzer = load(funcimg_folder / "roi_analyzer")
    imaging = analyzer.imaging
    if imaging.get_num_planes() > 1:
        raise ValueError(
            "The suite2p export only supports single-plane imaging."
        )

    F, Fneu, corrected = _get_traces(analyzer)
    rois = analyzer.rois

    np.save(output_folder / "stat.npy", _build_stat(rois, corrected))
    np.save(output_folder / "iscell.npy", _build_iscell(rois))
    np.save(output_folder / "F.npy", F)
    np.save(output_folder / "Fneu.npy", Fneu)
    np.save(output_folder / "spks.npy", _get_spikes(analyzer, F.shape))
    np.save(
        output_folder / "ops.npy",
        _build_ops(imaging, funcimg_folder, output_folder),
    )


def _get_traces(analyzer):
    """Return suite2p's F and Fneu, (num_rois, num_frames), and the
    neuropil-corrected traces, (num_frames, num_rois).

    The fluorescence extension stores ``F - neuropil_weight * Fneu`` and
    ``Fneu`` (its "background"), so suite2p's raw ``F`` is recovered by
    adding the weighted neuropil back.
    """
    extension = analyzer.get_extension("fluorescence")
    corrected = extension.get_data()
    background = extension.get_data(key="background")
    if background.shape[1] == 0:  # computed without a neuropil
        background = np.zeros_like(corrected)
    weight = extension.params.get("neuropil_weight", 0.7)
    F = corrected + weight * background
    return (
        F.T.astype(np.float32),
        background.T.astype(np.float32),
        corrected,
    )


def _get_spikes(analyzer, shape):
    """Return the deconvolved traces, (num_rois, num_frames), or zeros if
    no deconvolution has been computed. The suite2p GUI needs spks.npy to
    open a folder.
    """
    if analyzer.has_extension("deconvolution"):
        spks = analyzer.get_extension("deconvolution").get_data()
        return spks.T.astype(np.float32)
    return np.zeros(shape, np.float32)


def _build_stat(rois, corrected):
    """Build suite2p's per-ROI ``stat`` dicts from the ROI masks and
    properties.

    Parameters
    ----------
    rois : BaseRois
        The ROIs of the RoiAnalyzer.
    corrected : np.ndarray
        Neuropil-corrected traces, (num_frames, num_rois), for the ``skew``
        and ``std`` statistics suite2p computes on them.
    """
    masks = sparse.COO(rois.get_roi_image_masks())
    roi_index, ypix_all, xpix_all = masks.coords[:3]
    weights = masks.data.astype(np.float32)
    num_rois = rois.get_num_rois()

    properties = {
        key: rois.get_property(key)
        for key in rois.get_property_keys()
        if key not in ("iscell", "iscell_probability")
    }
    skews = skew(corrected, axis=0)
    stds = corrected.std(axis=0)

    stat = []
    for i in range(num_rois):
        in_roi = roi_index == i
        ypix = ypix_all[in_roi].astype(np.int32)
        xpix = xpix_all[in_roi].astype(np.int32)
        roi_stat = {
            "ypix": ypix,
            "xpix": xpix,
            "lam": weights[in_roi],
            "npix": len(ypix),
            "overlap": np.zeros(len(ypix), bool),
            "skew": float(skews[i]),
            "std": float(stds[i]),
        }
        for key, values in properties.items():
            roi_stat[key] = values[i]
        # ROIs from a backend other than suite2p have no med or radius
        roi_stat.setdefault(
            "med", [int(np.median(ypix)), int(np.median(xpix))]
        )
        roi_stat.setdefault("radius", float(np.sqrt(len(ypix) / np.pi)))
        stat.append(roi_stat)
    return np.array(stat, dtype=object)


def _build_iscell(rois):
    """Return suite2p's ``iscell``, (num_rois, 2): whether each ROI is a
    cell, and the classifier probability. All ROIs count as cells when no
    classifier ran.
    """
    keys = rois.get_property_keys()
    if "iscell" not in keys:
        return np.ones((rois.get_num_rois(), 2), np.float32)
    probability = (
        rois.get_property("iscell_probability")
        if "iscell_probability" in keys
        else rois.get_property("iscell")
    )
    return np.stack([rois.get_property("iscell"), probability], axis=1).astype(
        np.float32
    )


def _mean_image(imaging):
    """Return the mean frame of a single-plane imaging, read in chunks."""
    total = np.zeros(imaging.shape[:2], np.float64)
    for epoch in range(imaging.get_num_epochs()):
        num_frames = imaging.get_num_frames(epoch_index=epoch)
        for start in range(0, num_frames, _CHUNK_SIZE):
            end = min(start + _CHUNK_SIZE, num_frames)
            chunk = imaging.get_series(start, end, epoch_index=epoch)
            total += chunk[..., 0].sum(axis=0, dtype=np.float64)
    return (total / imaging.get_total_samples()).astype(np.float32)


def _build_ops(imaging, funcimg_folder, output_folder):
    """Build suite2p's ``ops`` dict with the keys the GUI reads.

    ``reg_file`` points at the registered movie when the GUI can read it
    directly: one epoch of int16 frames, the layout of suite2p's data.bin.
    """
    Ly, Lx = imaging.shape[:2]
    settings = (
        json.loads(
            (
                funcimg_folder / "segmentation" / "segmentation_params.json"
            ).read_text()
        )["settings"]
        or {}
    )

    registered_file = (
        funcimg_folder / "motion_correction" / "traces_cached_seg0.raw"
    ).resolve()
    first_frame = imaging.get_series(0, 1, epoch_index=0)
    readable = (
        imaging.get_num_epochs() == 1
        and first_frame.dtype == np.int16
        and registered_file.is_file()
    )

    return {
        "Ly": int(Ly),
        "Lx": int(Lx),
        "nframes": int(imaging.get_total_samples()),
        "fs": float(imaging.sampling_frequency),
        "tau": settings.get("tau", 1.0),
        "diameter": settings.get("diameter", [12.0, 12.0]),
        "meanImg": _mean_image(imaging),
        "yrange": [0, Ly],
        "xrange": [0, Lx],
        "batch_size": _CHUNK_SIZE,
        "save_path": str(output_folder),
        "reg_file": str(registered_file) if readable else "",
    }
