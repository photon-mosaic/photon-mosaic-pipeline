"""
Snakemake rules for the traces computed by a photon-mosaic RoiAnalyzer:
neuropil, fluorescence and dF/F.
"""

from pathlib import Path
from typing import Optional

import numpy as np
from photon_mosaic import create_roi_analyzer, load


def create_analyzer_with_neuropil(
    registered_folder: Path,
    segmentation_folder: Path,
    analyzer_folder: Path,
    params: Optional[dict] = None,
):
    """Pair the ROIs with the registered movie in a RoiAnalyzer saved to
    ``analyzer_folder``, and compute its neuropil extension.

    Parameters
    ----------
    registered_folder : Path
        The folder written by the motion correction rule.
    segmentation_folder : Path
        The folder written by the segmentation rule.
    analyzer_folder : Path
        The folder where the RoiAnalyzer will be saved.
    params : dict, optional
        Parameters of the neuropil extension. The default is None, the
        extension defaults.
    """
    analyzer = create_roi_analyzer(
        load(Path(segmentation_folder) / "rois"),
        load(registered_folder),
        format="binary_folder",
        folder=analyzer_folder,
        overwrite=True,
    )
    analyzer.compute("neuropil", **(params or {}))


def compute_extension(
    analyzer_folder: Path,
    extension_name: str,
    params: Optional[dict] = None,
    split_multitiff: bool = False,
):
    """Compute one extension of the RoiAnalyzer saved in ``analyzer_folder``.

    Parameters
    ----------
    analyzer_folder : Path
        The folder of the RoiAnalyzer.
    extension_name : str
        The extension to compute, e.g. "fluorescence" or "df_over_f".
    params : dict, optional
        Parameters of the extension. The default is None, the extension
        defaults.
    split_multitiff : bool, optional
        Also save the traces of each TIFF on their own, in a
        ``split_multitiff`` folder next to the RoiAnalyzer. The default is
        False.
    """
    analyzer = load(analyzer_folder)
    extension = analyzer.compute(extension_name, **(params or {}))
    if split_multitiff:
        _save_traces_per_tiff(
            extension.get_data(),
            analyzer.imaging,
            Path(analyzer_folder).parent / "split_multitiff",
            extension_name,
        )


def _save_traces_per_tiff(
    traces: np.ndarray, imaging, folder: Path, name: str
):
    """Cut the traces of a session where one TIFF ends and the next starts,
    and save each part as ``<name>_tiff<i>.npy`` in ``folder``.

    Each TIFF is one epoch of the imaging, and the traces hold the frames of
    all epochs one after the other.

    Parameters
    ----------
    traces : np.ndarray
        The traces of the whole session, (num_frames, num_rois).
    imaging : BaseImaging
        The imaging the traces were extracted from.
    folder : Path
        The folder where the parts are saved.
    name : str
        The prefix of the saved files.
    """
    frames_per_tiff = [
        imaging.get_num_frames(epoch_index=i)
        for i in range(imaging.get_num_epochs())
    ]
    parts = np.split(traces, np.cumsum(frames_per_tiff)[:-1])
    Path(folder).mkdir(exist_ok=True)
    for i, part in enumerate(parts):
        np.save(Path(folder) / f"{name}_tiff{i}.npy", part)
