"""
Snakemake rule for ROI detection with photon-mosaic.
"""

from pathlib import Path
from typing import Optional

from photon_mosaic import load
from photon_mosaic.segmentation import run_segmentation


def _common_range(ranges) -> Optional[list[int]]:
    """Return the one ``[low, high]`` pixel range valid on every epoch and
    plane, or None if the motion correction backend stored no ranges.

    Parameters
    ----------
    ranges : list[list[tuple[int, int]]] or None
        The valid pixel ranges of a motion, ``[epoch][plane] -> (low, high)``.
    """
    if ranges is None:
        return None
    pairs = [pair for epoch in ranges for pair in epoch]
    return [max(low for low, _ in pairs), min(high for _, high in pairs)]


def detect_rois(
    registered_folder: Path,
    output_folder: Path,
    method: str = "suite2p",
    settings: Optional[dict] = None,
):
    """Detect ROIs on the registered movie of a session.

    A movie loaded from disk no longer knows the region that stayed in view
    during motion correction, nor the frames it flagged as bad. Both are read
    from the motion stored in the movie's ``provenance.json`` and passed on.

    Parameters
    ----------
    registered_folder : Path
        The folder written by the motion correction rule.
    output_folder : Path
        The folder where the ROIs, the settings and the run log are saved.
    method : str, optional
        The segmentation backend. The default is "suite2p".
    settings : dict, optional
        Settings of the backend. The default is None, the backend defaults.
    """
    registered_folder = Path(registered_folder)
    registered = load(registered_folder)
    motion = load(
        registered_folder / "provenance.json", base_folder=registered_folder
    ).motion

    run_segmentation(
        registered,
        method=method,
        folder=output_folder,
        settings=settings,
        yrange=_common_range(motion.yranges),
        xrange=_common_range(motion.xranges),
        badframes=motion.corrected_badframes,
        remove_existing_folder=True,
    )
