"""
Snakemake rule for motion correction with photon-mosaic.
"""

from pathlib import Path
from typing import Optional

import photon_mosaic.extractors
from photon_mosaic import append_imaging
from photon_mosaic.preprocessing import compute_motion, register_motion


def run_motion_correction(
    tiff_paths: list[str],
    output_folder: Path,
    reader: str,
    reader_kwargs: Optional[dict] = None,
    method: str = "suite2p",
    settings: Optional[dict] = None,
):
    """Estimate the motion of a session and save the registered movie.

    Every TIFF becomes one epoch of a single imaging object, so the motion
    is estimated once for the whole session. The registered movie is written
    to ``output_folder`` as a photon-mosaic binary folder, whose
    ``provenance.json`` also holds the estimated motion.

    Parameters
    ----------
    tiff_paths : list[str]
        The preprocessed TIFFs of the session, sorted by path.
    output_folder : Path
        The folder where the registered movie will be saved.
    reader : str
        Name of the ``photon_mosaic.extractors`` function that opens one
        TIFF, e.g. ``read_tiff_imaging`` or ``read_scan_image_imaging``.
    reader_kwargs : dict, optional
        Extra arguments for the reader, e.g. ``sampling_frequency``.
    method : str, optional
        The motion correction backend. The default is "suite2p".
    settings : dict, optional
        Settings of the backend. The default is None, the backend defaults.
    """
    read = getattr(photon_mosaic.extractors, reader)
    imagings = [
        read(file_path=str(path), **(reader_kwargs or {}))
        for path in tiff_paths
    ]
    # append_imaging needs at least two imaging objects
    imaging = imagings[0] if len(imagings) == 1 else append_imaging(imagings)

    motion = compute_motion(imaging=imaging, method=method, settings=settings)
    registered = register_motion(imaging=imaging, motion=motion, method=method)
    registered.save(format="binary", folder=output_folder, overwrite=True)
