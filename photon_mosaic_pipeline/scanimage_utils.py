"""
Utilities for reading ScanImage TIFF metadata.
"""

import json
import logging
from pathlib import Path

import tifffile

logger = logging.getLogger(__name__)

_SI_FRAME_RATE_KEY = "SI.hRoiManager.scanVolumeRate"


def get_frame_rate_from_scanimage_tiff(tiff_path: str | Path,
                                        fallback: str | Path | None = None,
                                        ) -> float:
    """Read the frame rate from a ScanImage TIFF file header.

    Reads the ScanImage metadata embedded in the TIFF description tag
    and extracts the volume rate (frames per second).

    Parameters
    ----------
    tiff_path : str | Path
        Path to a ScanImage TIFF file.
    fallback : str | Path
        Path to a fallback file.
    Returns
    -------
    float
        Frame rate in Hz.

    Raises
    ------
    ValueError
        If the ScanImage metadata or frame rate key cannot be found.
    """
    tiff_path = Path(tiff_path)
    logger.debug(f"Reading ScanImage metadata from {tiff_path}")

    with tifffile.TiffFile(str(tiff_path)) as tif:
        metadata = tif.scanimage_metadata

    if metadata is None:
        if fallback is None:
            raise ValueError(
                f"No ScanImage metadata found in TIFF file: {tiff_path}"
            )

        fallback = Path(fallback)
        if not fallback.is_file():
            raise ValueError(
                f"No ScanImage metadata in {tiff_path}, and fallback "
                f"metadata file does not exist: {fallback}"
            )

        logger.warning(
            f"No ScanImage metadata in {tiff_path}; "
            f"falling back to {fallback}"
        )
        with fallback.open() as fh:
            metadata = json.load(fh)

    # ScanImage metadata is nested under 'FrameData' by defaut
    if "FrameData" in metadata:
        frame_data = metadata["FrameData"]
    elif any(k.startswith("SI.") for k in metadata):
        frame_data = metadata
    else:
        raise ValueError(
            f"Unrecognised ScanImage metadata layout for {tiff_path}. "
            f"Top-level keys: {list(metadata.keys())}"
        )

    if _SI_FRAME_RATE_KEY not in frame_data:
        raise ValueError(
            f"Key '{_SI_FRAME_RATE_KEY}' not found in ScanImage metadata. "
            f"Available keys: {list(frame_data.keys())}"
        )

    frame_rate = float(frame_data[_SI_FRAME_RATE_KEY])
    logger.info(f"Frame rate from ScanImage metadata: {frame_rate} Hz")
    return frame_rate
