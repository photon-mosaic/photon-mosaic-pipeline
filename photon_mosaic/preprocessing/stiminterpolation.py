"""
Photonstimulation artefact removal step for photon-mosaic.

This module provides a function to remove artefacts by holographic
photostimulation using the stiminterp package.
"""

import json
import logging
from pathlib import Path

import tifffile
from stiminterp import run_stiminterp

logger = logging.getLogger(__name__)


def run(
    dataset_folder: Path,
    output_folder: Path,
    tiff_name: str,
    path_to_stim_h5: str | None = None,
    save_metadata: bool = True,
    **kwargs,
):
    """
    Photostimulation artefact removal using stiminterp.

    Runs stiminterp on the input TIFF and saves the processed TIFF and its
    ScanImage metadata as a JSON file to the output folder if required.

    Parameters
    ----------
    dataset_folder : Path
        Path to the dataset folder containing the input TIFF files.
    output_folder : Path
        Path to the output folder where symlinks will be created.
    tiff_name : str
        Name of the TIFF file to process.
    path_to_stim_h5 : str | None
        Path to the stimulation TTL HDF5 file. Defaults to None.
    save_metadata : bool
        Whether to save metadata as separate json file
    **kwargs : dict
        Additional keyword arguments for stiminterp pipeline.
    """
    # Convert paths to Path objects if they're strings
    if isinstance(dataset_folder, str):
        dataset_folder = Path(dataset_folder)
    if isinstance(output_folder, str):
        output_folder = Path(output_folder)

    # Create output directory
    output_folder.mkdir(parents=True, exist_ok=True)

    # Define input and output paths
    output_path = output_folder / f"stiminterpolated_{tiff_name}"

    # Try to find the input file
    input_file = dataset_folder / tiff_name
    if not input_file.exists():
        # Use rglob to find the file recursively
        try:
            input_file = next(dataset_folder.rglob(tiff_name))
            logger.info(
                f"Found input file using recursive search: {input_file}"
            )
        except StopIteration:
            raise FileNotFoundError(
                f"Could not find {tiff_name} in {dataset_folder}"
            )

    # Run stiminterp
    logging.info(f"Running stiminterp pipeline for {str(input_file)}")
    run_stiminterp(
        input_tif=str(input_file),
        input_h5=path_to_stim_h5,
        output_tif=str(output_path),
    )

    # Save metadata as JSON
    metadata = parse_si_metadata(input_file)
    if metadata is not None:
        json_path = output_path.with_name(output_path.stem + "_metadata.json")
        with open(json_path, "w") as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Saved metadata to {json_path}")
    else:
        logger.warning(f"No ScanImage metadata found in {str(input_file)}")


def parse_si_metadata(tiff_path):
    """
    Reads metadata from a Scanimage TIFF and return a dictionary with
    specified key values.

    Args:
        tiff_path: path to TIFF or directory containing tiffs

    Returns:
        dict: dictionary of SI parameters

    """
    if tiff_path.suffix != ".tif":
        tiffs = [tiff_path / tiff for tiff in sorted(tiff_path.glob("*.tif"))]
    else:
        tiffs = [
            tiff_path,
        ]
    if tiffs:
        metadata = tifffile.TiffFile(tiffs[0]).scanimage_metadata["FrameData"]
    else:
        metadata = None

    return metadata
