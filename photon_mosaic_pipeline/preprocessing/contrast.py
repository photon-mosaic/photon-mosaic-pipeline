"""
Contrast enhancement preprocessing step.

This module provides functions to enhance the contrast of images.
"""

from pathlib import Path

import numpy as np
import tifffile
from skimage import exposure


def run(
    dataset_folder: Path,
    output_folder: Path,
    tiff_name: str,
    percentile_low: float = 1,
    percentile_high: float = 99,
    **kwargs,
) -> None:
    """
    Enhance the contrast of an image.

    Parameters
    ----------
    dataset_folder : Path
        Path to the dataset folder containing the input TIFF files.
    output_folder : Path
        Path to the output folder where the processed TIFF files will be saved.
    tiff_name : str
        Name of the TIFF file to process.
    percentile_low : float, optional
        Lower percentile for contrast stretching. Default is 1.
    percentile_high : float, optional
        Upper percentile for contrast stretching. Default is 99.
    **kwargs : dict
        Additional keyword arguments (unused).

    Returns
    -------
    None
        The function saves the enhanced image to the output folder with the
        prefix ``enhanced_`` and returns nothing.

    Notes
    -----
    The function will search for the TIFF file using rglob if it's not found
    at the expected location.
    """
    # Convert paths to Path objects if they're strings
    if isinstance(dataset_folder, str):
        dataset_folder = Path(dataset_folder)
    if isinstance(output_folder, str):
        output_folder = Path(output_folder)

    tiff_file = dataset_folder / tiff_name

    # Load the image
    try:
        img = tifffile.imread(tiff_file)
    except FileNotFoundError:
        #  use rglob to find the correct path
        correct_path = next(dataset_folder.rglob(tiff_name))
        img = tifffile.imread(correct_path)

    # Enhance contrast
    p_low, p_high = np.percentile(img, (percentile_low, percentile_high))
    img_enhanced = exposure.rescale_intensity(img, in_range=(p_low, p_high))

    # Append filename to output path
    output_path = output_folder / f"enhanced_{tiff_name}"

    # Save the enhanced image
    tifffile.imwrite(output_path, img_enhanced)
