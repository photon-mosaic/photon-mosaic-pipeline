"""
Derotation preprocessing step for photon-mosaic-pipeline.

This module provides a function to derotate image data using the derotation
package.
"""

import logging
from pathlib import Path
from typing import List, Union

from derotation.derotate_batch import derotate


def run(
    dataset_folder: Path,
    output_folder: Path,
    glob_naming_pattern_tif: Union[str, List[str]],
    glob_naming_pattern_bin: Union[str, List[str]],
    path_to_stimulus_randperm: str,
    ses_idx: int = 0,
    **kwargs,
):
    """
    Derotate image data using the derotation pipeline.

    Parameters
    ----------
    dataset_folder : Path
        The path to the dataset folder.
    output_folder : Path
        The path to the output folder.
    glob_naming_pattern_tif : Union[str, List[str]]
        Pattern(s) to match tif files. Can be a single pattern or a list of
        patterns where the session index selects the appropriate pattern.
    glob_naming_pattern_bin : Union[str, List[str]]
        Pattern(s) to match bin files. Can be a single pattern or a list of
        patterns where the session index selects the appropriate pattern.
    path_to_stimulus_randperm : str
        Path to the stimulus randomization file.
    ses_idx : int, optional
        Session index to process. Default is 0.
    **kwargs : dict
        Additional arguments for the derotation pipeline.

    Returns
    -------
    None
        The function saves the derotated data to the output folder and returns
        nothing.
    """

    # Convert string patterns to lists if needed
    if isinstance(glob_naming_pattern_tif, str):
        pattern_tif = glob_naming_pattern_tif
    else:
        pattern_tif = glob_naming_pattern_tif[ses_idx]

    if isinstance(glob_naming_pattern_bin, str):
        pattern_bin = glob_naming_pattern_bin
    else:
        pattern_bin = glob_naming_pattern_bin[ses_idx]

    logging.info(
        f"Running derotation pipeline for {pattern_tif} with" f" {pattern_bin}"
    )
    derotate(
        dataset_folder=dataset_folder,
        output_folder=output_folder,
        glob_naming_pattern_tif=pattern_tif,
        glob_naming_pattern_bin=pattern_bin,
        folder_suffix="incremental" if "increment" in pattern_tif else "full",
        path_to_stimulus_randperm=path_to_stimulus_randperm,
    )
