"""
Snakemake rule for neuropil correction.
"""

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


def calculate_neuropil_correction(
    input_path_F: str,
    input_path_Fneu: str,
    output_path: str,
    user_ops_dict: dict,
) -> None:
    """Apply neuropil correction to raw fluorescence traces.

    Computes Fc = F - neucoeff * (Fneu - median(Fneu)) and saves
    the result as Fc.npy.

    Parameters
    ----------
    input_path_F : str
        Path to F.npy from Suite2p.
    input_path_Fneu : str
        Path to Fneu.npy from Suite2p.
    output_path : str
        Path where Fc.npy will be saved.
    user_ops_dict : dict
        Dictionary of options. Must contain 'neucoeff'.
    """
    save_folder = Path(output_path).parent
    save_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Applying neuropil correction...")

    F = np.load(input_path_F)
    Fneu = np.load(input_path_Fneu)

    Fc = F - user_ops_dict["neucoeff"] * (
        Fneu - np.median(Fneu, axis=1)[:, None]
    )

    np.save(output_path, Fc)
    logger.info(f"Saved neuropil-corrected traces to {output_path}")
