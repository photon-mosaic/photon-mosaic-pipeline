"""
Snakemake rule for neuropil correction.
"""

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

def _save_split_neuropil_correction(save_folder: Path, dset_dir: Path, neucoeff: float) -> None:

    out_dset_dir = save_folder / "dset_separated"
    out_dset_dir.mkdir(parents=True, exist_ok=True)

    for f_file in sorted(dset_dir.glob("F_dset*.npy")):
        # "F_dset0.npy" -> "0"
        idx = f_file.stem[len("F_dset") :]
        fneu_file = dset_dir / f"Fneu_dset{idx}.npy"

        F_i = np.load(f_file)
        Fneu_i = np.load(fneu_file)
        Fc_i = _apply_neuropil_correction(F_i, Fneu_i, neucoeff)

        out_file = out_dset_dir / f"Fc_dset{idx}.npy"
        np.save(out_file, Fc_i)
        logger.info(f"Saved neuropil-corrected traces to {out_file}")


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
    F_path = Path(input_path_F)
    Fneu_path = Path(input_path_Fneu)
    out_path = Path(output_path)

    save_folder = out_path.parent
    save_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Applying neuropil correction using coefficient...")
    neucoeff = user_ops_dict["neucoeff"]
    F = np.load(F_path)
    Fneu = np.load(Fneu_path)
    Fc = _apply_neuropil_correction(F, Fneu, neucoeff)

    np.save(out_path, Fc)
    logger.info(f"Saved neuropil-corrected traces to {out_path}")

    # Also correct any per-dataset split traces, if present.
    dset_dir = F_path.parent / "dset_separated"
    if dset_dir.is_dir():
        _save_split_neuropil_correction(save_folder, dset_dir, neucoeff)


def _apply_neuropil_correction(
    F: np.ndarray, Fneu: np.ndarray, neucoeff: float
) -> np.ndarray:
    """Core correction: Fc = F - neucoeff * (Fneu - median(Fneu))."""
    return F - neucoeff * (Fneu - np.median(Fneu, axis=1)[:, None])
