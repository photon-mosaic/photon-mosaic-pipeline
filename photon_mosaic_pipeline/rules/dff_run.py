"""
Snakemake rule for calculating dF/F.
"""

import logging
from pathlib import Path

import numpy as np
from sklearn import mixture

logger = logging.getLogger(__name__)

def _save_split_dFF(save_folder: Path, dset_dir: Path, n_components: int) -> None:
    out_dset_dir = save_folder / "dset_separated"
    out_dset_dir.mkdir(parents=True, exist_ok=True)

    for fc_file in sorted(dset_dir.glob("Fc_dset*.npy")):
        # "Fc_dset0.npy" -> "0"
        idx = fc_file.stem[len("Fc_dset") :]

        dff_i, f0_i = dFF(np.load(fc_file), n_components=n_components)

        np.save(out_dset_dir / f"dFF_dset{idx}.npy", dff_i)
        np.save(out_dset_dir / f"F0_dset{idx}.npy", f0_i)
        logger.info(
            f"Saved dF/F traces to {out_dset_dir / f'dFF_dset{idx}.npy'}"
        )


def calculate_dFF(
    input_path_Fc: str,
    output_path: str,
    user_ops_dict: dict,
) -> None:
    """Calculate dF/F from neuropil-corrected fluorescence traces.
    Takes Fc.npy (output of neuropil correction) and computes dF/F
    using a Gaussian Mixture Model to estimate baseline F0.
    Saves dFF.npy and F0.npy to the output directory.

    Parameters
    ----------
    input_path_Fc : str
        Path to Fc.npy (neuropil-corrected traces from neuropil rule).
    output_path : str
        Path where dFF.npy and F0.npy will be saved.
    user_ops_dict : dict
        Dictionary of options. Must contain 'gmm_ncomponents'.
    """
    path_Fc = Path(input_path_Fc)
    save_folder = Path(output_path).parent
    save_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Calculating dF/F...")

    Fc = np.load(path_Fc)
    n_components = user_ops_dict["gmm_ncomponents"]

    logger.info(f"n components for dFF calculation: {n_components}")

    dff, f0 = dFF(Fc, n_components=n_components)

    np.save(save_folder / "dFF.npy", dff)
    np.save(save_folder / "F0.npy", f0)
    logger.info(f"Saved dF/F traces to {save_folder / 'dFF.npy'}")

    dset_dir = path_Fc.parent / "dset_separated"
    if dset_dir.is_dir():
        _save_split_dFF(save_folder, dset_dir, n_components)



def dFF(f, n_components=2, random_state=42):
    f0 = np.zeros(f.shape[0])
    for i in range(f.shape[0]):
        gmm = mixture.GaussianMixture(
            n_components=n_components, random_state=random_state
        ).fit(f[i].reshape(-1, 1))
        gmm_means = np.sort(gmm.means_[:, 0])
        f0[i] = gmm_means[0]
    f0 = f0.reshape(-1, 1)
    dff = (f - f0) / f0
    return dff, f0
