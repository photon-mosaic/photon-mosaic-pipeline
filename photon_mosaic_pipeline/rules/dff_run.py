"""
Snakemake rule for calculating dF/F.
"""

import logging
from pathlib import Path

import numpy as np
from sklearn import mixture

logger = logging.getLogger(__name__)


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
    save_folder = Path(output_path).parent
    save_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Calculating dF/F...")

    Fc = np.load(input_path_Fc)
    n_components = user_ops_dict["gmm_ncomponents"]

    logger.info(f"n components for dFF calculation: {n_components}")

    dff, f0 = dFF(Fc, n_components=n_components)

    np.save(save_folder / "dFF.npy", dff)
    np.save(save_folder / "F0.npy", f0)


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
