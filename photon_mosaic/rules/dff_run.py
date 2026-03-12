"""
Snakemake rule for calculating dF/F.
"""

from pathlib import Path

import numpy as np
from sklearn import mixture


def calculate_dFF(
    input_path_F: str,
    input_path_Fneu: str,
    output_path: str,
    user_ops_dict: dict,
):
    """
    This function calculates dF/F for the whole session after neuropil
    correction. It uses dF/F from suite2p outputs and saves the
    results in the specified paths. This function is adapted from
    https://github.com/znamlab/2p-preprocess/blob/f34c287d5830a3852fdf44f9bd8a826e70095471/twop_preprocess/calcium.py#L768

    Parameters
    ----------
    input_path_F : str
        The path where the F.npy from Suite2P is saved.
    input_path_Fneu : str
        The path where the Fneu.npy from Suite2P is saved.
    output_path : str
        The path where the dFF.npy and f0.npy will be saved.
    user_ops_dict : dict, optional
        A dictionary containing user-provided options to override
        the default Suite2P options. The default is None.

    Returns
    -------
    None
        The function calculates dF/F and saves results to the specified paths.
    """
    save_folder = Path(output_path).parents[0]
    save_folder.mkdir(parents=True, exist_ok=True)

    print("Calculating dF/F...")

    F = np.load(input_path_F)
    Fneu = np.load(input_path_Fneu)
    Fc = F - user_ops_dict["neucoeff"] * (
        Fneu - np.median(Fneu, axis=1)[:, None]
    )

    print(
        "n components for dFF calculation: {}".format(
            user_ops_dict["gmm_ncomponents"]
        )
    )

    dff, f0 = dFF(Fc, n_components=user_ops_dict["gmm_ncomponents"])

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
