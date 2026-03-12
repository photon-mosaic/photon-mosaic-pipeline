"""
Snakemake rule for running Suite2P.
"""

import traceback
from pathlib import Path
from typing import Optional
import tifffile
import matplotlib.pyplot as plt
from skimage import img_as_ubyte
from skimage.exposure import rescale_intensity
import numpy as np
from suite2p import run_s2p
from suite2p.default_ops import default_ops


def run_suite2p(
    stat_path: str,
    dataset_folder: Path,
    user_ops_dict: Optional[dict] = None,
):
    """
    This function runs Suite2P on a given dataset folder and saves the
    results in the specified paths. It also handles any exceptions
    that may occur during the process and logs them in an error
    file.

    Parameters
    ----------
    stat_path : str
        The path where the Suite2P statistics will be saved.
    dataset_folder : Path
        The path to the folder containing the dataset.
    user_ops_dict : dict, optional
        A dictionary containing user-provided options to override
        the default Suite2P options. The default is None.

    Returns
    -------
    ops: dict
        The function runs Suite2P and saves results to the specified paths.
        If an error occurs, it logs the error to an error.txt file in the
        dataset folder.
    """
    save_folder = Path(stat_path).parents[1]

    ops = get_edited_options(
        input_path=dataset_folder,
        save_folder=save_folder,
        user_ops_dict=user_ops_dict,
    )
    try:
        ops = run_s2p(ops=ops)
    except Exception as e:
        with open(dataset_folder / "error.txt", "a") as f:
            f.write(f"Error: {e}\n")
            f.write(traceback.format_exc())

    return ops


def get_edited_options(
    input_path: Path, save_folder: Path, user_ops_dict: Optional[dict] = None
) -> dict:
    """Generate a dictionary of options for Suite2P by loading the default
    options and then modifying them with user-provided options.

    The function also sets the required runtime paths for saving the results.

    Parameters
    ----------
    input_path : Path
        The path to the input data folder.
    save_folder : Path
        The path to the folder where the results will be saved.
    user_ops_dict : dict, optional
        A dictionary containing user-provided options to override
        the default options. The default is None.

    Returns
    -------
    dict
        A dictionary containing the Suite2P options, including
        the user-provided options and the required runtime paths.

    Raises
    ------
    ValueError
        If a user-provided option is not valid for Suite2P.
    """

    ops = default_ops()

    # Override with user-provided subset of keys
    if user_ops_dict:
        for key, val in user_ops_dict.items():
            if key not in ops:
                raise ValueError(f"Invalid Suite2p option: {key}")
            ops[key] = val

    # Add required runtime paths
    ops["save_folder"] = str(save_folder)
    ops["save_path0"] = str(save_folder)
    ops["fast_disk"] = str(save_folder.parent)
    ops["data_path"] = [str(input_path)]

    return ops


def save_suite2p_meanImgs(
    s2p_ops: dict
):
    """
    This function saves meanImg(s) from Suite2P output.

    Parameters
    ----------
    ops : dict
        A dictionary containing Suite2P output.

    Returns
    -------
    None
        The function saves meanImg(s) in the
        suite2p output folder.
    """
    print("Saving meanImgs from Suite2p ...")

    img1 = s2p_ops["meanImg"]
    img1_uint8 = img_as_ubyte(
        adjust_intensity(
            img_as_ubyte(
                rescale_intensity(img1, in_range="image", out_range=(0, 1))
            )
        )
    )

    plt.imsave(
        Path(s2p_ops["save_folder"]) / "plane0" / "meanImg.png",
        img1_uint8,
        cmap="gray",
    )
    tifffile.imwrite(Path(s2p_ops["save_folder"]) / "plane0" / "meanImg.tif",
                        img1.astype("int16"))

    if s2p_ops["nchannels"] == 2:
        img2 = s2p_ops["meanImg_chan2"]
        img2_uint8 = adjust_intensity(
            img_as_ubyte(
                rescale_intensity(img2, in_range="image", out_range=(0, 1))
            )
        )

        plt.imsave(
            Path(s2p_ops["save_folder"]) / "plane0" / "meanImg_chan2.png",
            img2_uint8,
            cmap="gray",
        )
        tifffile.imwrite(Path(s2p_ops["save_folder"]) / "plane0" / "meanImg_chan2.tif",
                            img2.astype("int16"))

def adjust_intensity(img):
    p2, p98 = np.percentile(img, (2, 98))  # Get min/max intensities
    img = rescale_intensity(img, in_range=(p2, p98), out_range=(0, 255))

    return img.astype(np.uint8)