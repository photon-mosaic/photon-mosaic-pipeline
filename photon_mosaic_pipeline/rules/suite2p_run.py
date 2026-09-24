"""
Snakemake rule for running Suite2P.
"""

import os
import traceback
from pathlib import Path
from typing import Optional

import numpy as np
from suite2p import run_s2p
from suite2p.default_ops import default_ops


def _force_cellpose_cpu_if_requested():
    """Force Cellpose onto CPU when ``PHOTON_MOSAIC_FORCE_CPU=1``.

    Suite2p's anatomical detection runs Cellpose on a GPU whenever
    ``cellpose.core.use_gpu()`` reports one is available. On a real
    Apple-Silicon machine the MPS backend works and is the right default, so
    this is **off by default**. But GitHub-hosted macOS runners expose an MPS
    device that torch detects and can allocate on, yet which computes
    Cellpose-SAM incorrectly -- it returns 0 masks, so Suite2p writes no
    ``F.npy`` and the pipeline fails. Setting ``PHOTON_MOSAIC_FORCE_CPU=1``
    pins Cellpose to the CPU, which produces correct masks everywhere. The
    test suite sets this var (see ``tests/conftest.py``); the snakemake
    subprocesses inherit it.
    """
    if os.environ.get("PHOTON_MOSAIC_FORCE_CPU") != "1":
        return
    try:
        import cellpose.core

        cellpose.core.use_gpu = lambda *args, **kwargs: False
    except ImportError:
        # Cellpose isn't importable (e.g. anatomical detection disabled) --
        # nothing to force; suite2p will run its non-Cellpose path.
        pass


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
    None
        The function runs Suite2P and saves results to the specified paths.
        If an error occurs, it logs the error to an error.txt file in the
        dataset folder.
    """
    save_folder = Path(stat_path).parents[1]

    _force_cellpose_cpu_if_requested()

    user_ops_dict = dict(user_ops_dict) if user_ops_dict else {}
    split_multitiff = user_ops_dict.pop("split_multitiff", False) # suite2p native behavior

    ops = get_edited_options(
        input_path=dataset_folder,
        save_folder=save_folder,
        user_ops_dict=user_ops_dict,
    )
    try:
        run_s2p(ops=ops)
        if split_multitiff:
            split_suite2p_output(save_folder)
    except Exception as e:
        with open(dataset_folder / "error.txt", "a") as f:
            f.write(f"Error: {e}\n")
            f.write(traceback.format_exc())


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


def split_suite2p_output(save_folder: Path) -> None:
    """Split each plane's combined Suite2p output back into one set of
    files per raw TIFF that Suite2p combined into it.

    Does nothing for a plane where Suite2p only saw one TIFF
    (``frames_per_file`` absent or length 1 -- nothing to split).
    Otherwise, for each chunk ``i`` in ``frames_per_file``, writes
    ``dset_separated/{name}_dset{i}.npy``: ``F``/``Fneu``/``spks`` sliced
    to that chunk's frames, ``stat``/``iscell`` copied as-is (they describe
    ROIs, not frames, so they're identical across chunks), and ``ops``
    copied with ``nframes`` updated to that chunk's length.
    """
    for plane_dir in sorted(save_folder.glob("plane*")):
        ops_path = plane_dir / "ops.npy"
        if not ops_path.exists():
            continue

        ops = np.load(ops_path, allow_pickle=True).item()
        frames_per_file = ops.get("frames_per_file")

        if frames_per_file is None or len(frames_per_file) <= 1:
            continue

        boundaries = np.cumsum([0] + list(frames_per_file))

        F = np.load(plane_dir / "F.npy")
        if boundaries[-1] != F.shape[1]:
            raise ValueError(
                f"Sum of frames_per_file ({boundaries[-1]}) does not match "
                f"F.shape[1] ({F.shape[1]}) in {plane_dir}."
            )

        # sliceable=True arrays are cut to each chunk's frames; the rest
        # describe ROIs, not frames, so they're saved as-is per chunk.
        arrays = {
            "F": (F, True),
            "Fneu": (np.load(plane_dir / "Fneu.npy"), True),
            "spks": (np.load(plane_dir / "spks.npy"), True),
            "stat": (np.load(plane_dir / "stat.npy", allow_pickle=True), False),
            "iscell": (np.load(plane_dir / "iscell.npy"), False),
        }

        out_dir = plane_dir / "dset_separated"
        out_dir.mkdir(parents=True, exist_ok=True)

        for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:])):
            for name, (arr, sliceable) in arrays.items():
                chunk = arr[:, start:end] if sliceable else arr
                np.save(out_dir / f"{name}_dset{i}.npy", chunk)

            np.save(out_dir / f"ops_dset{i}.npy", dict(ops, nframes=int(end - start)))



