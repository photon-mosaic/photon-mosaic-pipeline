"""
Snakemake rule for running Suite2P.
"""

import logging
import os
import traceback
from pathlib import Path
from typing import Optional

from suite2p import default_db, default_settings, run_s2p
from suite2p.default_ops import default_ops
from suite2p.parameters import convert_settings_orig

from photon_mosaic_pipeline.rules.split_suite2p_output import (
    split_suite2p_output,
)

logger = logging.getLogger(__name__)

# Old ``anatomical_only`` values -> suite2p>=1.0 Cellpose input image.
# Value 3 (enhanced meanImg) has no equivalent in suite2p>=1.0.
_ANATOMICAL_ONLY_TO_CELLPOSE_IMG = {
    1: "max_proj / meanImg",
    2: "meanImg",
    4: "max_proj",
}

# Cellpose-SAM's native cell diameter; using it means no image rescaling,
# which matches the old ``diameter: 0`` (auto) behaviour.
_CELLPOSE_NATIVE_DIAMETER = 30.0


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

    # suite2p native behavior
    split_multitiff = user_ops_dict.pop("split_multitiff", False)

    ops = get_edited_options(
        input_path=dataset_folder,
        save_folder=save_folder,
        user_ops_dict=user_ops_dict,
    )
    db, settings = convert_ops_to_db_and_settings(ops)
    try:
        run_s2p(db=db, settings=settings)
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


def convert_ops_to_db_and_settings(ops: dict) -> tuple[dict, dict]:
    """Convert a flat, old-style Suite2p ``ops`` dict into the ``db`` and
    nested ``settings`` dicts that ``run_s2p`` takes in suite2p>=1.0.

    Suite2p's own converter handles most keys. ``anatomical_only`` and
    ``diameter`` need translating here: the converter ignores
    ``anatomical_only`` (silently falling back to sparsery detection) and
    suite2p>=1.0 has no auto diameter (``diameter: 0``).

    Parameters
    ----------
    ops : dict
        Flat Suite2p options, as returned by ``get_edited_options``.

    Returns
    -------
    tuple[dict, dict]
        The ``db`` and ``settings`` dicts to pass to ``run_s2p``.

    Raises
    ------
    ValueError
        If ``anatomical_only`` has no suite2p>=1.0 equivalent.
    """
    ops = dict(ops)
    anatomical_only = ops.pop("anatomical_only", 0)
    diameter = ops.pop("diameter", 0)

    db, settings, unused = convert_settings_orig(
        ops, default_db(), default_settings()
    )

    # Old suite2p used [] / "" to mean "unset"; suite2p>=1.0 uses None and
    # treats e.g. ``subfolders: []`` as "search no folders" (no files found).
    for key, default in default_db().items():
        if default is None and db[key] in ([], ""):
            db[key] = None

    if anatomical_only:
        if anatomical_only not in _ANATOMICAL_ONLY_TO_CELLPOSE_IMG:
            raise ValueError(
                f"anatomical_only={anatomical_only} is not supported by "
                f"suite2p>=1.0; use one of "
                f"{sorted(_ANATOMICAL_ONLY_TO_CELLPOSE_IMG)}"
            )
        detection = settings["detection"]
        detection["algorithm"] = "cellpose"
        detection["cellpose_settings"]["img"] = (
            _ANATOMICAL_ONLY_TO_CELLPOSE_IMG[anatomical_only]
        )
        if "pretrained_model" in unused:
            detection["cellpose_settings"]["cellpose_model"] = unused.pop(
                "pretrained_model"
            )
        if "spatial_hp_cp" in unused:
            detection["cellpose_settings"]["highpass_spatial"] = unused.pop(
                "spatial_hp_cp"
            )

    if not diameter:
        diameter = _CELLPOSE_NATIVE_DIAMETER
    settings["diameter"] = (
        list(diameter)
        if isinstance(diameter, (list, tuple))
        else [diameter] * 2
    )

    if unused:
        logger.warning(
            "Ignoring options with no suite2p>=1.0 equivalent: "
            f"{sorted(unused)}"
        )

    return db, settings
