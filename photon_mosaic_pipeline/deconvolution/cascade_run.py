"""
Snakemake rule for spike deconvolution using CascadeTorch.
"""

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

_CASCADE_MODEL_DIR = Path.home() / ".cascade" / "Pretrained_models"
_DEFAULT_MODEL_NAME = "Global_EXC_40Hz_smoothing25ms_causalkernel"


def _predict(cascade, model_name, traces, model_folder, device):
    """Run Cascade on (neurons x time) traces; handles zero ROIs."""
    if traces.shape[0] == 0:
        logger.warning("No ROIs in dF/F traces; saving empty spike_prob.")
        return np.empty_like(traces, dtype=float)
    return cascade.predict(
        model_name, traces, model_folder=model_folder, device=device
    )


def run_cascade(
    input_path_dff: str,
    input_path_tiff: str,
    output_path: str,
    user_ops_dict: dict,
    metadata_fallbacks: list[str | Path] | None = None,
) -> None:
    """Run CascadeTorch spike deconvolution on dF/F traces.

    Loads dFF traces, reads the frame rate from the raw ScanImage TIFF
    header, runs the Cascade model, and saves spike probabilities.

    Models are downloaded to ~/.cascade/Pretrained_models/ on first run
    and reused on subsequent runs.

    If a ``dset_separated/`` folder exists next to ``input_path_dff`` (i.e.
    Suite2p's output was split into multiple datasets and dF/F was
    computed per-dataset -- see ``run_suite2p.split_suite2p_output`` and
    ``dff_run.calculate_dFF``), this also runs Cascade on each
    ``dFF_dsetN.npy`` found there, saving ``spike_prob_dsetN.npy`` into a
    matching ``dset_separated/`` folder next to ``output_path``. The frame
    rate and model are read/loaded only once and reused across all
    datasets -- they don't vary per dataset within the same session. This
    is a side effect, not a formally declared Snakemake output --
    deconvolution.smk only tracks the single combined spike_prob.npy.

    Parameters
    ----------
    input_path_dff : str
        Path to dFF.npy (output of dff rule).
    input_path_tiff : str
        Path to the raw ScanImage TIFF file, used to read frame rate.
    output_path : str
        Path where spike_prob.npy will be saved.
    user_ops_dict : dict
        Dictionary of options. ``model_name`` selects the pretrained
        model (defaults to ``Global_EXC_40Hz_smoothing25ms_causalkernel``).
    metadata_fallbacks : list[str | Path] | None
        Extra ScanImage metadata JSON files to try, in order, if the raw
        TIFF has no embedded metadata. ``<raw stem>_metadata.json`` next
        to the raw TIFF is always tried first.
    """
    import torch

    from photon_mosaic_pipeline.scanimage_utils import (
        get_frame_rate_from_scanimage_tiff,
    )

    from . import cascade

    dff_path = Path(input_path_dff)
    out_path = Path(output_path)
    save_folder = out_path.parent
    save_folder.mkdir(parents=True, exist_ok=True)

    # Ensure model directory exists
    model_folder = str(_CASCADE_MODEL_DIR)
    _CASCADE_MODEL_DIR.mkdir(parents=True, exist_ok=True)

    # Read frame rate from raw TIFF metadata -- shared across all datasets
    # in this session, since they all come from the same acquisition setup.
    path_raw_tiff = Path(input_path_tiff)
    candidates = [
        path_raw_tiff.with_name(path_raw_tiff.stem + "_metadata.json"),
        *(Path(p) for p in metadata_fallbacks or []),
    ]
    fallback = next((p for p in candidates if p.is_file()), None)
    frame_rate = get_frame_rate_from_scanimage_tiff(
        input_path_tiff, fallback=fallback
    )
    logger.info(f"Frame rate: {frame_rate} Hz")

    # Download model if not already present -- shared across all datasets.
    model_name = user_ops_dict.get("model_name", _DEFAULT_MODEL_NAME)
    model_path = _CASCADE_MODEL_DIR / model_name
    if model_path.exists() and any(model_path.glob("*.pth")):
        logger.info(f"Model '{model_name}' already exists, skipping download.")
    else:
        if model_path.exists():
            import shutil

            logger.info(f"Removing incomplete model directory: {model_path}")
            shutil.rmtree(model_path)
        logger.info(f"Downloading model '{model_name}'...")
        cascade.download_model(
            model_name, model_folder=model_folder, verbose=1
        )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Running Cascade on device: {device}")

    # Side effect: also run Cascade on any per-dataset split dFF traces,
    # if present.
    dset_dir = dff_path.parent / "dset_separated"
    if not dset_dir.is_dir():
        # Load combined dFF traces and run prediction
        traces = np.load(dff_path)
        logger.info(
            f"Loaded dFF traces: {traces.shape[0]} neurons, "
            f"{traces.shape[1]} timepoints"
        )
        spike_prob = _predict(
            cascade, model_name, traces, model_folder, device
        )
        logger.info(f"Spike probability matrix shape: {spike_prob.shape}")

        np.save(out_path, spike_prob)
        logger.info(f"Saved spike probabilities to {out_path}")

        return

    out_dset_dir = save_folder / "dset_separated"
    out_dset_dir.mkdir(parents=True, exist_ok=True)

    per_dset = []
    for dff_file in sorted(
        dset_dir.glob("dFF_dset*.npy"),
        key=lambda p: int(p.stem[len("dFF_dset") :]),
    ):
        idx = dff_file.stem[len("dFF_dset") :]

        traces_i = np.load(dff_file)
        logger.info(
            f"[dset{idx}] Loaded dFF traces: {traces_i.shape[0]} neurons, "
            f"{traces_i.shape[1]} timepoints"
        )
        spike_prob_i = _predict(
            cascade, model_name, traces_i, model_folder, device
        )
        logger.info(
            f"[dset{idx}] Spike probability matrix shape: "
            f"{spike_prob_i.shape}"
        )

        out_file = out_dset_dir / f"spike_prob_dset{idx}.npy"
        np.save(out_file, spike_prob_i)
        logger.info(f"Saved spike probabilities to {out_file}")
        per_dset.append(spike_prob_i)

    np.save(out_path, np.concatenate(per_dset, axis=1))
    logger.info(f"Saved combined spike probabilities to {out_path}")
