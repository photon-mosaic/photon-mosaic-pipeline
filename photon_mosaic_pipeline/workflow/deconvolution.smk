"""
Cascade Spike Deconvolution Module

This Snakefile module handles spike deconvolution using CascadeTorch.
It takes dF/F traces and infers spike probabilities.

The cascade rule:
- Takes dFF.npy from the dff step as input
- Reads frame rate from the raw ScanImage TIFF header
- Runs CascadeTorch spike deconvolution
- Outputs spike_prob.npy in cascade/plane0/ directory
- Supports SLURM cluster execution with configurable resources

Input:  dF/F traces (dFF.npy) in dff/plane0/ directory
Output: Spike probabilities (spike_prob.npy) in cascade/plane0/ directory
"""

from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _RAWDATA, _DERIVATIVES

_SUPPORTED_DECONVOLUTION_METHODS = ["cascade"]

deconvolution_ops = config.get("deconvolution_ops", {})
_deconvolution_method = deconvolution_ops.get("method", "cascade")
if _deconvolution_method not in _SUPPORTED_DECONVOLUTION_METHODS:
    raise ValueError(
        f"Unsupported deconvolution method '{_deconvolution_method}'. "
        f"Supported methods: {_SUPPORTED_DECONVOLUTION_METHODS}"
    )


def _get_raw_tiff_for_session(wildcards):
    """Return the first raw TIFF path for a given subject/session wildcard.

    Used to read ScanImage frame rate metadata.

    Raises
    ------
    ValueError
        If no raw TIFF is found for the given wildcards.
    """
    matches = [
        p
        for p in all_selected_tiff_paths
        if p.parts[p.parts.index(_RAWDATA) + 1] == wildcards.subject_name
        and p.parts[p.parts.index(_RAWDATA) + 2] == wildcards.session_name
    ]
    if not matches:
        raise ValueError(
            f"No raw TIFF found for subject={wildcards.subject_name}, "
            f"session={wildcards.session_name}"
        )
    return cross_platform_path(matches[0])


rule cascade:
    input:
        dFF=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "dff"
            / "plane0"
            / "dFF.npy"
        ),
        tiff=_get_raw_tiff_for_session,
    output:
        spike_prob=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "cascade"
            / "plane0"
            / "spike_prob.npy"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.deconvolution.cascade_run import run_cascade
        from pathlib import Path

        input_path_dff = Path(input.dFF).resolve()
        input_path_tiff = Path(input.tiff).resolve()
        output_path = Path(output.spike_prob).resolve()

        # Metadata JSON written by stiminterpolation next to the
        # preprocessed TIFF in derivatives, used if the raw TIFF has none.
        derivatives_metadata = (
            output_path.parents[2]
            / f"{output_pattern}{input_path_tiff.stem}_metadata.json"
        )

        run_cascade(
            str(input_path_dff),
            str(input_path_tiff),
            str(output_path),
            deconvolution_ops,
            metadata_fallbacks=[derivatives_metadata],
        )
