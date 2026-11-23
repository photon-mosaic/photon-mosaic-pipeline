"""
Neuropil Correction Module

Neuropil correction with photon-mosaic's ROI analyzer, on the ROIs from the
ROI extraction step.

The neuropil rule:
- Computes a neuropil surround mask for each ROI
- Extracts each ROI's fluorescence with the neuropil subtracted
- Saves both inside the ROI analyzer (extensions/neuropil/, extensions/fluorescence/)
- Supports SLURM cluster execution with configurable resources

Input:  ROI analyzer (funcimg/roi_analyzer/)
Output: roi_analyzer/extensions/fluorescence/fluorescence.npy (frames x ROIs)
"""

from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule neuropil:
    input:
        roi_analyzer=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "roi_analyzer"
            / "photon_mosaic_info.json"
        ),
    output:
        fluorescence=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "roi_analyzer"
            / "extensions"
            / "fluorescence"
            / "fluorescence.npy"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.neuropil_run import (
            calculate_neuropil_correction,
        )
        from pathlib import Path

        calculate_neuropil_correction(
            str(Path(input.roi_analyzer).resolve().parent),
            **config.get("neuropil_ops", {}),
        )
