"""
dF/F Calculation Module

dF/F with photon-mosaic's ROI analyzer, from the neuropil-corrected
fluorescence computed by the neuropil rule.

The dff rule:
- Estimates each ROI's baseline F0 and computes (F - F0) / F0
- Saves dF/F and F0 inside the ROI analyzer (extensions/df_over_f/)
- Supports SLURM cluster execution with configurable resources

Input:  roi_analyzer/extensions/fluorescence/fluorescence.npy
Output: roi_analyzer/extensions/df_over_f/df_over_f.npy (frames x ROIs)
"""

from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule dff:
    input:
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
    output:
        dFF=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "roi_analyzer"
            / "extensions"
            / "df_over_f"
            / "df_over_f.npy"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.dff_run import calculate_dFF
        from pathlib import Path

        # fluorescence.npy -> fluorescence/ -> extensions/ -> roi_analyzer/
        calculate_dFF(
            str(Path(input.fluorescence).resolve().parents[2]),
            **config.get("dff_ops", {}),
        )
