"""
Fluorescence Module

The fluorescence rule:
- Takes the RoiAnalyzer with its neuropil extension
- Computes its fluorescence extension, the neuropil-corrected trace of each
  ROI, with the parameters in config["fluorescence"]

Input: RoiAnalyzer with the neuropil extension in roi_analyzer/
Output: roi_analyzer/extensions/fluorescence/fluorescence.npy
"""


rule fluorescence:
    input:
        previous=session_path(_RULE_OUTPUTS["neuropil"]),
    output:
        session_path(_RULE_OUTPUTS["fluorescence"]),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.roi_analyzer_run import (
            compute_extension,
        )

        compute_extension(
            Path(output[0]).resolve().parents[2],
            "fluorescence",
            config.get("fluorescence"),
            config.get("split_multitiff", False),
        )
