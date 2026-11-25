"""
dF/F Calculation Module

The dff rule:
- Takes the RoiAnalyzer with its fluorescence extension
- Computes its df_over_f extension with the parameters in config["dff"]

Input: RoiAnalyzer with the fluorescence extension in roi_analyzer/
Output: roi_analyzer/extensions/df_over_f/df_over_f.npy and f0.npy
"""


rule dff:
    input:
        previous=session_path(_RULE_OUTPUTS["fluorescence"]),
    output:
        session_path(_RULE_OUTPUTS["dff"]),
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
            "df_over_f",
            config.get("dff"),
            config.get("split_multitiff", False),
        )
