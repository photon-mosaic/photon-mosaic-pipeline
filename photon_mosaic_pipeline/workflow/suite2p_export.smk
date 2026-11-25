"""
suite2p Export Module

The suite2p_export rule:
- Takes the RoiAnalyzer once all its traces are computed
- Writes the ROIs and traces in suite2p's output format, so the session can
  be opened in the suite2p GUI

Input: RoiAnalyzer with the df_over_f extension in roi_analyzer/
Output: stat.npy, iscell.npy, F.npy, Fneu.npy, spks.npy and ops.npy in
        suite2p/plane0/
"""


rule suite2p_export:
    input:
        previous=session_path(_RULE_OUTPUTS["dff"]),
    output:
        session_path(_RULE_OUTPUTS["suite2p_export"]),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.suite2p_export_run import (
            export_to_suite2p,
        )

        # suite2p/plane0/ops.npy -> plane0 -> suite2p -> funcimg
        export_to_suite2p(
            Path(output[0]).resolve().parents[2],
            Path(output[0]).resolve().parent,
        )
