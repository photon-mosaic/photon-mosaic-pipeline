"""
Neuropil Module

The neuropil rule:
- Takes the registered movie and the ROIs
- Pairs them in a photon-mosaic RoiAnalyzer saved in roi_analyzer/
- Computes its neuropil extension with the parameters in config["neuropil"]

Input: Registered movie (motion_correction/) and ROIs (segmentation/)
Output: RoiAnalyzer with the neuropil extension in roi_analyzer/
"""


rule neuropil:
    input:
        registered=session_path(_RULE_OUTPUTS["motion_correction"]),
        segmentation=session_path(_RULE_OUTPUTS["segmentation"]),
    output:
        session_path(_RULE_OUTPUTS["neuropil"]),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.roi_analyzer_run import (
            create_analyzer_with_neuropil,
        )

        create_analyzer_with_neuropil(
            Path(input.registered).resolve().parent,
            Path(input.segmentation).resolve().parent,
            Path(output[0]).resolve().parents[2],
            config.get("neuropil"),
        )
