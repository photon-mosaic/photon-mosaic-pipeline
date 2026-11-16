"""
Segmentation Module

The segmentation rule:
- Takes the registered movie from the motion correction step
- Detects ROIs with the backend in config["segmentation"]
- Saves the ROIs in segmentation/rois/, next to the settings and the run log

Input: Registered movie in the motion_correction/ directory
Output: ROIs in the segmentation/ directory
"""


rule segmentation:
    input:
        registered=session_path(_RULE_OUTPUTS["motion_correction"]),
    output:
        session_path(_RULE_OUTPUTS["segmentation"]),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.segmentation_run import detect_rois

        detect_rois(
            Path(input.registered).resolve().parent,
            Path(output[0]).resolve().parent,
            **config["segmentation"],
        )
