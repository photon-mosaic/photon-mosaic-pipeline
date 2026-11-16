"""
Motion Correction Module

The motion_correction rule:
- Takes the preprocessed TIFFs of a session, one epoch per TIFF
- Estimates the motion with the backend in config["motion_correction"]
- Saves the registered movie as a photon-mosaic binary folder, whose
  provenance.json also holds the estimated motion

Input: Preprocessed TIFF files from the preprocessing step
Output: Registered movie in the motion_correction/ directory
"""


rule motion_correction:
    input:
        tiffs=lambda wildcards: [
            cross_platform_path(
                project_path
                / _DERIVATIVES
                / wildcards.subject_name
                / wildcards.session_name
                / "funcimg"
                / (output_pattern + p.name)
            )
            for p in all_selected_tiff_paths
            if p.parts[p.parts.index(_RAWDATA) + 1] == wildcards.subject_name
            and p.parts[p.parts.index(_RAWDATA) + 2] == wildcards.session_name
        ],
    output:
        session_path(_RULE_OUTPUTS["motion_correction"]),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.motion_correction_run import (
            run_motion_correction,
        )

        run_motion_correction(
            input.tiffs,
            Path(output[0]).resolve().parent,
            **config["motion_correction"],
        )
