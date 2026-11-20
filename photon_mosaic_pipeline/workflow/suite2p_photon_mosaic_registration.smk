from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule suite2p_pm_motion_correction:
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
        json=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p_pm_motion_correction"
            / "binary.json"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.photon_mosaic_suite2p_run import run_suite2p_registration
        from pathlib import Path

        run_suite2p_registration(
            str(Path(output.json).resolve().parent),
            input.tiffs,
            **config.get("registration", {}),
        )
