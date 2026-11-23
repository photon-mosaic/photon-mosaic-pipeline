from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule suite2p_pm_roi_extraction:
    input:
        registered=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p_pm_motion_correction"
            / "traces_cached_seg0.raw"
        ),
    output:
        # written last by run_segmentation, after the ROIs are saved
        log=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p_pm_roi_extraction"
            / "segmentation_log.json"
        ),
        roi_analyzer=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p_pm_roi_extraction"
            / "roi_analyzer"
            / "photon_mosaic_info.json"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.photon_mosaic_suite2p_roi_extraction import run_suite2p_roi_extraction
        from pathlib import Path

        run_suite2p_roi_extraction(
            str(Path(output.log).resolve().parent),
            str(Path(input.registered).resolve().parent),
            **config.get("segmentation", {}),
        )
