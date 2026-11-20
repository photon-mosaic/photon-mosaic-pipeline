from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


def _registration_channels(ops):
    """(functional, align) channels, 1-based, from suite2p-style ops.

    Both None for single-channel data. Supports suite2p 1.x
    ``align_by_chan2`` (align on the non-functional channel) and the older
    ``align_by_chan`` (explicit channel number).
    """
    if ops.get("nchannels", 1) == 1:
        return None, None
    functional = ops.get("functional_chan", 1)
    if "align_by_chan" in ops:
        align = ops["align_by_chan"]
    elif ops.get("align_by_chan2", False):
        align = 2 if functional == 1 else 1
    else:
        align = functional
    return functional, align


def _session_raw_tiffs(wildcards):
    return [
        p
        for p in all_selected_tiff_paths
        if p.parts[p.parts.index(_RAWDATA) + 1] == wildcards.subject_name
        and p.parts[p.parts.index(_RAWDATA) + 2] == wildcards.session_name
    ]


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
            for p in _session_raw_tiffs(wildcards)
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

        output_path = Path(output.json).resolve().parent
        functional_channel, align_channel = _registration_channels(
            config.get("suite2p_ops", {})
        )
        run_suite2p_registration(
            str(output_path),
            input.tiffs,
            functional_channel=functional_channel,
            align_channel=align_channel,
        )
