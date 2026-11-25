import logging
import re
from pathlib import Path

import datashuttle as ds

logger = logging.getLogger(__name__)

_RAWDATA = "rawdata"
_DERIVATIVES = "derivatives"
# The file each rule declares as its output, relative to the funcimg folder
# of a session. Each is the last file photon-mosaic writes for that step.
_RULE_OUTPUTS = {
    "motion_correction": "motion_correction/si_folder.json",
    "segmentation": "segmentation/segmentation_log.json",
    "neuropil": "roi_analyzer/extensions/neuropil/run_info.json",
    "fluorescence": "roi_analyzer/extensions/fluorescence/run_info.json",
    "dff": "roi_analyzer/extensions/df_over_f/run_info.json",
    "suite2p_export": "suite2p/plane0/ops.npy",
}


def find_raw_data_paths(
    project_path: Path,
    tiff_patterns: list[str] = ["*.tif"],
    exclude_datasets: list[str] | None = None,
    exclude_sessions: list[str] | None = None,
) -> list[Path]:
    """Find all raw TIFF files under a NeuroBlueprint-compliant project.

    Parameters
    ----------
    project_path : Path
        Root of the project, expected to follow NeuroBlueprint format
        (i.e. contains a ``rawdata/sub-*/ses-*/funcimg/`` structure).
    tiff_patterns : list[str]
        Glob patterns for TIFF files to include (e.g. ``["*_00001.tif"]``).
    exclude_datasets : list[str] | None
        Regex patterns matched against subject folder names to exclude
        (e.g. ["sub-test", "sub-IAA.*"]).
    exclude_sessions : list[str] | None
        Regex patterns matched against session folder names to exclude
        (e.g. [".*protocol-screening.*", "ses-screening.*"]).

    Returns
    -------
    list[Path]
        Sorted list of paths to all matching TIFF files.
    """
    project_path = Path(project_path)
    ds.validate_project_from_path(
        project_path,
        display_mode="error",
        strict_mode=True,
        allow_letters_in_sub_ses_values=True,
    )

    raw_data_paths: list[Path] = []
    for pattern in tiff_patterns:
        raw_data_paths.extend(
            project_path.rglob(f"{_RAWDATA}/sub-*/ses-*/funcimg/{pattern}")
        )
    raw_data_paths = sorted(set(raw_data_paths))

    if exclude_datasets:
        raw_data_paths = [
            p
            for p in raw_data_paths
            if not any(
                re.fullmatch(pat, p.parts[p.parts.index(_RAWDATA) + 1])
                for pat in exclude_datasets
            )
        ]

    if exclude_sessions:
        raw_data_paths = [
            p
            for p in raw_data_paths
            if not any(
                re.fullmatch(pat, p.parts[p.parts.index(_RAWDATA) + 2])
                for pat in exclude_sessions
            )
        ]

    logger.info(
        f"Found {len(raw_data_paths)} TIFF file(s) "
        f"under {project_path / _RAWDATA}"
    )
    return raw_data_paths


def adapt_paths_to_output_pattern(
    all_selected_tiff_paths: list[Path],
    output_pattern: str,
) -> list[str]:
    """Convert raw TIFF paths to their expected preprocessed output paths.

    Replaces the rawdata directory component with derivatives and prepends
    output_pattern to the filename.

    Parameters
    ----------
    all_selected_tiff_paths : list[Path]
        Raw TIFF paths, expected to contain a rawdata component.
    output_pattern : str
        String to prepend to the output filename (e.g. ``motion_corrected_``).

    Returns
    -------
    list[str]
        Corresponding output paths under the derivatives directory.

    Raises
    ------
    ValueError
        If a path does not contain a rawdata component.
    """
    output_paths: list[str] = []
    for file_path in all_selected_tiff_paths:
        parts = file_path.parts
        try:
            rawdata_idx = parts.index(_RAWDATA)
        except ValueError:
            raise ValueError(
                f"Expected '{_RAWDATA}' in path but not found: {file_path}"
            )

        new_filename = f"{output_pattern}{file_path.name}"
        output_path = (
            Path(*parts[:rawdata_idx])
            / _DERIVATIVES
            / Path(*parts[rawdata_idx + 1 : -1])
            / new_filename
        )
        output_paths.append(str(output_path))

    return output_paths


def set_up_session_targets(preproc_targets: list[str]) -> list[str]:
    """Generate the output target paths of every session from the
    preprocessed TIFF paths.

    Parameters
    ----------
    preproc_targets : list[str]
        Preprocessed TIFF paths (output of adapt_paths_to_output_pattern).

    Returns
    -------
    list[str]
        One target per rule in ``_RULE_OUTPUTS`` for each session.
    """
    session_folders = sorted({Path(tiff).parent for tiff in preproc_targets})
    return [
        str(folder / output)
        for folder in session_folders
        for output in _RULE_OUTPUTS.values()
    ]
