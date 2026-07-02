import logging
import re
from pathlib import Path

import datashuttle as ds

logger = logging.getLogger(__name__)

_RAWDATA = "rawdata"
_DERIVATIVES = "derivatives"
_SUITE2P_FILES = ["F.npy", "Fneu.npy", "data.bin"]
_NEUROPIL_FILES = ["Fc.npy"]
_DFF_FILES = ["dFF.npy"]


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


def set_up_suite2p_targets(preproc_targets: list[str]) -> list[str]:
    """Generate Suite2p output target paths from preprocessed TIFF paths.

    For each preprocessed TIFF, generates the expected Suite2p output files
    (F.npy and data.bin) under a suite2p/plane0/ subdirectory.

    Parameters
    ----------
    preproc_targets : list[str]
        Preprocessed TIFF paths (output of adapt_paths_to_output_pattern).

    Returns
    -------
    list[str]
        Suite2p target paths (F.npy and data.bin for each input).
    """
    suite2p_targets: list[str] = []
    for tiff_path in preproc_targets:
        suite2p_dir = Path(tiff_path).parent / "suite2p" / "plane0"
        for fname in _SUITE2P_FILES:
            suite2p_targets.append(str(suite2p_dir / fname))

    return suite2p_targets


def set_up_neuropil_targets(preproc_targets: list[str]) -> list[str]:
    """Generate neuropil correction target paths from preprocessed TIFF paths.

    For each preprocessed TIFF, generates the expected neuropil output files
    under a neuropil/plane0/ subdirectory.

    Parameters
    ----------
    preproc_targets : list[str]
        Preprocessed TIFF paths (output of adapt_paths_to_output_pattern).

    Returns
    -------
    list[str]
        Neuropil target paths (Fc.npy for each input).
    """
    neuropil_targets: list[str] = []
    for tiff_path in preproc_targets:
        neuropil_dir = Path(tiff_path).parent / "neuropil" / "plane0"
        for fname in _NEUROPIL_FILES:
            neuropil_targets.append(str(neuropil_dir / fname))

    return neuropil_targets


def set_up_dff_targets(preproc_targets: list[str]) -> list[str]:
    """Generate dF/F target paths from preprocessed TIFF paths.

    For each preprocessed TIFF, generates the expected dFF output files
    under a dff/plane0/ subdirectory.

    Parameters
    ----------
    preproc_targets : list[str]
        Preprocessed TIFF paths (output of adapt_paths_to_output_pattern).

    Returns
    -------
    list[str]
        dFF target paths (dFF.npy for each input).
    """
    dff_targets: list[str] = []
    for tiff_path in preproc_targets:
        dff_dir = Path(tiff_path).parent / "dff" / "plane0"
        for fname in _DFF_FILES:
            dff_targets.append(str(dff_dir / fname))

    return dff_targets
