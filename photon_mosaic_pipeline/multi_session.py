"""
Combine several sessions into one merged session for a single Suite2p run.
"""

import logging
import re
from pathlib import Path

from photon_mosaic_pipeline.paths_selection import (
    _DERIVATIVES,
    _PIPELINE_DIR,
    _RAWDATA,
)

# Session order comes from a "-t<N>" suffix (ses-2LM001_date-...-t0) or,
# failing that, a numeric session label (ses-001_date-...)
_T_SUFFIX = re.compile(r"-t(\d+)$")
_SES_NUMBER = re.compile(r"^ses-(\d+)(?:_|$)")


def _is_session(name):
    return bool(_T_SUFFIX.search(name) or _SES_NUMBER.match(name))


def _alphanumeric(name):
    return re.sub(r"[^A-Za-z0-9]", "", name)


def order_sessions(folders):
    """Keep session folders and order them by their session number.

    Parameters
    ----------
    folders : list[Path]
        Candidate session folders. If any ends in ``-t<N>``, only those
        are kept and ordered by N; otherwise folders named
        ``ses-<NNN>_...`` (NNN all digits) are ordered by NNN. Other
        folders are skipped.

    Returns
    -------
    dict[Path, int]
        Session folder -> session number, in ascending order.
    """
    logger = logging.getLogger(__name__)

    if any(_T_SUFFIX.search(f.name) for f in folders):
        pattern, label = _T_SUFFIX.search, "-t<N> suffix"
    else:
        pattern, label = _SES_NUMBER.match, "ses-<NNN> name"

    order = {}
    for folder in folders:
        match = pattern(folder.name)
        if match is None:
            logger.info(f"Skipping folder without {label}: {folder}")
            continue
        order[folder] = int(match.group(1))

    if not order:
        raise FileNotFoundError(
            f"No session folders ending in -t<N> or named ses-<NNN> among: "
            f"{folders}"
        )
    if len(set(order.values())) != len(order):
        raise ValueError(f"Duplicate session numbers in sessions: {order}")

    return dict(sorted(order.items(), key=lambda item: item[1]))


def merged_session_name(session_order):
    """NeuroBlueprint session name for a set of ordered sessions.

    Parameters
    ----------
    session_order : dict[Path, int]
        Session folder -> session number, as returned by
        ``order_sessions``.

    Returns
    -------
    str
        For ``-t<N>`` sessions, ``<base>_merge-t<first>to<last>`` for
        consecutive indices, otherwise every index is listed
        (``<base>_merge-t0t1t5``). ``<base>`` is the first session's name
        without its ``-t<N>`` suffix, up to its first underscore (e.g.
        ``ses-2LM001``). For ``ses-<NNN>`` sessions, ``ses-001to003`` or
        ``ses-001and003and010``, keeping each label's zero padding.
    """
    first = next(iter(session_order))
    indices = list(session_order.values())
    consecutive = indices == list(range(indices[0], indices[-1] + 1))

    if not _T_SUFFIX.search(first.name):
        labels = [_SES_NUMBER.match(s.name).group(1) for s in session_order]
        if consecutive:
            return f"ses-{labels[0]}to{labels[-1]}"
        return "ses-" + "and".join(labels)

    base = _T_SUFFIX.sub("", first.name).split("_")[0]
    if not base.startswith("ses-"):
        base = "ses-" + _alphanumeric(base)
    if consecutive:
        tag = f"t{indices[0]}to{indices[-1]}"
    else:
        tag = "".join(f"t{i}" for i in indices)
    return f"{base}_merge-{tag}"


def subject_folder_name(name):
    """NeuroBlueprint-valid subject folder name for a session's parent.

    Parameters
    ----------
    name : str
        Name of the folder holding the session folders.

    Returns
    -------
    str
        ``name`` if it is already a ``sub-`` folder, otherwise ``sub-``
        followed by its letters and digits (``tests-multi-session`` ->
        ``sub-testsmultisession``).
    """
    if name.startswith("sub-"):
        return name
    return "sub-" + _alphanumeric(name)


def find_session_folders(session_args):
    """Candidate session folders from the ``--multi-session-path`` values.

    Parameters
    ----------
    session_args : list[str]
        A single main folder (its ``rawdata/`` is searched if present,
        else the folder itself), or several session folders.

    Returns
    -------
    list[Path]
        Several values are returned as given. For a single folder, its
        subfolders are returned, plus the subfolders of any that aren't
        sessions themselves, so sessions may sit inside subject folders.
    """
    paths = [Path(p).resolve() for p in session_args]
    if len(paths) > 1:
        return paths

    root = paths[0]
    if (root / _RAWDATA).is_dir():
        root = root / _RAWDATA
    folders = []
    for folder in sorted(p for p in root.iterdir() if p.is_dir()):
        folders.append(folder)
        if not _is_session(folder.name):
            folders.extend(p for p in folder.iterdir() if p.is_dir())
    return folders


def main_folder(session):
    """Folder holding ``rawdata/`` for a session, where output goes.

    Parameters
    ----------
    session : Path
        A session folder, at ``<main>/rawdata/<session>`` or
        ``<main>/rawdata/<subject>/<session>``.

    Returns
    -------
    Path
        ``<main>``, or the session's grandparent if no ``rawdata`` folder
        is found above it.
    """
    for folder in session.parents[:2]:
        if folder.name == _RAWDATA:
            return folder.parent
    return session.parents[1]


def _subject_name(session):
    """Name a session's subject folder is derived from: its parent, or
    ``<main>`` for sessions directly in ``rawdata/``."""
    if session.parent.name == _RAWDATA:
        return main_folder(session).name
    return session.parent.name


def drop_excluded(folders, exclude_datasets=None, exclude_sessions=None):
    """Remove folders matching the ``dataset_discovery`` exclude patterns.

    Parameters
    ----------
    folders : list[Path]
        Candidate session folders.
    exclude_datasets : list[str] | None
        Regex patterns fully matched against each folder's subject name
        (see ``_subject_name``), as in ``find_raw_data_paths``.
    exclude_sessions : list[str] | None
        Regex patterns fully matched against each session folder name.

    Returns
    -------
    list[Path]
        ``folders`` without the excluded ones, in the same order.
    """
    logger = logging.getLogger(__name__)

    def excluded(folder):
        return any(
            re.fullmatch(pat, _subject_name(folder))
            for pat in exclude_datasets or []
        ) or any(
            re.fullmatch(pat, folder.name) for pat in exclude_sessions or []
        )

    kept = []
    for folder in folders:
        if excluded(folder):
            logger.info(f"Excluding folder by config pattern: {folder}")
        else:
            kept.append(folder)
    return kept


def link_multi_session(session_args, config):
    """Symlink TIFFs from several sessions into one merged session.

    Parameters
    ----------
    session_args : list[str]
        A single main folder, or several session folders; see
        ``find_session_folders``.
    config : dict
        Configuration dictionary, used for ``dataset_discovery``'s
        ``tiff_patterns``, ``exclude_datasets`` and ``exclude_sessions``.

    Returns
    -------
    tuple[Path, Path]
        Root of a staging project holding
        ``rawdata/<subject>/<merged session>/funcimg/`` with the linked
        TIFFs, and ``<main>/derivatives``, where outputs go (so they land
        in ``<main>/derivatives/<subject>/<merged session>/``).
    """
    logger = logging.getLogger(__name__)

    discovery = config["dataset_discovery"]
    folders = drop_excluded(
        find_session_folders(session_args),
        discovery.get("exclude_datasets"),
        discovery.get("exclude_sessions"),
    )
    session_order = order_sessions(folders)
    sessions = list(session_order)

    main = main_folder(sessions[0])
    subjects = {s.parent for s in sessions}
    if len(subjects) > 1:
        raise ValueError(
            f"Sessions span several subjects {sorted(subjects)}; "
            "merge one subject at a time."
        )
    subject = subject_folder_name(_subject_name(sessions[0]))
    # Glob sessions directly: their names (e.g. date-...-t0) need not pass
    # NeuroBlueprint validation, only the merged project does.
    tiff_patterns = discovery.get("tiff_patterns", ["*.tif"])
    tiffs = sorted(
        {
            tiff
            for session in sessions
            for pattern in tiff_patterns
            for tiff in (session / "funcimg").glob(pattern)
        }
    )
    if not tiffs:
        raise FileNotFoundError(f"No TIFFs found in sessions: {sessions}")

    # Links are staged under derivatives/ so rawdata/ is left untouched
    session_name = merged_session_name(session_order)
    derivatives = main / _DERIVATIVES
    staging = (
        derivatives
        / _PIPELINE_DIR
        / "multisession"
        / f"{subject}_{session_name}"
    )
    funcimg = staging / _RAWDATA / subject / session_name / "funcimg"
    funcimg.mkdir(parents=True, exist_ok=True)

    # Prefix with t-index (suite2p natsorts files, so this sets the
    # concatenation order) and session name to avoid clashes
    links = {}
    for tiff in tiffs:
        session = tiff.parents[1]
        name = f"t{session_order[session]}_{session.name}_{tiff.name}"
        links[funcimg / name] = tiff
    # Drop links from earlier runs to TIFFs since removed or no longer
    # matched; existing links are kept so snakemake doesn't see them as new
    for old_link in funcimg.iterdir():
        if old_link.is_symlink() and old_link not in links:
            old_link.unlink()
    for link, tiff in links.items():
        if not link.is_symlink():
            link.symlink_to(tiff)
    logger.info(f"Linked {len(tiffs)} TIFF(s) into {funcimg}")

    return staging, derivatives
