import numpy as np
from pathlib import Path
from typing import Optional


def _get_ops(plane_dir: Path) -> Optional[dict]:
    """Load the ops dictionary for a given plane directory, if it exists.
    Parameters
    ----------
    plane_dir : Path
        The path to the plane directory containing the ops.npy file.
    Returns
    -------
    dict or None
        The loaded ops dictionary if it exists, otherwise None.
    """
    ops_path = plane_dir / "ops.npy"
    if not ops_path.exists():
        return None
    return np.load(ops_path, allow_pickle=True).item()


def _split_boundaries(ops: dict) -> Optional[np.ndarray]:
    """Cumulative frame boundaries per raw TIFF Suite2p combined, or None
    if there's only one (nothing to split).
    Parameters
    ----------
    ops : dict
        The Suite2p ops dictionary containing the 'frames_per_file' key.
    Returns
    -------
    np.ndarray or None
        The cumulative frame boundaries if there are multiple TIFFs,
        otherwise None.
    """
    frames_per_file = ops.get("frames_per_file")
    if frames_per_file is None or len(frames_per_file) <= 1:
        return None
    return np.cumsum([0, *frames_per_file])


def _check_total_frames(boundaries: np.ndarray, F: np.ndarray, plane_dir: Path) -> None:
    """Raise if frames_per_file's total doesn't match F's frame count.

    Parameters
    ----------
    boundaries : np.ndarray
        The cumulative frame boundaries derived from frames_per_file.
    F : np.ndarray
        The Suite2p F array with shape (num_cells, num_frames).
    plane_dir : Path
        The path to the plane directory containing the Suite2p outputs.
    Raises
    ------
    ValueError
        If the total number of frames in boundaries does not match F.shape[1].
    """
    if boundaries[-1] != F.shape[1]:
        raise ValueError(
            f"Sum of frames_per_file ({boundaries[-1]}) does not match "
            f"F.shape[1] ({F.shape[1]}) in {plane_dir}."
        )

def _open_suite2p_outputs(plane_dir: Path) -> dict:
    """Open the main Suite2p output arrays for a given plane directory.
    Parameters
    ----------
    plane_dir : Path
        The path to the plane directory containing the Suite2p outputs.
    Returns
    -------
    dict
        A dictionary containing the main Suite2p output arrays.
    """
    return {
        "F": np.load(plane_dir / "F.npy"),
        "Fneu": np.load(plane_dir / "Fneu.npy"),
        "spks": np.load(plane_dir / "spks.npy"),
        "stat": np.load(plane_dir / "stat.npy", allow_pickle=True),
        "iscell": np.load(plane_dir / "iscell.npy"),
    }

def _save_split_chunk(
    out_dir: Path, i: int, start: int, end: int, arrays: dict, ops: dict
) -> None:
    """Save split suite2p output for each chunk/tiff.
    Parameters
    ----------
    out_dir : Path
        The output directory where the split files will be saved.
    i : int
        The index of the current split chunk.
    start : int
        The starting frame index for the current chunk.
    end : int
        The ending frame index for the current chunk.
    arrays : dict
        A dictionary containing the main Suite2p output arrays.
    ops : dict
        The Suite2p ops dictionary.
    """
    sliceable_arrays = {"F", "Fneu", "spks"}
    for name, arr in arrays.items():
        chunk = arr[:, start:end] if name in sliceable_arrays else arr
        np.save(out_dir / f"{name}_dset{i}.npy", chunk)
    np.save(out_dir / f"ops_dset{i}.npy", dict(ops, nframes=int(end - start)))


def split_suite2p_output(save_folder: Path) -> None:
    """Split each plane's combined Suite2p output back into one set of
    files per raw TIFF that Suite2p combined into it.
    Parameters
    ----------
    save_folder : Path
        The folder containing the plane directories with combined Suite2p outputs.
    Returns
    -------
    None
        The function saves the split Suite2p outputs into separate directories for each plane.
    """
    for plane_dir in sorted(save_folder.glob("plane*")):
        ops = _get_ops(plane_dir)
        if ops is None:
            continue
        boundaries = _split_boundaries(ops)
        if boundaries is None:
            continue

        outputs = _open_suite2p_outputs(plane_dir)
        _check_total_frames(boundaries, outputs["F"], plane_dir)

        out_dir = plane_dir / "dset_separated"
        out_dir.mkdir(parents=True, exist_ok=True)

        for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:])):
            _save_split_chunk(out_dir, i, start, end, outputs, ops)
