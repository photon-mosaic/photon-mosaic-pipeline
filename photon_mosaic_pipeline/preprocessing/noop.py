"""
No-operation preprocessing step using symlinks.

This preprocessing step creates symbolic links to the original files instead
of copying them, which is much faster and saves disk space for large files.
"""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def run(
    dataset_folder: Path,
    output_folder: Path,
    tiff_name: str,
    **kwargs,
):
    """
    No-operation preprocessing step using symlinks instead of copying.

    Creates a symbolic link to the original TIFF file in the output directory,
    avoiding the need to copy large files when no processing is required.

    Parameters
    ----------
    dataset_folder : Path
        Path to the dataset folder containing the input TIFF files.
    output_folder : Path
        Path to the output folder where symlinks will be created.
    tiff_name : str
        Name of the TIFF file to symlink.
    **kwargs : dict
        Additional keyword arguments (unused).
    """
    # Convert paths to Path objects if they're strings
    if isinstance(dataset_folder, str):
        dataset_folder = Path(dataset_folder)
    if isinstance(output_folder, str):
        output_folder = Path(output_folder)

    # Create output directory
    output_folder.mkdir(parents=True, exist_ok=True)

    # Define input and output paths
    output_file = output_folder / tiff_name

    # Skip if symlink already exists and points to the right location
    if output_file.is_symlink():
        logger.info(f"Symlink already exists: {output_file}")
        return
    elif output_file.exists():
        logger.warning(
            f"File exists but is not a symlink: {output_file}. Removing."
        )
        output_file.unlink()

    # Try to find the input file
    input_file = dataset_folder / tiff_name

    if not input_file.exists():
        # Use rglob to find the file recursively
        try:
            input_file = next(dataset_folder.rglob(tiff_name))
            logger.info(
                f"Found input file using recursive search: {input_file}"
            )
        except StopIteration:
            raise FileNotFoundError(
                f"Could not find {tiff_name} in {dataset_folder}"
            )

    # Create symlink
    try:
        output_file.symlink_to(input_file.resolve())
        logger.info(
            f"Created symlink: {output_file} -> {input_file.resolve()}"
        )
    except OSError as e:
        logger.error(f"Failed to create symlink: {e}")
        # Fallback to copying if symlink fails (e.g., cross-filesystem)
        logger.info(
            f"Falling back to copying file: {input_file} -> {output_file}"
        )
        shutil.copy2(input_file, output_file)
