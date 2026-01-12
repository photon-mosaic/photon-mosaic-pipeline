import re
import shutil
import subprocess
from pathlib import Path

import numpy as np
import yaml
from tifffile import imread

from photon_mosaic import get_snakefile_path


def run_snakemake(workdir, configfile, dry_run=False):
    """Helper function to run snakemake with common parameters."""

    cmd = [
        "snakemake",
        "--cores",
        "1",
        "--verbose",
        "--keep-going",
        "-s",
        str(get_snakefile_path()),
        "--configfile",
        str(configfile),
        "--debug-dag",
    ]

    if dry_run:
        cmd.insert(1, "--dry-run")

    #  print the command that will be run
    print(" ".join(cmd))

    result = subprocess.run(
        cmd,
        cwd=workdir,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    return result


def check_output_files(
    workdir, datasets, map_of_tiffs, tiff_patterns=None, check_enhanced=False
):
    """Helper function to check output files."""
    # Default tiff patterns if not provided
    if tiff_patterns is None:
        tiff_patterns = ["type_1*.tif", "type_2*.tif"]

    for sub_idx, dataset in enumerate(datasets):
        # Get the tiff files for this dataset
        dataset_tiffs = map_of_tiffs[dataset]

        # Group tiff files by session based on their pattern
        session_files = {i: [] for i in range(len(tiff_patterns))}

        for tiff in dataset_tiffs:
            # Find which pattern matches this tiff file
            for ses_idx, pattern in enumerate(tiff_patterns):
                # Convert glob pattern to regex: type_1*.tif -> type_1.*\.tif
                regex_pattern = pattern.replace("*", ".*").replace(".", r"\.")
                if re.match(regex_pattern, tiff):
                    session_files[ses_idx].append(tiff)
                    break

        # Check each session that has files
        for ses_idx, tiffs in session_files.items():
            if not tiffs:  # Skip empty sessions
                continue

            for tiff in tiffs:
                output_base = (
                    workdir
                    / "derivatives"
                    / f"sub-{sub_idx}_{dataset}"
                    / f"ses-{ses_idx}"
                    / "funcimg"
                    / "suite2p"
                    / "plane0"
                )

                # Print information about the expected output files
                print(
                    "\n=== Checking files for "
                    f"{dataset}/ses-{ses_idx}/{tiff} ==="
                )
                print(f"Checking for files in: {output_base}")
                print(
                    "Directory contents:",
                    list(output_base.iterdir())
                    if output_base.exists()
                    else "Directory does not exist",
                )
                print("=== End of Expected Output Files ===\n")

                assert (output_base / "F.npy").exists(), (
                    f"Missing output: F.npy for {dataset}/ses-{ses_idx}/{tiff}"
                    f"Output directory contents: {list(output_base.iterdir())}"
                )
                assert (output_base / "data.bin").exists(), (
                    "Missing output: data.bin for "
                    f"{dataset}/ses-{ses_idx}/{tiff}"
                    f"Output directory contents: {list(output_base.iterdir())}"
                )

                if check_enhanced:
                    enhanced_file = (
                        workdir
                        / "derivatives"
                        / f"sub-{sub_idx}_{dataset}"
                        / f"ses-{ses_idx}"
                        / "funcimg"
                        / f"enhanced_{tiff}"
                    )

                    assert (
                        enhanced_file.exists()
                    ), f"Missing enhanced output: {enhanced_file}"

                    # Verify that the enhanced file contains valid image data
                    enhanced_data = imread(enhanced_file)
                    assert (
                        enhanced_data.dtype == np.int16
                    ), f"Enhanced file {enhanced_file} has "
                    f"incorrect data type: {enhanced_data.dtype}"
                    assert (
                        enhanced_data.size > 0
                    ), f"Enhanced file {enhanced_file} is empty"


def test_snakemake_dry_run(snake_test_env, check_logs):
    """Test that snakemake can do a dry run."""
    print("\n=== Starting snakemake dry run test ===")
    print(f"Working directory: {snake_test_env['workdir']}")
    print(f"Config file: {snake_test_env['configfile']}")

    # List contents of working directory
    print("\nWorking directory contents:")
    for path in Path(snake_test_env["workdir"]).glob("**/*"):
        print(f"  {path}")

    # Let's also check what the expected preprocessing output files should be
    print("\n=== Expected preprocessing outputs ===")
    expected_preprocessing_dir = (
        Path(snake_test_env["workdir"])
        / "derivatives"
        / "sub-0_001"
        / "ses-0"
        / "funcimg"
    )
    print(f"Expected preprocessing directory: {expected_preprocessing_dir}")
    if expected_preprocessing_dir.exists():
        print(
            "Directory exists, contents: "
            f"{list(expected_preprocessing_dir.iterdir())}"
        )
    else:
        print("Directory does not exist")

    result = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"], dry_run=True
    )

    print(f"\n=== Snakemake return code: {result.returncode} ===")
    print(f"=== STDOUT ===\n{result.stdout}")
    print(f"=== STDERR ===\n{result.stderr}")

    assert result.returncode == 0, (
        f"Snakemake dry-run failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    check_logs()


def test_snakemake_execution(snake_test_env, check_logs):
    """Test that snakemake can execute the workflow."""
    result = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"]
    )

    assert result.returncode == 0, (
        f"Snakemake execution failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

    #  print sterr and stdout
    print(f"STDOUT:\n{result.stdout}")
    print(f"STDERR:\n{result.stderr}")

    # Check that output files were created for each dataset and tiff
    datasets = ["001", "002", "003"]

    # Load config from file to get tiff_patterns
    with open(snake_test_env["configfile"], "r") as f:
        config = yaml.safe_load(f)

    tiff_patterns = config["dataset_discovery"]["tiff_patterns"]
    check_output_files(
        snake_test_env["workdir"],
        datasets,
        snake_test_env["map_of_tiffs"],
        tiff_patterns,
    )
    check_logs()


def test_snakemake_with_contrast(
    snake_test_env, test_config_with_contrast, check_logs
):
    """
    Test that snakemake can execute the workflow with contrast enhancement
    preprocessing.
    """
    # Update config with contrast enhancement settings
    config = test_config_with_contrast.copy()
    config["raw_data_base"] = str(Path(snake_test_env["workdir"]) / "raw_data")
    config["processed_data_base"] = str(
        Path(snake_test_env["workdir"]) / "derivatives"
    )

    # Create config file
    config_path = Path(snake_test_env["workdir"]) / "config.yaml"
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_style='"', allow_unicode=True)

    # Run snakemake with real contrast enhancement
    result = run_snakemake(snake_test_env["workdir"], config_path)
    assert result.returncode == 0, (
        f"Snakemake execution with contrast enhancement failed:\nSTDOUT:\n"
        f"{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    # Check that enhanced output files were created and contain valid image
    # data
    datasets = ["001", "002", "003"]
    tiff_patterns = config["dataset_discovery"]["tiff_patterns"]
    check_output_files(
        snake_test_env["workdir"],
        datasets,
        snake_test_env["map_of_tiffs"],
        tiff_patterns,
        check_enhanced=True,
    )
    check_logs()


def test_photon_mosaic_cli_dry_run(snake_test_env, check_logs):
    """Test that photon-mosaic can do a dry run."""
    cmd = [
        "photon-mosaic",
        "--config",
        str(snake_test_env["configfile"]),
    ]

    result = subprocess.run(
        cmd,
        cwd=snake_test_env["workdir"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert result.returncode == 0, (
        f"photon-mosaic CLI run failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    check_logs()


def test_photon_mosaic_cli(snake_test_env, check_logs):
    """Test photon-mosaic pipeline."""
    cmd = [
        "photon-mosaic",
        "--config",
        str(snake_test_env["configfile"]),
    ]

    result = subprocess.run(
        cmd,
        cwd=snake_test_env["workdir"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert result.returncode == 0, (
        f"photon-mosaic CLI run failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    check_logs()


def test_incremental_processing(snake_test_env, check_logs):
    """Test that adding a new TIFF only triggers processing of the new file.

    This test verifies that Snakemake correctly detects when files already
    exist and only processes newly added files, addressing the issue where
    the entire dataset was being reprocessed unnecessarily.
    """
    # First run: process initial data
    result = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"]
    )
    assert result.returncode == 0, (
        f"Initial Snakemake execution failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

    print("\n=== Initial run completed successfully ===")

    # Add a new TIFF file to dataset 001 (which should trigger session 1
    # processing)
    raw_data_path = Path(snake_test_env["workdir"]) / "raw_data" / "001"
    new_tiff = raw_data_path / "type_1_03.tif"

    # Copy the master TIFF file to create a new TIFF
    master_tiff = Path(__file__).parent.parent / "data" / "master.tif"
    shutil.copy2(master_tiff, new_tiff)

    print(f"\n=== Added new TIFF: {new_tiff} ===")

    # Run dry-run to see what Snakemake plans to do
    result_dry = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"], dry_run=True
    )

    assert result_dry.returncode == 0, (
        f"Dry-run after adding TIFF failed:\nSTDOUT:\n{result_dry.stdout}\n"
        f"STDERR:\n{result_dry.stderr}"
    )

    print(f"\n=== Dry-run output ===\n{result_dry.stdout}")
    print(f"\n=== Dry-run stderr ===\n{result_dry.stderr}")

    # Parse the dry-run output to check what jobs would be executed
    stdout = result_dry.stdout

    # Count how many preprocessing jobs would run
    # Look for lines like "rule preprocessing:" or job mentions
    preprocessing_jobs = stdout.count("rule preprocessing")

    print(f"\n=== Preprocessing jobs to run: {preprocessing_jobs} ===")

    # We expect only 1 preprocessing job (for the new TIFF)
    # NOT 3 jobs (which would mean reprocessing all type_1*.tif files
    # in dataset 001)
    assert preprocessing_jobs == 1, (
        f"Expected only 1 preprocessing job for the new TIFF, "
        f"but found {preprocessing_jobs} jobs. This suggests the workflow "
        f"is reprocessing existing files unnecessarily.\n"
        f"Dry-run output:\n{stdout}"
    )

    # Verify the new file would be processed
    # (check for type_1_03.tif in output)
    assert "type_1_03.tif" in stdout, (
        "Expected to find type_1_03.tif in the dry-run output, "
        "indicating the new file would be processed"
    )

    # Verify existing files are NOT being reprocessed
    # Count references to the original files - they should only appear in
    # dependency lists, not as targets to be built
    lines_with_type_1_01 = [
        line for line in stdout.split("\n") if "type_1_01.tif" in line
    ]
    lines_with_type_1_02 = [
        line for line in stdout.split("\n") if "type_1_02.tif" in line
    ]

    print(
        f"\n=== Lines mentioning"
        f" type_1_01.tif: {len(lines_with_type_1_01)} ==="
    )
    print(
        f"=== Lines mentioning "
        f"type_1_02.tif: {len(lines_with_type_1_02)} ==="
    )

    print("\n=== Test passed: Only new TIFF will be processed ===")
    check_logs()
