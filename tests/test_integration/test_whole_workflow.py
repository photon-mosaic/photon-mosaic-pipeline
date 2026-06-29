import os
import subprocess
from pathlib import Path

import yaml

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

    print(" ".join(cmd))

    if os.getenv("CI"):
        cmd.append("--nolock")
        cmd.append("--latency-wait")
        cmd.append("30")

    result = subprocess.run(
        cmd,
        cwd=workdir,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    return result


def check_output_files(workdir, map_of_tiffs, check_enhanced=False):
    """Helper function to check output files."""
    # Print full derivatives tree for debugging
    derivatives = workdir / "derivatives"
    print("\n=== Full derivatives tree ===")
    for p in sorted(derivatives.rglob("*")):
        print(f"  {p.relative_to(workdir)}")
    print("=== End derivatives tree ===\n")

    for subject_session, tiff_files in map_of_tiffs.items():
        subject, session = subject_session.split("/")

        for tiff in tiff_files:
            output_base = (
                workdir
                / "derivatives"
                / subject
                / session
                / "funcimg"
                / "suite2p"
                / "plane0"
            )

            print(f"\n=== Checking files for {subject}/{session}/{tiff} ===")
            print(f"Checking for files in: {output_base}")
            print(
                "Directory contents:",
                list(output_base.iterdir())
                if output_base.exists()
                else "Directory does not exist",
            )
            print("=== End of Expected Output Files ===\n")

            assert (
                output_base / "F.npy"
            ).exists(), f"Missing output: F.npy for {subject}/{session}/{tiff}"
            assert (
                output_base / "data.bin"
            ).exists(), (
                f"Missing output: data.bin for {subject}/{session}/{tiff}"
            )

            if check_enhanced:
                enhanced_file = (
                    workdir
                    / "derivatives"
                    / subject
                    / session
                    / "funcimg"
                    / f"enhanced_{tiff}"
                )
                assert (
                    enhanced_file.exists()
                ), f"Missing enhanced output: {enhanced_file}"


def test_snakemake_dry_run(snake_test_env):
    """Test that snakemake can do a dry run."""
    print("\n=== Starting snakemake dry run test ===")
    print(f"Working directory: {snake_test_env['workdir']}")
    print(f"Config file: {snake_test_env['configfile']}")

    for path in Path(snake_test_env["workdir"]).glob("**/*"):
        print(f"  {path}")

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


def test_snakemake_execution(snake_test_env):
    """Test that snakemake can execute the workflow."""
    result = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"]
    )

    assert result.returncode == 0, (
        f"Snakemake execution failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

    print(f"STDOUT:\n{result.stdout}")
    print(f"STDERR:\n{result.stderr}")

    check_output_files(
        snake_test_env["workdir"],
        snake_test_env["map_of_tiffs"],
    )


def test_snakemake_with_contrast(snake_test_env, test_config_with_contrast):
    """
    Test that snakemake can execute the workflow with contrast enhancement
    preprocessing.
    """
    config = test_config_with_contrast.copy()
    config["project_path"] = str(snake_test_env["workdir"])

    config_path = Path(snake_test_env["workdir"]) / "config.yaml"
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_style='"', allow_unicode=True)

    result = run_snakemake(snake_test_env["workdir"], config_path)
    assert result.returncode == 0, (
        f"Snakemake execution with contrast enhancement failed:\nSTDOUT:\n"
        f"{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    check_output_files(
        snake_test_env["workdir"],
        snake_test_env["map_of_tiffs"],
        check_enhanced=True,
    )


def test_photon_mosaic_cli_dry_run(snake_test_env, run_photon_mosaic):
    """Test that photon-mosaic can do a dry run."""
    result = run_photon_mosaic(
        snake_test_env["workdir"],
        snake_test_env["configfile"],
    )

    assert result.returncode == 0, (
        f"photon-mosaic CLI run failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def test_photon_mosaic_cli(snake_test_env, run_photon_mosaic):
    """Test photon-mosaic pipeline."""
    result = run_photon_mosaic(
        snake_test_env["workdir"],
        snake_test_env["configfile"],
    )

    assert result.returncode == 0, (
        f"photon-mosaic CLI run failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def test_incremental_processing(snake_test_env, data_factory):
    """Test that adding a new TIFF only triggers processing of the new file."""
    # First run: process initial data
    result = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"]
    )
    assert result.returncode == 0, (
        f"Initial Snakemake execution failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

    print("\n=== Initial run completed successfully ===")

    # Add a new TIFF to the first subject/session
    rawdata_path = snake_test_env["workdir"] / "rawdata"
    first_subject = sorted(rawdata_path.glob("sub-*"))[0]
    first_session = sorted(first_subject.glob("ses-*"))[0]
    funcimg_path = first_session / "funcimg"

    new_tiff = funcimg_path / "recording_new.tif"
    master_tiff = Path(__file__).parent.parent / "data" / "master.tif"
    import shutil

    shutil.copy2(master_tiff, new_tiff)

    print(f"\n=== Added new TIFF: {new_tiff} ===")

    # Dry-run to see what Snakemake plans to do
    result_dry = run_snakemake(
        snake_test_env["workdir"], snake_test_env["configfile"], dry_run=True
    )

    assert result_dry.returncode == 0, (
        f"Dry-run after adding TIFF failed:\nSTDOUT:\n{result_dry.stdout}\n"
        f"STDERR:\n{result_dry.stderr}"
    )

    print(f"\n=== Dry-run output ===\n{result_dry.stdout}")
    print(f"\n=== Dry-run stderr ===\n{result_dry.stderr}")

    stdout = result_dry.stdout
    preprocessing_jobs = stdout.count("rule preprocessing")

    print(f"\n=== Preprocessing jobs to run: {preprocessing_jobs} ===")

    assert preprocessing_jobs == 1, (
        f"Expected only 1 preprocessing job for the new TIFF, "
        f"but found {preprocessing_jobs} jobs.\n"
        f"Dry-run output:\n{stdout}"
    )

    assert (
        "recording_new.tif" in stdout
    ), "Expected to find recording_new.tif in the dry-run output"

    print("\n=== Test passed: Only new TIFF will be processed ===")
