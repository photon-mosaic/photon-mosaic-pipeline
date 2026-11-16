"""End-to-end tests for the pipeline, driven through the CLI.

Everything here goes through the ``photon-mosaic-pipeline`` CLI rather than
invoking ``snakemake`` directly. The CLI is the only entry point users have,
and it is a thin wrapper around snakemake, so a second set of tests calling
snakemake by hand duplicated the same DAG for no extra coverage -- one of the
CI costs identified in issue #74.
"""

import shutil
from pathlib import Path

import yaml

FAILURE_MARKERS = (
    "Error in rule",
    "Exiting because a job execution failed",
    "WorkflowError",
)


def read_snakemake_log(workdir):
    """Return the contents of the most recent snakemake log.

    The CLI sends snakemake's stdout/stderr to
    ``derivatives/photon-mosaic-pipeline/logs/snakemake_<timestamp>.log``
    rather than to the caller's stdout, so this is where the job listing and
    any workflow error live.
    """
    logs_dir = (
        Path(workdir) / "derivatives" / "photon-mosaic-pipeline" / "logs"
    )
    logs = sorted(logs_dir.glob("snakemake_*.log"))
    assert logs, f"No snakemake log written under {logs_dir}"
    return logs[-1].read_text(encoding="utf-8", errors="replace")


def assert_no_workflow_error(workdir):
    """Assert the latest snakemake run reported no failure.

    ``cli.main()`` logs snakemake's exit code but does not propagate it, so
    the CLI process exits 0 even when the workflow fails -- asserting on
    ``result.returncode`` would pass unconditionally. Snakemake is not
    pinned, so this matches on failure markers rather than on a
    version-specific success message; the positive signal is the output
    files themselves.
    """
    log = read_snakemake_log(workdir)
    for marker in FAILURE_MARKERS:
        assert (
            marker not in log
        ), f"Snakemake reported a failure ({marker!r}):\n{log}"
    return log


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


def suite2p_outputs(workdir, map_of_tiffs):
    """Paths suite2p would write, one per subject/session in the fixture."""
    return [
        workdir
        / "derivatives"
        / subject_session.split("/")[0]
        / subject_session.split("/")[1]
        / "funcimg"
        / "suite2p"
        / "plane0"
        / "F.npy"
        for subject_session in map_of_tiffs
    ]


def test_photon_mosaic_pipeline_cli_dry_run(
    snake_test_env, run_photon_mosaic_pipeline
):
    """A dry run resolves the DAG and executes nothing.

    The CLI still writes its own config snapshot and log; what must not
    appear is any rule output.
    """
    run_photon_mosaic_pipeline(
        snake_test_env["workdir"],
        snake_test_env["configfile"],
        dry_run=True,
    )

    log = read_snakemake_log(snake_test_env["workdir"])
    print(f"\n=== Snakemake dry-run log ===\n{log}")

    assert (
        "This was a dry-run" in log
    ), f"Snakemake did not report a dry run:\n{log}"
    for marker in FAILURE_MARKERS:
        assert (
            marker not in log
        ), f"Dry run reported a failure ({marker!r}):\n{log}"

    # The DAG should plan work for every session in the fixture.
    n_sessions = len(snake_test_env["map_of_tiffs"])
    assert log.count("rule suite2p:") == n_sessions, (
        f"Expected {n_sessions} planned suite2p jobs, "
        f"found {log.count('rule suite2p:')}\n{log}"
    )

    # And it should not have executed any of it. This is the assertion the
    # old test could not make: the helper never passed --dry-run, so the
    # "dry run" test ran the full pipeline (issue #74).
    for output in suite2p_outputs(
        snake_test_env["workdir"], snake_test_env["map_of_tiffs"]
    ):
        assert (
            not output.exists()
        ), f"Dry run produced an output file: {output}"


def test_photon_mosaic_pipeline_cli(
    snake_test_env, run_photon_mosaic_pipeline
):
    """Test the photon-mosaic-pipeline CLI end-to-end."""
    run_photon_mosaic_pipeline(
        snake_test_env["workdir"],
        snake_test_env["configfile"],
    )

    assert_no_workflow_error(snake_test_env["workdir"])

    check_output_files(
        snake_test_env["workdir"],
        snake_test_env["map_of_tiffs"],
    )


def test_cli_with_contrast(
    snake_test_env, test_config_with_contrast, run_photon_mosaic_pipeline
):
    """
    Test that the pipeline runs with contrast enhancement preprocessing.
    """
    config = test_config_with_contrast.copy()
    config["project_path"] = str(snake_test_env["workdir"])

    config_path = Path(snake_test_env["workdir"]) / "config.yaml"
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_style='"', allow_unicode=True)

    run_photon_mosaic_pipeline(snake_test_env["workdir"], config_path)

    assert_no_workflow_error(snake_test_env["workdir"])

    check_output_files(
        snake_test_env["workdir"],
        snake_test_env["map_of_tiffs"],
        check_enhanced=True,
    )


def test_incremental_processing(snake_test_env, run_photon_mosaic_pipeline):
    """Test that adding a new TIFF only triggers processing of the new file."""
    # First run: process initial data
    run_photon_mosaic_pipeline(
        snake_test_env["workdir"],
        snake_test_env["configfile"],
    )
    assert_no_workflow_error(snake_test_env["workdir"])

    print("\n=== Initial run completed successfully ===")

    # Add a new TIFF to the first subject/session
    rawdata_path = snake_test_env["workdir"] / "rawdata"
    first_subject = sorted(rawdata_path.glob("sub-*"))[0]
    first_session = sorted(first_subject.glob("ses-*"))[0]
    funcimg_path = first_session / "funcimg"

    new_tiff = funcimg_path / "recording_new.tif"
    master_tiff = Path(__file__).parent.parent / "data" / "master.tif"
    shutil.copy2(master_tiff, new_tiff)

    print(f"\n=== Added new TIFF: {new_tiff} ===")

    # Dry-run to see what the pipeline plans to do
    run_photon_mosaic_pipeline(
        snake_test_env["workdir"],
        snake_test_env["configfile"],
        dry_run=True,
    )

    log = read_snakemake_log(snake_test_env["workdir"])
    for marker in FAILURE_MARKERS:
        assert (
            marker not in log
        ), f"Dry run reported a failure ({marker!r}):\n{log}"

    print(f"\n=== Dry-run log ===\n{log}")

    preprocessing_jobs = log.count("rule preprocessing:")

    print(f"\n=== Preprocessing jobs to run: {preprocessing_jobs} ===")

    assert preprocessing_jobs == 1, (
        f"Expected only 1 preprocessing job for the new TIFF, "
        f"but found {preprocessing_jobs} jobs.\n"
        f"Dry-run log:\n{log}"
    )

    assert (
        "recording_new.tif" in log
    ), "Expected to find recording_new.tif in the dry-run log"

    print("\n=== Test passed: Only new TIFF will be processed ===")
