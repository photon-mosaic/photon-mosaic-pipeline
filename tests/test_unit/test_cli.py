"""
Unit tests for the CLI module.
"""

from photon_mosaic.cli import build_snakemake_command


def test_unlock_argument_is_appended_when_lock_exists(
    snake_test_env, cli_args
):
    """Test that --unlock is correctly appended when lock files exist."""

    workdir = snake_test_env["workdir"]
    configfile = snake_test_env["configfile"]

    # Create a fake .snakemake/locks directory to simulate a locked workflow
    snakemake_dir = workdir / ".snakemake"
    locks_dir = snakemake_dir / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)

    # Create a fake lock file to simulate a locked workflow
    lock_file = locks_dir / "0.preprocessing.lock"
    lock_file.write_text("locked")
    # We do not need to clean up as the test environment is temporary

    # Build the command with lock present
    cmd_with_lock = build_snakemake_command(cli_args, configfile, workdir)

    assert (
        "--unlock" in cmd_with_lock
    ), "Expected --unlock flag when lock files are present"


def test_unlock_argument_not_appended_when_no_lock(snake_test_env, cli_args):
    """Test that --unlock is not appended when no lock files exist."""

    workdir = snake_test_env["workdir"]
    configfile = snake_test_env["configfile"]

    # Build the command without any lock files
    cmd_without_lock = build_snakemake_command(cli_args, configfile, workdir)

    assert (
        "--unlock" not in cmd_without_lock
    ), "Did not expect --unlock flag when no lock files are present"
