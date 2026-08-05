"""
Unit tests for the CLI module.
"""

from photon_mosaic_pipeline.cli import (
    build_snakemake_command,
    configure_slurm_execution,
)


def test_build_snakemake_command_basic(cli_args, snake_test_env):
    """Test that build_snakemake_command returns a valid command."""
    configfile = snake_test_env["configfile"]

    cmd = build_snakemake_command(cli_args, configfile)

    assert "snakemake" in cmd
    assert "--configfile" in cmd
    assert str(configfile) in cmd


def test_build_snakemake_command_dry_run(cli_args, snake_test_env):
    """Test that --dry-run is added when set in args."""
    import argparse

    configfile = snake_test_env["configfile"]

    dry_run_args = argparse.Namespace(
        config=str(configfile),
        jobs="1",
        dry_run=True,
        forcerun=None,
        rerun_incomplete=False,
        latency_wait=10,
        verbose=False,
    )

    cmd = build_snakemake_command(dry_run_args, configfile)

    assert "--dry-run" in cmd


# ---------------------------------------------------------------------------
# configure_slurm_execution
#
# Regression guard for commit 713390e, where the SLURM logging directives were
# silently replaced by `slurm_extra = "--nodelist=gpu-350-05"`. The tests below
# encode the contract this function must keep: log flags present, logdir under
# the project, slurm_extra built from the logdir (NOT a hardcoded nodelist),
# and rule-level GPU resources excluded from --default-resources.
# ---------------------------------------------------------------------------


def test_configure_slurm_disabled_returns_cmd_unchanged(tmp_path):
    """With use_slurm: false, the function must not touch cmd."""
    base_cmd = ["snakemake", "--configfile", "x.yaml"]
    config = {"use_slurm": False, "project_path": str(tmp_path)}

    out = configure_slurm_execution(list(base_cmd), config)

    assert out == base_cmd


def test_configure_slurm_enabled_adds_executor_and_logdir(tmp_path):
    """Enabling SLURM must add the executor flag and the keep/logdir flags."""
    config = {
        "use_slurm": True,
        "project_path": str(tmp_path),
        "slurm": {"slurm_partition": "gpu", "mem_mb": 8000},
    }

    cmd = configure_slurm_execution(["snakemake"], config)

    expected_logdir = (
        tmp_path / "derivatives" / "photon-mosaic-pipeline" / "logs" / "slurm"
    )
    assert "--executor" in cmd
    assert cmd[cmd.index("--executor") + 1] == "slurm"
    assert "--slurm-keep-successful-logs" in cmd
    assert "--slurm-logdir" in cmd
    assert cmd[cmd.index("--slurm-logdir") + 1] == str(expected_logdir)
    assert expected_logdir.is_dir()


def test_configure_slurm_extra_uses_logdir_not_nodelist(tmp_path):
    """slurm_extra must point sbatch --output / --error at the logdir.

    Regression guard for 713390e: that commit replaced this assignment with
    `slurm_extra = "--nodelist=gpu-350-05"`, dropping the logging directives
    AND pinning every job to one node. Both regressions are caught here.
    """
    config = {
        "use_slurm": True,
        "project_path": str(tmp_path),
        "slurm": {"slurm_partition": "gpu"},
    }

    cmd = configure_slurm_execution(["snakemake"], config)

    resources = cmd[cmd.index("--default-resources") + 1]
    expected_logdir = (
        tmp_path / "derivatives" / "photon-mosaic-pipeline" / "logs" / "slurm"
    )
    assert f"--output={expected_logdir}/%j_%x.out" in resources
    assert f"--error={expected_logdir}/%j_%x.err" in resources
    assert "--nodelist" not in resources


def test_configure_slurm_excludes_rule_level_gpu_resources(tmp_path):
    """gpu / gres / cpus_per_gpu live at rule level, not in defaults.

    Passing them via --default-resources triggers SLURM TRES conflicts.
    The function must strip them while keeping other slurm: keys.
    """
    config = {
        "use_slurm": True,
        "project_path": str(tmp_path),
        "slurm": {
            "slurm_partition": "gpu",
            "mem_mb": 16000,
            "gpu": 1,
            "gres": "gpu:1",
            "cpus_per_gpu": 4,
        },
    }

    cmd = configure_slurm_execution(["snakemake"], config)

    resources = cmd[cmd.index("--default-resources") + 1]
    assert "slurm_partition='gpu'" in resources
    assert "mem_mb=16000" in resources
    assert "gpu=" not in resources
    assert "gres=" not in resources
    assert "cpus_per_gpu=" not in resources
