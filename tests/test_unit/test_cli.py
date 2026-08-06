"""
Unit tests for the CLI module.
"""

from photon_mosaic_pipeline.cli import (
    build_snakemake_command,
    configure_slurm_execution,
    inject_slurm_log_paths,
)
from photon_mosaic_pipeline.snakemake_utils import slurm_resources


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
# the project, and slurm_extra built from the logdir (NOT a hardcoded
# nodelist) -- see inject_slurm_log_paths further down, which now owns that.
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


def test_configure_slurm_emits_no_default_resources(tmp_path):
    """--default-resources must not be passed at all.

    Every rule reads its own block from the config, so there is nothing for
    the defaults to supply. Keeping them would be actively harmful: snakemake
    fills defaults in for any resource a rule does NOT set, silently putting
    back values a rule deliberately left out.
    """
    config = {
        "use_slurm": True,
        "project_path": str(tmp_path),
        "slurm": {
            "preprocessing": {"slurm_partition": "cpu"},
            "suite2p": {"slurm_partition": "gpu", "gres": "gpu:a4500:1"},
        },
    }

    cmd = configure_slurm_execution(["snakemake"], config)

    assert "--default-resources" not in cmd


# ---------------------------------------------------------------------------
# inject_slurm_log_paths
#
# slurm_extra carries the sbatch --output/--error paths. Rules read their
# resources from the WRITTEN config file, and there are no default resources
# to fall back on, so the paths have to be in every rule's block before that
# file is saved. If this silently missed a block, that rule's logs would land
# wherever SLURM defaults to and #108-style "no error log" reports follow.
# ---------------------------------------------------------------------------


def test_log_paths_reach_every_rule_block(tmp_path):
    """Each rule's block gets slurm_extra pointing at the log directory."""
    config = {
        "use_slurm": True,
        "project_path": str(tmp_path),
        "slurm": {
            "_common": {"tasks": 1},
            "preprocessing": {"slurm_partition": "cpu"},
            "suite2p": {"slurm_partition": "gpu"},
        },
    }

    inject_slurm_log_paths(config)

    expected_logdir = (
        tmp_path / "derivatives" / "photon-mosaic-pipeline" / "logs" / "slurm"
    )
    for rule_name in ("preprocessing", "suite2p"):
        slurm_extra = slurm_resources(config, rule_name)["slurm_extra"]
        assert f"--output={expected_logdir}/%j_%x.out" in slurm_extra
        assert f"--error={expected_logdir}/%j_%x.err" in slurm_extra
        assert "--nodelist" not in slurm_extra


def test_log_paths_reach_a_flat_config(tmp_path):
    """A flat slurm: block gets the log paths on the block itself."""
    config = {
        "use_slurm": True,
        "project_path": str(tmp_path),
        "slurm": {"slurm_partition": "gpu"},
    }

    inject_slurm_log_paths(config)

    assert "--output=" in slurm_resources(config, "suite2p")["slurm_extra"]


def test_log_paths_not_injected_when_slurm_disabled(tmp_path):
    """With use_slurm false nothing is touched."""
    config = {
        "use_slurm": False,
        "project_path": str(tmp_path),
        "slurm": {"suite2p": {"slurm_partition": "gpu"}},
    }

    inject_slurm_log_paths(config)

    assert "slurm_extra" not in config["slurm"]["suite2p"]
