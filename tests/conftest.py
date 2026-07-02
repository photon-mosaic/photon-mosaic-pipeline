"""
Shared fixtures for all tests in the photon-mosaic test suite.

This module provides common fixtures used across both unit and integration
tests, following the DRY principle to avoid duplication.
"""

import argparse
import os
import subprocess
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from tests.test_data_factory import DataFactory
from tests.tree_helpers import tree as tree_lines

# On CI, pin Cellpose to the CPU. The integration tests run the pipeline by
# spawning snakemake as a subprocess (which inherits os.environ), and
# GitHub-hosted macOS runners expose a non-functional MPS device that makes
# Cellpose-SAM return 0 masks -> Suite2p writes no F.npy -> the workflow
# fails. We gate on CI so local test runs keep using a working GPU/MPS (much
# faster); the env var is also a manual override anywhere. See
# photon_mosaic.rules.suite2p_run._force_cellpose_cpu_if_requested.
if os.environ.get("CI"):
    os.environ.setdefault("PHOTON_MOSAIC_FORCE_CPU", "1")


@pytest.fixture
def run_photon_mosaic():
    def inner_run_photon_mosaic(workdir, configfile, timeout=None):
        """Helper function to run photon-mosaic CLI with dry-run.

        timeout: seconds to wait for the subprocess to complete. If None,
        wait indefinitely (no timeout).
        """
        cmd = [
            "photon-mosaic",
            "--config",
            str(configfile),
            "--log-level",
            "DEBUG",
        ]

        result = subprocess.run(
            cmd,
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )

        return result

    return inner_run_photon_mosaic


@pytest.fixture
def test_data_root():
    """Return the path to test data directory."""
    return Path(__file__).parent


@pytest.fixture
def data_factory():
    """Return a DataFactory instance for creating test data dynamically."""
    return DataFactory()


@pytest.fixture
def base_config():
    """Create a base configuration that can be extended."""
    photon_mosaic_path = Path(__file__).parent.parent
    with open(
        photon_mosaic_path / "photon_mosaic" / "workflow" / "config.yaml", "r"
    ) as f:
        config = yaml.safe_load(f)

    config["project_path"] = "."
    config["dataset_discovery"]["tiff_patterns"] = ["*.tif"]

    return config


@pytest.fixture
def metadata_base_config():
    """Create a base configuration for metadata testing."""
    photon_mosaic_path = Path(__file__).parent.parent
    with open(
        photon_mosaic_path / "photon_mosaic" / "workflow" / "config.yaml", "r"
    ) as f:
        config = yaml.safe_load(f)

    config["dataset_discovery"]["tiff_patterns"] = ["*.tif"]
    return config


@pytest.fixture
def cli_args(snake_test_env):
    """
    Create a standard argparse.Namespace for CLI testing.
    """
    configfile = snake_test_env["configfile"]

    return argparse.Namespace(
        config=str(configfile),
        jobs="1",
        dry_run=False,
        forcerun=None,
        rerun_incomplete=False,
        latency_wait=10,
        verbose=False,
    )


def create_map_of_tiffs(raw_data_path: Path) -> dict:
    """
    Create a map of tiffs for a given rawdata directory.

    Args:
        raw_data_path: Path to rawdata directory

    Returns:
        Dictionary mapping subject/session paths to list of TIFF filenames
    """
    map_of_tiffs = {}
    for subject in raw_data_path.glob("sub-*"):
        if subject.is_dir():
            for session in subject.glob("ses-*"):
                if session.is_dir():
                    tiff_files = [f.name for f in session.rglob("*.tif")]
                    key = f"{subject.name}/{session.name}"
                    map_of_tiffs[key] = tiff_files
    return map_of_tiffs


@pytest.fixture
def test_config_with_contrast(base_config):
    """
    Create a test configuration with contrast enhancement preprocessing step.
    """
    config = base_config.copy()
    config["preprocessing"] = {
        "steps": [
            {
                "name": "contrast",
                "kwargs": {
                    "percentile_low": 1.0,
                    "percentile_high": 99.0,
                },
            }
        ],
        "output_pattern": "enhanced_",
    }
    return config


@pytest.fixture
def snake_test_env(tmp_path, base_config, data_factory):
    """
    Fixture that sets up the test environment with data and configuration.
    """
    print("\n=== Setting up test environment ===")
    print(f"Temporary directory: {tmp_path}")

    # Use factory to create NeuroBlueprint dataset structure dynamically
    raw_data = data_factory.create_neuroblueprint_dataset(tmp_path)
    print(f"Raw data directory: {raw_data}")
    print(f"Raw data contents after creation: {list(raw_data.glob('**/*'))}")

    processed_data = tmp_path / "derivatives"
    processed_data.mkdir()
    print(f"Processed data directory: {processed_data}")

    # Update paths in config
    config = base_config.copy()
    config["project_path"] = str(tmp_path.resolve())

    print("\n=== Configuration ===")
    print(f"Project path: {config['project_path']}")

    # Create config file
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_style='"', allow_unicode=True)
    print(f"Config file created at: {config_path}")

    # Generate map of tiffs from the dynamically created data
    map_of_tiffs = create_map_of_tiffs(raw_data)
    print(f"Generated map_of_tiffs: {map_of_tiffs}")
    print("=== End of test environment setup ===\n")

    return {
        "workdir": tmp_path,
        "configfile": config_path,
        "map_of_tiffs": map_of_tiffs,
    }


@pytest.fixture
def neuroblueprint_env(tmp_path, metadata_base_config, data_factory):
    """Set up test environment for NeuroBlueprint metadata format."""
    raw_data = data_factory.create_neuroblueprint_dataset(tmp_path)

    processed_data = tmp_path / "derivatives"
    processed_data.mkdir()

    config = metadata_base_config.copy()
    config["project_path"] = str(tmp_path.resolve())

    # Create config file
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)

    return {
        "workdir": tmp_path,
        "configfile": config_path,
        "raw_data": raw_data,
        "processed_data": processed_data,
    }


@pytest.fixture(autouse=True)
def log_test_fs(request):
    """
    After each test, write a snapshot of relevant test directories to
    `tests/logs/<testname>_<timestamp>.log`.
    """
    yield

    try:
        test_name = request.node.name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        log_path = logs_dir / f"{test_name}_{timestamp}.log"

        roots = []
        funcargs = getattr(request.node, "funcargs", {}) or {}

        def add_path_like(value):
            if isinstance(value, dict):
                for key in (
                    "workdir",
                    "raw_data",
                    "processed_data",
                    "base_path",
                ):
                    v = value.get(key)
                    if v:
                        add_path_like(v)
                return

            if isinstance(value, (str, Path)):
                p = Path(value)
                if p.exists():
                    roots.append(p)

        for v in funcargs.values():
            add_path_like(v)

        if "tmp_path" in funcargs:
            add_path_like(funcargs.get("tmp_path"))

        if not roots:
            fallback = Path(__file__).parent / "data"
            if fallback.exists():
                roots.append(fallback)

        seen = set()
        unique_roots = []
        for p in roots:
            try:
                rp = p.resolve()
            except Exception:
                rp = p
            if rp.is_file():
                rp = rp.parent
            if rp not in seen:
                seen.add(rp)
                unique_roots.append(rp)

        parent_candidates = []
        for r in unique_roots:
            if any((r / t).exists() for t in ("derivatives", "rawdata")):
                parent_candidates.append(r)

        if parent_candidates:
            best = next(
                (
                    r
                    for r in parent_candidates
                    if all(
                        (r / t).exists() for t in ("derivatives", "rawdata")
                    )
                ),
                None,
            )
            roots = [best or parent_candidates[0]]
        else:
            children = [
                r for r in unique_roots if r.name in ("rawdata", "derivatives")
            ]
            if children:
                roots = list(dict.fromkeys(children))
            else:
                roots = unique_roots

        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(f"Test: {request.node.nodeid}\n")
            lf.write(f"Timestamp: {timestamp}\n\n")

            for root in roots:
                lf.write(f"Root: {root}\n")

                for top in ("derivatives", "rawdata"):
                    top_path = root / top
                    if top_path.exists():
                        lf.write(f"{top}/\n")
                        for line in tree_lines(top_path):
                            lf.write(f"{line}\n")
                        lf.write("\n")

                if not any(
                    (root / t).exists() for t in ("derivatives", "rawdata")
                ):
                    for p in sorted(root.rglob("*")):
                        try:
                            rel = p.relative_to(root)
                        except Exception:
                            rel = p
                        if p.is_dir():
                            lf.write(f"{rel}/\n")
                        else:
                            lf.write(f"{rel}\n")
                    lf.write("\n")

    except Exception as exc:
        print(
            f"Error writing test filesystem log for {request.node.name}: {exc}"
        )
