"""
Shared fixtures for all tests in the photon-mosaic test suite.

This module provides common fixtures used across both unit and integration
tests, following the DRY principle to avoid duplication.
"""

import argparse
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from tests.regression_helpers import check_logs as _check_logs
from tests.test_data_factory import DataFactory
from tests.tree_helpers import tree as tree_lines


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

    config["raw_data_base"] = "raw_data"
    config["processed_data_base"] = "derivatives"
    #  match different file naming patterns to different sessions
    config["dataset_discovery"]["tiff_patterns"] = [
        "type_1*.tif",
        "type_2*.tif",
    ]

    return config


@pytest.fixture
def metadata_base_config():
    """Create a base configuration for metadata testing."""
    photon_mosaic_path = Path(__file__).parent.parent
    with open(
        photon_mosaic_path / "photon_mosaic" / "workflow" / "config.yaml", "r"
    ) as f:
        config = yaml.safe_load(f)

    # Set default values for metadata testing
    config["dataset_discovery"]["tiff_patterns"] = ["*.tif"]
    return config


@pytest.fixture
def map_of_tiffs():
    """
    Create a map of tiffs in test data using rglob -
    for backward compatibility with unit tests that use static data.
    For integration tests, use the map_of_tiffs from snake_test_env instead.
    """

    photon_mosaic_path = Path(__file__).parent / "data"
    map_of_tiffs = {}
    for dataset in photon_mosaic_path.glob("*"):
        if dataset.is_dir():
            # Get just the filenames, not the full paths
            tiff_files = [f.name for f in dataset.rglob("*.tif")]
            map_of_tiffs[dataset.name] = tiff_files
    return map_of_tiffs


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
    Create a map of tiffs for a given raw data directory.

    Args:
        raw_data_path: Path to raw data directory

    Returns:
        Dictionary mapping dataset names to list of TIFF filenames
    """
    map_of_tiffs = {}
    for dataset in raw_data_path.glob("*"):
        if dataset.is_dir():
            # Get just the filenames, not the full paths
            tiff_files = [f.name for f in dataset.rglob("*.tif")]
            map_of_tiffs[dataset.name] = tiff_files
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

    # Use factory to create basic dataset structure dynamically
    raw_data = data_factory.create_basic_dataset(tmp_path)
    print(f"Raw data directory: {raw_data}")
    print(f"Raw data contents after creation: {list(raw_data.glob('**/*'))}")

    processed_data = tmp_path / "derivatives"
    processed_data.mkdir()
    print(f"Processed data directory: {processed_data}")

    # Update paths in config
    config = base_config.copy()
    config["raw_data_base"] = str(raw_data.resolve())
    config["processed_data_base"] = str(processed_data.resolve())

    print("\n=== Configuration ===")
    print(f"Raw data base: {config['raw_data_base']}")
    print(f"Processed data base: {config['processed_data_base']}")

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
def custom_metadata_env(tmp_path, metadata_base_config, data_factory):
    """Set up test environment for custom metadata format."""
    # Use factory to create custom metadata dataset dynamically
    raw_data = data_factory.create_custom_metadata_dataset(tmp_path)

    processed_data = tmp_path / "derivatives"
    processed_data.mkdir()

    # Update config for custom format
    config = metadata_base_config.copy()
    config["raw_data_base"] = str(raw_data.resolve())
    config["processed_data_base"] = str(processed_data.resolve())
    config["dataset_discovery"]["neuroblueprint_format"] = False
    config["dataset_discovery"]["pattern"] = ".*"

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


@pytest.fixture
def neuroblueprint_env(tmp_path, metadata_base_config, data_factory):
    """Set up test environment for NeuroBlueprint metadata format."""
    # Use factory to create NeuroBlueprint dataset dynamically
    raw_data = data_factory.create_neuroblueprint_dataset(tmp_path)

    processed_data = tmp_path / "derivatives"
    processed_data.mkdir()

    # Update config for NeuroBlueprint format
    config = metadata_base_config.copy()
    config["raw_data_base"] = str(raw_data.resolve())
    config["processed_data_base"] = str(processed_data.resolve())
    config["dataset_discovery"]["neuroblueprint_format"] = True

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


@pytest.fixture
def neuroblueprint_noncontinuous_env(
    tmp_path, metadata_base_config, data_factory
):
    """
    Set up test environment for NeuroBlueprint format with non-continuous IDs.
    """
    # Use factory to create non-continuous ID dataset dynamically
    raw_data = data_factory.create_noncontinuous_neuroblueprint_dataset(
        tmp_path
    )

    processed_data = tmp_path / "derivatives"
    processed_data.mkdir()

    # Update config for NeuroBlueprint format
    config = metadata_base_config.copy()
    config["raw_data_base"] = str(raw_data.resolve())
    config["processed_data_base"] = str(processed_data.resolve())
    config["dataset_discovery"]["neuroblueprint_format"] = True

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


def _write_test_log(request) -> Path | None:
    """
    Write a snapshot of relevant test directories to
    `tests/logs/<testname>_<timestamp>.log`.

    This helper inspects fixtures used by the test (via
    `request.node.funcargs`) to discover Path-like objects such as
    `workdir`, `raw_data` or `processed_data`. If none are found it
    falls back to the static `tests/data` directory.

    Returns:
        Path to the written log file, or None if writing failed.
    """
    try:
        test_name = request.node.name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        log_path = logs_dir / f"{test_name}_{timestamp}.log"

        # Collect candidate roots from fixture funcargs
        roots = []
        funcargs = getattr(request.node, "funcargs", {}) or {}

        def add_path_like(value):
            # Handle dicts returned by some fixtures
            if isinstance(value, dict):
                for key in (
                    "workdir",
                    "raw_data",
                    "processed_data",
                    "raw_data_base",
                ):
                    v = value.get(key)
                    if v:
                        add_path_like(v)
                return

            if isinstance(value, (str, Path)):
                p = Path(value)
                if p.exists():
                    roots.append(p)

        # Inspect all funcargs the test received
        for v in funcargs.values():
            add_path_like(v)

        # Also include tmp_path if present
        if "tmp_path" in funcargs:
            add_path_like(funcargs.get("tmp_path"))

        # Fallback to tests/data
        if not roots:
            fallback = Path(__file__).parent / "data"
            if fallback.exists():
                roots.append(fallback)

        # Normalize and deduplicate roots so we don't write the same
        # directory multiple times (the same path can be referenced from
        # multiple fixtures, e.g. `workdir`, `tmp_path`, or nested values).
        seen = set()
        unique_roots = []
        for p in roots:
            try:
                rp = p.resolve()
            except Exception:
                rp = p
            # If a file was added (e.g. a config file), prefer its parent dir
            if rp.is_file():
                rp = rp.parent
            if rp not in seen:
                seen.add(rp)
                unique_roots.append(rp)

        parent_candidates = []
        for r in unique_roots:
            if any((r / t).exists() for t in ("derivatives", "raw_data")):
                parent_candidates.append(r)

        if parent_candidates:
            # Prefer a parent that has both children when available
            best = next(
                (
                    r
                    for r in parent_candidates
                    if all(
                        (r / t).exists() for t in ("derivatives", "raw_data")
                    )
                ),
                None,
            )
            roots = [best or parent_candidates[0]]
        else:
            # Keep explicit child dirs if present (e.g. paths that are
            # literally `.../raw_data` or `.../derivatives`).
            children = [
                r
                for r in unique_roots
                if r.name in ("raw_data", "derivatives")
            ]
            if children:
                # preserve order but remove duplicates
                roots = list(dict.fromkeys(children))
            else:
                roots = unique_roots

        # Write the log file as a simple listing of each root's tree
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(f"Test: {request.node.nodeid}\n")
            lf.write(f"Timestamp: {timestamp}\n\n")

            for root in roots:
                lf.write(f"Root: {root}\n")

                # Prefer to only show the two top-level directories of interest
                # if they exist: derivatives/ and raw_data/
                for top in ("derivatives", "raw_data"):
                    top_path = root / top
                    if top_path.exists():
                        lf.write(f"{top}/\n")
                        # write pretty tree lines (indented)
                        for line in tree_lines(top_path):
                            lf.write(f"{line}\n")
                        lf.write("\n")

                # If neither was present,
                # fall back to the previous full listing
                if not any(
                    (root / t).exists() for t in ("derivatives", "raw_data")
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

        return log_path

    except (
        Exception
    ) as exc:  # pragma: no cover - avoid breaking tests on logging errors
        # Don't let logging break tests
        print(
            f"Error writing test filesystem log for {request.node.name}: {exc}"
        )
        return None


# Track which tests have had their logs written by check_logs fixture
_tests_logged_by_check_logs: set = set()


@pytest.fixture
def check_logs(request):
    """
    Fixture that provides a function to check file structure logs.

    This fixture can be used in tests to verify that generated file
    structures match expected baselines. Call it at the end of your test.

    Usage:
        def test_my_feature(check_logs):
            # ... test logic ...
            check_logs()

    The function will automatically:
    1. Find the most recent generated log in tests/logs/
    2. Check if an expected baseline exists in tests/expected_logs/
    3. If no baseline exists, normalize and save the current log as the
       baseline
    4. If baseline exists, compare the normalized current log against the
       baseline and fail the test on mismatch
    """
    # Track if check_logs was called
    check_requested = {"called": False, "fail_on_mismatch": True}

    def _checker(fail_on_mismatch: bool = True):
        check_requested["called"] = True
        check_requested["fail_on_mismatch"] = fail_on_mismatch

    yield _checker

    # Mark this test as handled by check_logs so log_test_fs skips it
    _tests_logged_by_check_logs.add(request.node.nodeid)

    # Write the log file first, then check it
    if check_requested["called"]:
        log_path = _write_test_log(request)
        if log_path is None:
            # Log writing failed - skip checking to avoid confusing error
            print(
                f"Warning: Could not write log for {request.node.name}, "
                "skipping regression check"
            )
            return
        _check_logs(request, check_requested["fail_on_mismatch"])
