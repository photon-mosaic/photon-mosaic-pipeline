"""
Helper functions for regression testing of file structure logs.

This module provides utilities for checking generated file structure logs
against expected baselines. These helpers can be called at the end of any
test to verify that the generated file structures match expectations.
"""

import re
from pathlib import Path


def normalize_log_content(log_path: Path) -> str:
    """
    Read log and normalize for comparison.

    Removes:
    - Timestamp lines (vary between runs)
    - Root path lines (vary between systems/runs)
    - Timestamps in filenames (YYYYMMDD_HHMMSS format)
    - Full absolute paths (replace with relative paths)

    Args:
        log_path: Path to the log file to normalize

    Returns:
        Normalized log content as a string
    """
    with open(log_path, "r") as f:
        lines = f.readlines()

    normalized = []
    timestamp_pattern = re.compile(r"_\d{8}_\d{6}")
    # Pattern to match absolute paths (both Unix and Windows style)
    abs_path_pattern = re.compile(r"(/[^\s:]+|[A-Z]:\\[^\s:]+)")

    for line in lines:
        # Skip timestamp and root path lines entirely
        if line.startswith("Timestamp:") or line.startswith("Root:"):
            continue

        # Replace timestamp patterns in filenames with placeholder
        normalized_line = timestamp_pattern.sub("_TIMESTAMP", line)

        # Replace absolute paths with placeholder
        # This handles paths in any context (not just Root: lines)
        normalized_line = abs_path_pattern.sub("<PATH>", normalized_line)

        normalized.append(normalized_line)

    return "".join(normalized)


def check_logs(request, fail_on_mismatch: bool = True) -> dict:
    """
    Check current test's file structure log against expected baseline.

    This function should be called at the end of test functions to verify
    that the generated file structure matches the expected baseline.

    Args:
        request: pytest request fixture
        fail_on_mismatch: If True, fail the test on mismatch. If False,
            return results without failing (default: True)

    Returns:
        Dictionary with keys:
        - 'status': 'match', 'mismatch', 'newly_created', or 'no_log'
        - 'test_name': name of the test
        - 'message': descriptive message about the result

    Raises:
        AssertionError: If fail_on_mismatch=True and logs don't match
    """
    import pytest

    test_name = request.node.name
    test_root = Path(__file__).parent
    logs_dir = test_root / "logs"
    expected_logs_dir = test_root / "expected_logs"

    # Ensure directories exist
    logs_dir.mkdir(exist_ok=True)
    expected_logs_dir.mkdir(exist_ok=True)

    # Find the most recent log for this test
    test_logs = sorted(logs_dir.glob(f"{test_name}_*.log"), reverse=True)

    if not test_logs:
        result = {
            "status": "no_log",
            "test_name": test_name,
            "message": f"No log file found for test: {test_name}",
        }
        if fail_on_mismatch:
            pytest.fail(result["message"])
        return result

    latest_log = test_logs[0]
    expected_log = expected_logs_dir / f"{test_name}.log"

    if not expected_log.exists():
        # First run - save normalized version as expected log
        normalized_content = normalize_log_content(latest_log)
        with open(expected_log, "w") as f:
            f.write(normalized_content)
        result = {
            "status": "newly_created",
            "test_name": test_name,
            "message": f"Created new expected log for: {test_name}",
        }
        return result

    # Compare file structure (ignore timestamps and root paths)
    current_content = normalize_log_content(latest_log)
    expected_content = normalize_log_content(expected_log)

    if current_content != expected_content:
        error_msg = (
            f"\n❌ File structure mismatch for test: {test_name}\n\n"
            f"Expected log: {expected_log}\n"
            f"Current log: {latest_log}\n\n"
            "To update expected logs after intentional changes:\n"
            f"  rm {expected_log}\n"
            "  pytest\n"
        )
        result = {
            "status": "mismatch",
            "test_name": test_name,
            "message": error_msg,
        }
        if fail_on_mismatch:
            pytest.fail(error_msg)
        return result

    result = {
        "status": "match",
        "test_name": test_name,
        "message": f"✓ File structure matches for: {test_name}",
    }
    return result
