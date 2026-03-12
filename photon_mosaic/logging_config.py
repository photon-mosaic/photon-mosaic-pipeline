"""
Logging configuration for photon-mosaic with formatting and colors.
"""

import logging
import sys
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels."""

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def __init__(self, fmt=None, use_colors=True):
        super().__init__(fmt)
        # Allow callers to explicitly enable/disable colors.
        # We still check isatty at format time to avoid emitting
        # ANSI sequences when output is not a terminal.
        self.use_colors = use_colors

    def format(self, record):
        # Add color to level name when colors are enabled and
        # the stderr stream is a TTY. This avoids coloring when
        # logs are redirected to files or captured in tests.
        if self.use_colors and sys.stderr.isatty():
            levelname = record.levelname
            if levelname in self.COLORS:
                record.levelname = (
                    f"{self.COLORS[levelname]}{self.BOLD}"
                    f"{levelname}{self.RESET}"
                )
        return super().format(record)


def ensure_dir(path, mode=0o755, parents=True, exist_ok=True):
    """Create a directory (Path or str) with optional permission setting.

    Parameters
    ----------
    path : str | Path
        Directory to create.
    mode : int | None
        POSIX permission bits to set on the directory (e.g. 0o755). If None,
        permission change is skipped.
    parents : bool
        Whether to create parent directories.
    exist_ok : bool
        Whether to ignore if the directory already exists.

    Returns
    -------
    Path
        The Path object for the created directory.
    """
    p = Path(path)
    p.mkdir(parents=parents, exist_ok=exist_ok)
    if mode is not None:
        try:
            p.chmod(mode)
        except PermissionError:
            # Best-effort: if chmod fails (e.g., on non-POSIX FS), ignore
            pass
    return p


def setup_logging(log_level="INFO", log_file=None, use_colors=True):
    """
    Configure logging with improved formatting.

    This function is safe to call multiple times. If logging handlers are
    already configured (e.g., by the CLI), it will preserve them and only
    update the log level. This allows the Snakefile to adjust verbosity
    without losing the file handler set up by the CLI.

    Parameters
    ----------
    log_level : str
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    log_file : Path, optional
        Path to log file for file handler. Only used if no handlers exist yet.
    use_colors : bool
        Whether to use colored output for console. Only used if no handlers
        exist yet.

    Returns
    -------
    logger : logging.Logger
        Configured root logger

    Notes
    -----
    When called from the Snakefile after the CLI has already configured
    logging, this function will:
    - Keep the existing file handler (so Snakemake logs go to the same file)
    - Keep the existing console handler
    - Update the log level on both root logger and console handler
    - Ignore the `log_file` and `use_colors` parameters
    """
    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    # Check if logging is already configured (avoid clearing handlers set by
    # CLI)
    # If we already have handlers, just update the log level and return
    if logger.handlers:
        logger.setLevel(getattr(logging, log_level.upper()))
        # Update level on existing handlers too
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, logging.FileHandler
            ):
                # Update console handler level
                handler.setLevel(getattr(logging, log_level.upper()))
        return logger

    # Console handler with colors and better formatting
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(getattr(logging, log_level.upper()))

    if use_colors:
        console_format = "%(levelname)s | %(name)s | %(message)s"
        console_formatter = ColoredFormatter(console_format, use_colors=True)
    else:
        console_format = (
            "[%(asctime)s] %(levelname)-8s | %(name)s | %(message)s"
        )
        console_formatter = logging.Formatter(
            console_format, datefmt="%Y-%m-%d %H:%M:%S"
        )

    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler with detailed formatting (no colors)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file
        file_format = (
            "[%(asctime)s] %(levelname)-8s | %(name)-30s | "
            "%(filename)s:%(lineno)d | %(message)s"
        )
        file_formatter = logging.Formatter(
            file_format, datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def log_section_header(logger, title, char="="):
    """Log a section header for better visual separation."""
    width = 80
    logger.info("")
    logger.info(char * width)
    logger.info(f"{title.center(width)}")
    logger.info(char * width)
    logger.info("")


def log_subsection(logger, title):
    """Log a subsection for grouping related information."""
    logger.info("")
    logger.info(f"--- {title} ---")


def log_list_summary(logger, items, item_type, preview_count=5):
    """Log a summary of a list with optional debug preview.

    Parameters
    ----------
    logger : logging.Logger
        Logger instance to use
    items : list
        List of items to summarize
    item_type : str
        Description of the items (e.g., "Preprocessing targets",
        "Suite2p targets")
    preview_count : int
        Number of items to show in debug mode (default: 5)
    """
    logger.info(f"{item_type}: {len(items)} file(s)")

    if logger.isEnabledFor(logging.DEBUG):
        for item in items[:preview_count]:
            logger.debug(f"  → {item}")
        if len(items) > preview_count:
            logger.debug(f"  ... and {len(items) - preview_count} more")
