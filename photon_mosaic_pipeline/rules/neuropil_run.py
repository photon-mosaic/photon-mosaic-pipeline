"""
Snakemake rule for neuropil correction, with photon-mosaic's ROI analyzer.
"""

import logging

import photon_mosaic as pm

logger = logging.getLogger(__name__)


def calculate_neuropil_correction(
    roi_analyzer_path: str,
    neuropil: dict | None = None,
    fluorescence: dict | None = None,
    **unknown,
) -> None:
    """Extract neuropil-corrected fluorescence traces for each ROI.

    Builds a neuropil surround mask for each ROI, then extracts each ROI's
    fluorescence with the neuropil subtracted. Both are saved inside the
    ROI analyzer, under ``extensions/neuropil/`` and
    ``extensions/fluorescence/``.

    Arguments after ``roi_analyzer_path`` match the config's
    ``neuropil_ops`` section.

    Parameters
    ----------
    roi_analyzer_path : str
        ROI analyzer folder saved by the ROI extraction rule.
    neuropil : dict, optional
        Settings for ``roi_analyzer.compute("neuropil")``.
    fluorescence : dict, optional
        Settings for ``roi_analyzer.compute("fluorescence")``.
        ``use_neuropil`` defaults to True, so the neuropil is subtracted.
    """
    if unknown:
        raise ValueError(
            f"Unknown neuropil_ops settings: {sorted(unknown)}. neuropil_ops "
            "now takes 'neuropil' and 'fluorescence' subsections (the old "
            "'neucoeff' is fluorescence.neuropil_weight); see the default "
            "config.yaml."
        )

    roi_analyzer = pm.load_roi_analyzer(roi_analyzer_path)

    logger.info("Computing neuropil masks...")
    roi_analyzer.compute("neuropil", **(neuropil or {}))

    logger.info("Extracting neuropil-corrected fluorescence...")
    roi_analyzer.compute(
        "fluorescence", **{"use_neuropil": True, **(fluorescence or {})}
    )
    logger.info(f"Saved neuropil and fluorescence to {roi_analyzer_path}")
