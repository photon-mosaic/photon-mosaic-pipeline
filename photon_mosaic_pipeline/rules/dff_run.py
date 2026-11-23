"""
Snakemake rule for calculating dF/F, with photon-mosaic's ROI analyzer.
"""

import logging

import photon_mosaic as pm

logger = logging.getLogger(__name__)


def calculate_dFF(roi_analyzer_path: str, **df_over_f) -> None:
    """Calculate dF/F from the neuropil-corrected fluorescence traces.

    Needs the fluorescence computed by the neuropil rule. The result is
    saved inside the ROI analyzer, under ``extensions/df_over_f/``
    (``df_over_f.npy`` and ``f0.npy``, each frames x ROIs). Load it back
    with::

        analyzer = pm.load_roi_analyzer(roi_analyzer_path)
        dff = analyzer.get_extension("df_over_f").get_data()

    Parameters
    ----------
    roi_analyzer_path : str
        ROI analyzer folder saved by the ROI extraction rule.
    **df_over_f
        Settings for ``roi_analyzer.compute("df_over_f")``, from the
        config's ``dff_ops`` section.
    """
    if "gmm_ncomponents" in df_over_f:
        raise ValueError(
            "dff_ops.gmm_ncomponents is from the old GMM-baseline dF/F and is "
            "no longer used. Replace dff_ops with photon-mosaic df_over_f "
            "settings (see the default config.yaml)."
        )

    roi_analyzer = pm.load_roi_analyzer(roi_analyzer_path)

    logger.info("Calculating dF/F...")
    roi_analyzer.compute("df_over_f", **df_over_f)
    logger.info(f"Saved dF/F to {roi_analyzer_path}")
