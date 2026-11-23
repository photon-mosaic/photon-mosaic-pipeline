from pathlib import Path

import photon_mosaic as pm
from photon_mosaic.segmentation import run_segmentation


def run_suite2p_roi_extraction(
    output_path,
    registered_path,
    algorithm="cellpose",
    suite2p=None,
):
    """
    Detect ROIs with Suite2p on the registered imaging and set up an
    ROI analyzer on the ROIs classified as cells.

    Arguments after ``registered_path`` match the config's ``segmentation``
    section.

    Parameters
    ----------
    output_path : str
        Segmentation folder. Receives photon-mosaic's
        ``segmentation_params.json``, ``segmentation_log.json``, imaging
        provenance, all detected ROIs under ``rois/`` and the cell-only ROI
        analyzer under ``roi_analyzer/`` (load with
        ``pm.load_roi_analyzer``).
    registered_path : str
        Folder holding the registered imaging saved by the registration
        rule (the folder containing its
        ``traces_cached_seg0.raw``).
    algorithm : str
        Suite2p detection algorithm: "cellpose", "sparsery" or "sourcery".
    suite2p : dict, optional
        Settings passed straight to photon-mosaic's ``run_segmentation``
        (any ``Suite2pSegmentationSettings`` field). ``fs`` defaults to the
        imaging's frame rate, which comes from the ScanImage metadata.
    """
    registered_imaging = pm.load(registered_path)

    settings = {
        "fs": registered_imaging.sampling_frequency,
        **(suite2p or {}),
    }

    # remove_existing_folder=True: Snakemake creates the output folder
    # before the rule runs
    rois = run_segmentation(
        registered_imaging,
        method="suite2p",
        folder=output_path,
        remove_existing_folder=True,
        algorithm=algorithm,
        **settings,
    )

    # suite2p's built-in classifier labels each ROI as cell / not cell
    # (the "iscell" property); keep only the cells.
    if rois.get_num_rois() == 0:
        raise ValueError(f"Segmentation found no ROIs; see {output_path}")
    is_cell = rois.get_property("iscell")
    cell_rois = rois.select_rois(
        [roi_id for roi_id, cell in zip(rois.roi_ids, is_cell) if cell]
    )

    pm.create_roi_analyzer(
        cell_rois,
        registered_imaging,
        format="binary_folder",
        folder=Path(output_path) / "roi_analyzer",
        overwrite=True,
    )
