"""
Snakemake rule for preprocessing image data.

This module provides a function to run preprocessing on image data by directly
importing preprocessing modules based on step names.
"""

import importlib


def run_preprocessing(
    output_path, config, dataset_folder=None, ses_idx=0, tiff_name=None
):
    """
    Run preprocessing on image data.

    Parameters
    ----------
    output_path : str
        Path to save the preprocessed image.
    config : dict
        Configuration dictionary containing preprocessing step.
    dataset_folder : str, optional
        Path to the dataset folder. This is needed for some preprocessing steps
        that require access to the dataset folder.
    ses_idx : int, optional
        Session index to process. Default is 0.
    tiff_name : str, optional
        Name of the TIFF file to process. Required for 'noop', 'contrast',
        and 'stiminterpolation' preprocessing steps.

    Returns
    -------
    None
        The function saves the preprocessed data to the output path and returns
        nothing.

    Raises
    ------
    ValueError
        If a preprocessing step cannot be found or imported.
    """

    # Get the single preprocessing step
    step = config["steps"][0]  # Only one step
    step_name = step["name"]
    step_kwargs = step.get("kwargs", {})

    # Import the preprocessing module and get the run function
    try:
        module = importlib.import_module(
            f"photon_mosaic.preprocessing.{step_name}"
        )
        func = getattr(module, "run")
    except (ImportError, AttributeError) as e:
        raise ValueError(
            f"Could not find preprocessing step '{step_name}': {e}"
        )

    # Call the preprocessing function with all parameters
    func(
        dataset_folder=dataset_folder,
        output_folder=output_path,
        tiff_name=tiff_name,
        **step_kwargs,
    )
