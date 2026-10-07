from photon_mosaic.preprocessing.suite2p_registration import register_suite2p, compute_motion_suite2p

from photon_mosaic.extractors import read_scan_image_imaging
#from photon_mosaic.extractors import read_multi_tiff_multi_page, MultiTIFFMultiPageExtractor
#import photon_mosaic.extractors.roiextractors as _pm_roiextractors

# Workaround for a photon-mosaic bug: the dynamically generated extractor classes
# report their module as photon_mosaic.extractors.roiextractors but are not set as
# attributes there, so spikeinterface cannot reload them by name when saving.
#_pm_roiextractors.MultiTIFFMultiPageExtractor = MultiTIFFMultiPageExtractor


def run_suite2p_registration(output_path, tiff_paths, config):
    """
    Run Suite2p registration on the given data using the specified configuration.

    Parameters:
    - output_path: Path to save the registered data.
    - tiff_paths: List of TIFF files to register, in temporal order.
    - config: Configuration dictionary for Suite2p registration.

    Returns:
    - Registered data.
    """
    imaging = read_multi_tiff_multi_page(file_paths=tiff_paths, sampling_frequency=44.0)
    # Workaround for a photon-mosaic bug: imaging_name is stored in _kwargs and then
    # passed twice when the extractor is reloaded during save.
    imaging._kwargs.pop("imaging_name", None)
    motion = compute_motion_suite2p(imaging=imaging)
    registered_imaging = register_suite2p(imaging=imaging, motion=motion)
    # Workaround for a photon-mosaic bug: Suite2PMotion is not JSON serializable, so
    # fall back to pickle for the provenance file (as photon-mosaic does elsewhere).
    registered_imaging._serializability["json"] = False
    # overwrite=True because Snakemake creates the output folder before the rule runs
    registered_imaging.save(folder=output_path, format="binary", overwrite=True)
    return None
