import tifffile
from photon_mosaic.extractors import read_scan_image_imaging
from photon_mosaic.preprocessing import compute_motion, register_motion
from roiextractors import ScanImageImagingExtractor


def _check_scanimage(tiff_paths):
    for path in tiff_paths:
        with tifffile.TiffFile(path) as tif:
            if not tif.is_scanimage:
                raise ValueError(
                    f"{path} is not a ScanImage TIFF. Registration reads "
                    "ScanImage files directly, so use the noop preprocessing "
                    "step (other steps rewrite the TIFFs without their "
                    "ScanImage metadata)."
                )


def load_imaging(tiff_paths, channel=None):
    """Load one channel of a ScanImage recording as a single imaging object.

    Frame rate, channels and planes come from the ScanImage metadata.

    Parameters
    ----------
    tiff_paths : list of str
        ScanImage TIFF files, in temporal order.
    channel : int, optional
        1-based position among the saved channels, as in suite2p's
        ``functional_chan``. Required when more than one channel was saved.
    """
    _check_scanimage(tiff_paths)
    channel_name = None
    if channel is not None:
        names = ScanImageImagingExtractor.get_available_channel_names(
            tiff_paths[0]
        )
        channel_name = names[channel - 1]
    return read_scan_image_imaging(
        file_paths=tiff_paths, channel_name=channel_name
    )


def run_suite2p_registration(
    output_path, tiff_paths, functional_channel=None, align_channel=None
):
    """
    Run Suite2p registration and save the registered imaging as binary.

    Motion is estimated on ``align_channel`` and applied to
    ``functional_channel`` (as suite2p does with ``align_by_chan2``).

    Parameters
    ----------
    output_path : str
        Folder to save the registered imaging in.
    tiff_paths : list of str
        ScanImage TIFF files to register, in temporal order.
    functional_channel : int, optional
        1-based channel to register and save. Required for multi-channel data.
    align_channel : int, optional
        1-based channel to estimate motion on. Defaults to
        ``functional_channel``.
    """
    if align_channel is None:
        align_channel = functional_channel
    imaging = load_imaging(tiff_paths, functional_channel)
    align_imaging = (
        imaging
        if align_channel == functional_channel
        else load_imaging(tiff_paths, align_channel)
    )
    motion = compute_motion(imaging=align_imaging, method="suite2p")
    registered_imaging = register_motion(imaging=imaging, motion=motion)
    # overwrite=True: Snakemake creates the output folder before the rule runs
    registered_imaging.save(
        folder=output_path, format="binary", overwrite=True
    )
