from pathlib import Path

import photon_mosaic as pm
import tifffile
from photon_mosaic.extractors import read_scan_image_imaging
from photon_mosaic.preprocessing import (
    Suite2pRegistrationSettings,
    compute_motion,
    register_motion,
)
from roiextractors import ScanImageImagingExtractor


def check_suite2p_settings(settings):
    """Raise on settings unknown to both photon-mosaic and suite2p.

    photon-mosaic merges suite2p's registration defaults, its own
    Suite2pRegistrationSettings, then these settings, and silently ignores
    keys it doesn't use, so catch typos here.
    """
    from suite2p.registration.register import default_settings

    known = set(Suite2pRegistrationSettings.model_fields) | set(
        default_settings()["registration"]
    )
    unknown = sorted(set(settings) - known)
    if unknown:
        raise ValueError(
            f"Unknown registration.suite2p settings: {unknown}. "
            f"Known settings: {sorted(known)}"
        )


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


def load_imaging(tiff_paths, channel=1):
    """Load one channel of a ScanImage recording as a single imaging object.

    Frame rate, channels and planes come from the ScanImage metadata.

    Parameters
    ----------
    tiff_paths : list of str
        ScanImage TIFF files, in temporal order.
    channel : int
        1-based position among the saved channels.
    """
    _check_scanimage(tiff_paths)
    names = ScanImageImagingExtractor.get_available_channel_names(
        tiff_paths[0]
    )
    if not 1 <= channel <= len(names):
        raise ValueError(
            f"Channel {channel} requested, but the data has {names}."
        )
    return read_scan_image_imaging(
        file_paths=tiff_paths, channel_name=names[channel - 1]
    )


def _save_tiffs(imaging, folder, chunk=500):
    """Write imaging to one TIFF per plane, ``chunk`` frames at a time."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    n_frames = imaging.get_total_samples()
    for plane in range(imaging.get_num_planes()):
        path = folder / f"plane{plane}.tif"
        with tifffile.TiffWriter(path, bigtiff=True) as tif:
            for start in range(0, n_frames, chunk):
                end = min(start + chunk, n_frames)
                # metadata=None: plain pages, so batches read back as one stack
                tif.write(
                    imaging.get_series(start, end)[..., plane], metadata=None
                )


def run_suite2p_registration(
    output_path,
    tiff_paths,
    functional_channel=1,
    align_channel=1,
    two_step=False,
    reg_tif=False,
    reg_tif_chan2=False,
    suite2p=None,
):
    """
    Run Suite2p registration and save the registered imaging as binary.

    Arguments after ``tiff_paths`` match the config's ``registration``
    section. Motion is estimated on ``align_channel`` and applied to
    ``functional_channel``.

    Parameters
    ----------
    output_path : str
        Folder to save the registered imaging in.
    tiff_paths : list of str
        ScanImage TIFF files to register, in temporal order.
    functional_channel : int
        1-based channel to register and save.
    align_channel : int
        1-based channel to estimate motion on.
    two_step : bool
        As suite2p's two_step_registration: register, then register the
        result again against a reference built from the registered frames.
    reg_tif : bool
        Also save the registered functional channel as TIFFs (``reg_tif/``).
    reg_tif_chan2 : bool
        Also register the other channel with the same motion and save it as
        TIFFs (``reg_tif_chan2/``). Needs two channels.
    suite2p : dict, optional
        Settings passed straight to photon-mosaic's ``compute_motion``.
    """
    settings = suite2p or {}
    check_suite2p_settings(settings)

    def register(imaging, motions):
        for motion in motions:
            imaging = register_motion(imaging=imaging, motion=motion)
        return imaging

    imaging = load_imaging(tiff_paths, functional_channel)
    align_imaging = (
        imaging
        if align_channel == functional_channel
        else load_imaging(tiff_paths, align_channel)
    )
    motions = [
        compute_motion(imaging=align_imaging, method="suite2p", **settings)
    ]
    if two_step:
        motions.append(
            compute_motion(
                imaging=register(align_imaging, motions),
                method="suite2p",
                **settings,
            )
        )

    # overwrite=True: Snakemake creates the output folder before the rule runs
    register(imaging, motions).save(
        folder=output_path, format="binary", overwrite=True
    )

    if reg_tif:
        # write from the saved binary rather than re-registering
        _save_tiffs(pm.load(output_path), Path(output_path) / "reg_tif")
    if reg_tif_chan2:
        other = load_imaging(tiff_paths, 2 if functional_channel == 1 else 1)
        _save_tiffs(
            register(other, motions), Path(output_path) / "reg_tif_chan2"
        )
