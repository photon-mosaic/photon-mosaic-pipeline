from photon_mosaic_pipeline.rules.segmentation_run import _common_range


def test_common_range_is_valid_on_every_epoch_and_plane():
    """The range is the overlap of the ranges of all epochs and planes:
    the highest lower bound and the lowest upper bound.
    """
    ranges = [[(1, 127), (4, 126)], [(2, 120), (0, 128)]]

    assert _common_range(ranges) == [4, 120]


def test_common_range_without_ranges():
    """A backend that stores no ranges leaves the choice to photon-mosaic."""
    assert _common_range(None) is None
