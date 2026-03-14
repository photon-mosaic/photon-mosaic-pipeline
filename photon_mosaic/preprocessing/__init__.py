"""
Preprocessing module for photon-mosaic.

This module provides preprocessing steps that can be applied to image data.
Each preprocessing step is a function that takes an image array and returns a processed image array.
"""

from . import contrast
from . import derotation
from . import noop

__all__ = ["contrast", "derotation", "noop", "stiminterpolation"]
