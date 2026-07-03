# Preprocessing

The preprocessing module in `photon-mosaic-pipeline` provides a flexible system for applying preprocessing steps to image data. Each preprocessing step is a function that takes an image array and returns a processed image array.

There are three available preprocessing steps:
- `noop`: No operation, simply copies the input files to the output directory without any modification.
- `contrast`: Contrast enhancement using percentile-based contrast stretching.
- `derotation`: Image derotation using the derotation package.

The are mutually exclusive. To trigger one or the other you need to edit the configuration file.

## Available Preprocessing Steps

### Contrast Enhancement

The contrast enhancement step uses percentile-based contrast stretching to improve image contrast.

The contrast enhancement works by:
1. Finding the pixel values at the specified percentiles
2. Stretching the image intensity to use the full range between these values
3. Saving the enhanced image with the prefix "enhanced_" in the output folder

**Note**: If you are using the additional options in Suite2p (available in our custom fork), you may not need to perform contrast enhancement as a separate preprocessing step. This option automatically handles contrast normalization during the registration process. See the Suite2p configuration section for more details.

### Derotation

The derotation step handles image derotation using the derotation package. Please refer to the [derotation package documentation](https://derotation.neuroinformatics.dev/) for more details.

### No-Operation (Noop)

The noop step simply copies the input files to the output directory without any modification. Use this when you want to skip preprocessing but still need the files in the output directory.

## Configuration

To use preprocessing in your configuration, add a `preprocessing` section to your configuration file. Each step is defined as a dictionary with a `name` and optional `kwargs` for step-specific parameters.

Here the suggested configuration for each step:

### For noop

```yaml
preprocessing:
  output_pattern: ""
  steps:
    - name: noop
```

### For contrast enhancement

```yaml
preprocessing:
  output_pattern: "enhanced_"
  steps:
    - name: contrast
      kwargs:
        percentile_low: 1
        percentile_high: 99
```

### For derotation

```yaml
preprocessing:
  output_pattern: "derotated_"
  steps:
    - name: derotation
      kwargs:
        glob_naming_pattern_tif: "*.tif"
        glob_naming_pattern_bin: "*.bin"
        path_to_stimulus_randperm: "/path/to/stimulus_randperm.npy"
```
