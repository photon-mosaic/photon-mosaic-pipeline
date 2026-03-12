(user_guide/configuration)=
# Configuration

The configuration system in `photon-mosaic` is designed to be flexible and user-friendly. It allows you to customize the behavior of the pipeline at different levels.

## Configuration Files

### User Configuration
On first run, photon-mosaic will create a user config at `~/.photon_mosaic/config.yaml` if it does not exist. This serves as your default configuration.

The default config expects you to run the pipeline in the directory that contains the raw data and will create a `derivatives` directory its root directory, following `NeuroBlueprint` conventions. It is advised to specify the paths for the raw and processed data using the `raw_data_base` and `processed_data_base` parameters on the first run via the command line:

```bash
photon-mosaic --raw_data_base /my/raw_data --processed_data_base /my/derivatives
```
After the first run, you can edit the config file to your liking by manually opening the file and editing it or using command line arguments. You will be able to run photon-mosaic without specifying the paths again, by just running `photon-mosaic`.

In case you want to reset the config to the default values, you can run `photon-mosaic --reset-config`. You can also specify the paths to raw and derivative again with the appropriate flags.

If you want to store your config file somewhere else, you can specify the path to the config file with the `--config` flag.

The config file that is used for each run (with any overrides) is exported to `derivatives/photon-mosaic/configs/YYYYMMDD_HHMMSS_config.yaml`.

## Configuration Structure

The configuration file is organized into several main sections. Here is a simplified example showing the key sections:

```yaml
# Data paths
raw_data_base: "path/to/raw_data/"
processed_data_base: "path/to/derivatives/"

# Dataset discovery settings
dataset_discovery:
  pattern: "^.*$"
  tiff_patterns: ["*.tif"]
    exclude_datasets:
      - ".*_test$"
      - ".*_backup$"
    exclude_sessions: []

# Preprocessing configuration
preprocessing:
  output_pattern: ""  # "" for noop, "enhanced_" for contrast, "derotated_" for derotation
  steps:
    - name: noop  # Only one step should be active at a time

# Suite2p settings
suite2p_ops:
  # Acquisition parameters
  nplanes: 1
  nchannels: 1
  fs: 10.0
  tau: 1.0

  # Registration settings
  do_registration: true
  nonrigid: true

  # Custom registration parameters (our fork)
  refImg_min_percentile: 1
  refImg_max_percentile: 99

  # ROI detection
  roidetect: true
  anatomical_only: 0

# SLURM settings
use_slurm: false
slurm:
  slurm_partition: "gpu"
  mem_mb: 32000
  tasks: 1
  nodes: 1
```

For the complete configuration file with all available parameters and detailed comments, see [photon_mosaic/workflow/config.yaml](https://github.com/neuroinformatics-unit/photon-mosaic/blob/main/photon_mosaic/workflow/config.yaml) or the YAML file in `~/.photon_mosaic/config.yaml` generated on first run.

## Key Configuration Notes

### Dataset Discovery
See the [dataset discovery documentation](dataset_discovery.md) for detailed information about configuring dataset discovery.

### Preprocessing
See the [preprocessing documentation](preprocessing.md) for step-specific configuration

### Suite2p Parameters
The configuration includes all standard Suite2p parameters plus custom additions:

#### Custom Registration Parameters
Our fork includes additional parameters for improved registration:
- `refImg_min_percentile`: Minimum percentile for reference image selection (default: 1)
- `refImg_max_percentile`: Maximum percentile for reference image selection (default: 99)

These parameters control contrast normalization during registration and are especially useful for low SNR datasets like three-photon imaging.

#### Cell Detection
To use Cellpose for cell detection, set `anatomical_only` to a value greater than 0:

```yaml
suite2p_ops:
  anatomical_only: 4  # Use maximum projection image for cell detection
```
Choose the value of `anatomical_only` based on the following table:

| Value | Description |
|-------|-------------|
| 1     | Use max projection image divided by mean image |
| 2     | Use mean image |
| 3     | Use enhanced mean image |
| 4     | Use maximum projection image |

For a complete list of all available Suite2p parameters, refer to the [official Suite2p documentation](https://suite2p.readthedocs.io/en/latest/parameters).

#### Cellpose 3 vs Cellpose 4
Photon-mosaic uses Cellpose 4 by default, with `cpsam` model. If you want to use Cellpose 3, you can uninstall the Cellpose 4 from your conda environment and install Cellpose 3: `pip uninstall cellpose` and `pip install cellpose==3.0.0`. In such a case remember to change the `flow_threshold` to 1.5.

### SLURM
- `use_slurm`: Enable/disable SLURM job scheduling (default: false)
- `slurm_partition`: Compute partition to use
- `mem_mb`: Memory allocation per job
- `tasks`: Number of parallel tasks
- `nodes`: Number of compute nodes

In order for SLURM jobs to be executed, you have to launch `photon-mosaic` inside an environment in an interactive job in your cluster.

For more details about SLURM configuration options, see the [Snakemake SLURM executor plugin documentation](https://github.com/snakemake/snakemake-executor-plugin-slurm).
