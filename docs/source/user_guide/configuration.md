(user_guide/configuration)=
# Configuration

The configuration system in `photon-mosaic` is designed to be flexible and user-friendly. It allows you to customize the behavior of the pipeline at different levels.

## Configuration Files

### User Configuration
On first run, photon-mosaic will create a user config at `~/.photon_mosaic/config.yaml` if it does not exist. This serves as your default configuration.

`photon-mosaic` expects you to organise your data in a project directory that follows the [NeuroBlueprint](https://neuroblueprint.neuroinformatics.dev/) specification. Raw data lives under `<project_path>/rawdata/`, and derivatives are written next to it under `<project_path>/derivatives/`. See the [data input documentation](data_input.md) for the required directory layout.

You set the project directory with `--project_path` on the first run:

```bash
photon-mosaic --project_path /my/project
```

After the first run, the path is stored in `~/.photon_mosaic/config.yaml` and you can simply run `photon-mosaic`.

In case you want to reset the config to the default values, you can run `photon-mosaic --reset-config`. You can also specify `--project_path` again on subsequent runs to override what is stored in the config.

If you want to store your config file somewhere else, you can specify the path to the config file with the `--config` flag.

The config file that is used for each run (with any overrides) is exported to `derivatives/photon-mosaic/configs/YYYYMMDD_HHMMSS_config.yaml`.

## Configuration Structure

The configuration file is organized into several main sections. Here is a simplified example showing the key sections:

```yaml
# Project path (must follow NeuroBlueprint: rawdata/sub-*/ses-*/funcimg/)
project_path: "/path/to/project/"

# Filters applied to the NeuroBlueprint tree
dataset_discovery:
  tiff_patterns: ["*.tif"]
  exclude_datasets:
    - "sub-test.*"
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

## Further Configuration Notes

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

#### GPU resources: `gpu` vs `gres`

The Snakemake SLURM executor plugin offers **two mutually exclusive ways** to request a GPU. You have to pick one! Combining them produces TRES (Trackable RESources) conflicts at the scheduler.

- **`gpu`**: request a number of GPUs of any type. The plugin translates this into `--gpus`. Pair with `cpus_per_gpu` if needed.

  ```yaml
  slurm:
    gpu: 1
    cpus_per_gpu: 4
  ```

- **`gres`**: request a specific GPU model via SLURM's Generic Resource string. The plugin translates this into `--gres`. Use this when the cluster has mixed GPU types and you need a specific one (e.g. `"gpu:a100:1"` for one A100). Do **not** also set `gpu`.

  ```yaml
  slurm:
    gres: "gpu:a100:1"
  ```

These keys live under `slurm:` but are forwarded to the rule level, not to Snakemake's `--default-resources`. On startup you will see an `INFO` log line such as:

```
INFO:photon_mosaic.cli:Skipping gres in --default-resources (set at rule level to avoid conflicts): gpu:a100:1
```

That is expected behaviour, not an error: it confirms the value was picked up and routed to per-rule resources to avoid TRES conflicts. The same applies to `gpu` and `cpus_per_gpu`.

For the upstream mechanics, see the [GRES alternative method](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html#alternative-method-using-the-gres-resource) in the Snakemake SLURM plugin docs.

For more details about SLURM configuration options, see the [Snakemake SLURM executor plugin documentation](https://github.com/snakemake/snakemake-executor-plugin-slurm).
