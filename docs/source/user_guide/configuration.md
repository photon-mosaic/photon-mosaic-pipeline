(user_guide/configuration)=
# Configuration

The configuration system in `photon-mosaic-pipeline` is designed to be flexible and user-friendly. It allows you to customize the behavior of the pipeline at different levels.

## Configuration Files

### User Configuration
On first run, `photon-mosaic-pipeline` will create a user config at `~/.photon_mosaic_pipeline/config.yaml` if it does not exist. This serves as your default configuration.

`photon-mosaic-pipeline` expects you to organise your data in a project directory that follows the [NeuroBlueprint](https://neuroblueprint.neuroinformatics.dev/) specification. Raw data lives under `<project_path>/rawdata/`, and derivatives are written next to it under `<project_path>/derivatives/`. See the [data input documentation](data_input.md) for the required directory layout.

By default `project_path` is set to `./`, so if you launch the pipeline from
inside your project directory you can run it without any arguments. Otherwise,
set the project directory explicitly with `--project_path` on the first run:

```bash
photon-mosaic-pipeline --project_path /my/project
```

After the first run, the path is stored in `~/.photon_mosaic_pipeline/config.yaml` and you can simply run `photon-mosaic-pipeline`.

In case you want to reset the config to the default values, you can run `photon-mosaic-pipeline --reset-config`. You can also specify `--project_path` again on subsequent runs to override what is stored in the config.

If you want to store your config file somewhere else, you can specify the path to the config file with the `--config` flag.

The config file that is used for each run (with any overrides) is exported to `derivatives/photon-mosaic-pipeline/configs/YYYYMMDD_HHMMSS_config.yaml`.

## Configuration Structure

The configuration file is organized into several main sections. Here is a simplified example showing the key sections:

```yaml
# Project path (must follow NeuroBlueprint: rawdata/sub-*/ses-*/funcimg/)
# Defaults to "./" (the current folder); override with --project_path
project_path: ./

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

# SLURM settings (one block per rule, named after the rule)
use_slurm: false
slurm:
  preprocessing:
    slurm_partition: "cpu"
    mem_mb: 8000
  suite2p:
    slurm_partition: "gpu"
    mem_mb: 32000
```

For the complete configuration file with all available parameters and detailed comments, see [photon_mosaic_pipeline/workflow/config.yaml](https://github.com/photon-mosaic/photon-mosaic-pipeline/blob/main/photon_mosaic_pipeline/workflow/config.yaml) or the YAML file in `~/.photon_mosaic_pipeline/config.yaml` generated on first run.

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
- `slurm`: One sub-block per rule (see below), each holding that rule's resources:
  - `slurm_partition`: Compute partition to use
  - `mem_mb`: Memory allocation per job
  - `tasks`: Number of parallel tasks
  - `nodes`: Number of compute nodes

In order for SLURM jobs to be executed, you have to launch `photon-mosaic-pipeline` inside an environment in an interactive job in your cluster.

#### Resources are set per rule

The `slurm:` block holds **one sub-block per rule, named after the rule**. Each rule uses only its own block:

```yaml
slurm:
  preprocessing:
    slurm_partition: "cpu"
    mem_mb: 8000

  suite2p:
    slurm_partition: "gpu"
    gres: "gpu:a4500:1"
    mem_mb: 32000
```

Rules have genuinely different needs — `suite2p` wants a GPU, preprocessing is CPU-only — and giving every rule the same resources means the CPU step requests a GPU it never uses and then waits in the GPU queue for it. With its own block, preprocessing goes straight to the CPU partition.

Nothing is inherited between blocks, so a rule never has to *remove* a resource it did not want.

**Every rule needs a block.** A rule with no block of its own requests nothing but its log paths, and will be submitted with whatever your cluster defaults to. If you add a rule, add its block.

#### Sharing keys between rules

For keys that really are the same everywhere, use a YAML anchor rather than repeating them. Define them once under a key starting with `_` (keys starting with `_` are treated as config scaffolding and are never passed to SLURM), then pull them in with `<<:`:

```yaml
slurm:
  _common: &common
    tasks: 1
    nodes: 1
    runtime: 120
    # slurm_account: "your_account_name"   # replace with YOUR cluster account

  preprocessing:
    <<: *common
    slurm_partition: "cpu"
    mem_mb: 8000

  suite2p:
    <<: *common
    slurm_partition: "gpu"
    mem_mb: 32000
```

A key set in a rule's own block wins over the anchor, so `mem_mb` above differs per rule while `runtime` is shared.

`slurm_account` is specific to your cluster — the shipped config leaves it commented out with a placeholder. Replace `your_account_name` with your own account, or delete the line if your cluster does not require one.

#### Older, flat configs

A `slurm:` block with no per-rule sub-blocks still applies to every rule, so existing configs keep working and per-rule blocks are opt-in. Two small differences from before: `null` values and any nested blocks are no longer passed to SLURM as resources.

You can also migrate one rule at a time — give a rule its own block and leave the rest on the flat keys. A rule with its own block uses **only** that block; it does not pick up the flat keys as well.

#### GPU resources: `gpu` vs `gres`

The Snakemake SLURM executor plugin offers **two mutually exclusive ways** to request a GPU. You have to pick one! Combining them produces TRES (Trackable RESources) conflicts at the scheduler. Put them in the block of the rule that needs the GPU — usually `suite2p` — so that no other rule requests one.

- **`gpu`**: request a number of GPUs of any type. The plugin translates this into `--gpus`. Pair with `cpus_per_gpu` if needed.

  ```yaml
  slurm:
    suite2p:
      gpu: 1
      cpus_per_gpu: 4
  ```

- **`gres`**: request a specific GPU model via SLURM's Generic Resource string. The plugin translates this into `--gres`. Use this when the cluster has mixed GPU types and you need a specific one (e.g. `"gpu:a100:1"` for one A100). Do **not** also set `gpu`.

  ```yaml
  slurm:
    suite2p:
      gres: "gpu:a100:1"
  ```

For the upstream mechanics, see the [GRES alternative method](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html#alternative-method-using-the-gres-resource) in the Snakemake SLURM plugin docs.

For more details about SLURM configuration options, see the [Snakemake SLURM executor plugin documentation](https://github.com/snakemake/snakemake-executor-plugin-slurm).
