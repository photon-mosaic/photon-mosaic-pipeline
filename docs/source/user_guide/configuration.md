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
  _common: &common
    tasks: 1
    nodes: 1

  preprocessing:
    <<: *common
    slurm_partition: "cpu"
    mem_mb: 8000

  suite2p:
    <<: *common
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

Each step of the pipeline — each Snakemake *rule* — is sent to SLURM as its own job, and every job has to say what hardware it needs: which partition, how much memory, and so on. Different steps need different things: `suite2p` needs a GPU, preprocessing does not.

So `slurm:` holds **one block per rule, named after the rule**, and a rule reads only its own block. If both steps shared one set of resources, preprocessing would ask for a GPU as well — and a job that asks for a GPU waits in the queue for one, so a step that never touches the GPU would sit waiting for it anyway.

Repeating the settings that genuinely are the same in every block gets tedious, so write them once and reuse them. YAML does this on its own, with an anchor (`&common`) and a merge (`<<: *common`). Any key starting with `_` is ignored when the resources are read, which makes `_common` a safe place to keep the shared ones:

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

A rule's own keys win over the shared ones: above, `runtime` is the same everywhere while `mem_mb` differs. Apart from the anchor nothing is inherited — a rule gets exactly what its own block lists — so you never have to switch off something another rule asked for. `slurm_account` is specific to your cluster: replace the placeholder with your own account, or delete the line if your cluster does not need one.

**Every rule needs a block.** A rule with no block of its own asks for nothing except its log paths, and is submitted with whatever your cluster gives it by default. If you add a rule, add its block. (A `slurm:` block written the old way, with no per-rule blocks inside it at all, still applies to every rule, so existing configs keep working — except that `null` values and nested blocks are no longer passed on as resources.)

#### GPU resources: `gpu` vs `gres`

The Snakemake SLURM executor plugin offers **two mutually exclusive ways** to request a GPU, because `--gpus` and `--gres` describe the same TRES (Trackable RESource) two different ways, and asking for both is ambiguous. You have to pick one! Set both and Snakemake refuses to build the submission at all, with `GRES and GPU are set. Please only set one of them.` Put the key in the block of the rule that needs the GPU — usually `suite2p` — so that no other rule requests one.

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
