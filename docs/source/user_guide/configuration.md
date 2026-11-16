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

# Motion correction (photon-mosaic)
motion_correction:
  reader: read_tiff_imaging  # function of photon_mosaic.extractors
  reader_kwargs:
    sampling_frequency: 10.0
  method: suite2p
  settings:
    nonrigid: true

# ROI detection (photon-mosaic)
segmentation:
  method: suite2p
  settings:
    algorithm: cellpose
    fs: 10.0
    tau: 1.0

# Traces (photon-mosaic RoiAnalyzer extensions)
neuropil:
  inner_neuropil_radius: 2
fluorescence:
  neuropil_weight: 0.7
dff:
  win_baseline: 60.0

# Also save the traces of each TIFF on their own
split_multitiff: false

# SLURM settings
use_slurm: false
slurm:
  slurm_partition: "gpu"
  mem_mb: 32000
  tasks: 1
  nodes: 1
```

For the complete configuration file with all available parameters and detailed comments, see [photon_mosaic_pipeline/workflow/config.yaml](https://github.com/photon-mosaic/photon-mosaic-pipeline/blob/main/photon_mosaic_pipeline/workflow/config.yaml) or the YAML file in `~/.photon_mosaic_pipeline/config.yaml` generated on first run.

## Further Configuration Notes

### Preprocessing
See the [preprocessing documentation](preprocessing.md) for step-specific configuration

### Motion correction, segmentation and traces
After preprocessing, every step is run with the [photon-mosaic](https://api.photon-mosaic.org) package, one Snakemake rule per step. All the TIFFs of a session are processed together, one epoch per TIFF.

| Rule | Config section | What it does | Output folder in `funcimg/` |
|------|----------------|--------------|-----------------------------|
| `motion_correction` | `motion_correction` | Estimates the motion and saves the registered movie | `motion_correction/` |
| `segmentation` | `segmentation` | Detects ROIs on the registered movie | `segmentation/` |
| `neuropil` | `neuropil` | Builds the neuropil mask of each ROI | `roi_analyzer/extensions/neuropil/` |
| `fluorescence` | `fluorescence` | Extracts the neuropil-corrected trace of each ROI | `roi_analyzer/extensions/fluorescence/` |
| `dff` | `dff` | Computes dF/F | `roi_analyzer/extensions/df_over_f/` |

Every output folder can be opened in Python with `photon_mosaic.load(folder)`: `motion_correction/` as an imaging object, `segmentation/rois/` as ROIs and `roi_analyzer/` as a `RoiAnalyzer` holding the three trace extensions.

#### Reading the TIFFs
`motion_correction.reader` is the name of the function of `photon_mosaic.extractors` that opens one TIFF, and `reader_kwargs` its extra arguments. A plain TIFF does not store its sampling rate, so `read_tiff_imaging` needs `sampling_frequency`. `read_scan_image_imaging` reads it from the ScanImage metadata, which only the `noop` preprocessing step keeps: the other steps write plain TIFFs.

#### Settings
`method` selects the backend of a step (`suite2p` for now) and `settings` holds the backend's settings. The ones you leave out keep their default. The keys under `neuropil`, `fluorescence` and `dff` are the parameters of the matching `RoiAnalyzer` extension. Refer to the [photon-mosaic documentation](https://api.photon-mosaic.org) for the complete lists.

Two settings to check for your data:
- `segmentation.settings.fs` is the sampling rate used to bin the movie for detection. Set it to the sampling rate of your data.
- `device` is `cpu` by default for both motion correction and segmentation. Set it to `cuda` (or `mps`) to use a GPU.

#### Cell Detection
`segmentation.settings.algorithm` selects how ROIs are found: `sparsery` and `sourcery` are Suite2p's functional detection, `cellpose` runs Cellpose-SAM on the registered movie.

#### One output per TIFF
With `split_multitiff: true`, the `fluorescence` and `dff` rules also save the traces of each TIFF on their own, as `fluorescence_tiff<i>.npy` and `df_over_f_tiff<i>.npy` in `funcimg/split_multitiff/`. The TIFFs are numbered in the order they were read, sorted by path.

### SLURM
- `use_slurm`: Enable/disable SLURM job scheduling (default: false)
- `slurm_partition`: Compute partition to use
- `mem_mb`: Memory allocation per job
- `tasks`: Number of parallel tasks
- `nodes`: Number of compute nodes

In order for SLURM jobs to be executed, you have to launch `photon-mosaic-pipeline` inside an environment in an interactive job in your cluster.

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
INFO:photon_mosaic_pipeline.cli:Skipping gres in --default-resources (set at rule level to avoid conflicts): gpu:a100:1
```

That is expected behaviour, not an error: it confirms the value was picked up and routed to per-rule resources to avoid TRES conflicts. The same applies to `gpu` and `cpus_per_gpu`.

For the upstream mechanics, see the [GRES alternative method](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html#alternative-method-using-the-gres-resource) in the Snakemake SLURM plugin docs.

For more details about SLURM configuration options, see the [Snakemake SLURM executor plugin documentation](https://github.com/snakemake/snakemake-executor-plugin-slurm).
