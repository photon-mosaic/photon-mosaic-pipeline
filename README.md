[![Python Version](https://img.shields.io/pypi/pyversions/photon-mosaic-pipeline.svg)](https://pypi.org/project/photon-mosaic-pipeline)
[![PyPI Version](https://img.shields.io/pypi/v/photon-mosaic-pipeline.svg)](https://pypi.org/project/photon-mosaic-pipeline)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-orange.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![CI](https://img.shields.io/github/actions/workflow/status/photon-mosaic/photon-mosaic-pipeline/test_and_deploy.yml?label=CI)](https://github.com/photon-mosaic/photon-mosaic-pipeline/actions)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/format.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

# <img src="docs/source/_static/logo.png" alt="photon-mosaic logo" width="36" style="vertical-align: middle; margin-right:8px"> `photon-mosaic-pipeline`
`photon-mosaic-pipeline` is a Snakemake-based toolkit for processing multiphoton datasets. It orchestrates a curated collection of algorithms to transform your raw data (e.g., TIFF files) into analysis-ready outputs, such as ΔF/F traces, NWB files, or inferred spikes.

> [!NOTE]
> **This project was renamed from `photon-mosaic` to `photon-mosaic-pipeline`.**
>
> The name `photon-mosaic` now refers to a separate package under the same organisation: [`photon-mosaic`](https://github.com/photon-mosaic/photon-mosaic), a Python API (built on [SpikeInterface](https://github.com/SpikeInterface/spikeinterface)) for *exploring and comparing* multiphoton analysis methods interactively: mixing and matching algorithms for each step to find what works for your data.
>
> `photon-mosaic-pipeline` (this repository) is the complementary package for *running the analysis you settled on* as a reproducible, batch-processed workflow at scale.
>
> **This rename is a breaking change.** The PyPI package, CLI command, and import name all move to `photon-mosaic-pipeline`/`photon_mosaic_pipeline`, as do the config dir (`~/.photon_mosaic_pipeline/`) and output folder (`derivatives/photon-mosaic-pipeline/`). Existing configs and derivatives under the old names are **not** picked up automatically: move or re-generate them after upgrading.
>
> Both are part of the broader [**PhotonMosaic** framework](https://photon-mosaic.org), and we plan to integrate the two so the pipeline can run the methods you prototype with the API.

Each analysis step is integrated into an automated workflow, allowing you to chain preprocessing, registration, signal extraction, and post-processing steps into a single, reproducible pipeline. The design prioritizes usability for labs that process many imaging sessions and need to scale across an HPC cluster.

<p align="center">
  <img src="https://raw.githubusercontent.com/photon-mosaic/photon-mosaic-pipeline/refs/heads/main/docs/source/_static/pm_illustration1.png" alt="photon-mosaic"/>
</p>

This is made possible by [Snakemake](https://snakemake.readthedocs.io/en/stable/), a workflow management system that provides a powerful and flexible framework for defining and executing complex data processing pipelines. Snakemake automatically builds a directed acyclic graph (DAG) of all the steps in your analysis, ensuring that each step is executed in the correct order and that intermediate results are cached to avoid redundant computations. `photon-mosaic-pipeline` also includes a [SLURM executor plugin for Snakemake](https://github.com/snakemake/snakemake-executor-plugin-slurm) to seamlessly scale your analysis across an HPC cluster. To ensure consistency and reproducibility, `photon-mosaic-pipeline` writes processed data according to the [NeuroBlueprint](https://neuroblueprint.neuroinformatics.dev/latest/index.html) data standard for organizing and storing multiphoton imaging data.

The goal of `photon-mosaic-pipeline` is to provide a modular, extensible, and user-friendly framework for multiphoton data analysis that can be easily adapted to different experimental designs and analysis requirements. For each processing step, we aim to vet and integrate the best available open-source tools, providing sensible defaults tailored to the specific data type and experimental modality.

## Roadmap
### Current features
- Preprocessing: [derotation](https://github.com/neuroinformatics-unit/derotation) and contrast enhancement (see `photon_mosaic_pipeline/preprocessing`).
- Registration & source extraction using [Suite2p](https://github.com/MouseLand/suite2p).
- Cell detection / anatomical ROI extraction using [Cellpose (v3 or v4, including Cellpose-SAM)](https://github.com/MouseLand/cellpose).

### Planned additions
- Registration using [NoRMCorre](https://github.com/flatironinstitute/NoRMCorre) for non-rigid motion correction.
- ROI matching using [ROICat](https://github.com/RichieHakim/ROICaT) for inter-session / inter-plane ROI matching.
- Neuropil subtraction / decontamination: methods from the [AllenSDK](https://allensdk.readthedocs.io/en/latest/allensdk.brain_observatory.r_neuropil.html) and [AST-model](https://github.com/znamlab/2p-preprocess).
- Spike deconvolution: [OASIS](https://github.com/j-friedrich/OASIS) and [CASCADE](https://github.com/HelmchenLabSoftware/Cascade).

See [issues on GitHub](https://github.com/photon-mosaic/photon-mosaic-pipeline/issues) for more details and participate in planning. Please refer to our [guidelines](https://pipeline.photon-mosaic.org/contributing.html) to understand how to contribute.

## Installation

`photon-mosaic-pipeline` requires **Python 3.11** or **3.12**.

```bash
conda create -n photon-mosaic-pipeline python=3.12
conda activate photon-mosaic-pipeline
pip install photon-mosaic-pipeline
```

To install with developer tools (e.g., testing and linting):

```bash
pip install 'photon-mosaic-pipeline[dev]'
```
