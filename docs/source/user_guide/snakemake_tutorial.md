(user_guide/snakemake_tutorial)=
# Basic Snakemake Tutorial

With `photon-mosaic`, you can run a Snakemake workflow that automatically executes the necessary steps to process your data. The workflow is included in the installed package and can be customized using a YAML configuration file.

The pipeline expects a [NeuroBlueprint](https://neuroblueprint.neuroinformatics.dev/)-compliant project at `--project_path`, with raw TIFFs under `rawdata/sub-*/ses-*/funcimg/`. Each `(subject, session)` is processed in parallel, and results are written to a mirrored layout under `derivatives/`. See the [data input documentation](data_input.md) for the required structure.

## Why use Snakemake?

Snakemake is a powerful workflow management system that allows you to run complex data analysis pipelines in a reproducible and efficient manner. For each defined rule (a rule is a step in the workflow, for instance running Suite2p), Snakemake checks if the output files already exist and whether they are up to date. If not, it runs the rule and generates the outputs.

This approach lets you rerun only the parts of the workflow that need to be updated, avoiding the need to repeat the entire analysis each time.

Here we show examples that do not call directly the `snakemake` command, but instead use the `photon-mosaic` CLI, which is a wrapper around Snakemake that simplifies the execution of the workflow.

## Dry Run
A dry run is a simulation that shows what would happen if the workflow were executed, without actually running any commands. This is useful for verifying that everything is set up correctly. The output includes a DAG (directed acyclic graph) showing dependencies between rules, which files will be created, and which rules will be executed.

To preview the workflow without running it:

```bash
photon-mosaic --dry-run
```

`--jobs` specifies the number of jobs to run in parallel. You can increase this number to parallelize execution across datasets. A dry run can also be abbreviated to `-np` if using Snakemake directly.

## Running the Workflow

On the first run, you need to point the pipeline at your NeuroBlueprint project:

```bash
photon-mosaic --project_path /path/to/project
```
For more details, see the [configuration documentation](configuration.md).

To run the full workflow after the first run, you can simply run:

```bash
photon-mosaic
```
You can also specify how many jobs you want to run in parallel with the `--jobs` flag.
```bash
photon-mosaic --jobs 5
```

To force the re-execution of a specific rule:

```bash
photon-mosaic --forcerun suite2p
```

To reprocess a specific session, you can specify a target output file (e.g., `F.npy`) from the derivatives tree:

```bash
photon-mosaic /path/to/derivatives/sub-001/ses-001/funcimg/suite2p/plane0/F.npy
```

To run the workflow on a cluster, check instructions in the [configuration documentation](configuration.md).

## Additional Options

Other useful arguments you can pass:

- `--latency-wait`: wait time before checking if output files are ready
- `--rerun-incomplete`: rerun any incomplete jobs

## Direct Snakemake Usage

You can also run the workflow directly with `snakemake`, using the programmatic path to the bundled Snakefile:

```bash
snakemake --snakefile $(python -c 'import photon_mosaic; print(photon_mosaic.get_snakefile_path())') \
          --configfile path/to/config.yaml \
          --jobs 1
```

This is equivalent to using the `photon-mosaic` CLI but gives full control over the Snakemake interface.
