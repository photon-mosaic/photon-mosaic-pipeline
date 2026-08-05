(user_guide/data_input)=
# Data Input

`photon-mosaic-pipeline` expects raw data to follow the [NeuroBlueprint](https://neuroblueprint.neuroinformatics.dev/) specification. This means your `project_path` will contain a `rawdata/` folder organised like this. `photon-mosaic-pipeline` will only look at `funcimg` subfolders in your project.

```
project_path/
└── rawdata/
    ├── sub-001_id-M001/
    │   ├── ses-001_date-20250101/
    │   │   └── funcimg/
    │   │       ├── recording_00001.tif
    │   │       └── ...
    │   └── ses-002_date-20250102/
    │       └── funcimg/
    │           └── ...
    └── sub-002_id-M002/
        └── ses-001_date-20250105/
            └── funcimg/
                └── ...
```

Outputs are written by `photon-mosaic-pipeline` next to `rawdata/` under `derivatives/`, mirroring the `sub-*/ses-*/funcimg/` structure.

The project is validated on every run via [`datashuttle.validate_project_from_path`](https://datashuttle.neuroinformatics.dev/) and should give you a clear error if anything went wrong.

## Converting an existing dataset

If your raw data does not yet follow the NeuroBlueprint specification, we recommend you organise it first with [datashuttle](https://datashuttle.neuroinformatics.dev/), then point `photon-mosaic-pipeline` at the resulting project folder.

## Filtering which files are processed

Inside `rawdata/`, three keys under `dataset_discovery` in your `config.yaml` control which TIFFs feed into the workflow:

```yaml
dataset_discovery:
  # Glob patterns for the TIFF files to include, applied under
  # rawdata/sub-*/ses-*/funcimg/. Most projects need only "*.tif".
  tiff_patterns: ["*.tif"]

  # Regex patterns matched against subject folder names. Any subject
  # whose folder fully matches one of these patterns is skipped.
  exclude_datasets:
    - "sub-test.*"
    - "sub-IAA.*"

  # Regex patterns matched against session folder names. Any session
  # whose folder fully matches one of these patterns is skipped.
  exclude_sessions:
    - "ses-.*protocol-screening.*"
```

Notes:

- `tiff_patterns` is a list of *glob* patterns. A glob is a filename pattern where `*` matches any sequence of characters, so `*.tif` matches every file ending in `.tif`, and `recording_*.tif` matches only files starting with `recording_` (and ending in `*.tif`).
- When you list more than one pattern, the results are *unioned*: a file is included if it matches **any** pattern in the list. For example, `["*.tif", "*.tiff"]` picks up files with either extension. Most projects need only `"*.tif"`.
- The pipeline does not infer session numbering from the filename — sessions come from `ses-*` folder names.
- `exclude_datasets` and `exclude_sessions` use full-match regex (`re.fullmatch`), not glob.
- Both lists default to empty; nothing is excluded unless you say so.

## What changed (for users coming from earlier versions)

Earlier versions shipped a `dataset_discovery` module that auto-numbered subjects and sessions from sorted dataset folder names (e.g. `full002` → `ses-002`). That module has been removed. Two consequences:

- The `pattern` key under `dataset_discovery` in `config.yaml` is no longer read — you can delete it.
- Adding a new raw recording no longer shifts the session indices of existing recordings, because the pipeline never assigns those indices in the first place. Naming is whatever you (or datashuttle) put on disk.

If you maintained name-based session ordering by relying on the old auto-numbering, you will need to migrate that data into a NeuroBlueprint tree before running the pipeline.
