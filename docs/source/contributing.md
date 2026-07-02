# Contributing Guide

Contributions to ``photon-mosaic-pipeline`` are very welcome and appreciated. This could be
fixing a bug, improving the documentation or developing a new feature.

If you're unsure about any part of the contributing process or have any questions, please
get in touch through our [Zulip chat](https://neuroinformatics.zulipchat.com/#narrow/channel/500681-photon-mosaic).
Otherwise, feel free to dive right in and start contributing by
[creating a development environment](#creating-a-development-environment)
and [opening a pull request](#pull-requests).

## Creating a development environment

To install ``photon-mosaic-pipeline`` for development, first the
[GitHub repository](https://github.com/photon-mosaic/photon-mosaic-pipeline)
should be cloned. Then, you can change-directory
to the cloned repository and run pip install with the developer tag:

```sh
pip install -e .[dev]
```

or if using `zsh`:

```sh
pip install -e '.[dev]'
```

Finally, initialise the [pre-commit hooks](#formatting-and-pre-commit-hooks):

```bash
pre-commit install
```

## Adding a New Feature

To add a new feature, start by creating a new `.smk` rule file inside the `photon_mosaic_pipeline/workflow/` folder. This rule should either call an installed package or use a wrapper function defined in the `photon_mosaic_pipeline/rules/` Python module.

If your feature needs specific parameters, make sure these are read from the `config.yaml` file, so they can be easily adjusted by the user.

If the feature requires additional Python packages, you must add them to the `project.dependencies` section of the `pyproject.toml` file. Please make sure the package is compatible with the rest of the environment. If the latest version is not suitable or you're unsure which version to use, feel free to ask for help in the Zulip chat or open a discussion in your pull request.

You should also add tests under the `tests/` folder to ensure the rule runs correctly, produces the expected output, and handles edge cases.

Finally, document your feature. If you’re writing a wrapper function, include a clear docstring: it will be automatically included in the API reference docs. For user-facing features, add a short section to the `docs/` folder describing what the feature does, what inputs and config options it expects, and what outputs it generates. An example usage is helpful, but optional.

Making sure each feature is modular, tested, and documented keeps the project clean and easy to maintain.


## Path and Wildcard Handling in Snakemake

Key considerations for handling paths and wildcards in Snakemake:

### Wildcard Syntax
Wildcards are used to define the pattern of the output files and are inferred via the outputs paths. Use single curly braces for wildcards in output paths: `{wildcard_name}`. You can use the `wildcards` object to access the values of the wildcards in the rule by constructing a lambda function: `lambda wildcards: str(Path(base_dir) / f"sub-{wildcards.sub_idx}" / "data.npy")`. In the parameters, you can use the wildcards to construct the path to the input files. Wildcards can be constrained to a specific set of values using the `wildcard_constraints` section.

Use the `cross_platform_path` function to construct paths that are compatible with different operating systems.

Example:
```python
from photon_mosaic_pipeline.snakemake_utils import cross_platform_path

rule example:
    input:
        file=lambda wildcards: cross_platform_path(Path(base_dir) / f"sub-{wildcards.sub_idx}" / "data.npy")
    output:
        result=cross_platform_path(Path(output_dir) / "sub-{sub_idx}" / "{my_file_name}")
    params:
        folder=lambda wildcards: cross_platform_path(Path(base_dir) / f"sub-{wildcards.sub_idx}")
    wildcard_constraints:
        my_file_name="|".join(["data_1.npy", "data_2.bin"])
```

## Pull requests

In all cases, please submit code to the main repository via a pull request. The developers recommend and adhere
to the following conventions:

- Please submit *draft* pull requests as early as possible (you can still push to the branch once submitted) to
  allow for discussion.
- One approval of a PR (by a repo maintainer) is sufficient for it to be merged.
- If the PR receives approval without additional comments, it will be merged immediately by the approving reviewer.

A typical PR workflow would be as follows:
* Create a new branch, make your changes, and add them with `git add`.
* When you attempt to commit, the [pre-commit hooks](#formatting-and-pre-commit-hooks) will be triggered.
* `git add` any changes made by the hooks, and commit. More information on dealing with the [pre-commit hooks](#formatting-and-pre-commit-hooks) is available below.
* Push your changes to GitHub and open a draft pull request.
* Please don't hesitate to ask any developer for help on draft pull requests at our [Zulip chat](https://neuroinformatics.zulipchat.com/#narrow/channel/500681-photon-mosaic).
* If all checks run successfully, mark the pull request as ready for review.
* Respond to review comments and implement any requested changes.
* One of the maintainers will approve the PR.
* Your PR will be merged into the *main* branch!

## Formatting and pre-commit hooks

Running `pre-commit install` will set up [pre-commit hooks](https://pre-commit.com/) to ensure a consistent formatting style. Currently, these include:
* [ruff](https://github.com/astral-sh/ruff), which does a number of jobs including code linting and auto-formatting.
* [mypy](https://mypy.readthedocs.io/en/stable/index.html), a static type checker.
* [check-manifest](https://github.com/mgedmin/check-manifest), to ensure that the right files are included in the pip package.
* [codespell](https://github.com/codespell-project/codespell), to check for common misspellings.


Pre-commit hooks will automatically run when a commit is made.
To manually run all the hooks before committing:

```sh
pre-commit run     # for staged files
pre-commit run -a  # for all files in the repository
```

Some problems will be automatically fixed by the hooks. In this case, you should
stage the auto-fixed changes and run the hooks again:

```sh
git add .
pre-commit run
```

If a problem cannot be auto-fixed, the corresponding tool will provide
information on what the issue is and how to fix it. For example, `ruff` might
output something like:

```sh
my_photon-mosaic.py:551:80: E501 Line too long (90 > 79)
```

This pinpoints the problem to a single line of code and a specific [ruff rule](https://docs.astral.sh/ruff/rules/) violation.
Sometimes you may have good reasons to ignore a particular rule for a specific line.
You can do this by adding an inline comment, e.g. `# noqa: E501`. Replace `E501` with the code of the rule you want to ignore.

Don't worry if you are stuck and are not sure how to fix linting
issues. Feel free to commit with the `--no-verify` option which will
skip pre-commit checks, and ask for help in your PR.

For docstrings, we adhere to the [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html) style.
Make sure to provide docstrings for all public functions, classes, and methods.

## Contributing documentation

If you notice any areas where the documentation can be improved,
please don't hesitate to make a contribution.

### Working with the documentation

The documentation is found in the `docs/source` folder, where the structure mirrors the rendered website.

Dependencies for building the documentation locally can be found at `docs/requirements.txt`.
To install these, change directory to the `docs` folder in your terminal and type:

```
pip install -r requirements.txt
```

The command to build the documentation is:

```
make clean api_index.rst html
```

Any existing builds will be removed, and documentation will be built and output
to the `build` folder. To read the built documentation in a browser, navigate to the `build`
folder and open the `index.html` file.



### Editing the documentation

The documentation is hosted using [GitHub Pages](https://pages.github.com/), and the source can be found at
[GitHub](https://github.com/photon-mosaic/photon-mosaic-pipeline/tree/main/docs).
Most content is found under `docs/source`, where the structure mirrors the rendered website.

To edit a page, please:

- Fork the repository
- Make edits to the relevant pages
- Build the pages as above to inspect your changes
- Create a pull request outlining the changes made

If you aren't sure where the changes should be made, please
[get in touch!](https://neuroinformatics.zulipchat.com/#narrow/channel/500681-photon-mosaic)
