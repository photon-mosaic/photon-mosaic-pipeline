import os
from pathlib import Path

# Modules to exclude from the API index
EXCLUDE_MODULES = {"__init__"}

# Set the current working directory to the directory of this script
SCRIPT_DIR = Path(__file__).resolve().parent
os.chdir(SCRIPT_DIR)


def make_api_index():
    """
    Generate a properly formatted `api_index.rst` file for
    Sphinx documentation."""

    api_path = Path("../photon_mosaic_pipeline")
    module_entries = []

    for path in sorted(api_path.rglob("*.py")):
        # Convert file path to module name
        rel_path = path.relative_to(api_path.parent)
        module_name = str(rel_path.with_suffix("")).replace(os.sep, ".")

        if rel_path.stem not in EXCLUDE_MODULES:
            module_entries.append(module_name)

    # Construct the API index content
    api_index_content = """\
.. _target-api:

API Reference
=============

This section contains automatically generated documentation for the
`photon-mosaic-pipeline` package.

.. rubric:: Modules

.. autosummary::
   :toctree: api
   :nosignatures:

"""

    # Add module entries for autosummary
    api_index_content += (
        "\n".join(f"   {module}" for module in module_entries) + "\n"
    )

    # Write the generated content to `api_index.rst`
    output_path = Path("source") / "api_index.rst"
    output_path.write_text(api_index_content, encoding="utf-8")

    print(f"Generated {output_path}")


if __name__ == "__main__":
    make_api_index()
