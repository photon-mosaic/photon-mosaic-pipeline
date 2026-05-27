# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

import setuptools_scm

# Used when building API docs, put the dependencies
# of any class you are documenting here
autodoc_mock_imports = []

# Add the module path to sys.path here.
# If the directory is relative to the documentation root,
# use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath("../.."))

project = "photon-mosaic"
copyright = "2025, University College London"
author = "Laura Porta"
try:
    release = setuptools_scm.get_version(root="../..", relative_to=__file__)
    release = release.split(".dev")[0]
except LookupError:
    # if git is not initialised, still allow local build
    # with a dummy version
    release = "0.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.githubpages",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "myst_parser",
    "numpydoc",
    "numpydoc",
    "sphinx_autodoc_typehints",
    "sphinx_design",
    "sphinxarg.ext",
    # 'sphinx_gallery.gen_gallery',
]

# Configure the myst parser to enable cool markdown features
# See https://sphinx-design.readthedocs.io
myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]
# Automatically add anchors to markdown headings
myst_heading_anchors = 3

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# Automatically generate stub pages for API
autosummary_generate = True
numpydoc_class_members_toctree = False  # stops stubs warning
# Don't enumerate inherited members in each class' Attributes/Methods summary.
# Otherwise numpydoc lists ``logging.Formatter.converter`` (== ``time.localtime``)
# on ``ColoredFormatter``, which autodoc cannot introspect (spurious warning).
numpydoc_show_inherited_class_members = False
#toc_object_entries_show_parents = "all"
html_show_sourcelink = False

autodoc_default_options = {
    'members': True,
    "member-order": "bysource",
    'special-members': False,
    'private-members': False,
    'inherited-members': False,
    'undoc-members': True,
    'exclude-members': "",
}

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    "**.ipynb_checkpoints",
    # to ensure that include files (partial pages) aren't built, exclude them
    # https://github.com/sphinx-doc/sphinx/issues/1965#issuecomment-124732907
    "**/includes/**",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_theme = "pydata_sphinx_theme"
html_title = "photon-mosaic"

# Customize the theme
html_theme_options = {
    "icon_links": [
        {
            # Label for this link
            "name": "GitHub",
            # URL where the link will redirect
            "url": "https://github.com/neuroinformatics-unit/photon-mosaic",  # required
            # Icon class (if "type": "fontawesome"),
            # or path to local image (if "type": "local")
            "icon": "fa-brands fa-github",
            # The type of image to be used (see below for details)
            "type": "fontawesome",
            "use_edit_page_button": False,  # Ensure the edit button doesn't interfere
            "navigation_with_keys": False,  # Disable keyboard navigation between sections
            "collapse_navigation": False,  # Ensure full page loads rather than AJAX content swap
        },
        {
            # Label for this link
            "name": "Zulip (chat)",
            # URL where the link will redirect
            "url": "https://neuroinformatics.zulipchat.com/#narrow/channel/500681-photon-mosaic",  # required
            # Icon class (if "type": "fontawesome"), or path to local image (if "type": "local")
            "icon": "fa-solid fa-comments",
            # The type of image to be used (see below for details)
            "type": "fontawesome",
        },
    ],
    "logo": {
        "text": f"{project} v{release}",
    },
    "footer_start": ["footer_start"],
    "footer_end": ["footer_end"],
}

# Redirect the webpage to another URL
# Sphinx will create the appropriate CNAME file in the build directory
# The default is the URL of the GitHub pages
# https://www.sphinx-doc.org/en/master/usage/extensions/githubpages.html
github_user = "neuroinformatics-unit"
html_baseurl = "https://photon-mosaic.neuroinformatics.dev/"
sitemap_url_scheme = "{link}"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = [
    'css/custom.css',
]

html_favicon = "_static/light-logo-niu.png"
html_logo = "_static/logo.png"

# # Configure Sphinx gallery
# sphinx_gallery_conf = {
#     "examples_dirs": ["../../examples"],
#     "filename_pattern": "/*.py",  # which files to execute before inclusion
#     "gallery_dirs": ["examples"],  # output directory
#     "run_stale_examples": True,  # re-run examples on each build
#     # Integration with Binder, see https://sphinx-gallery.github.io/stable/configuration.html#generate-binder-links-for-gallery-notebooks-experimental
#     "binder": {
#         "org": "neuroinformatics-unit",
#         "repo": "photon-mosaic",
#         "branch": "gh-pages",
#         "binderhub_url": "https://mybinder.org",
#         "dependencies": ["requirements.txt"],
#     },
#     "reference_url": {"photon-mosaic": None},
#     # "default_thumb_file": "source/_static/data_icon.png",  # default thumbnail image
#     "remove_config_comments": True,
#     # do not render config params set as # sphinx_gallery_config [= value]
# }

linkcheck_ignore = [
    "https://neuroinformatics.zulipchat.com/#narrow/channel/500681-photon-mosaic",
    "https://www.sciencedirect.com/science/article/pii/S089662731930889X",
    "https://photon-mosaic.neuroinformatics.dev/*",
]
