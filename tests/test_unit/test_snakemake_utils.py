"""
Unit tests for the Snakemake utility functions.
"""

import re

import pytest
import yaml

from photon_mosaic_pipeline import get_snakefile_path
from photon_mosaic_pipeline.snakemake_utils import slurm_resources

# ---------------------------------------------------------------------------
# slurm_resources
#
# The `slurm:` block holds one sub-block per rule, named after the rule. A rule
# reads ONLY its own block: nothing is inherited, so nothing ever has to be
# removed. The cases below encode that, plus the backwards-compatible reading
# of a flat `slurm:` block (which applied to every rule before per-rule blocks
# existed) and the two kinds of key that must never reach SLURM -- nulls and
# the `_`-prefixed anchors used for YAML reuse.
# ---------------------------------------------------------------------------

PER_RULE = {
    "use_slurm": True,
    "slurm": {
        "preprocessing": {"slurm_partition": "cpu", "mem_mb": 8000},
        "suite2p": {"slurm_partition": "gpu", "gres": "gpu:a4500:1"},
    },
}

FLAT = {
    "use_slurm": True,
    "slurm": {"slurm_partition": "gpu", "mem_mb": 32000},
}

MIXED = {
    "use_slurm": True,
    "slurm": {
        "slurm_partition": "gpu",
        "suite2p": {"gres": "gpu:a4500:1"},
    },
}

ANCHORED = {
    "use_slurm": True,
    "slurm": {
        "_common": {"tasks": 1},
        "suite2p": {"_note": "ignored", "tasks": 1, "mem_mb": 32000},
    },
}


@pytest.mark.parametrize(
    "config, rule_name, expected",
    [
        # Each rule resolves to its own sub-block, and nothing else. `gres`
        # belongs to suite2p only: under the previous shape every rule got the
        # whole `slurm:` block, so preprocessing requested a GPU it never used
        # and queued behind busy GPUs for nothing.
        pytest.param(
            PER_RULE,
            "preprocessing",
            {"slurm_partition": "cpu", "mem_mb": 8000},
            id="own-block",
        ),
        pytest.param(
            PER_RULE,
            "suite2p",
            {"slurm_partition": "gpu", "gres": "gpu:a4500:1"},
            id="no-inheritance",
        ),
        # Backwards compatibility: existing user configs are flat and must
        # keep behaving exactly as before, the whole block going to every rule.
        pytest.param(
            FLAT,
            "preprocessing",
            {"slurm_partition": "gpu", "mem_mb": 32000},
            id="flat-applies-to-preprocessing",
        ),
        pytest.param(
            FLAT,
            "suite2p",
            {"slurm_partition": "gpu", "mem_mb": 32000},
            id="flat-applies-to-suite2p",
        ),
        # A rule with no block of its own falls back to the flat keys ONLY.
        # Another rule's sub-block is not a resource, and Snakemake resources
        # must be scalars -- a dict there is an error.
        pytest.param(
            MIXED,
            "preprocessing",
            {"slurm_partition": "gpu"},
            id="flat-fallback-ignores-sibling-blocks",
        ),
        # ... and the blocked rule does not pick the flat keys up as well.
        pytest.param(
            MIXED,
            "suite2p",
            {"gres": "gpu:a4500:1"},
            id="fallback-is-not-inheritance",
        ),
        # `_`-prefixed keys hold YAML anchors: config scaffolding, never a
        # resource, at either level.
        pytest.param(
            ANCHORED,
            "suite2p",
            {"tasks": 1, "mem_mb": 32000},
            id="anchor-scaffolding-is-not-a-resource",
        ),
        pytest.param(ANCHORED, "preprocessing", {}, id="anchor-is-not-a-rule"),
        # A null is the absence of a resource, not a resource valued None.
        pytest.param(
            {
                "use_slurm": True,
                "slurm": {"suite2p": {"slurm_partition": "gpu", "gres": None}},
            },
            "suite2p",
            {"slurm_partition": "gpu"},
            id="nulls-dropped",
        ),
        # With use_slurm false the rules must request nothing at all, and a
        # missing block resolves to no resources rather than raising.
        pytest.param(
            {
                "use_slurm": False,
                "slurm": {"suite2p": {"slurm_partition": "gpu"}},
            },
            "suite2p",
            {},
            id="slurm-disabled",
        ),
        pytest.param({"use_slurm": True}, "suite2p", {}, id="no-slurm-block"),
        pytest.param(
            {"use_slurm": True, "slurm": None},
            "suite2p",
            {},
            id="null-slurm-block",
        ),
    ],
)
def test_slurm_resources(config, rule_name, expected):
    """A rule resolves to its own block, minus scaffolding and nulls."""
    assert slurm_resources(config, rule_name) == expected


# ---------------------------------------------------------------------------
# The shipped config and the workflow must agree
#
# slurm_resources is keyed by rule name, so the feature hinges on each .smk
# passing its OWN name and on the shipped config using those same names. A typo
# on either side is silent -- the rule just gets nothing -- so pin both against
# each other rather than trusting them separately.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "smk_filename, rule_name",
    [("preprocessing.smk", "preprocessing"), ("suite2p.smk", "suite2p")],
)
def test_smk_file_resolves_resources_for_its_own_rule(smk_filename, rule_name):
    """Each workflow file must declare and resolve the SAME rule name."""
    source = (get_snakefile_path().parent / smk_filename).read_text()

    declared = re.findall(r"^rule\s+(\w+)\s*:", source, flags=re.MULTILINE)
    resolved = re.findall(
        r"slurm_resources\(\s*config\s*,\s*[\"'](\w+)[\"']", source
    )

    assert declared == [rule_name], f"{smk_filename} declares {declared}"
    assert resolved == [rule_name], f"{smk_filename} resolves {resolved}"


def test_shipped_config_has_a_block_for_every_rule():
    """Every rule in the workflow needs a block in the shipped config.

    A rule with no block silently requests nothing but its log paths, and is
    submitted with whatever the cluster defaults to -- so when a new rule is
    added (dff, neuropil, cascade ...) this fails until its block exists.
    """
    workflow_dir = get_snakefile_path().parent
    config = yaml.safe_load((workflow_dir / "config.yaml").read_text())
    config["use_slurm"] = True

    rule_names = [
        rule
        for smk in sorted(workflow_dir.glob("*.smk"))
        for rule in re.findall(
            r"^rule\s+(\w+)\s*:", smk.read_text(), flags=re.MULTILINE
        )
    ]

    assert rule_names, "no rules found in the workflow"
    for rule_name in rule_names:
        assert rule_name in config["slurm"], f"no slurm block for {rule_name}"
        assert slurm_resources(config, rule_name), f"empty block: {rule_name}"
