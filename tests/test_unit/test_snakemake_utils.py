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
# removed. These tests encode that, plus the backwards-compatible reading of a
# flat `slurm:` block (which applied to every rule before per-rule blocks
# existed) and the two kinds of key that must never reach SLURM -- nulls and
# the `_`-prefixed anchors used for YAML reuse.
# ---------------------------------------------------------------------------


def test_rule_gets_its_own_block():
    """Each rule resolves to its own sub-block, nothing else."""
    config = {
        "use_slurm": True,
        "slurm": {
            "preprocessing": {"slurm_partition": "cpu", "mem_mb": 8000},
            "suite2p": {"slurm_partition": "gpu", "gres": "gpu:a4500:1"},
        },
    }

    assert slurm_resources(config, "preprocessing") == {
        "slurm_partition": "cpu",
        "mem_mb": 8000,
    }
    assert slurm_resources(config, "suite2p") == {
        "slurm_partition": "gpu",
        "gres": "gpu:a4500:1",
    }


def test_a_rule_never_inherits_another_rules_keys():
    """The point of the design: no inheritance, so no removal needed.

    `gres` belongs to suite2p only. Under the previous shape every rule got
    the whole `slurm:` block, so preprocessing requested a GPU it never used
    and queued behind busy GPUs for nothing.
    """
    config = {
        "use_slurm": True,
        "slurm": {
            "preprocessing": {"slurm_partition": "cpu"},
            "suite2p": {"slurm_partition": "gpu", "gres": "gpu:a4500:1"},
        },
    }

    assert "gres" not in slurm_resources(config, "preprocessing")


def test_flat_block_applies_to_every_rule():
    """Backwards compatibility: a config with no per-rule sub-blocks.

    Existing user configs are flat. They must keep behaving exactly as before,
    with the whole block going to every rule.
    """
    config = {
        "use_slurm": True,
        "slurm": {"slurm_partition": "gpu", "mem_mb": 32000},
    }

    expected = {"slurm_partition": "gpu", "mem_mb": 32000}
    assert slurm_resources(config, "preprocessing") == expected
    assert slurm_resources(config, "suite2p") == expected


def test_flat_fallback_ignores_unrelated_sub_blocks():
    """A rule with no block of its own falls back to the flat keys only.

    The other rules' sub-blocks are not resources and must not leak in --
    Snakemake resources must be scalars, and a dict there is an error.
    """
    config = {
        "use_slurm": True,
        "slurm": {
            "slurm_partition": "gpu",
            "suite2p": {"gres": "gpu:a4500:1"},
        },
    }

    resources = slurm_resources(config, "preprocessing")

    assert resources == {"slurm_partition": "gpu"}
    assert not any(isinstance(v, dict) for v in resources.values())


def test_anchor_scaffolding_is_not_a_resource():
    """`_`-prefixed keys hold YAML anchors and must never reach SLURM."""
    config = {
        "use_slurm": True,
        "slurm": {
            "_common": {"tasks": 1},
            "suite2p": {"_note": "ignored", "tasks": 1, "mem_mb": 32000},
        },
    }

    assert slurm_resources(config, "suite2p") == {"tasks": 1, "mem_mb": 32000}
    assert slurm_resources(config, "preprocessing") == {}


def test_null_values_are_dropped():
    """A null is the absence of a resource, not a resource valued None."""
    config = {
        "use_slurm": True,
        "slurm": {"suite2p": {"slurm_partition": "gpu", "gres": None}},
    }

    assert slurm_resources(config, "suite2p") == {"slurm_partition": "gpu"}


def test_no_resources_when_slurm_is_disabled():
    """With use_slurm false the rules must request nothing at all."""
    config = {
        "use_slurm": False,
        "slurm": {"suite2p": {"slurm_partition": "gpu"}},
    }

    assert slurm_resources(config, "suite2p") == {}


def test_missing_slurm_block_is_tolerated():
    """No slurm: block at all resolves to no resources, not an error."""
    assert slurm_resources({"use_slurm": True}, "suite2p") == {}
    assert slurm_resources({"use_slurm": True, "slurm": None}, "suite2p") == {}


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

    A rule with no block silently gets no resources -- so when a new rule is
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


def test_shipped_config_ships_no_site_specific_values():
    """The example config must not carry one site's account or partition.

    `slurm_account` is cluster-specific; shipping a real one (e.g. an SWC
    account) makes the example wrong everywhere else, so it stays commented
    out with an obvious placeholder.
    """
    workflow_dir = get_snakefile_path().parent
    raw = (workflow_dir / "config.yaml").read_text()
    config = yaml.safe_load(raw)

    def accounts(block):
        if isinstance(block, dict):
            for key, value in block.items():
                if key == "slurm_account":
                    yield value
                yield from accounts(value)

    assert not list(accounts(config["slurm"])), "slurm_account must be unset"
    assert "your_account_name" in raw
