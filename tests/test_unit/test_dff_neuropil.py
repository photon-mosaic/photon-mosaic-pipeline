"""
Unit tests for the dF/F and neuropil correction science layers.

These functions produce the actual numbers downstream analyses depend on,
so the tests pin the expected behaviour on synthetic inputs where the
right answer is analytically known.
"""

import numpy as np

from photon_mosaic_pipeline.rules.dff_run import calculate_dFF, dFF
from photon_mosaic_pipeline.rules.neuropil_run import (
    calculate_neuropil_correction,
)

# ---------------------------------------------------------------------------
# dFF
# ---------------------------------------------------------------------------


def test_dff_near_constant_trace_returns_near_zero():
    """A trace with tiny noise around a fixed baseline has near-zero dFF.

    GMM cannot fit n_components=2 to a perfectly constant trace (sklearn
    raises ConvergenceWarning), so this test uses ε-jittered data — which
    matches real fluorescence (always has some shot noise) better anyway.
    """
    rng = np.random.default_rng(0)
    eps = 1e-3
    baseline = 100.0
    f = baseline + eps * rng.standard_normal((3, 500))

    dff, f0 = dFF(f, n_components=2)

    assert dff.shape == f.shape
    # dFF amplitude is bounded by the noise floor / baseline ratio
    assert np.max(np.abs(dff)) < 10 * eps / baseline
    assert np.allclose(f0, baseline, atol=1e-2)


def test_dff_sinusoidal_trace_recovers_expected_modulation():
    """A sinusoid around a known baseline produces dFF with predictable
    peak-to-peak amplitude (= 2*amp/baseline)."""
    n_timepoints = 1000
    baseline = 100.0
    amp = 20.0
    t = np.linspace(0, 4 * np.pi, n_timepoints)
    f = (baseline + amp * np.sin(t))[None, :]

    dff, f0 = dFF(f, n_components=2)

    # GMM picks the lower mean of two components on a symmetric sinusoid;
    # F0 is biased below the true baseline. The peak-to-peak amplitude of
    # dFF is the meaningful invariant (independent of where F0 sits).
    expected_pp = 2 * amp / f0[0, 0]
    actual_pp = dff.max() - dff.min()
    assert np.isclose(actual_pp, expected_pp, rtol=1e-3)


def test_dff_output_shape_matches_input():
    """dFF preserves input shape; F0 is one entry per row."""
    f = np.random.default_rng(0).normal(100, 10, size=(7, 250))

    dff, f0 = dFF(f, n_components=2)

    assert dff.shape == f.shape
    assert f0.shape == (f.shape[0], 1)


def test_dff_n_components_is_configurable():
    """The n_components kwarg threads through and changes the GMM result.

    This is a smoke test: changing n_components shouldn't crash and should
    produce a different F0 (because more components mean a different fit).
    """
    rng = np.random.default_rng(0)
    f = rng.normal(100, 10, size=(2, 500))

    _, f0_two = dFF(f, n_components=2)
    _, f0_three = dFF(f, n_components=3)

    assert f0_two.shape == f0_three.shape
    # Different component counts must produce different baselines on
    # noisy data — otherwise the kwarg isn't actually doing anything.
    assert not np.allclose(f0_two, f0_three)


def test_calculate_dFF_writes_dFF_and_F0(tmp_path):
    """End-to-end: load Fc.npy, fit, save dFF.npy and F0.npy."""
    rng = np.random.default_rng(0)
    fc = 50.0 + rng.standard_normal((2, 200))
    input_path = tmp_path / "Fc.npy"
    np.save(input_path, fc)

    output_path = tmp_path / "dff" / "plane0" / "dFF.npy"

    calculate_dFF(
        str(input_path),
        str(output_path),
        {"gmm_ncomponents": 2},
    )

    assert output_path.exists()
    assert (output_path.parent / "F0.npy").exists()
    dff_loaded = np.load(output_path)
    assert dff_loaded.shape == fc.shape


# ---------------------------------------------------------------------------
# Neuropil correction
# ---------------------------------------------------------------------------


def test_neuropil_correction_zero_neuropil_returns_F(tmp_path):
    """Fneu = 0 (median is also 0) means Fc = F exactly."""
    F = np.array([[100.0, 110.0, 105.0]])
    Fneu = np.zeros_like(F)

    f_path = tmp_path / "F.npy"
    fneu_path = tmp_path / "Fneu.npy"
    out_path = tmp_path / "neuropil" / "plane0" / "Fc.npy"
    np.save(f_path, F)
    np.save(fneu_path, Fneu)

    calculate_neuropil_correction(
        str(f_path),
        str(fneu_path),
        str(out_path),
        {"neucoeff": 0.7},
    )

    assert np.allclose(np.load(out_path), F)


def test_neuropil_correction_known_input_matches_formula(tmp_path):
    """Fc = F - neucoeff * (Fneu - median(Fneu)) on a known input."""
    F = np.array([[100.0, 100.0, 100.0]])
    Fneu = np.array([[10.0, 20.0, 30.0]])  # median = 20
    neucoeff = 0.5
    expected = F - neucoeff * (Fneu - 20.0)

    f_path = tmp_path / "F.npy"
    fneu_path = tmp_path / "Fneu.npy"
    out_path = tmp_path / "Fc.npy"
    np.save(f_path, F)
    np.save(fneu_path, Fneu)

    calculate_neuropil_correction(
        str(f_path),
        str(fneu_path),
        str(out_path),
        {"neucoeff": neucoeff},
    )

    assert np.allclose(np.load(out_path), expected)


def test_neuropil_correction_per_neuron_median(tmp_path):
    """Median is computed per-neuron (axis=1), not globally."""
    F = np.zeros((2, 3))
    Fneu = np.array(
        [
            [10.0, 10.0, 10.0],  # median 10 → contribution 0
            [0.0, 100.0, 200.0],  # median 100 → contribution scales
        ]
    )

    f_path = tmp_path / "F.npy"
    fneu_path = tmp_path / "Fneu.npy"
    out_path = tmp_path / "Fc.npy"
    np.save(f_path, F)
    np.save(fneu_path, Fneu)

    calculate_neuropil_correction(
        str(f_path),
        str(fneu_path),
        str(out_path),
        {"neucoeff": 1.0},
    )

    Fc = np.load(out_path)
    assert np.allclose(Fc[0], 0.0)
    assert np.allclose(Fc[1], -(Fneu[1] - 100.0))
