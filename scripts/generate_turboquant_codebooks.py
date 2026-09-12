#!/usr/bin/env python3
"""Regenerate and verify the frozen standard-normal Lloyd-Max tables.

Development dependency: mpmath==1.3.0. The solver uses 100 decimal digits,
symmetric quantiles for initialization, analytic truncated-normal moments, and
iteration until the maximum centroid delta is below 1e-80. It then performs
one final update and rounds directly to IEEE-754 binary64.

The research basis is TurboQuant_mse in arXiv:2504.19874. This utility derives
the scalar tables independently; it contains no third-party TurboQuant source
or wire-format logic.
"""

from __future__ import annotations

import argparse
import json
import re
import struct
from pathlib import Path

try:
    import mpmath as mp
except ImportError as error:  # pragma: no cover - developer setup failure
    raise SystemExit("install the pinned generator dependency: pip install mpmath==1.3.0") from error


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src/prolly/proximity/accelerator/turboquant.rs"
PRECISION_DIGITS = 100
TOLERANCE = mp.mpf("1e-80")
MAX_ITERATIONS = 20_000


def phi(value: mp.mpf) -> mp.mpf:
    return mp.exp(-(value * value) / 2) / mp.sqrt(2 * mp.pi)


def cdf(value: mp.mpf) -> mp.mpf:
    return (1 + mp.erf(value / mp.sqrt(2))) / 2


def conditional_mean(lower: mp.mpf, upper: mp.mpf) -> mp.mpf:
    lower_density = mp.mpf(0) if lower == -mp.inf else phi(lower)
    upper_density = mp.mpf(0) if upper == mp.inf else phi(upper)
    return (lower_density - upper_density) / (cdf(upper) - cdf(lower))


def solve(bit_width: int) -> tuple[list[mp.mpf], list[mp.mpf], int]:
    levels = 1 << bit_width
    half = levels // 2
    centroids = [
        mp.sqrt(2) * mp.erfinv(2 * (mp.mpf(half + index) + mp.mpf("0.5")) / levels - 1)
        for index in range(half)
    ]
    for iteration in range(1, MAX_ITERATIONS + 1):
        thresholds = [mp.mpf(0)] + [
            (centroids[index] + centroids[index + 1]) / 2
            for index in range(half - 1)
        ]
        updated = [
            conditional_mean(
                thresholds[index],
                mp.inf if index == half - 1 else thresholds[index + 1],
            )
            for index in range(half)
        ]
        delta = max(abs(left - right) for left, right in zip(centroids, updated))
        centroids = updated
        if delta < TOLERANCE:
            break
    else:
        raise RuntimeError(f"{bit_width}-bit Lloyd-Max solver did not converge")

    all_centroids = [-value for value in reversed(centroids)] + centroids
    all_thresholds = [
        (all_centroids[index] + all_centroids[index + 1]) / 2
        for index in range(levels - 1)
    ]
    return all_thresholds, all_centroids, iteration


def binary64_bits(value: mp.mpf) -> str:
    return f"{struct.unpack('>Q', struct.pack('>d', float(value)))[0]:016x}"


def interval_second_moment(lower: mp.mpf, upper: mp.mpf) -> mp.mpf:
    lower_term = mp.mpf(0) if lower == -mp.inf else -lower * phi(lower) + cdf(lower)
    upper_term = mp.mpf(1) if upper == mp.inf else -upper * phi(upper) + cdf(upper)
    return upper_term - lower_term


def mean_squared_error(thresholds: list[mp.mpf], centroids: list[mp.mpf]) -> mp.mpf:
    boundaries = [-mp.inf, *thresholds, mp.inf]
    total = mp.mpf(0)
    for index, centroid in enumerate(centroids):
        lower, upper = boundaries[index], boundaries[index + 1]
        probability = cdf(upper) - cdf(lower)
        first_moment = (
            (mp.mpf(0) if lower == -mp.inf else phi(lower))
            - (mp.mpf(0) if upper == mp.inf else phi(upper))
        )
        total += interval_second_moment(lower, upper)
        total -= 2 * centroid * first_moment
        total += centroid * centroid * probability
    return total


def frozen_values(name: str) -> list[str]:
    source = SOURCE.read_text(encoding="utf-8")
    match = re.search(rf"const {name}: &\[u64\] = &\[(.*?)\];", source, re.S)
    if match is None:
        raise RuntimeError(f"cannot find {name} in {SOURCE}")
    return [token.replace("_", "").removeprefix("0x") for token in re.findall(r"0x[0-9a-f_]+", match.group(1))]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", action="store_true", help="print Rust-ready table values")
    args = parser.parse_args()
    mp.mp.dps = PRECISION_DIGITS

    report = {
        "method": "symmetric Lloyd-Max with analytic standard-normal moments",
        "source_precision_decimal_digits": PRECISION_DIGITS,
        "convergence_tolerance": "1e-80",
        "paper": "TurboQuant_mse, arXiv:2504.19874",
        "tables": {},
    }
    failures = []
    for bit_width in (2, 3, 4):
        thresholds, centroids, iterations = solve(bit_width)
        threshold_bits = [binary64_bits(value) for value in thresholds]
        centroid_bits = [binary64_bits(value) for value in centroids]
        report["tables"][str(bit_width)] = {
            "iterations": iterations,
            "threshold_bits": threshold_bits,
            "centroid_bits": centroid_bits,
            "standard_normal_mse": mp.nstr(
                mean_squared_error(thresholds, centroids), 30
            ),
        }
        if threshold_bits != frozen_values(f"THRESHOLDS_{bit_width}"):
            failures.append(f"THRESHOLDS_{bit_width}")
        if centroid_bits != frozen_values(f"CENTROIDS_{bit_width}"):
            failures.append(f"CENTROIDS_{bit_width}")

    print(json.dumps(report, indent=2, sort_keys=True))
    if args.emit:
        for bit_width, table in report["tables"].items():
            print(f"// {bit_width}-bit thresholds")
            print("\n".join(f"0x{bits}," for bits in table["threshold_bits"]))
            print(f"// {bit_width}-bit centroids")
            print("\n".join(f"0x{bits}," for bits in table["centroid_bits"]))
    if failures:
        raise SystemExit("generated tables differ from frozen Rust constants: " + ", ".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
