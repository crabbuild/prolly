#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "numpy>=2.3,<3",
# ]
# ///
"""Compare Prolly's structured TurboQuant rotation with the paper reference.

This is development-only qualification code.  It constructs the dense random
rotation described by Algorithm 1 of arXiv:2504.19874 by QR-decomposing an
i.i.d. standard-normal matrix.  It independently reproduces Prolly's frozen
structured transform, uses the checked-in Lloyd-Max tables, and reports
distortion plus exhaustive-oracle recall for both rotations.

The script is deliberately outside the production crate: NumPy/BLAS behavior
is evidence, not wire-format behavior.  No generated value is loaded by the
runtime and no third-party TurboQuant implementation is used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import struct
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "conformance/proximity-fixtures.json"
DEFAULT_DIMENSIONS = (128, 200, 768, 1536, 3072)
PERMUTATION_DOMAIN = 0x5451_5045_524D_0001
SIGN_DOMAIN = 0x5451_5349_474E_0001
MASK64 = (1 << 64) - 1


class SplitMix64:
    """Frozen SplitMix64V1 stream used by the production transform."""

    def __init__(self, state: int) -> None:
        self.state = state & MASK64

    def next(self) -> int:
        self.state = (self.state + 0x9E37_79B9_7F4A_7C15) & MASK64
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58_476D_1CE4_E5B9) & MASK64
        value = ((value ^ (value >> 27)) * 0x94D0_49BB_1331_11EB) & MASK64
        return (value ^ (value >> 31)) & MASK64


def multiply_high(left: int, right: int) -> int:
    return (left * right) >> 64


def sqrt_down(value: float) -> float:
    """Match the production square-root direction for ordinary positive inputs."""

    candidate = math.sqrt(value)
    while candidate * candidate > value:
        candidate = math.nextafter(candidate, -math.inf)
    while True:
        following = math.nextafter(candidate, math.inf)
        if following * following <= value:
            candidate = following
        else:
            return candidate


@dataclass(frozen=True)
class StructuredRound:
    permutation: np.ndarray
    signs: np.ndarray


class StructuredRotation:
    """Independent Python reproduction of STRUCTURED_ROTATION_ID=1."""

    def __init__(self, dimensions: int, seed: int) -> None:
        if not 8 <= dimensions <= 16_384 or dimensions % 8:
            raise ValueError("dimensions must be in 8..=16384 and divisible by eight")
        self.dimensions = dimensions
        self.block_width = dimensions & -dimensions
        self.normalization = 1.0 / sqrt_down(float(self.block_width))
        domain_dimensions = (dimensions << 16) & MASK64
        rounds: list[StructuredRound] = []
        for round_index in range(2):
            permutation = list(range(dimensions))
            permutation_stream = SplitMix64(
                seed ^ PERMUTATION_DOMAIN ^ domain_dimensions ^ round_index
            )
            for index in range(dimensions - 1, 0, -1):
                selected = multiply_high(permutation_stream.next(), index + 1)
                permutation[index], permutation[selected] = (
                    permutation[selected],
                    permutation[index],
                )
            sign_stream = SplitMix64(
                seed ^ SIGN_DOMAIN ^ domain_dimensions ^ round_index
            )
            signs = [1.0 if sign_stream.next() & 1 == 0 else -1.0 for _ in range(dimensions)]
            rounds.append(
                StructuredRound(
                    np.asarray(permutation, dtype=np.int64),
                    np.asarray(signs, dtype=np.float64),
                )
            )
        self.rounds = tuple(rounds)

    def apply_rows(self, values: np.ndarray) -> np.ndarray:
        if values.ndim != 2 or values.shape[1] != self.dimensions:
            raise ValueError("structured rotation input has the wrong shape")
        current = np.asarray(values, dtype=np.float64).copy()
        for round_plan in self.rounds:
            # Advanced column indexing can produce a Fortran-strided matrix.
            # Force row-major storage so the block reshape below remains a
            # view; otherwise the Hadamard writes would modify a temporary.
            current = np.ascontiguousarray(
                current[:, round_plan.permutation] * round_plan.signs
            )
            blocks = current.reshape(-1, self.block_width)
            width = 1
            while width < self.block_width:
                for start in range(0, self.block_width, width * 2):
                    left = blocks[:, start : start + width].copy()
                    right = blocks[:, start + width : start + width * 2].copy()
                    blocks[:, start : start + width] = left + right
                    blocks[:, start + width : start + width * 2] = left - right
                width *= 2
            current *= self.normalization
            current[current == 0.0] = 0.0
        return current


def parse_dimensions(value: str) -> tuple[int, ...]:
    dimensions = tuple(int(item) for item in value.split(",") if item)
    if not dimensions:
        raise argparse.ArgumentTypeError("at least one dimension is required")
    for dimension in dimensions:
        if not 8 <= dimension <= 16_384 or dimension % 8:
            raise argparse.ArgumentTypeError(
                f"unsupported dimension {dimension}; expected 8..16384 divisible by eight"
            )
    return dimensions


def bits_to_float(text: str) -> float:
    return struct.unpack(">d", bytes.fromhex(text))[0]


def frozen_rotation_input(dimensions: int) -> np.ndarray:
    stream = SplitMix64(0x5451_4649_5854_0001)
    values: list[float] = []
    for _ in range(dimensions):
        draw = stream.next()
        sign = draw & (1 << 63)
        fraction = draw & ((1 << 52) - 1)
        bits = sign | 0x3FE0_0000_0000_0000 | fraction
        values.append(struct.unpack("<d", struct.pack("<Q", bits))[0])
    return np.asarray([values], dtype=np.float64)


def load_codebooks() -> dict[int, tuple[np.ndarray, np.ndarray]]:
    fixture = json.loads(FIXTURES.read_text())["turboquant"]["codebooks"]
    return {
        int(bit_width): (
            np.asarray([bits_to_float(value) for value in table["threshold_bits"]]),
            np.asarray([bits_to_float(value) for value in table["centroid_bits"]]),
        )
        for bit_width, table in fixture.items()
    }


def verify_structured_fixtures() -> None:
    """Prove that the independent implementation reaches the frozen bytes."""

    fixture = json.loads(FIXTURES.read_text())["turboquant"]
    stream = SplitMix64(0)
    actual_stream = [f"{stream.next():016x}" for _ in fixture["splitmix64_seed_zero"]]
    if actual_stream != fixture["splitmix64_seed_zero"]:
        raise RuntimeError("SplitMix64 reproduction does not match frozen fixtures")
    for dimensions_text, expected in fixture["rotation"].items():
        dimensions = int(dimensions_text)
        rotation = StructuredRotation(dimensions, 0x5EED)
        plan_bytes = bytearray()
        for round_plan in rotation.rounds:
            for index in round_plan.permutation.tolist():
                plan_bytes.extend(struct.pack("<Q", index))
            plan_bytes.extend(1 if sign > 0 else 255 for sign in round_plan.signs)
        if hashlib.sha256(plan_bytes).hexdigest() != expected["plan_sha256"]:
            raise RuntimeError(f"structured plan mismatch at dimension {dimensions}")
        input_values = frozen_rotation_input(dimensions)
        input_bytes = b"".join(
            struct.pack("<d", float(value)) for value in input_values[0]
        )
        if hashlib.sha256(input_bytes).hexdigest() != expected["input_sha256"]:
            raise RuntimeError(f"structured input mismatch at dimension {dimensions}")
        output = rotation.apply_rows(input_values)[0]
        output_bytes = b"".join(struct.pack("<d", float(value)) for value in output)
        if hashlib.sha256(output_bytes).hexdigest() != expected["output_sha256"]:
            raise RuntimeError(f"structured output mismatch at dimension {dimensions}")


def dense_gaussian_qr(dimensions: int, seed: int) -> np.ndarray:
    """Return the sign-canonicalized Q from an i.i.d. Gaussian matrix."""

    generator = np.random.Generator(np.random.PCG64(seed))
    gaussian = generator.standard_normal((dimensions, dimensions), dtype=np.float64)
    orthogonal, upper = np.linalg.qr(gaussian)
    diagonal_sign = np.where(np.diag(upper) < 0.0, -1.0, 1.0)
    orthogonal *= diagonal_sign
    return orthogonal


def deterministic_dataset(
    dimensions: int, records: int, queries: int, seed: int
) -> tuple[np.ndarray, np.ndarray]:
    generator = np.random.Generator(np.random.PCG64(seed ^ (dimensions << 17)))
    sources = generator.standard_normal((records, dimensions), dtype=np.float64)
    query_values = generator.standard_normal((queries, dimensions), dtype=np.float64)

    # Add deterministic near-neighbour structure. This prevents recall from
    # measuring only accidental extremes in an isotropic cloud.
    anchors = min(queries, records)
    sources[:anchors] = query_values[:anchors] + generator.standard_normal(
        (anchors, dimensions), dtype=np.float64
    ) * 0.05
    return sources, query_values


def quantized_reconstruction(
    rotated_unit: np.ndarray,
    thresholds: np.ndarray,
    centroids: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    dimensions = rotated_unit.shape[1]
    scaled = rotated_unit * math.sqrt(dimensions)
    # side="left" freezes the production rule that threshold equality selects
    # the lower code.
    codes = np.searchsorted(thresholds, scaled, side="left")
    reconstructed = centroids[codes] / math.sqrt(dimensions)
    errors = np.sum((rotated_unit - reconstructed) ** 2, axis=1)
    return reconstructed, errors


def exact_scores(metric: str, sources: np.ndarray, query: np.ndarray) -> np.ndarray:
    if metric == "l2":
        delta = sources - query
        return np.einsum("ij,ij->i", delta, delta)
    dot = sources @ query
    if metric == "cosine":
        return 1.0 - np.clip(dot, -1.0, 1.0)
    return -dot


def approximate_scores(
    metric: str,
    source_norms: np.ndarray,
    reconstructed: np.ndarray,
    rotated_query: np.ndarray,
) -> np.ndarray:
    dot = source_norms * (reconstructed @ rotated_query)
    if metric == "l2":
        query_norm_squared = float(rotated_query @ rotated_query)
        return np.maximum(query_norm_squared + source_norms**2 - 2.0 * dot, 0.0)
    if metric == "cosine":
        return 1.0 - np.clip(dot, -1.0, 1.0)
    return -dot


def stable_smallest(
    scores: np.ndarray, count: int, keys: np.ndarray | None = None
) -> np.ndarray:
    if keys is None:
        keys = np.arange(scores.shape[0], dtype=np.int64)
    return np.lexsort((keys, scores))[:count]


def recall_rows(
    sources: np.ndarray,
    queries: np.ndarray,
    source_norms: np.ndarray,
    reconstructed: np.ndarray,
    rotate_queries: Any,
    k: int,
    rerank_multiplier: int,
) -> dict[str, float]:
    result: dict[str, float] = {}
    normalized_sources = sources / source_norms[:, None]
    normalized_queries = queries / np.linalg.norm(queries, axis=1)[:, None]
    shortlist = min(sources.shape[0], max(k, k * rerank_multiplier))
    for metric in ("l2", "cosine", "inner_product"):
        per_query: list[float] = []
        metric_sources = normalized_sources if metric == "cosine" else sources
        metric_queries = normalized_queries if metric == "cosine" else queries
        metric_norms = np.ones_like(source_norms) if metric == "cosine" else source_norms
        # Cosine source quantization operates on the already normalized source.
        metric_reconstruction = reconstructed
        for query in metric_queries:
            exact = exact_scores(metric, metric_sources, query)
            expected = stable_smallest(exact, k)
            rotated_query = rotate_queries(query[None, :])[0]
            approximate = approximate_scores(
                metric, metric_norms, metric_reconstruction, rotated_query
            )
            candidates = stable_smallest(approximate, shortlist)
            reranked_local = stable_smallest(
                exact[candidates], min(k, len(candidates)), candidates
            )
            actual = candidates[reranked_local]
            per_query.append(len(set(expected.tolist()) & set(actual.tolist())) / k)
        result[metric] = float(np.mean(per_query))
    return result


def comparison_for_dimension(
    dimensions: int,
    records: int,
    queries: int,
    seed: int,
    k: int,
    rerank_multiplier: int,
    codebooks: dict[int, tuple[np.ndarray, np.ndarray]],
) -> list[dict[str, Any]]:
    sources, query_values = deterministic_dataset(dimensions, records, queries, seed)
    source_norms = np.linalg.norm(sources, axis=1)
    unit_sources = sources / source_norms[:, None]

    structured = StructuredRotation(dimensions, seed)
    structured_rotated = structured.apply_rows(unit_sources)

    dense_started = time.perf_counter()
    dense = dense_gaussian_qr(dimensions, seed ^ 0xD3E5_E0A1_5EED_0001)
    dense_seconds = time.perf_counter() - dense_started
    dense_rotated = unit_sources @ dense.T

    rows: list[dict[str, Any]] = []
    for bit_width in (2, 3, 4):
        thresholds, centroids = codebooks[bit_width]
        structured_reconstruction, structured_errors = quantized_reconstruction(
            structured_rotated, thresholds, centroids
        )
        dense_reconstruction, dense_errors = quantized_reconstruction(
            dense_rotated, thresholds, centroids
        )
        structured_recall = recall_rows(
            sources,
            query_values,
            source_norms,
            structured_reconstruction,
            structured.apply_rows,
            k,
            rerank_multiplier,
        )
        dense_recall = recall_rows(
            sources,
            query_values,
            source_norms,
            dense_reconstruction,
            lambda values: values @ dense.T,
            k,
            rerank_multiplier,
        )
        rows.append(
            {
                "dimensions": dimensions,
                "bit_width": bit_width,
                "records": records,
                "queries": queries,
                "k": k,
                "rerank_multiplier": rerank_multiplier,
                "dense_qr_seconds": dense_seconds,
                "structured": {
                    "mean_routing_mse": float(np.mean(structured_errors)),
                    "maximum_routing_mse": float(np.max(structured_errors)),
                    "mean_norm_error": float(
                        np.mean(
                            np.abs(
                                np.linalg.norm(structured_rotated, axis=1) - 1.0
                            )
                        )
                    ),
                    "recall_at_k": structured_recall,
                },
                "dense_gaussian_qr": {
                    "mean_routing_mse": float(np.mean(dense_errors)),
                    "maximum_routing_mse": float(np.max(dense_errors)),
                    "mean_norm_error": float(
                        np.mean(np.abs(np.linalg.norm(dense_rotated, axis=1) - 1.0))
                    ),
                    "recall_at_k": dense_recall,
                },
            }
        )
    return rows


def git_revision() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_report(path: Path, report: dict[str, Any]) -> None:
    encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if path == Path("-"):
        sys.stdout.write(encoded)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dimensions",
        type=parse_dimensions,
        default=DEFAULT_DIMENSIONS,
        help="comma-separated dimensions (default: 128,200,768,1536,3072)",
    )
    parser.add_argument("--records", type=int, default=128)
    parser.add_argument("--queries", type=int, default=16)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--rerank-multiplier", type=int, default=8)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("-"),
        help="JSON output path or '-' for stdout",
    )
    args = parser.parse_args()
    if args.records <= 0 or args.queries <= 0:
        parser.error("records and queries must be positive")
    if args.k <= 0 or args.k > args.records:
        parser.error("k must be positive and no greater than records")
    if args.rerank_multiplier <= 0:
        parser.error("rerank multiplier must be positive")

    started = time.perf_counter()
    verify_structured_fixtures()
    codebooks = load_codebooks()
    rows: list[dict[str, Any]] = []
    for dimensions in args.dimensions:
        print(f"dense-reference: dimensions={dimensions}", file=sys.stderr, flush=True)
        rows.extend(
            comparison_for_dimension(
                dimensions,
                args.records,
                args.queries,
                args.seed,
                args.k,
                args.rerank_multiplier,
                codebooks,
            )
        )
    report = {
        "schema": "prolly-turboquant-dense-reference-v1",
        "research_basis": "https://arxiv.org/abs/2504.19874",
        "algorithm": (
            "Algorithm 1 dense i.i.d. Gaussian QR rotation compared with "
            "Prolly STRUCTURED_ROTATION_ID=1"
        ),
        "scope": (
            "development qualification only; not production, persistence, "
            "wire-format, or Auto-planner input"
        ),
        "revision": git_revision(),
        "generator": {
            "path": str(Path(__file__).resolve().relative_to(ROOT)),
            "sha256": file_sha256(Path(__file__).resolve()),
        },
        "environment": {
            "python": sys.version.split()[0],
            "numpy": np.__version__,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
        },
        "parameters": {
            "dimensions": list(args.dimensions),
            "records": args.records,
            "queries": args.queries,
            "k": args.k,
            "rerank_multiplier": args.rerank_multiplier,
            "seed": args.seed,
            "dataset_rng": "NumPy PCG64",
            "dense_matrix_rng": "NumPy PCG64",
            "dense_qr_sign_rule": "multiply Q columns by sign(diag(R))",
        },
        "elapsed_seconds": time.perf_counter() - started,
        "rows": rows,
    }
    write_report(args.output, report)


if __name__ == "__main__":
    main()
