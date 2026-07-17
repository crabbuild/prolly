#!/usr/bin/env python3
"""Validate and summarize Rust-through-Go-binding versus native Go results."""

from __future__ import annotations

import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


BINDING = "rust-go-binding"
NATIVE = "native-go"


def fail(message: str) -> None:
    raise SystemExit(f"invalid benchmark results: {message}")


def read_peak_rss(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    mac = re.search(r"^\s*(\d+)\s+maximum resident set size\s*$", text, re.MULTILINE)
    if mac:
        return int(mac.group(1))
    linux = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", text)
    if linux:
        return int(linux.group(1)) * 1024
    fail(f"cannot parse peak RSS from {path}")
    return 0


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: summarize_go_binding_comparison.py RESULTS.csv OUT_DIR")
    source = Path(sys.argv[1])
    out = Path(sys.argv[2])
    machine = {}
    for line in (out / "machine.txt").read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            machine[key] = value
    rows = list(csv.DictReader(source.open(newline="", encoding="utf-8")))
    if not rows:
        fail("no measurement rows")

    manifest_path = out / "manifest.csv"
    manifest = list(csv.DictReader(manifest_path.open(newline="", encoding="utf-8")))
    rss_groups: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for item in manifest:
        key = (item["records"], item["phase"], item["workload"], item["implementation"])
        rss_groups[key].append(read_peak_rss(Path(item["time"])))

    by_run: dict[tuple[str, ...], dict[str, dict[str, str]]] = defaultdict(dict)
    for row in rows:
        if row["validated"] != "true":
            fail(f"unvalidated row: {row}")
        key = (row["records"], row["phase"], row["workload"], row["operation"], row["run"])
        implementation = row["implementation"]
        if implementation in by_run[key]:
            fail(f"duplicate {implementation} row for {key}")
        by_run[key][implementation] = row

    for key, pair in by_run.items():
        if set(pair) != {BINDING, NATIVE}:
            fail(f"missing implementation for {key}: {sorted(pair)}")
        binding, native = pair[BINDING], pair[NATIVE]
        for field in ("operations", "workload_digest", "result_count"):
            if binding[field] != native[field]:
                fail(f"{field} mismatch for {key}: binding={binding[field]} native={native[field]}")

    paired_run_wins: dict[str, int] = defaultdict(int)
    paired_run_wins_by_size: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    cell_run_ratios: dict[tuple[str, ...], list[float]] = defaultdict(list)
    for key, pair in by_run.items():
        ratio = float(pair[BINDING]["ns_per_op"]) / float(
            pair[NATIVE]["ns_per_op"]
        )
        winner = BINDING if ratio < 1.0 else NATIVE if ratio > 1.0 else "tie"
        paired_run_wins[winner] += 1
        paired_run_wins_by_size[key[0]][winner] += 1
        cell_run_ratios[key[:-1]].append(ratio)

    groups: dict[tuple[str, ...], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        key = (row["records"], row["phase"], row["workload"], row["operation"])
        groups[key][row["implementation"]].append(float(row["ns_per_op"]))

    summary_rows: list[dict[str, str]] = []
    for key in sorted(groups, key=lambda item: (int(item[0]), item[1], item[2], item[3])):
        values = groups[key]
        binding = statistics.median(values[BINDING])
        native = statistics.median(values[NATIVE])
        winner = BINDING if binding < native else NATIVE if native < binding else "tie"
        speedup = max(binding, native) / min(binding, native) if min(binding, native) else float("inf")
        summary_rows.append(
            {
                "records": key[0],
                "phase": key[1],
                "workload": key[2],
                "operation": key[3],
                "runs": str(len(values[BINDING])),
                "rust_go_binding_median_ns_per_op": f"{binding:.3f}",
                "native_go_median_ns_per_op": f"{native:.3f}",
                "winner": winner,
                "winner_speedup": f"{speedup:.3f}",
            }
        )

    fields = list(summary_rows[0])
    with (out / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(summary_rows)

    memory_rows = []
    for key in sorted(rss_groups, key=lambda item: (int(item[0]), item[1], item[2], item[3])):
        values = rss_groups[key]
        memory_rows.append(
            {
                "records": key[0],
                "phase": key[1],
                "workload": key[2],
                "implementation": key[3],
                "runs": str(len(values)),
                "median_peak_rss_bytes": f"{statistics.median(values):.0f}",
            }
        )
    with (out / "memory.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(memory_rows[0]), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(memory_rows)

    wins = defaultdict(int)
    operation_wins: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    binding_native_ratios: dict[str, list[float]] = defaultdict(list)
    ten_million_ratios: dict[str, list[float]] = defaultdict(list)
    scale_operation_ratios: dict[tuple[str, str], list[float]] = defaultdict(list)
    scale_operation_wins: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for row in summary_rows:
        wins[row["winner"]] += 1
        operation_wins[row["operation"]][row["winner"]] += 1
        ratio = float(row["rust_go_binding_median_ns_per_op"]) / float(
            row["native_go_median_ns_per_op"]
        )
        binding_native_ratios[row["operation"]].append(ratio)
        scale_operation_ratios[(row["records"], row["operation"])].append(ratio)
        scale_operation_wins[(row["records"], row["operation"])][row["winner"]] += 1
        if int(row["records"]) == 10_000_000:
            ten_million_ratios[row["operation"]].append(ratio)
    run_counts = [int(row["runs"]) for row in summary_rows]
    peak_rss = defaultdict(float)
    for row in memory_rows:
        peak_rss[row["implementation"]] = max(
            peak_rss[row["implementation"]], float(row["median_peak_rss_bytes"])
        )
    changed_winner_cells = [
        (key, ratios)
        for key, ratios in cell_run_ratios.items()
        if any(ratio < 1.0 for ratio in ratios)
        and any(ratio > 1.0 for ratio in ratios)
    ]
    largest_ratio_span_key, largest_ratio_span_values = max(
        cell_run_ratios.items(), key=lambda item: max(item[1]) - min(item[1])
    )
    if max(largest_ratio_span_values) < 1.0:
        largest_span_winner = "the Rust binding won all repetitions"
    elif min(largest_ratio_span_values) > 1.0:
        largest_span_winner = "native Go won all repetitions"
    else:
        largest_span_winner = "the per-run winner changed"
    swap_before = machine.get("host_swap_before")
    swap_after = machine.get("host_swap_after")
    if swap_before and swap_after:
        swap_note = (
            f"Host swap before the run: {swap_before}. Host swap after the run: {swap_after}."
        )
        if swap_before != swap_after:
            swap_note += (
                " Machine-wide swap changed during the measured tiers, so exact medians may "
                "include memory-pressure noise and should be repeated on an idle dedicated "
                "host before publication."
            )
    else:
        swap_note = "Host swap before/after the run was not recorded."

    report = [
        "# Rust Prolly Go Binding vs Native Go Prolly Performance",
        "",
        "This comparison measures the API seen by a Go application, not direct Rust calls. "
        "The Rust implementation runs as an optimized native library behind cgo/UniFFI; "
        "the native implementation is the Go prolly tree implementation. All scenarios are "
        "process-isolated, single-worker, and in-memory. Lower nanoseconds per operation is better.",
        "",
        f"Rust-through-Go-binding wins: {wins[BINDING]}; native Go wins: {wins[NATIVE]}; ties: {wins['tie']}.",
        "",
        "| Operation | Rust Go binding wins | Native Go wins | Ties |",
        "|---|---:|---:|---:|",
    ]
    for operation in ("write", "point_read", "range_scan"):
        counts = operation_wins[operation]
        report.append(f"| {operation} | {counts[BINDING]} | {counts[NATIVE]} | {counts['tie']} |")
    report.extend(
        [
            "",
            f"Measured repetitions: {min(run_counts)}–{max(run_counts)} per scenario.",
            "",
            "## Aggregate signal",
            "",
            "The ratios below are medians of the per-scenario binding/native-Go latency ratios; below 1.0 means the binding is faster, above 1.0 means native Go is faster.",
            "",
            "| Operation | All sizes median ratio | 10M ratio range |",
            "|---|---:|---:|",
        ]
    )
    for operation in ("write", "point_read", "range_scan"):
        ratios = binding_native_ratios[operation]
        ten_million = ten_million_ratios[operation]
        ten_million_text = (
            f"{min(ten_million):.2f}–{max(ten_million):.2f}x" if ten_million else "not measured"
        )
        report.append(
            f"| {operation} | {statistics.median(ratios):.2f}x | {ten_million_text} |"
        )
    report.extend(
        [
            "",
            "## Scale-by-scale result",
            "",
            "Binding/native-Go ratios below 1.0 favor the Rust binding. Each row contains six median workload cells.",
            "",
            "| Records | Operation | Binding median wins | Binding/native ratio range |",
            "|---:|---|---:|---:|",
        ]
    )
    for records in sorted({row["records"] for row in summary_rows}, key=int):
        for operation in ("write", "point_read", "range_scan"):
            ratios = scale_operation_ratios[(records, operation)]
            counts = scale_operation_wins[(records, operation)]
            report.append(
                f"| {int(records):,} | {operation} | {counts[BINDING]}/6 | "
                f"{min(ratios):.3f}–{max(ratios):.3f}x |"
            )
    ten_million_point = scale_operation_ratios.get(("10000000", "point_read"), [])
    ten_million_point_target = sum(ratio <= (1.0 / 1.5) for ratio in ten_million_point)
    if ten_million_point:
        report.extend(
            [
                "",
                f"At 10M, {ten_million_point_target}/6 point-read cells meet or exceed a 1.5x binding advantage. "
                "The universal 1.5–2x point-read goal is therefore not yet met. Full-scan gains are smaller still and do not meet that target.",
            ]
        )
    report.extend(
        [
            "",
            "## Repetition stability",
            "",
            f"Across all paired repetitions, the Rust binding won {paired_run_wins[BINDING]}/"
            f"{len(by_run)} operation runs; native Go won {paired_run_wins[NATIVE]}/"
            f"{len(by_run)}.",
            "",
        ]
    )
    for records in sorted(paired_run_wins_by_size, key=int):
        counts = paired_run_wins_by_size[records]
        total = sum(counts.values())
        report.append(
            f"- {int(records):,}: binding {counts[BINDING]}/{total}; native Go {counts[NATIVE]}/{total}."
        )
    report.extend(
        [
            f"- {len(changed_winner_cells)}/{len(cell_run_ratios)} scenario cells changed per-run winner.",
            "- Largest paired-ratio span: "
            f"{int(largest_ratio_span_key[0]):,} {largest_ratio_span_key[1]} "
            f"{largest_ratio_span_key[2]} {largest_ratio_span_key[3]}, "
            f"{min(largest_ratio_span_values):.3f}–{max(largest_ratio_span_values):.3f}x; "
            f"{largest_span_winner}.",
        ]
    )
    report.extend(
        [
            "",
            "## What is included at the binding boundary",
            "",
            "- Write timing includes encoding the complete Go mutation slice into UniFFI bytes, one cgo call, Rust tree work, and decoding the returned tree handle.",
            f"- Binding point-read path: {machine.get('binding_point_api', 'not recorded')}.",
            f"- Binding scan path: {machine.get('binding_scan_api', 'not recorded')}.",
            "- Fixture/key/value construction is outside write timing; one untimed point-read warm pass precedes measurement.",
            f"- Highest scenario median peak RSS: Rust Go binding {peak_rss[BINDING] / 2**30:.2f} GiB; native Go {peak_rss[NATIVE] / 2**30:.2f} GiB.",
            "",
            "### Peak RSS by workload",
            "",
            "| Records | Phase | Workload | Rust Go binding | Native Go | Binding delta |",
            "|---:|---|---|---:|---:|---:|",
        ]
    )
    memory_by_scenario: dict[tuple[str, str, str], dict[str, float]] = defaultdict(dict)
    for row in memory_rows:
        memory_by_scenario[(row["records"], row["phase"], row["workload"])][
            row["implementation"]
        ] = float(row["median_peak_rss_bytes"])
    for key in sorted(memory_by_scenario, key=lambda item: (int(item[0]), item[1], item[2])):
        values = memory_by_scenario[key]
        binding_rss = values[BINDING]
        native_rss = values[NATIVE]
        report.append(
            f"| {int(key[0]):,} | {key[1]} | {key[2]} | "
            f"{binding_rss / 2**30:.2f} GiB | {native_rss / 2**30:.2f} GiB | "
            f"{(binding_rss / native_rss - 1.0) * 100:+.1f}% |"
        )
    report.extend(
        [
            "",
            "| Records | Phase | Workload | Operation | Runs | Rust Go binding ns/op | Native Go ns/op | Winner | Speedup |",
            "|---:|---|---|---|---:|---:|---:|---|---:|",
        ]
    )
    for row in summary_rows:
        report.append(
            "| {records} | {phase} | {workload} | {operation} | {runs} | "
            "{rust_go_binding_median_ns_per_op} | {native_go_median_ns_per_op} | "
            "{winner} | {winner_speedup}x |".format(**row)
        )
    report.extend(
        [
            "",
            "## Workload and validation contract",
            "",
            f"- Dataset sizes measured in this report: {machine.get('sizes', 'not recorded')} base records.",
            "- Keys: fixed-width, zero-padded UTF-8 strings; values: deterministic pseudo-random 1–100 byte payloads.",
            "- Fresh workloads: ascending append order, uniform deterministic permutation, and permuted 1,000-key clusters.",
            "- Mutation workloads: 30% of base size; random and clustered use 50% inserts and 50% updates.",
            "- Point reads use at most 100,000 existing keys; scans traverse the complete resulting tree.",
            "- Paired runs must match operation count, workload digest, result cardinality, point values, and scan ordering.",
            "- Implementations use their product-default encoding and chunking; this is not a common-wire-format microbenchmark.",
            "",
            "## Interpretation limits",
            "",
            "The result isolates neither cgo nor UniFFI serialization. Those costs are intentionally included because they are paid by a Go caller. "
            "It does not cover disk I/O, cold cache, multiple workers, deployment packaging, partial/selective range scans, or concurrent readers. "
            "Medians describe these measured runs; they are not confidence intervals.",
            "",
            swap_note,
            "",
            "## Implemented mechanisms and remaining work",
            "",
            "The benchmark measures these mechanisms together; it does not isolate each contribution:",
            "",
            "1. Point reads reuse a root-bound native session; owned reads use caller-provided output and view reads retain the immutable packed leaf for callback scope.",
            "2. Multi-get crosses cgo once with a packed key arena and returns one validated packed result page in caller order.",
            "3. Full scans seek once, retain the native traversal stack, and return validated 4,096-record pages; `ScanRangeView` allocates no Go key/value slices per row.",
            "4. Opaque registry handles reject stale IDs; page/value release and scan close are idempotent.",
            "5. Remaining work is to profile the scan gap, add packed retained diff/conflict pages, benchmark multi-get widths, add transport counters, and reduce write-side mutation/RSS copies.",
            "",
            "Owned compatibility APIs remain unchanged. View APIs are opt-in and callback-scoped; callers must copy any bytes retained after the callback.",
            "",
            "Raw process output is retained in `raw/`; `results.csv` contains normalized measurements, and `machine.txt` identifies the exact release library loaded.",
        ]
    )
    (out / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
