#!/usr/bin/env python3
import argparse
import csv
import pathlib
import re


CELL = re.compile(r"clients-(?P<clients>[1-9][0-9]*)-pool-(?P<pool>[1-9][0-9]*)$")


def load_rows(root: pathlib.Path) -> list[dict[str, str]]:
    rows = []
    for path in sorted(root.glob("*/service-comparison.csv")):
        match = CELL.fullmatch(path.parent.name)
        if not match:
            raise ValueError(f"unexpected service cell directory: {path.parent.name}")
        with path.open(newline="") as handle:
            matches = list(csv.DictReader(handle))
        if len(matches) != 4:
            raise ValueError(f"{path} must contain four service operation rows")
        for row in matches:
            row["clients"] = match.group("clients")
            row["pool_size"] = match.group("pool")
            rows.append(row)
    if not rows:
        raise ValueError("service matrix contains no completed cells")
    return rows


def render(rows: list[dict[str, str]]) -> str:
    lines = [
        "# MySQL vs PostgreSQL service saturation",
        "",
        "Each cell runs byte-identical adapter workloads with one excluded "
        "warmup and at least seven measured repetitions. Concurrent-get and "
        "contended-root-CAS p99 are request-level tail latency.",
        "",
        "| Clients | Pool | Operation | PostgreSQL ops/s | PostgreSQL p99 | MySQL ops/s | "
        "MySQL p99 | MySQL/PG latency CI | Result |",
        "|---:|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in sorted(
        rows,
        key=lambda item: (
            int(item["clients"]),
            int(item["pool_size"]),
            item["operation"],
        ),
    ):
        lines.append(
            "| {clients} | {pool_size} | {operation} | {pg_ops:.2f} | {pg_p99:.2f} ms | "
            "{my_ops:.2f} | {my_p99:.2f} ms | {low:.3f}–{high:.3f} | "
            "{winner} |".format(
                clients=row["clients"],
                pool_size=row["pool_size"],
                operation=row["operation"],
                pg_ops=float(row["postgres_ops_per_sec"]),
                pg_p99=float(row["postgres_p99_ms"]),
                my_ops=float(row["mysql_ops_per_sec"]),
                my_p99=float(row["mysql_p99_ms"]),
                low=float(row["ratio_ci_low"]),
                high=float(row["ratio_ci_high"]),
                winner=row["winner"].replace("_", " "),
            )
        )
    lines.extend(
        [
            "",
            "These local-container results show adapter and pool behavior on the "
            "captured host. They are not universal production capacity claims.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    rows = load_rows(args.input)
    report = render(rows)
    output = args.output / "service-matrix.csv"
    if output.exists() or (args.output / "report.md").exists():
        raise ValueError("refusing to overwrite service matrix summary")
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=rows[0].keys(), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    (args.output / "report.md").write_text(report)


if __name__ == "__main__":
    main()
