#!/usr/bin/env python3
"""Verify that every qualified accepted-CAS outage workflow completed exactly once."""

from __future__ import annotations

import argparse
import re


EXPECTED = {
    "ordinary",
    "merge",
    "multipart",
    "workspace",
    "multi-delete",
    "restore",
    "reset",
    "branch-delete",
}
LINE = re.compile(r"ACTIVE_OUTAGE_CHAOS\s+(.*)$")
FIELD = re.compile(r"([a-z_]+)=([^\s]+)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-log", required=True)
    args = parser.parse_args()
    scenarios: dict[str, dict[str, str]] = {}
    with open(args.test_log, encoding="utf-8") as test_log:
        for raw in test_log:
            match = LINE.search(raw)
            if not match:
                continue
            fields = dict(FIELD.findall(match.group(1)))
            scenario = fields.get("scenario")
            if not scenario:
                raise SystemExit("active-outage evidence line has no scenario")
            if scenario in scenarios:
                raise SystemExit(f"duplicate active-outage scenario: {scenario}")
            scenarios[scenario] = fields
    if set(scenarios) != EXPECTED:
        missing = sorted(EXPECTED - scenarios.keys())
        unexpected = sorted(scenarios.keys() - EXPECTED)
        raise SystemExit(
            f"active-outage scenario mismatch: missing={missing} unexpected={unexpected}"
        )
    for scenario, fields in scenarios.items():
        required = {
            "accepted_lost_responses": "1",
            "provider_restarts": "1",
            "first_wire_retries": "0",
            "final_fsck": "ok",
        }
        for key, expected in required.items():
            if fields.get(key) != expected:
                raise SystemExit(
                    f"{scenario} evidence mismatch: {key}={fields.get(key)!r}, expected={expected!r}"
                )
        if scenario in {"reset", "branch-delete"}:
            if fields.get("bucket_commits_created") != "0":
                raise SystemExit(f"{scenario} created a bucket commit")
            if fields.get("duplicate_versions") != "0":
                raise SystemExit(f"{scenario} duplicate command created a physical version")
        elif fields.get("reconciled_operations") != "1":
            raise SystemExit(f"{scenario} did not reconcile exactly one operation")
    restart_millis = sum(int(fields["restart_ms"]) for fields in scenarios.values())
    first_calls = sum(int(fields["first_sdk_calls"]) for fields in scenarios.values())
    print(
        "ACTIVE_OUTAGE_MATRIX_VERIFIED "
        f"scenarios={len(scenarios)} provider_restarts={len(scenarios)} "
        f"restart_millis_total={restart_millis} first_sdk_calls_total={first_calls} "
        f"names={','.join(sorted(scenarios))}"
    )


if __name__ == "__main__":
    main()
