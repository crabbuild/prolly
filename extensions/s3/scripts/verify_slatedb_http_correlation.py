#!/usr/bin/env python3
"""Verify SlateDB object_store API calls against body-blind proxy metrics."""

from __future__ import annotations

import argparse
import collections
import json
import re


TOTAL = re.compile(r"ADVISORY_STORE_TOTAL\s+(.*)$")
FIELD = re.compile(r"([a-z_]+)=([0-9]+)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-log", required=True)
    parser.add_argument("--http-metrics", required=True)
    args = parser.parse_args()

    total_fields: dict[str, int] | None = None
    with open(args.test_log, encoding="utf-8") as test_log:
        for line in test_log:
            match = TOTAL.search(line.strip())
            if match:
                total_fields = {
                    key: int(value) for key, value in FIELD.findall(match.group(1))
                }
    if total_fields is None:
        raise SystemExit("missing ADVISORY_STORE_TOTAL in test output")

    with open(args.http_metrics, encoding="utf-8") as metrics:
        records = [json.loads(line) for line in metrics if line.strip()]
    if not records:
        raise SystemExit("proxy observed no SlateDB HTTP requests")
    # SlateDB legitimately probes for absent manifests and SST objects while
    # discovering the current database state. Keep those terminal object misses
    # visible in the evidence, but reject every other non-2xx response.
    bad_statuses = [
        record["status"]
        for record in records
        if record["status_class"] != 2 and record["status"] != 404
    ]
    if bad_statuses:
        raise SystemExit(f"unexpected or response-less HTTP attempts: {bad_statuses}")
    missing_request_ids = sum(not record.get("request_id") for record in records)
    if missing_request_ids:
        raise SystemExit(f"HTTP attempts without RustFS request IDs: {missing_request_ids}")
    unique_request_ids = {record["request_id"] for record in records}
    if len(unique_request_ids) != len(records):
        raise SystemExit("RustFS request IDs were not unique per observed HTTP attempt")

    api_calls = total_fields["api_calls"]
    if len(records) < api_calls:
        raise SystemExit(
            f"fewer HTTP attempts than object_store calls: http={len(records)} api={api_calls}"
        )
    methods = collections.Counter(record["method"] for record in records)
    minimum_gets = total_fields["get"] + total_fields["list"] + total_fields["delimiter_list"]
    if methods["PUT"] < total_fields["put"]:
        raise SystemExit("HTTP PUT count is below object_store put count")
    if methods["GET"] < minimum_gets:
        raise SystemExit("HTTP GET count is below object_store get/list count")
    if methods["HEAD"] < total_fields["head"]:
        raise SystemExit("HTTP HEAD count is below object_store head count")

    ratio = len(records) / api_calls
    method_summary = ",".join(f"{key}:{methods[key]}" for key in sorted(methods))
    status_summary = ",".join(
        f"{key}:{value}"
        for key, value in sorted(collections.Counter(r["status"] for r in records).items())
    )
    request_bytes = sum(record["request_bytes"] for record in records)
    response_bytes = sum(record["response_bytes"] for record in records)
    print(
        "SLATEDB_HTTP_CORRELATION_COMPLETE "
        f"api_calls={api_calls} http_attempts={len(records)} "
        f"http_per_api_call={ratio:.3f} methods={method_summary} "
        f"statuses={status_summary} unique_request_ids={len(unique_request_ids)} "
        f"request_body_bytes={request_bytes} response_body_bytes={response_bytes}"
    )


if __name__ == "__main__":
    main()
