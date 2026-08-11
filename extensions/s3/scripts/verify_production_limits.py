#!/usr/bin/env python3
"""Fail closed when cost, contention, AWS load, or scale evidence exceeds v1 limits."""

from __future__ import annotations

import argparse
import json
import pathlib
import shlex
from typing import Any


SCHEMA = "prolly-s3-production-limits/v1"
PRICE_FIELDS = {"get", "head", "put", "list", "list_versions", "delete"}


class LimitsError(ValueError):
    pass


def load_limits(path: pathlib.Path) -> dict[str, Any]:
    try:
        limits = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LimitsError(f"cannot load limits from {path}: {error}") from error
    if limits.get("schema") != SCHEMA:
        raise LimitsError(f"limits schema must be {SCHEMA!r}")
    budgets = limits.get("request_budgets")
    profiles = limits.get("profiles")
    if not isinstance(budgets, dict) or not budgets:
        raise LimitsError("request_budgets must be a non-empty object")
    if not isinstance(profiles, dict) or not profiles:
        raise LimitsError("profiles must be a non-empty object")
    for operation, budget in budgets.items():
        if not isinstance(operation, str) or not operation:
            raise LimitsError("request budget operation names must be non-empty strings")
        if not isinstance(budget, dict):
            raise LimitsError(f"request budget for {operation} must be an object")
        positive_number(budget, "max_sdk_calls", f"request budget {operation}")
        if "max_request_cost_usd_per_1000_logical_operations" in budget:
            positive_number(
                budget,
                "max_request_cost_usd_per_1000_logical_operations",
                f"request budget {operation}",
            )
    for name, profile in profiles.items():
        if not isinstance(profile, dict):
            raise LimitsError(f"profile {name} must be an object")
        contention = profile.get("contention")
        if contention is not None:
            positive_number(contention, "logical_retry_limit", f"profile {name} contention")
            tiers = contention.get("tiers")
            if not isinstance(tiers, dict) or not tiers:
                raise LimitsError(f"profile {name} contention tiers must be non-empty")
            for writers, tier in tiers.items():
                if not writers.isdigit() or int(writers) < 1:
                    raise LimitsError(f"profile {name} has invalid writer tier {writers!r}")
                for field in ("max_p95_ms", "max_calls_per_write"):
                    positive_number(tier, field, f"profile {name} contention tier {writers}")
                rate(tier, "max_wire_retry_rate", f"profile {name} contention tier {writers}")
    return limits


def positive_number(record: dict[str, Any], field: str, context: str) -> float:
    value = record.get(field)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise LimitsError(f"{context} field {field} must be positive")
    return float(value)


def rate(record: dict[str, Any], field: str, context: str) -> float:
    value = record.get(field)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value <= 1:
        raise LimitsError(f"{context} field {field} must be between 0 and 1")
    return float(value)


def records(path: pathlib.Path, kind: str) -> list[dict[str, str]]:
    if not path.is_file() or path.is_symlink():
        raise LimitsError(f"evidence is not a regular file: {path}")
    prefix = f"{kind} "
    found: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        offset = line.find(prefix)
        if offset < 0:
            continue
        parsed: dict[str, str] = {}
        for item in shlex.split(line[offset + len(prefix) :]):
            key, separator, value = item.partition("=")
            if not separator or not key or not value:
                raise LimitsError(f"malformed {kind} field: {item!r}")
            if key in parsed:
                raise LimitsError(f"duplicate {kind} field: {key}")
            parsed[key] = value
        found.append(parsed)
    return found


def integer(record: dict[str, str], field: str, context: str) -> int:
    try:
        value = int(record[field])
    except KeyError as error:
        raise LimitsError(f"{context} is missing {field}") from error
    except ValueError as error:
        raise LimitsError(f"{context} field {field} is not an integer") from error
    if value < 0:
        raise LimitsError(f"{context} field {field} must be nonnegative")
    return value


def number(record: dict[str, str], field: str, context: str) -> float:
    try:
        value = float(record[field])
    except KeyError as error:
        raise LimitsError(f"{context} is missing {field}") from error
    except ValueError as error:
        raise LimitsError(f"{context} field {field} is not numeric") from error
    if value < 0:
        raise LimitsError(f"{context} field {field} must be nonnegative")
    return value


def unique_by(items: list[dict[str, str]], field: str, kind: str) -> dict[str, dict[str, str]]:
    indexed: dict[str, dict[str, str]] = {}
    for item in items:
        value = item.get(field)
        if not value:
            raise LimitsError(f"{kind} record has no {field}")
        if value in indexed:
            raise LimitsError(f"duplicate {kind} record for {field}={value}")
        indexed[value] = item
    return indexed


def load_prices(path: pathlib.Path) -> dict[str, float]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LimitsError(f"cannot load request prices from {path}: {error}") from error
    if set(raw) != PRICE_FIELDS:
        raise LimitsError(
            f"request prices must contain exactly {sorted(PRICE_FIELDS)}, found {sorted(raw)}"
        )
    prices: dict[str, float] = {}
    for field, value in raw.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise LimitsError(f"request price {field} must be nonnegative")
        prices[field] = float(value)
    return prices


def verify_cost(
    path: pathlib.Path,
    limits: dict[str, Any],
    profile_name: str,
    price_path: pathlib.Path | None = None,
) -> str:
    profile = limits["profiles"][profile_name]
    cost_limits = profile.get("cost", {})
    price_required = cost_limits.get("require_request_prices") is True
    if price_required and price_path is None:
        raise LimitsError(f"profile {profile_name} requires --request-prices")
    prices = load_prices(price_path) if price_path is not None else None
    observed = unique_by(records(path, "OPERATION_COST"), "operation", "OPERATION_COST")
    budgets = limits["request_budgets"]
    if set(observed) != set(budgets):
        missing = sorted(set(budgets) - set(observed))
        unexpected = sorted(set(observed) - set(budgets))
        raise LimitsError(f"cost operation mismatch: missing={missing} unexpected={unexpected}")
    total_transmissions = 0
    total_retries = 0
    max_retries = cost_limits.get("max_wire_retries_per_operation")
    for operation, budget in budgets.items():
        record = observed[operation]
        context = f"operation {operation}"
        calls = integer(record, "sdk_calls", context)
        if calls > budget["max_sdk_calls"]:
            raise LimitsError(
                f"{context} sdk_calls={calls} exceeds {budget['max_sdk_calls']}"
            )
        transmissions = integer(record, "wire_transmissions", context)
        retries = integer(record, "wire_retries", context)
        total_transmissions += transmissions
        total_retries += retries
        if max_retries is not None and retries > max_retries:
            raise LimitsError(f"{context} wire_retries={retries} exceeds {max_retries}")
        if prices is not None and "max_request_cost_usd_per_1000_logical_operations" in budget:
            modeled = sum(integer(record, field, context) * prices[field] for field in PRICE_FIELDS)
            maximum = budget["max_request_cost_usd_per_1000_logical_operations"]
            if modeled > maximum:
                raise LimitsError(
                    f"{context} modeled request cost ${modeled:.6f}/1000 exceeds ${maximum:.6f}"
                )
    maximum_rate = cost_limits.get("max_wire_retry_rate")
    retry_rate = total_retries / total_transmissions if total_transmissions else 0.0
    if maximum_rate is not None and retry_rate > maximum_rate:
        raise LimitsError(
            f"cost matrix wire retry rate {retry_rate:.6f} exceeds {maximum_rate:.6f}"
        )
    return (
        f"PRODUCTION_COST_LIMITS_VERIFIED profile={profile_name} operations={len(observed)} "
        f"wire_transmissions={total_transmissions} wire_retries={total_retries} "
        f"request_prices={'provided' if prices is not None else 'not-required'}"
    )


def verify_contention(
    path: pathlib.Path,
    limits: dict[str, Any],
    profile_name: str,
    expected_writers: set[str] | None = None,
) -> str:
    contention = limits["profiles"][profile_name].get("contention")
    if not isinstance(contention, dict):
        raise LimitsError(f"profile {profile_name} has no contention limits")
    tiers = contention["tiers"]
    expected = expected_writers if expected_writers is not None else set(tiers)
    unknown = expected - set(tiers)
    if unknown:
        raise LimitsError(f"profile {profile_name} has no budget for writer tiers {sorted(unknown)}")
    observed = unique_by(records(path, "CONTENTION_PROBE"), "writers", "CONTENTION_PROBE")
    if set(observed) != expected:
        raise LimitsError(
            f"contention tier mismatch: expected={sorted(expected)} observed={sorted(observed)}"
        )
    for writers in expected:
        record = observed[writers]
        tier = tiers[writers]
        context = f"contention tier {writers}"
        logical_retry_limit = integer(record, "logical_retry_limit", context)
        if logical_retry_limit != contention["logical_retry_limit"]:
            raise LimitsError(
                f"{context} logical_retry_limit={logical_retry_limit} expected={contention['logical_retry_limit']}"
            )
        p95 = number(record, "p95_ms", context)
        if p95 > tier["max_p95_ms"]:
            raise LimitsError(f"{context} p95_ms={p95:.3f} exceeds {tier['max_p95_ms']}")
        calls = number(record, "calls_per_write", context)
        if calls > tier["max_calls_per_write"]:
            raise LimitsError(
                f"{context} calls_per_write={calls:.3f} exceeds {tier['max_calls_per_write']}"
            )
        transmissions = integer(record, "wire_transmissions", context)
        retries = integer(record, "wire_retries", context)
        retry_rate = retries / transmissions if transmissions else 0.0
        if retry_rate > tier["max_wire_retry_rate"]:
            raise LimitsError(
                f"{context} wire retry rate {retry_rate:.6f} exceeds {tier['max_wire_retry_rate']:.6f}"
            )
    return (
        f"PRODUCTION_CONTENTION_LIMITS_VERIFIED profile={profile_name} "
        f"writers={','.join(sorted(expected, key=int))}"
    )


def verify_load(path: pathlib.Path, limits: dict[str, Any], profile_name: str) -> str:
    load = limits["profiles"][profile_name].get("load")
    if not isinstance(load, dict):
        raise LimitsError(f"profile {profile_name} has no load limits")
    observed = unique_by(records(path, "LOAD_QUALIFICATION"), "operation", "LOAD_QUALIFICATION")
    operations = load["operations"]
    if set(observed) != set(operations):
        raise LimitsError(
            f"load operation mismatch: expected={sorted(operations)} observed={sorted(observed)}"
        )
    for operation, budget in operations.items():
        record = observed[operation]
        context = f"load operation {operation}"
        if record.get("provider") != load["provider"]:
            raise LimitsError(f"{context} provider must be {load['provider']}")
        if integer(record, "samples", context) < budget["min_samples"]:
            raise LimitsError(f"{context} has too few samples")
        for field, maximum in (
            ("p95_ms", budget["max_p95_ms"]),
            ("error_rate", budget["max_error_rate"]),
            ("throttle_rate", budget["max_throttle_rate"]),
        ):
            observed_value = number(record, field, context)
            if observed_value > maximum:
                raise LimitsError(f"{context} {field}={observed_value} exceeds {maximum}")
        request_budget = limits["request_budgets"].get(operation, {})
        maximum_cost = request_budget.get("max_request_cost_usd_per_1000_logical_operations")
        if maximum_cost is not None:
            cost = number(record, "request_cost_usd_per_1000", context)
            if cost > maximum_cost:
                raise LimitsError(f"{context} request cost ${cost:.6f} exceeds ${maximum_cost:.6f}")
    return f"PRODUCTION_LOAD_LIMITS_VERIFIED profile={profile_name} operations={len(observed)}"


def verify_scale(path: pathlib.Path, limits: dict[str, Any], profile_name: str) -> str:
    scale = limits["profiles"][profile_name].get("scale")
    if not isinstance(scale, dict):
        raise LimitsError(f"profile {profile_name} has no scale limits")
    found = records(path, "SCALE_QUALIFICATION")
    if len(found) != 1:
        raise LimitsError(f"expected one SCALE_QUALIFICATION record, found {len(found)}")
    record = found[0]
    if record.get("provider") != scale["provider"]:
        raise LimitsError(f"scale provider must be {scale['provider']}")
    minimums = {
        "live_keys": scale["min_live_keys"],
        "retained_versions": scale["min_retained_versions"],
        "bulk_files_per_second": scale["min_bulk_files_per_second"],
    }
    maximums = {
        "cold_read_p95_ms": scale["max_cold_read_p95_ms"],
        "writable_reopen_seconds": scale["max_writable_reopen_seconds"],
        "memory_bytes": scale["max_memory_bytes"],
    }
    for field, minimum in minimums.items():
        if number(record, field, "scale qualification") < minimum:
            raise LimitsError(f"scale qualification {field} is below {minimum}")
    for field, maximum in maximums.items():
        if number(record, field, "scale qualification") > maximum:
            raise LimitsError(f"scale qualification {field} exceeds {maximum}")
    return (
        f"PRODUCTION_SCALE_LIMITS_VERIFIED profile={profile_name} "
        f"live_keys={record['live_keys']} retained_versions={record['retained_versions']}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limits",
        default=str(pathlib.Path(__file__).resolve().parent.parent / "qualification" / "production-limits-v1.json"),
    )
    parser.add_argument("--profile", default="rustfs-development")
    parser.add_argument("--cost-log")
    parser.add_argument("--contention-log")
    parser.add_argument("--load-log")
    parser.add_argument("--scale-log")
    parser.add_argument("--request-prices")
    parser.add_argument("--expected-writers")
    parser.add_argument("--validate-limits", action="store_true")
    args = parser.parse_args()

    try:
        limits = load_limits(pathlib.Path(args.limits))
        if args.profile not in limits["profiles"]:
            raise LimitsError(f"unknown profile {args.profile!r}")
        outputs: list[str] = []
        if args.cost_log:
            outputs.append(
                verify_cost(
                    pathlib.Path(args.cost_log),
                    limits,
                    args.profile,
                    pathlib.Path(args.request_prices) if args.request_prices else None,
                )
            )
        if args.contention_log:
            expected = set(args.expected_writers.split(",")) if args.expected_writers else None
            outputs.append(
                verify_contention(
                    pathlib.Path(args.contention_log), limits, args.profile, expected
                )
            )
        if args.load_log:
            outputs.append(verify_load(pathlib.Path(args.load_log), limits, args.profile))
        if args.scale_log:
            outputs.append(verify_scale(pathlib.Path(args.scale_log), limits, args.profile))
        if not outputs and not args.validate_limits:
            raise LimitsError("provide evidence logs or --validate-limits")
        if args.validate_limits:
            outputs.insert(0, f"PRODUCTION_LIMITS_VALID schema={SCHEMA} profile={args.profile}")
        for output in outputs:
            print(output)
    except LimitsError as error:
        raise SystemExit(f"invalid production qualification evidence: {error}") from error


if __name__ == "__main__":
    main()
