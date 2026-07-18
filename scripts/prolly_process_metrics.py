#!/usr/bin/env python3
"""Normalize peak resident-set measurements from BSD and GNU time output."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


_GNU_RSS = re.compile(r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$")
_MACOS_RSS = re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$")


def parse_peak_rss(text: str) -> int:
    """Return peak RSS in bytes, rejecting missing or ambiguous measurements."""
    values: list[int] = []
    saw_label = False
    for line in text.splitlines():
        if "maximum resident set size" not in line.lower():
            continue
        saw_label = True
        if match := _GNU_RSS.fullmatch(line):
            values.append(int(match.group(1)) * 1024)
        elif match := _MACOS_RSS.fullmatch(line):
            values.append(int(match.group(1)))
        else:
            raise ValueError(f"malformed peak RSS metric: {line!r}")
    if not saw_label:
        raise ValueError("peak RSS metric not found")
    if len(set(values)) != 1:
        raise ValueError(f"conflicting peak RSS metrics: {values}")
    return values[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("timing_output", type=Path)
    args = parser.parse_args()
    print(parse_peak_rss(args.timing_output.read_text(encoding="utf-8")))


if __name__ == "__main__":
    main()
