from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCENARIOS = [
    "batch_build.py",
    "local_first_state.py",
    "resolver.py",
    "crdt_merge.py",
    "conversation_memory.py",
    "agent_event_log.py",
    "background_compaction.py",
    "deterministic_rag_snapshot.py",
    "document_chunk_index.py",
    "vector_sidecar.py",
    "provenance_values.py",
    "materialized_view.py",
    "filesystem_snapshot.py",
    "durable_sqlite.py",
]


def main() -> None:
    here = Path(__file__).resolve().parent
    for scenario in SCENARIOS:
        subprocess.run([sys.executable, str(here / scenario)], check=True)


if __name__ == "__main__":
    main()
