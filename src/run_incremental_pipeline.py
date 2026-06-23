from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


PIPELINE_STEPS = [
    ("Move next archive file to landing", ["src/move_next_archive_to_landing.py"]),
    ("Ingest landing files into Bronze", ["src/ingest_bronze.py"]),
    ("Transform new Bronze rows into Silver", ["src/transform_silver.py"]),
    ("Rebuild Gold aggregations", ["src/build_gold.py"]),
]


def run_step(name: str, script_args: list[str]) -> None:
    print(f"\n=== {name} ===", flush=True)
    command = [PYTHON, *script_args]
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)

    if completed.returncode != 0:
        raise RuntimeError(f"Pipeline step failed: {name}")


def main() -> None:
    print("Starting local incremental pipeline run", flush=True)

    for name, script_args in PIPELINE_STEPS:
        run_step(name, script_args)

    print("\nLocal incremental pipeline run completed", flush=True)


if __name__ == "__main__":
    main()
