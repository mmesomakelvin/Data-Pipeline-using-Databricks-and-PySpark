from __future__ import annotations

import argparse
import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARCHIVES_DIR = PROJECT_ROOT / "archives"
LANDING_DIR = PROJECT_ROOT / "landing"


def get_next_archive_file() -> Path | None:
    if not ARCHIVES_DIR.exists():
        raise FileNotFoundError(f"Missing archives folder: {ARCHIVES_DIR}")

    archive_files = sorted(ARCHIVES_DIR.glob("sales_*.csv"))
    return archive_files[0] if archive_files else None


def move_next_file(dry_run: bool) -> None:
    next_file = get_next_archive_file()

    if next_file is None:
        print("No archive files left to move.")
        return

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    landing_file = LANDING_DIR / next_file.name

    if landing_file.exists():
        raise FileExistsError(f"Landing file already exists: {landing_file}")

    if dry_run:
        print(f"Dry run: would move {next_file.relative_to(PROJECT_ROOT)}")
        print(f"Dry run: destination would be {landing_file.relative_to(PROJECT_ROOT)}")
        return

    shutil.move(str(next_file), str(landing_file))
    print(f"Moved {next_file.relative_to(PROJECT_ROOT)}")
    print(f"To {landing_file.relative_to(PROJECT_ROOT)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move the next monthly archive CSV into the landing folder."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the next file movement without changing any files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    move_next_file(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
