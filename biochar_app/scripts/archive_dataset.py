#!/usr/bin/env python3

#python -m biochar_app.scripts.archive_dataset

from pathlib import Path
from datetime import datetime
import shutil

def archive_dataset(path: Path) -> None:
    if not path.exists():
        return

    archive_dir = path.parent / "archive"
    archive_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    archived = archive_dir / f"{path.stem}_{timestamp}{path.suffix}"

    shutil.copy2(path, archived)
    print(f"Archived previous dataset → {archived}")


if __name__ == "__main__":
    from biochar_app.config.paths import (
        WARD_MASTER_NIR_CSV,
        WARD_MASTER_SOILBIO_CSV,
        WARD_MASTER_SOILCHEM_CSV,
    )

    for dataset in [
        WARD_MASTER_NIR_CSV,
        WARD_MASTER_SOILBIO_CSV,
        WARD_MASTER_SOILCHEM_CSV,
    ]:
        archive_dataset(dataset)