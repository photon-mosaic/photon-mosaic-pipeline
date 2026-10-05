"""
Test data factory for creating dynamic test environments.

This module provides utilities to create test data structures on-the-fly
instead of using static test data directories. This approach:
1. Uses a single sample TIFF file copied with different names
2. Generates folder structures dynamically in pytest temp directories
3. The format follows NeuroBlueprint format
4. Allows flexible test scenarios without hardcoded folder names

Generated folder structures:

NeuroBlueprint format (create_neuroblueprint_dataset):
├── rawdata/
│   ├── sub-001_strain-C57BL6_sex-M/
│   │   └── ses-001_date-20250225_protocol-training/
│   │       └── funcimg/
│   │           └── recording.tif
│   ├── sub-002_strain-C57BL6_sex-F/
│   │   └── ses-001_date-20250226_protocol-testing/
│   │       └── funcimg/
│   │           └── recording.tif
│   ├── sub-005_strain-BALBC_sex-M/
│   │   ├── ses-001_date-20250221_protocol-test/
│   │   │   └── funcimg/
│   │   │       └── recording.tif
│   │   ├── ses-003_date-20250223_protocol-test/
│   │   │   └── funcimg/
│   │   │       └── recording.tif
│   │   └── ses-007_date-20250227_protocol-test/
│   │       └── funcimg/
│   │           └── recording.tif
"""

import shutil
from pathlib import Path
from typing import Dict, List, Optional


class DataFactory:
    """Factory for creating test data structures dynamically."""

    def __init__(self, base_tiff_path: Optional[Path] = None):
        """
        Initialize the factory.

        Args:
            base_tiff_path: Path to a sample TIFF file to use as template.
                If None, uses the master.tif from test data.
        """
        if base_tiff_path is None:
            # Use master TIFF file as our template
            self.base_tiff_path = Path(__file__).parent / "data" / "master.tif"
        else:
            self.base_tiff_path = base_tiff_path

        self.base_metadata_path = self.base_tiff_path.with_name(
            f"{self.base_tiff_path.stem}_metadata.json"
        )

        if not self.base_tiff_path.exists():
            raise FileNotFoundError(
                f"Base TIFF file not found: {self.base_tiff_path}"
            )

    def create_neuroblueprint_dataset(
        self,
        tmp_path: Path,
        subjects: Optional[List[Dict[str, str]]] = None,
        sessions_per_subject: Optional[List[List[Dict[str, str]]]] = None,
        tiff_files: Optional[List[str]] = None,
    ) -> Path:
        """
        Create a NeuroBlueprint format dataset.

        Args:
            tmp_path: Pytest temporary path
            subjects: List of subject metadata dicts
                    (must include 'id', other keys become metadata)
            sessions_per_subject: List of session lists, one per subject
                                Each session dict must include 'id',
                                other keys become metadata
            tiff_files: List of TIFF filenames to create in each session

        Returns:
            Path to created rawdata directory
        """
        if subjects is None:
            subjects = [
                {"id": "001", "strain": "C57BL6", "sex": "M"},
                {"id": "002", "strain": "C57BL6", "sex": "F"},
                {"id": "005", "strain": "BALBC", "sex": "M"},
                {"id": "010", "strain": "BALBC", "sex": "F"},
                {"id": "025", "strain": "BALBC", "sex": "M"},
            ]

        if sessions_per_subject is None:
            sessions_per_subject = [
                [{"id": "001", "date": "20250225", "protocol": "training"}],
                [{"id": "001", "date": "20250226", "protocol": "testing"}],
                [
                    {"id": "001", "date": "20250221", "protocol": "test"},
                    {"id": "003", "date": "20250223", "protocol": "test"},
                    {"id": "007", "date": "20250227", "protocol": "test"},
                ],
                [
                    {"id": "002", "date": "20250222", "protocol": "test"},
                    {"id": "005", "date": "20250225", "protocol": "test"},
                ],
                [
                    {"id": "001", "date": "20250221", "protocol": "test"},
                    {"id": "004", "date": "20250224", "protocol": "test"},
                    {"id": "008", "date": "20250228", "protocol": "test"},
                    {"id": "009", "date": "20250228", "protocol": "test"},
                ],
            ]

        if tiff_files is None:
            tiff_files = ["recording.tif"]

        raw_data = tmp_path / "rawdata"

        for i, subject_meta in enumerate(subjects):
            subject_id = subject_meta["id"]
            subject_parts = [f"sub-{subject_id}"]

            for key, value in subject_meta.items():
                if key != "id":
                    subject_parts.append(f"{key}-{value}")

            subject_name = "_".join(subject_parts)
            subject_path = raw_data / subject_name

            sessions = (
                sessions_per_subject[i]
                if i < len(sessions_per_subject)
                else []
            )

            for session_meta in sessions:
                session_id = session_meta["id"]
                session_parts = [f"ses-{session_id}"]

                for key, value in session_meta.items():
                    if key != "id":
                        session_parts.append(f"{key}-{value}")

                session_name = "_".join(session_parts)
                session_path = subject_path / session_name
                session_path.mkdir(parents=True, exist_ok=True)

                Path(session_path / "funcimg").mkdir(
                    parents=True, exist_ok=True
                )

                for tiff_name in tiff_files:
                    shutil.copy2(
                        self.base_tiff_path,
                        session_path / "funcimg" / tiff_name,
                    )
                    # The template TIFF has no embedded ScanImage header, so
                    # ship a fake metadata JSON alongside it (read by the
                    # cascade step to get the frame rate).
                    if self.base_metadata_path.exists():
                        shutil.copy2(
                            self.base_metadata_path,
                            session_path
                            / "funcimg"
                            / f"{Path(tiff_name).stem}_metadata.json",
                        )

        return raw_data
