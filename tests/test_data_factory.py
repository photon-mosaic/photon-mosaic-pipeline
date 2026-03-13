"""
Test data factory for creating dynamic test environments.

This module provides utilities to create test data structures on-the-fly
instead of using static test data directories. This approach:
1. Uses a single sample TIFF file copied with different names
2. Generates folder structures dynamically in pytest temp directories
3. Allows flexible test scenarios without hardcoded folder names

Generated folder structures:

Basic dataset (create_basic_dataset):
├── raw_data/
│   ├── 001/
│   │   ├── type_1_01.tif
│   │   ├── type_1_02.tif
│   │   └── type_2_02.tif
│   ├── 002/
│   │   ├── type_1_01.tif
│   │   ├── type_1_02.tif
│   │   ├── type_2_01.tif
│   │   └── type_2_02.tif
│   └── 003/
│       └── imaging/
│           ├── type_1_01.tif
│           └── type_1_02.tif

Custom metadata (create_custom_metadata_dataset):
├── raw_data/
│   └── mouse-001_genotype-WT_age-P60_treatment-saline/
│       └── session-001_condition-baseline_paradigm-open-field/
│           └── recording.tif

NeuroBlueprint format (create_neuroblueprint_dataset):
├── raw_data/
│   ├── sub-001_strain-C57BL6_sex-M/
│   │   └── ses-001_date-20250225_protocol-training/
│   │       └── recording.tif
│   └── sub-002_strain-C57BL6_sex-F/
│       └── ses-001_date-20250226_protocol-testing/
│           └── recording.tif

Non-continuous NeuroBlueprint (create_noncontinuous_neuroblueprint_dataset):
├── raw_data/
│   ├── sub-005_strain-BALBC_sex-M/
│   │   ├── ses-001_date-20250221_protocol-test/
│   │   │   └── recording.tif
│   │   ├── ses-003_date-20250223_protocol-test/
│   │   │   └── recording.tif
│   │   └── ses-007_date-20250227_protocol-test/
│   │       └── recording.tif
│   ├── sub-010_strain-BALBC_sex-F/
│   │   ├── ses-002_date-20250222_protocol-test/
│   │   │   └── recording.tif
│   │   └── ses-005_date-20250225_protocol-test/
│   │       └── recording.tif
│   └── sub-025_strain-BALBC_sex-M/
│       ├── ses-001_date-20250221_protocol-test/
│       │   └── recording.tif
│       ├── ses-004_date-20250224_protocol-test/
│       │   └── recording.tif
│       ├── ses-008_date-20250228_protocol-test/
│       │   └── recording.tif
│       └── ses-009_date-20250229_protocol-test/
│           └── recording.tif
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

        if not self.base_tiff_path.exists():
            raise FileNotFoundError(
                f"Base TIFF file not found: {self.base_tiff_path}"
            )

    def create_basic_dataset(
        self,
        tmp_path: Path,
        dataset_names: Optional[List[str]] = None,
        custom_tiff_patterns: Optional[Dict[str, List[str]]] = None,
    ) -> Path:
        """
        Create a basic dataset structure similar to the current tests/data.

        Replicates the exact structure from the original test data:
        - 001/: type_1_01.tif, type_1_02.tif, type_2_02.tif
        - 002/: type_1_01.tif, type_1_02.tif, type_2_01.tif, type_2_02.tif
        - 003/imaging/: type_1_01.tif, type_1_02.tif

        Args:
            tmp_path: Pytest temporary path
            dataset_names: List of dataset folder names
                         (default: ["001", "002", "003"])
            custom_tiff_patterns: Dict mapping dataset names to TIFF lists.
                                If None, uses the original test data structure.

        Returns:
            Path to created raw_data directory
        """
        if dataset_names is None:
            dataset_names = ["001", "002", "003"]

        if custom_tiff_patterns is None:
            # Replicate exact structure from original test data
            custom_tiff_patterns = {
                "001": ["type_1_01.tif", "type_1_02.tif", "type_2_02.tif"],
                "002": [
                    "type_1_01.tif",
                    "type_1_02.tif",
                    "type_2_01.tif",
                    "type_2_02.tif",
                ],
                "003": ["imaging/type_1_01.tif", "imaging/type_1_02.tif"],
            }

        raw_data = tmp_path / "raw_data"

        for dataset_name in dataset_names:
            dataset_path = raw_data / dataset_name

            # Get TIFF patterns for this dataset
            tiff_files = custom_tiff_patterns.get(dataset_name, [])

            for tiff_file in tiff_files:
                # Handle subdirectories (like 003/imaging/)
                full_path = dataset_path / tiff_file
                full_path.parent.mkdir(parents=True, exist_ok=True)

                # Copy our master TIFF file with the target name
                shutil.copy2(self.base_tiff_path, full_path)

        return raw_data

    def create_custom_metadata_dataset(
        self,
        tmp_path: Path,
        subjects: Optional[List[Dict[str, str]]] = None,
        sessions: Optional[List[Dict[str, str]]] = None,
        tiff_files: Optional[List[str]] = None,
    ) -> Path:
        """
        Create a custom metadata format dataset.

        Args:
            tmp_path: Pytest temporary path
            subjects: List of subject metadata dicts
                    (keys become folder name parts)
            sessions: List of session metadata dicts
                    (keys become folder name parts)
            tiff_files: List of TIFF filenames to create in each session

        Returns:
            Path to created raw_data directory
        """
        if subjects is None:
            subjects = [
                {
                    "mouse": "001",
                    "genotype": "WT",
                    "age": "P60",
                    "treatment": "saline",
                }
            ]

        if sessions is None:
            sessions = [
                {
                    "session": "001",
                    "condition": "baseline",
                    "paradigm": "open-field",
                }
            ]

        if tiff_files is None:
            tiff_files = ["recording.tif"]

        raw_data = tmp_path / "raw_data"

        for subject_meta in subjects:
            # Create subject folder name from metadata
            subject_parts = [
                f"{key}-{value}" for key, value in subject_meta.items()
            ]
            subject_name = "_".join(subject_parts)
            subject_path = raw_data / subject_name

            for session_meta in sessions:
                # Create session folder name from metadata
                session_parts = [
                    f"{key}-{value}" for key, value in session_meta.items()
                ]
                session_name = "_".join(session_parts)
                session_path = subject_path / session_name
                session_path.mkdir(parents=True, exist_ok=True)

                # Copy TIFF files
                for tiff_name in tiff_files:
                    shutil.copy2(self.base_tiff_path, session_path / tiff_name)

        return raw_data

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
            Path to created raw_data directory
        """
        if subjects is None:
            subjects = [
                {"id": "001", "strain": "C57BL6", "sex": "M"},
                {"id": "002", "strain": "C57BL6", "sex": "F"},
            ]

        if sessions_per_subject is None:
            sessions_per_subject = [
                [{"id": "001", "date": "20250225", "protocol": "training"}],
                [{"id": "001", "date": "20250226", "protocol": "testing"}],
            ]

        if tiff_files is None:
            tiff_files = ["recording.tif"]

        raw_data = tmp_path / "rawdata"

        for i, subject_meta in enumerate(subjects):
            # Create NeuroBlueprint subject folder name
            subject_id = subject_meta["id"]
            subject_parts = [f"sub-{subject_id}"]

            # Add other metadata as key-value pairs
            for key, value in subject_meta.items():
                if key != "id":
                    subject_parts.append(f"{key}-{value}")

            subject_name = "_".join(subject_parts)
            subject_path = raw_data / subject_name

            # Get sessions for this subject
            sessions = (
                sessions_per_subject[i]
                if i < len(sessions_per_subject)
                else []
            )

            for session_meta in sessions:
                # Create NeuroBlueprint session folder name
                session_id = session_meta["id"]
                session_parts = [f"ses-{session_id}"]

                # Add other metadata as key-value pairs
                for key, value in session_meta.items():
                    if key != "id":
                        session_parts.append(f"{key}-{value}")

                session_name = "_".join(session_parts)
                session_path = subject_path / session_name
                session_path.mkdir(parents=True, exist_ok=True)

                Path(session_path / "funcimg").mkdir(
                    parents=True, exist_ok=True
                )

                # Copy TIFF files
                for tiff_name in tiff_files:
                    shutil.copy2(
                        self.base_tiff_path,
                        session_path / "funcimg" / tiff_name,
                    )

        return raw_data

    def create_noncontinuous_neuroblueprint_dataset(
        self,
        tmp_path: Path,
        subject_ids: Optional[List[int]] = None,
        session_ids_per_subject: Optional[List[List[int]]] = None,
        tiff_files: Optional[List[str]] = None,
    ) -> Path:
        """
        Create NeuroBlueprint dataset with non-continuous IDs for testing.

        Args:
            tmp_path: Pytest temporary path
            subject_ids: List of non-continuous subject IDs
            session_ids_per_subject: List of non-continuous session ID lists,
                                   one per subject
            tiff_files: List of TIFF filenames to create in each session

        Returns:
            Path to created raw_data directory
        """
        if subject_ids is None:
            subject_ids = [5, 10, 25]  # Non-continuous IDs

        if session_ids_per_subject is None:
            session_ids_per_subject = [
                [1, 3, 7],  # Subject 5 has sessions 1, 3, 7
                [2, 5],  # Subject 10 has sessions 2, 5
                [1, 4, 8, 9],  # Subject 25 has sessions 1, 4, 8, 9
            ]

        if tiff_files is None:
            tiff_files = ["recording.tif"]

        raw_data = tmp_path / "raw_data"

        for i, subject_id in enumerate(subject_ids):
            # Create subject folder with some metadata
            sex = "M" if i % 2 == 0 else "F"
            subject_name = f"sub-{subject_id:03d}_strain-BALBC_sex-{sex}"
            subject_path = raw_data / subject_name

            # Get session IDs for this subject
            session_ids = (
                session_ids_per_subject[i]
                if i < len(session_ids_per_subject)
                else []
            )

            for session_id in session_ids:
                date_part = f"2025022{session_id % 9}"
                session_name = (
                    f"ses-{session_id:03d}_date-{date_part}_protocol-test"
                )
                session_path = subject_path / session_name
                session_path.mkdir(parents=True, exist_ok=True)

                # Copy TIFF files
                for tiff_name in tiff_files:
                    shutil.copy2(self.base_tiff_path, session_path / tiff_name)

        return raw_data
