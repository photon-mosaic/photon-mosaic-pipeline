"""
Dataset discovery module.

This module provides a class-based approach to discover datasets using regex
patterns.
All filtering and transformations are handled through regex substitutions.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union


@dataclass
class DatasetInfo:
    """Container for dataset information."""

    original_name: str
    transformed_name: str
    tiff_files: Dict[int, List[str]]  # session_idx -> list of files
    subject_metadata: str
    session_metadata: Dict[int, str]  # session_idx -> metadata string


class DatasetDiscoverer:
    """
    A class for discovering and organizing datasets with TIFF files.

    This class handles both NeuroBlueprint format and custom format datasets,
    providing methods to discover datasets, extract metadata, and organize
    TIFF files by sessions.
    """

    def __init__(
        self,
        base_path: Union[str, Path],
        pattern: str = ".*",
        exclude_datasets: Optional[List[str]] = None,
        exclude_sessions: Optional[List[str]] = None,
        tiff_patterns: Optional[List[str]] = None,
        neuroblueprint_format: bool = False,
    ):
        """
        Initialize the dataset discoverer.

        Parameters
        ----------
        base_path : str or Path
            Base path to search for datasets.
        pattern : str, optional
            Regex pattern to match dataset names, defaults to ".*"
            (all directories). Only used when neuroblueprint_format is False.
        exclude_datasets : List[str], optional
            List of regex patterns for dataset folder names to exclude.
            Applied to both NeuroBlueprint and custom discovery.
        exclude_sessions : List[str], optional
            List of regex patterns for session folder names to exclude.
            Applied when scanning session folders for NeuroBlueprint or
            custom session folders.
        tiff_patterns : List[str], optional
            List of glob patterns for TIFF files. Each pattern corresponds to a
            session. Defaults to ["*.tif"] for a single session.
        neuroblueprint_format : bool, optional
            If True, validates and uses NeuroBlueprint format
            (sub-XXX_key-value/ses-XXX_key-value/)
            with automatic metadata extraction. If False or if validation
            fails, transforms folder names to NeuroBlueprint compliant
            format. Defaults to False.
        """
        self.base_path = Path(base_path)
        self.pattern = pattern
        # Patterns to exclude dataset folder names (regex)
        self.exclude_datasets = exclude_datasets or []
        # Patterns to exclude session folder names (regex)
        self.exclude_sessions = exclude_sessions or []
        self.tiff_patterns = tiff_patterns or ["*.tif"]
        self.neuroblueprint_format = neuroblueprint_format

        # Will be populated by discover()
        self.datasets: List[DatasetInfo] = []
        self._all_tiff_files: List[str] = []

    @property
    def original_datasets(self) -> List[str]:
        """Get list of original dataset names."""
        return [ds.original_name for ds in self.datasets]

    @property
    def transformed_datasets(self) -> List[str]:
        """Get list of transformed dataset names."""
        return [ds.transformed_name for ds in self.datasets]

    @property
    def tiff_files(self) -> Dict[str, Dict[int, List[str]]]:
        """Get TIFF files organized by original dataset name and session."""
        return {ds.original_name: ds.tiff_files for ds in self.datasets}

    @property
    def tiff_files_flat(self) -> List[str]:
        """Get flat list of all TIFF files."""
        return self._all_tiff_files.copy()

    @property
    def subject_metadata(self) -> Dict[str, str]:
        """Get subject metadata by original dataset name."""
        return {ds.original_name: ds.subject_metadata for ds in self.datasets}

    @property
    def session_metadata(self) -> Dict[str, Dict[int, str]]:
        """Get session metadata by original dataset name and session."""
        return {ds.original_name: ds.session_metadata for ds in self.datasets}

    def get_session_name(self, dataset_idx: int, session_idx: int) -> str:
        """
        Get session name for given dataset and session indices.

        Parameters
        ----------
        dataset_idx : int
            Index of the dataset in the discovered datasets list
        session_idx : int
            Index of the session within the dataset

        Returns
        -------
        str
            Formatted session name like "ses-0_metadata" or
            "ses-1_date-20250225"
        """
        if dataset_idx >= len(self.datasets):
            raise IndexError(
                f"Dataset index {dataset_idx} out of range "
                f"(0-{len(self.datasets)-1})"
            )

        dataset = self.datasets[dataset_idx]
        session_meta = dataset.session_metadata.get(session_idx, "")

        # Format: ses-{session_idx}_{metadata}
        # session_idx here is actually the original session ID from the
        # folder name
        if session_meta:
            return f"ses-{session_idx:03d}_{session_meta}"
        else:
            return f"ses-{session_idx:03d}"

    def get_tiff_relative_path_for_subject_session_file(
        self, subject_name: str, session_idx: int, filename: str
    ) -> str:
        """
        Get the relative path for a specific TIFF file by its filename.

        Parameters
        ----------
        subject_name : str
            Transformed subject name (e.g., "sub-001_date-20250225")
        session_idx : int
            Session index
        filename : str
            Just the filename (e.g., "type_1_01.tif")

        Returns
        -------
        str
            Relative path to the TIFF file from dataset folder
            (e.g., "imaging/type_1_01.tif" or just "type_1_01.tif")
        """
        # Find the dataset with matching transformed name
        dataset = next(
            (
                ds
                for ds in self.datasets
                if ds.transformed_name == subject_name
            ),
            None,
        )
        if not dataset:
            raise ValueError(f"Subject {subject_name} not found")

        # Get the relative TIFF paths for this session
        tiff_files = dataset.tiff_files.get(session_idx, [])

        for tiff_path in tiff_files:
            if Path(tiff_path).name == filename:
                return tiff_path

        raise ValueError(
            f"TIFF file {filename} not found in subject {subject_name}, "
            f"session {session_idx}"
        )

    @staticmethod
    def extract_session_idx_from_session_name(session_name: str) -> int:
        """
        Extract session index from session name.

        Parameters
        ----------
        session_name : str
            Session name like "ses-001_date-20250225" or "ses-003"

        Returns
        -------
        int
            Session index
        """
        match = re.match(r"ses-(\d+)", session_name)
        if match:
            return int(match.group(1))
        raise ValueError(
            f"Could not extract session index from {session_name}"
        )

    @staticmethod
    def _extract_session_id_from_folder_name(folder_name: str) -> str:
        """
        Extract session ID from neuroblueprint session folder name.

        Parameters
        ----------
        folder_name : str
            Session folder name like 'ses-003_date-20250301_protocol-baseline'

        Returns
        -------
        str
            Session ID like '003'
        """
        # Extract the ID part after 'ses-' and
        # before first '_' or end of string

        match = re.match(r"ses-([^_]+)", folder_name)
        if match:
            return match.group(1)
        return "0"  # fallback

    @staticmethod
    def _infer_metadata_keys_from_folder_names(
        folder_names: List[str],
        accept_structural: bool = False,
    ) -> Dict[str, str]:
        """
        Automatically infer metadata keys and patterns from actual folder
        names.

        Parameters
        ----------
        folder_names : list
            List of folder names to analyze for metadata patterns

        Returns
        -------
        dict
            Dictionary of inferred metadata keys and regex patterns
        """
        metadata_patterns = {}

        for folder_name in folder_names:
            # Split by underscore and look for key-value patterns
            parts = folder_name.split("_")
            for part in parts:
                # Look for patterns like "key-value"
                if "-" in part:
                    key, value = part.split("-", 1)
                    # Skip 'sub', 'ses', and 'session' as they are structural,
                    # not metadata
                    if accept_structural or key not in [
                        "sub",
                        "ses",
                        "session",
                    ]:
                        # Create a flexible regex pattern for this key
                        metadata_patterns[key] = f"{key}-([^_]+)"

        return metadata_patterns

    @staticmethod
    def _is_neuroblueprint_format(
        folder_name: str, expected_prefix: str
    ) -> bool:
        """
        Check if a folder name follows NeuroBlueprint format.

        Parameters
        ----------
        folder_name : str
            Name of the folder to check
        expected_prefix : str
            Expected prefix ("sub" or "ses")

        Returns
        -------
        bool
            True if folder follows NeuroBlueprint format, False otherwise
        """
        # Check if it starts with the expected prefix followed by a dash
        if not folder_name.startswith(f"{expected_prefix}-"):
            return False

        # Split by underscores and check each part
        parts = folder_name.split("_")

        # First part should be prefix-identifier
        first_part = parts[0]
        if not re.fullmatch(rf"{expected_prefix}-\d+", first_part):
            return False

        # Remaining parts should be key-value pairs
        for part in parts[1:]:
            if not re.match(r"[a-zA-Z][a-zA-Z0-9]*-[a-zA-Z0-9]+", part):
                return False

        return True

    @staticmethod
    def _extract_metadata_from_name(
        folder_name: str, metadata_extraction: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Extract metadata from folder name and format as key-value pairs.

        Parameters
        ----------
        folder_name : str
            Name of the folder to extract metadata from
        metadata_extraction : dict, optional
            Dictionary of metadata keys and regex patterns.
            If None, will infer from folder name.

        Returns
        -------
        str
            Formatted metadata string like "date-20250225_protocol-training"
        """
        # If no metadata extraction patterns provided, infer from folder name
        if not metadata_extraction:
            metadata_extraction = (
                DatasetDiscoverer._infer_metadata_keys_from_folder_names(
                    [folder_name]
                )
            )
            if not metadata_extraction:
                return ""

        metadata_pairs = []
        for key, pattern in metadata_extraction.items():
            match = re.search(pattern, folder_name)
            if match:
                value = match.group(1)
                metadata_pairs.append(f"{key}-{value}")

        return "_".join(metadata_pairs)

    def discover(self) -> None:
        """
        Discover datasets and their TIFF files in the directory.

        This method populates the datasets list and all related metadata.
        After calling this method, you can access the results through the
        class properties.
        """
        # Clear any existing data
        self.datasets.clear()
        self._all_tiff_files.clear()

        # Discover and transform dataset names
        original_datasets, transformed_datasets = (
            self._discover_dataset_folders()
        )

        # Process each dataset to extract TIFF files and metadata
        for orig_name, trans_name in zip(
            original_datasets, transformed_datasets
        ):
            dataset_info = self._process_dataset(orig_name, trans_name)
            if dataset_info:
                self.datasets.append(dataset_info)

    def _discover_dataset_folders(self) -> tuple[List[str], List[str]]:
        """
        Discover dataset folders and return original and transformed names.

        Returns
        -------
        tuple[List[str], List[str]]
            Tuple of (original_datasets, transformed_datasets)
        """
        original_datasets: List[str] = []
        transformed_datasets: List[str] = []

        if self.neuroblueprint_format:
            original_datasets, transformed_datasets = (
                self._discover_neuroblueprint_datasets()
            )

            # If no valid NeuroBlueprint folders found,
            # fallback to custom format
            if not original_datasets:
                logging.info(
                    "No valid NeuroBlueprint format folders found in "
                    f"{self.base_path}. "
                    "Falling back to custom format processing."
                )
                self.neuroblueprint_format = False

        if not self.neuroblueprint_format:
            original_datasets, transformed_datasets = (
                self._discover_custom_datasets()
            )

        # Sort the datasets as per original names to ensure consistent order
        sorted_indices = sorted(
            range(len(original_datasets)), key=lambda i: original_datasets[i]
        )
        original_datasets = [original_datasets[i] for i in sorted_indices]
        transformed_datasets = [
            transformed_datasets[i] for i in sorted_indices
        ]

        return original_datasets, transformed_datasets

    def _discover_neuroblueprint_datasets(self) -> tuple[List[str], List[str]]:
        """
        Discover datasets in NeuroBlueprint format.

        Returns
        -------
        tuple[List[str], List[str]]
            Tuple of (original_datasets, transformed_datasets)
        """
        # For NeuroBlueprint format, look for sub-XXX
        # directories and validate format
        all_sub_folders = [
            d.name
            for d in self.base_path.iterdir()
            if d.is_dir() and d.name.startswith("sub-")
        ]

        candidate_datasets = [
            folder
            for folder in all_sub_folders
            if self._is_neuroblueprint_format(folder, "sub")
        ]

        # Apply dataset exclusions if any
        if self.exclude_datasets:
            candidate_datasets = [
                ds
                for ds in candidate_datasets
                if not any(re.match(pat, ds) for pat in self.exclude_datasets)
            ]

        # Names are already compliant, so original and
        # transformed are identical
        return candidate_datasets, candidate_datasets

    def _discover_custom_datasets(self) -> tuple[List[str], List[str]]:
        """
        Discover datasets in custom format and transform to
        NeuroBlueprint compliant names.

        Returns
        -------
        tuple[List[str], List[str]]
            Tuple of (original_datasets, transformed_datasets)
        """
        # For custom format, use pattern matching
        candidate_datasets = [
            d.name
            for d in self.base_path.iterdir()
            if d.is_dir() and re.match(self.pattern, d.name)
        ]

        # Apply dataset exclusions if any
        if self.exclude_datasets:
            candidate_datasets = [
                ds
                for ds in candidate_datasets
                if not any(re.match(pat, ds) for pat in self.exclude_datasets)
            ]

        original_datasets = candidate_datasets.copy()

        # No substitutions: use the discovered dataset names directly
        working_datasets = candidate_datasets.copy()

        # Transform to NeuroBlueprint compliant format
        transformed_datasets = []
        for i, ds in enumerate(working_datasets):
            # Check if the dataset name already contains key-value pairs
            # (indicated by underscore-hyphen patterns like "key-value")
            if "_" in ds and "-" in ds:
                # Already has key-value structure, append directly
                transformed_name = f"sub-{i+1:03d}_{ds}"
            else:
                # Use counter as subject ID and original name as metadata
                # with id- prefix
                transformed_name = f"sub-{i+1:03d}_id-{ds}"
            transformed_datasets.append(transformed_name)

        return original_datasets, transformed_datasets

    def _process_dataset(
        self, orig_name: str, trans_name: str
    ) -> Optional[DatasetInfo]:
        """
        Process a single dataset to extract TIFF files and metadata.

        Parameters
        ----------
        orig_name : str
            Original dataset folder name
        trans_name : str
            Transformed NeuroBlueprint compliant name

        Returns
        -------
        Optional[DatasetInfo]
            DatasetInfo object if dataset has TIFF files, None otherwise
        """
        dataset_path = self.base_path / orig_name

        # Check if there is at least one tiff in
        # the dataset using configured patterns
        has_tiff_files = any(
            dataset_path.rglob(pattern) for pattern in self.tiff_patterns
        )
        if not has_tiff_files:
            logging.info(
                f"No tiff files found in {dataset_path} "
                f"matching patterns {self.tiff_patterns}"
            )
            return None

        # Extract subject metadata
        subject_meta, inferred_metadata = self._extract_subject_metadata(
            orig_name, dataset_path
        )

        # Extract TIFF files and session metadata
        tiff_files_by_session, session_meta_by_session = (
            self._extract_tiff_files_and_metadata(
                dataset_path, inferred_metadata
            )
        )

        # Create DatasetInfo object
        return DatasetInfo(
            original_name=orig_name,
            transformed_name=trans_name,
            tiff_files=tiff_files_by_session,
            subject_metadata=subject_meta,
            session_metadata=session_meta_by_session,
        )

    def _extract_subject_metadata(
        self, orig_name: str, dataset_path: Path
    ) -> tuple[str, Dict[str, str]]:
        """
        Extract subject metadata from dataset folder name.

        Parameters
        ----------
        orig_name : str
            Original dataset folder name
        dataset_path : Path
            Path to the dataset folder

        Returns
        -------
        tuple[str, Dict[str, str]]
            Tuple of (subject_metadata, inferred_metadata_patterns)
        """
        if self.neuroblueprint_format:
            # Auto-infer metadata patterns from all folder names in dataset
            all_folder_names = [orig_name]
            if dataset_path.is_dir():
                session_folders = [
                    d.name
                    for d in dataset_path.iterdir()
                    if d.is_dir()
                    and self._is_neuroblueprint_format(d.name, "ses")
                    and not any(
                        re.match(pat, d.name) for pat in self.exclude_sessions
                    )
                ]
                all_folder_names.extend(session_folders)
            inferred_metadata = self._infer_metadata_keys_from_folder_names(
                all_folder_names
            )
            subject_meta = self._extract_metadata_from_name(
                orig_name, inferred_metadata
            )
            return subject_meta, inferred_metadata
        else:
            # For custom format, no metadata extraction
            return "", {}

    def _extract_tiff_files_and_metadata(
        self, dataset_path: Path, inferred_metadata: Dict[str, str]
    ) -> tuple[Dict[int, List[str]], Dict[int, str]]:
        """
        Extract TIFF files and session metadata for a dataset.

        Parameters
        ----------
        dataset_path : Path
            Path to the dataset folder
        inferred_metadata : Dict[str, str]
            Inferred metadata patterns for extraction

        Returns
        -------
        tuple[Dict[int, List[str]], Dict[int, str]]
            Tuple of (tiff_files_by_session, session_metadata_by_session)
        """
        tiff_files_by_session: Dict[int, List[str]] = {}
        session_meta_by_session: Dict[int, str] = {}

        # Always check for NeuroBlueprint session folders first,
        # regardless of global neuroblueprint_format setting
        has_neuroblueprint_sessions = any(
            d.is_dir() and self._is_neuroblueprint_format(d.name, "ses")
            for d in dataset_path.iterdir()
        )

        if has_neuroblueprint_sessions:
            # Use NeuroBlueprint session processing
            tiff_files_by_session, session_meta_by_session = (
                self._extract_neuroblueprint_files(
                    dataset_path, inferred_metadata
                )
            )
            logging.debug(
                f"Found NeuroBlueprint sessions in {dataset_path}, "
                f"extracted {len(tiff_files_by_session)} sessions"
            )
        else:
            # Fall back to custom format processing
            tiff_files_by_session, session_meta_by_session = (
                self._extract_custom_files(dataset_path)
            )
            logging.debug(
                f"No NeuroBlueprint sessions in {dataset_path}, "
                f"using custom format processing"
            )

        return tiff_files_by_session, session_meta_by_session

    def _extract_neuroblueprint_files(
        self, dataset_path: Path, inferred_metadata: Dict[str, str]
    ) -> tuple[Dict[int, List[str]], Dict[int, str]]:
        """
        Extract TIFF files from NeuroBlueprint format dataset.

        Parameters
        ----------
        dataset_path : Path
            Path to the dataset folder
        inferred_metadata : Dict[str, str]
            Inferred metadata patterns for extraction

        Returns
        -------
        tuple[Dict[int, List[str]], Dict[int, str]]
            Tuple of (tiff_files_by_session, session_metadata_by_session)
        """
        tiff_files_by_session = {}
        session_meta_by_session = {}

        # For NeuroBlueprint format, look for ses-XXX folders that are valid
        session_folders = sorted(
            [
                d
                for d in dataset_path.iterdir()
                if d.is_dir()
                and self._is_neuroblueprint_format(d.name, "ses")
                and not any(
                    re.match(pat, d.name) for pat in self.exclude_sessions
                )
            ]
        )

        logging.debug(
            f"Found session folders in {dataset_path}: "
            f"{[s.name for s in session_folders]}"
        )

        # Map each session folder to its actual session ID and extract files
        for session_folder in session_folders:
            # Extract the actual session ID from folder name
            session_id = self._extract_session_id_from_folder_name(
                session_folder.name
            )

            # Try each TIFF pattern to find files in this session
            for tiff_pattern in self.tiff_patterns:
                files_in_session = sorted(
                    [
                        str(f.relative_to(dataset_path))
                        for f in session_folder.rglob(tiff_pattern)
                        if f.is_file()
                    ]
                )

                if files_in_session:
                    # Extract metadata from session folder name
                    session_meta = self._extract_metadata_from_name(
                        session_folder.name, inferred_metadata
                    )

                    # Use the actual session ID as key, not enumerate index
                    tiff_files_by_session[int(session_id)] = files_in_session
                    session_meta_by_session[int(session_id)] = session_meta
                    self._all_tiff_files.extend(files_in_session)

                    logging.debug(
                        f"Session {session_id} matched "
                        f"pattern {tiff_pattern} "
                        f"in {session_folder.name} with "
                        f"metadata: {session_meta}"
                    )
                    break

        if not tiff_files_by_session:
            logging.info(
                f"No files found for patterns {self.tiff_patterns} in "
                f"session folders of {dataset_path}"
            )

        return tiff_files_by_session, session_meta_by_session

    def _extract_custom_files(
        self, dataset_path: Path
    ) -> tuple[Dict[int, List[str]], Dict[int, str]]:
        """
        Extract TIFF files from custom format dataset.

        Parameters
        ----------
        dataset_path : Path
            Path to the dataset folder

        Returns
        -------
        tuple[Dict[int, List[str]], Dict[int, str]]
            Tuple of (tiff_files_by_session, session_metadata_by_session)
        """
        tiff_files_by_session = {}
        session_meta_by_session = {}

        # Check if this dataset contains NeuroBlueprint session folders
        session_folders = [
            d
            for d in dataset_path.iterdir()
            if d.is_dir()
            and self._is_neuroblueprint_format(d.name, "ses")
            and not any(re.match(pat, d.name) for pat in self.exclude_sessions)
        ]

        if session_folders:
            # Hybrid mode: custom subject folder with NeuroBlueprint sessions
            logging.info(
                f"Detected NeuroBlueprint session folders in custom "
                f"dataset {dataset_path.name}: "
                f"{[s.name for s in session_folders]}"
            )

            # Infer metadata patterns from session folder names
            inferred_metadata = self._infer_metadata_keys_from_folder_names(
                [s.name for s in session_folders]
            )

            # Process each session folder similar to NeuroBlueprint mode
            for session_folder in sorted(session_folders):
                session_id = self._extract_session_id_from_folder_name(
                    session_folder.name
                )

                # Try each TIFF pattern to find files in this session
                for tiff_pattern in self.tiff_patterns:
                    files_in_session = sorted(
                        [
                            str(f.relative_to(dataset_path))
                            for f in session_folder.rglob(tiff_pattern)
                            if f.is_file()
                        ]
                    )

                    if files_in_session:
                        # Extract metadata from session folder name
                        session_meta = self._extract_metadata_from_name(
                            session_folder.name, inferred_metadata
                        )

                        # Use the actual session ID as key
                        tiff_files_by_session[int(session_id)] = (
                            files_in_session
                        )
                        session_meta_by_session[int(session_id)] = session_meta
                        self._all_tiff_files.extend(files_in_session)

                        logging.debug(
                            f"Session {session_id} matched pattern "
                            f"{tiff_pattern} "
                            f"in {session_folder.name} with metadata: "
                            f"{session_meta}"
                        )
                        break
        else:
            # Check for custom session folders (not NeuroBlueprint but still
            # organized)
            custom_session_folders = [
                d
                for d in dataset_path.iterdir()
                if d.is_dir()
                and not any(
                    re.match(pat, d.name) for pat in self.exclude_sessions
                )
            ]

            if custom_session_folders:
                # Custom session folders exist - extract metadata from them
                logging.info(
                    f"Detected custom session folders in "
                    f"dataset {dataset_path.name}: "
                    f"{[s.name for s in custom_session_folders]}"
                )

                # Infer metadata patterns from session folder names
                inferred_metadata = (
                    self._infer_metadata_keys_from_folder_names(
                        [s.name for s in custom_session_folders],
                        accept_structural=True,
                    )
                )

                # Process each session folder
                for session_folder in sorted(custom_session_folders):
                    # Extract session ID from folder name (look for
                    # session-XXX pattern)
                    session_id_match = re.search(
                        r"session-(\d+)", session_folder.name
                    )
                    if session_id_match:
                        custom_session_id = int(session_id_match.group(1))
                    else:
                        # If no session-XXX pattern, use enumerate starting
                        # from 1
                        custom_session_id = len(tiff_files_by_session) + 1

                    # Try each TIFF pattern to find files in this session
                    for tiff_pattern in self.tiff_patterns:
                        files_in_session = sorted(
                            [
                                str(f.relative_to(dataset_path))
                                for f in session_folder.rglob(tiff_pattern)
                                if f.is_file()
                            ]
                        )

                        if files_in_session:
                            # Extract metadata from session folder name
                            session_meta = self._extract_metadata_from_name(
                                session_folder.name, inferred_metadata
                            )

                            # Replace structural metadata keys to id
                            for k in ["sub-", "ses-", "session-"]:
                                session_meta = session_meta.replace(k, "id-")

                            # If no metadata could be inferred from a custom
                            # session folder (e.g. plain alphanumeric names
                            # like 'novelEnv07'), preserve the raw folder name
                            # as id-<name> so downstream code can see the
                            # original identifier.
                            if not session_meta:
                                session_meta = f"id-{session_folder.name}"

                            tiff_files_by_session[custom_session_id] = (
                                files_in_session
                            )
                            session_meta_by_session[custom_session_id] = (
                                session_meta
                            )
                            self._all_tiff_files.extend(files_in_session)

                            logging.debug(
                                f"Session {custom_session_id} matched pattern "
                                f"{tiff_pattern} in {session_folder.name} "
                                f"with metadata: {session_meta}"
                            )
                            break
            else:
                # Pure custom format: search directly in dataset folder
                for session_idx, tiff_pattern in enumerate(
                    self.tiff_patterns, start=1
                ):
                    files_found = sorted(
                        [
                            str(f.relative_to(dataset_path))
                            for f in dataset_path.rglob(tiff_pattern)
                            if f.is_file()
                        ]
                    )

                    tiff_files_by_session[session_idx] = files_found
                    if files_found:
                        self._all_tiff_files.extend(files_found)

                    # No session metadata for pure custom format
                    session_meta_by_session[session_idx] = ""

        return tiff_files_by_session, session_meta_by_session
