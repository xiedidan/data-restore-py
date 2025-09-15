"""
File scanning utilities for Oracle to PostgreSQL migration tool.
"""

import os
import glob
from typing import List, Optional, Generator
from dataclasses import dataclass


@dataclass
class FileInfo:
    """Information about a scanned file."""
    file_path: str
    file_name: str
    file_size: int
    file_size_mb: float
    
    @classmethod
    def from_path(cls, file_path: str) -> 'FileInfo':
        """Create FileInfo from file path."""
        file_size = os.path.getsize(file_path)
        return cls(
            file_path=file_path,
            file_name=os.path.basename(file_path),
            file_size=file_size,
            file_size_mb=file_size / (1024 * 1024)
        )


class FileScanner:
    """Scanner for SQL dump files in directories."""
    
    def __init__(self, file_extensions: Optional[List[str]] = None):
        """
        Initialize file scanner.
        
        Args:
            file_extensions: List of file extensions to scan (default: ['.sql'])
        """
        self.file_extensions = file_extensions or ['.sql']
    
    def scan_directory(self, directory: str, recursive: bool = False) -> List[FileInfo]:
        """
        Scan directory for SQL files.
        
        Args:
            directory: Directory path to scan
            recursive: Whether to scan subdirectories recursively
            
        Returns:
            List of FileInfo objects for found files
            
        Raises:
            FileNotFoundError: If directory doesn't exist
            PermissionError: If directory cannot be accessed
        """
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory not found: {directory}")
        
        if not os.path.isdir(directory):
            raise ValueError(f"Path is not a directory: {directory}")
        
        if not os.access(directory, os.R_OK):
            raise PermissionError(f"Cannot read directory: {directory}")
        
        files = []
        
        for file_path in self._find_files(directory, recursive):
            try:
                file_info = FileInfo.from_path(file_path)
                files.append(file_info)
            except (OSError, PermissionError):
                # Skip files that cannot be accessed
                continue
        
        return sorted(files, key=lambda f: f.file_name)
    
    def _find_files(self, directory: str, recursive: bool) -> Generator[str, None, None]:
        """Find files with matching extensions."""
        for extension in self.file_extensions:
            if recursive:
                pattern = os.path.join(directory, '**', f'*{extension}')
                yield from glob.glob(pattern, recursive=True)
            else:
                pattern = os.path.join(directory, f'*{extension}')
                yield from glob.glob(pattern)
    
    def filter_by_size(self, files: List[FileInfo], 
                      min_size_mb: float = 0.0, 
                      max_size_mb: Optional[float] = None) -> List[FileInfo]:
        """
        Filter files by size.
        
        Args:
            files: List of FileInfo objects
            min_size_mb: Minimum file size in MB
            max_size_mb: Maximum file size in MB (None for no limit)
            
        Returns:
            Filtered list of FileInfo objects
        """
        filtered = []
        
        for file_info in files:
            if file_info.file_size_mb < min_size_mb:
                continue
            
            if max_size_mb is not None and file_info.file_size_mb > max_size_mb:
                continue
            
            filtered.append(file_info)
        
        return filtered
    
    def get_total_size(self, files: List[FileInfo]) -> tuple[int, float]:
        """
        Get total size of files.
        
        Args:
            files: List of FileInfo objects
            
        Returns:
            Tuple of (total_bytes, total_mb)
        """
        total_bytes = sum(f.file_size for f in files)
        total_mb = total_bytes / (1024 * 1024)
        return total_bytes, total_mb
    
    def validate_files(self, files: List[FileInfo]) -> List[FileInfo]:
        """
        Validate that files exist and are readable.
        
        Args:
            files: List of FileInfo objects to validate
            
        Returns:
            List of valid FileInfo objects
        """
        valid_files = []
        
        for file_info in files:
            if (os.path.exists(file_info.file_path) and 
                os.path.isfile(file_info.file_path) and 
                os.access(file_info.file_path, os.R_OK)):
                valid_files.append(file_info)
        
        return valid_files
    
    def extract_table_name_from_filename(self, file_info: FileInfo) -> str:
        """
        Extract table name from filename.
        
        Args:
            file_info: FileInfo object
            
        Returns:
            Extracted table name (filename without extension)
        """
        name = file_info.file_name
        
        # Remove file extension
        for ext in self.file_extensions:
            if name.lower().endswith(ext.lower()):
                name = name[:-len(ext)]
                break
        
        # Clean up common prefixes/suffixes
        prefixes_to_remove = ['dump_', 'export_', 'table_']
        suffixes_to_remove = ['_dump', '_export', '_data']
        
        name_lower = name.lower()
        
        for prefix in prefixes_to_remove:
            if name_lower.startswith(prefix):
                name = name[len(prefix):]
                break
        
        for suffix in suffixes_to_remove:
            if name_lower.endswith(suffix):
                name = name[:-len(suffix)]
                break
        
        return name
    
    def group_files_by_size(self, files: List[FileInfo]) -> dict[str, List[FileInfo]]:
        """
        Group files by size categories.
        
        Args:
            files: List of FileInfo objects
            
        Returns:
            Dictionary with size categories as keys and file lists as values
        """
        groups = {
            'small': [],    # < 10 MB
            'medium': [],   # 10 MB - 100 MB
            'large': [],    # 100 MB - 1 GB
            'xlarge': []    # > 1 GB
        }
        
        for file_info in files:
            if file_info.file_size_mb < 10:
                groups['small'].append(file_info)
            elif file_info.file_size_mb < 100:
                groups['medium'].append(file_info)
            elif file_info.file_size_mb < 1024:
                groups['large'].append(file_info)
            else:
                groups['xlarge'].append(file_info)
        
        return groups