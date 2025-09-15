"""
Parallel data importer for Oracle to PostgreSQL migration.
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Callable, Any


@dataclass
class ImportTask:
    """Single file import task."""
    file_path: str
    table_name: str
    encoding: str
    target_encoding: str = 'utf-8'


@dataclass
class ImportResult:
    """Result of a single file import."""
    file_path: str
    table_name: str
    success: bool
    records_processed: int
    records_failed: int
    processing_time: float
    error_message: Optional[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


@dataclass
class ImportProgress:
    """Progress tracking for import operations."""
    total_files: int
    completed_files: int
    total_records: int
    processed_records: int
    failed_records: int
    start_time: float
    current_file: str = ""
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_files == 0:
            return 0.0
        return (self.completed_files / self.total_files) * 100
    
    @property
    def elapsed_time(self) -> float:
        """Calculate elapsed time in seconds."""
        return time.time() - self.start_time
    
    @property
    def estimated_remaining_time(self) -> float:
        """Estimate remaining time in seconds."""
        if self.completed_files == 0:
            return 0.0
        
        avg_time_per_file = self.elapsed_time / self.completed_files
        remaining_files = self.total_files - self.completed_files
        return avg_time_per_file * remaining_files


class ImportProgressMonitor:
    """Thread-safe progress monitor for import operations."""
    
    def __init__(self, logger=None):
        """Initialize progress monitor."""
        self.logger = logger
        self._lock = threading.Lock()
        self._progress = None
        self._callbacks: List[Callable[[ImportProgress], None]] = []
    
    def start_monitoring(self, total_files: int) -> None:
        """Start monitoring progress."""
        with self._lock:
            self._progress = ImportProgress(
                total_files=total_files,
                completed_files=0,
                total_records=0,
                processed_records=0,
                failed_records=0,
                start_time=time.time()
            )
    
    def update_file_started(self, file_path: str) -> None:
        """Update progress when a file starts processing."""
        with self._lock:
            if self._progress:
                self._progress.current_file = os.path.basename(file_path)
                self._notify_callbacks()
    
    def update_file_completed(self, result: ImportResult) -> None:
        """Update progress when a file completes processing."""
        with self._lock:
            if self._progress:
                self._progress.completed_files += 1
                self._progress.processed_records += result.records_processed
                self._progress.failed_records += result.records_failed
                self._progress.current_file = ""
                self._notify_callbacks()
    
    def add_progress_callback(self, callback: Callable[[ImportProgress], None]) -> None:
        """Add a progress callback function."""
        with self._lock:
            self._callbacks.append(callback)
    
    def get_progress(self) -> Optional[ImportProgress]:
        """Get current progress snapshot."""
        with self._lock:
            if self._progress:
                # Return a copy to avoid threading issues
                return ImportProgress(
                    total_files=self._progress.total_files,
                    completed_files=self._progress.completed_files,
                    total_records=self._progress.total_records,
                    processed_records=self._progress.processed_records,
                    failed_records=self._progress.failed_records,
                    start_time=self._progress.start_time,
                    current_file=self._progress.current_file
                )
            return None
    
    def _notify_callbacks(self) -> None:
        """Notify all registered callbacks of progress update."""
        if self._progress:
            for callback in self._callbacks:
                try:
                    callback(self._progress)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Error in progress callback: {str(e)}")


class SingleFileImporter:
    """Handler for importing a single SQL file."""
    
    def __init__(self, db_manager, sql_rewriter, batch_size: int = 1000, logger=None):
        """
        Initialize single file importer.
        
        Args:
            db_manager: Database manager instance
            sql_rewriter: SQL rewriter instance
            batch_size: Number of records to process in each batch
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.sql_rewriter = sql_rewriter
        self.batch_size = batch_size
        self.logger = logger
    
    def import_file(self, task: ImportTask) -> ImportResult:
        """
        Import a single SQL file.
        
        Args:
            task: Import task containing file information
            
        Returns:
            ImportResult with processing details
        """
        start_time = time.time()
        records_processed = 0
        records_failed = 0
        warnings = []
        
        try:
            if self.logger:
                self.logger.info(f"Starting import of {task.file_path}")
            
            # Read and process the file
            with open(task.file_path, 'r', encoding=task.encoding) as f:
                content = f.read()
            
            # Split into SQL statements
            statements = self._split_sql_statements(content)
            
            # Process statements in batches
            batch = []
            for statement in statements:
                if statement.strip():
                    # Rewrite the statement
                    rewritten = self.sql_rewriter.rewrite_insert_statement(statement)
                    batch.append(rewritten)
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= self.batch_size:
                        batch_result = self._execute_batch(batch)
                        records_processed += batch_result['processed']
                        records_failed += batch_result['failed']
                        warnings.extend(batch_result['warnings'])
                        batch = []
            
            # Process remaining statements
            if batch:
                batch_result = self._execute_batch(batch)
                records_processed += batch_result['processed']
                records_failed += batch_result['failed']
                warnings.extend(batch_result['warnings'])
            
            processing_time = time.time() - start_time
            
            if self.logger:
                self.logger.info(f"Completed import of {task.file_path}: "
                               f"{records_processed} processed, {records_failed} failed")
            
            return ImportResult(
                file_path=task.file_path,
                table_name=task.table_name,
                success=records_failed == 0,
                records_processed=records_processed,
                records_failed=records_failed,
                processing_time=processing_time,
                warnings=warnings
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Error importing {task.file_path}: {str(e)}"
            if self.logger:
                self.logger.error(error_msg)
            
            return ImportResult(
                file_path=task.file_path,
                table_name=task.table_name,
                success=False,
                records_processed=records_processed,
                records_failed=records_failed + 1,
                processing_time=processing_time,
                error_message=error_msg,
                warnings=warnings
            )
    
    def _split_sql_statements(self, content: str) -> List[str]:
        """Split SQL content into individual statements."""
        statements = []
        current_statement = ""
        in_string = False
        escape_next = False
        
        for char in content:
            if escape_next:
                current_statement += char
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                current_statement += char
                continue
            
            if char == "'" and not escape_next:
                in_string = not in_string
            
            current_statement += char
            
            if char == ';' and not in_string:
                if current_statement.strip():
                    statements.append(current_statement.strip())
                current_statement = ""
        
        # Add remaining statement if any
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return statements
    
    def _execute_batch(self, statements: List[str]) -> Dict[str, Any]:
        """
        Execute a batch of SQL statements.
        
        Args:
            statements: List of SQL statements to execute
            
        Returns:
            Dictionary with processing results
        """
        processed = 0
        failed = 0
        warnings = []
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    for statement in statements:
                        try:
                            cursor.execute(statement)
                            processed += 1
                        except Exception as e:
                            failed += 1
                            warning = f"Failed to execute statement: {str(e)[:100]}..."
                            warnings.append(warning)
                            if self.logger:
                                self.logger.warning(warning)
                    
                    # Commit the batch
                    conn.commit()
                    
        except Exception as e:
            # If batch fails completely, mark all as failed
            failed = len(statements)
            processed = 0
            warnings.append(f"Batch execution failed: {str(e)}")
            if self.logger:
                self.logger.error(f"Batch execution failed: {str(e)}")
        
        return {
            'processed': processed,
            'failed': failed,
            'warnings': warnings
        }


class ParallelImporter:
    """Multi-threaded data importer for Oracle to PostgreSQL migration."""
    
    def __init__(self, db_manager, sql_rewriter, max_workers: int = 4, 
                 batch_size: int = 1000, logger=None):
        """
        Initialize parallel importer.
        
        Args:
            db_manager: Database manager instance
            sql_rewriter: SQL rewriter instance
            max_workers: Maximum number of worker threads
            batch_size: Number of records to process in each batch
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.sql_rewriter = sql_rewriter
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.logger = logger
        
        # Progress monitoring
        self.progress_monitor = ImportProgressMonitor(logger)
        
        # Statistics
        self.import_stats = {
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'total_time': 0.0
        }
    
    def import_files(self, import_tasks: List[ImportTask], 
                    progress_callback: Optional[Callable[[ImportProgress], None]] = None) -> List[ImportResult]:
        """
        Import multiple SQL files in parallel.
        
        Args:
            import_tasks: List of import tasks
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of import results
        """
        if not import_tasks:
            if self.logger:
                self.logger.warning("No import tasks provided")
            return []
        
        if self.logger:
            self.logger.info(f"Starting parallel import of {len(import_tasks)} files "
                            f"with {self.max_workers} workers")
        
        # Initialize progress monitoring
        self.progress_monitor.start_monitoring(len(import_tasks))
        if progress_callback:
            self.progress_monitor.add_progress_callback(progress_callback)
        
        # Reset statistics
        self._reset_statistics()
        self.import_stats['total_files'] = len(import_tasks)
        
        start_time = time.time()
        results = []
        
        # Execute tasks in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {}
            for task in import_tasks:
                file_importer = SingleFileImporter(
                    self.db_manager, self.sql_rewriter, 
                    self.batch_size, self.logger
                )
                future = executor.submit(self._import_with_monitoring, file_importer, task)
                future_to_task[future] = task
            
            # Collect results as they complete
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                    self._update_statistics(result)
                    
                except Exception as e:
                    # Create error result for failed task
                    error_result = ImportResult(
                        file_path=task.file_path,
                        table_name=task.table_name,
                        success=False,
                        records_processed=0,
                        records_failed=1,
                        processing_time=0.0,
                        error_message=f"Task execution failed: {str(e)}"
                    )
                    results.append(error_result)
                    self._update_statistics(error_result)
                    
                    if self.logger:
                        self.logger.error(f"Task execution failed for {task.file_path}: {str(e)}")
        
        # Finalize statistics
        self.import_stats['total_time'] = time.time() - start_time
        
        if self.logger:
            self.logger.info(f"Parallel import completed: "
                            f"{self.import_stats['successful_files']}/{self.import_stats['total_files']} files successful, "
                            f"{self.import_stats['successful_records']} records processed")
        
        return results
    
    def _import_with_monitoring(self, file_importer: SingleFileImporter, 
                              task: ImportTask) -> ImportResult:
        """
        Import a file with progress monitoring.
        
        Args:
            file_importer: File importer instance
            task: Import task
            
        Returns:
            Import result
        """
        # Notify progress monitor that file started
        self.progress_monitor.update_file_started(task.file_path)
        
        # Import the file
        result = file_importer.import_file(task)
        
        # Notify progress monitor that file completed
        self.progress_monitor.update_file_completed(result)
        
        return result
    
    def _update_statistics(self, result: ImportResult) -> None:
        """Update import statistics with result."""
        if result.success:
            self.import_stats['successful_files'] += 1
        else:
            self.import_stats['failed_files'] += 1
        
        self.import_stats['successful_records'] += result.records_processed
        self.import_stats['failed_records'] += result.records_failed
        self.import_stats['total_records'] += (result.records_processed + result.records_failed)
    
    def _reset_statistics(self) -> None:
        """Reset import statistics."""
        self.import_stats.update({
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'total_time': 0.0
        })
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current import statistics."""
        return dict(self.import_stats)
    
    def get_progress(self) -> Optional[ImportProgress]:
        """Get current import progress."""
        return self.progress_monitor.get_progress()
    
    def add_progress_callback(self, callback: Callable[[ImportProgress], None]) -> None:
        """Add a progress callback function."""
        self.progress_monitor.add_progress_callback(callback)