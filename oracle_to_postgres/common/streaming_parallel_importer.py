"""
Streaming parallel importer for large SQL files.
Uses producer-consumer pattern with chunked reading and parallel processing.
"""

import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any, Iterator
from pathlib import Path


@dataclass
class ImportChunk:
    """A chunk of SQL statements to be processed."""
    chunk_id: int
    file_path: str
    statements: List[str]
    start_line: int
    end_line: int


@dataclass
class ChunkResult:
    """Result of processing a chunk."""
    chunk_id: int
    success: bool
    processed_statements: int
    failed_statements: int
    processing_time: float
    error_message: Optional[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


@dataclass
class StreamingProgress:
    """Progress tracking for streaming import."""
    file_path: str
    total_chunks: int
    completed_chunks: int
    total_statements: int
    processed_statements: int
    failed_statements: int
    start_time: float
    current_chunk: int = 0
    
    @property
    def completion_percentage(self) -> float:
        if self.total_chunks == 0:
            return 0.0
        return (self.completed_chunks / self.total_chunks) * 100
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def estimated_remaining_time(self) -> float:
        if self.completed_chunks == 0:
            return 0.0
        avg_time_per_chunk = self.elapsed_time / self.completed_chunks
        remaining_chunks = self.total_chunks - self.completed_chunks
        return avg_time_per_chunk * remaining_chunks


class SQLFileReader:
    """Streaming reader for large SQL files."""
    
    def __init__(self, file_path: str, encoding: str, chunk_size: int = 10000, logger=None):
        """
        Initialize SQL file reader.
        
        Args:
            file_path: Path to SQL file
            encoding: File encoding
            chunk_size: Number of statements per chunk
            logger: Optional logger
        """
        self.file_path = file_path
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.logger = logger
        
    def read_chunks(self) -> Iterator[ImportChunk]:
        """
        Read SQL file in chunks using true streaming approach.
        
        Yields:
            ImportChunk objects containing SQL statements
        """
        chunk_id = 0
        current_chunk = []
        current_line = 0
        chunk_start_line = 0
        
        try:
            if self.logger:
                self.logger.info(f"Starting streaming read of {self.file_path}")
            
            # Stream read file line by line
            for statement in self._stream_sql_statements():
                if statement.strip():
                    current_chunk.append(statement)
                    current_line += 1
                    
                    # When chunk is full, yield it immediately
                    if len(current_chunk) >= self.chunk_size:
                        if self.logger:
                            self.logger.debug(f"Yielding chunk {chunk_id} with {len(current_chunk)} statements")
                        
                        yield ImportChunk(
                            chunk_id=chunk_id,
                            file_path=self.file_path,
                            statements=current_chunk.copy(),
                            start_line=chunk_start_line,
                            end_line=current_line
                        )
                        
                        chunk_id += 1
                        current_chunk.clear()
                        chunk_start_line = current_line
            
            # Yield remaining statements
            if current_chunk:
                if self.logger:
                    self.logger.debug(f"Yielding final chunk {chunk_id} with {len(current_chunk)} statements")
                
                yield ImportChunk(
                    chunk_id=chunk_id,
                    file_path=self.file_path,
                    statements=current_chunk,
                    start_line=chunk_start_line,
                    end_line=current_line
                )
            
            if self.logger:
                self.logger.info(f"Completed streaming read: {chunk_id + 1} chunks, {current_line} statements")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error reading file {self.file_path}: {str(e)}")
            raise
    
    def _stream_sql_statements(self) -> Iterator[str]:
        """
        Stream SQL statements from file without loading entire file into memory.
        
        Yields:
            Individual SQL statements
        """
        # Try different encodings
        encodings_to_try = [
            self.encoding,
            'utf-8',
            'gbk',
            'gb2312',
            'gb18030',
            'latin-1',
            'cp1252',
            'iso-8859-1'
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_encodings = []
        for enc in encodings_to_try:
            if enc not in seen:
                seen.add(enc)
                unique_encodings.append(enc)
        
        file_handle = None
        encoding_used = None
        
        # Try to open file with different encodings
        for encoding in unique_encodings:
            try:
                if self.logger:
                    self.logger.debug(f"Trying to open {self.file_path} with encoding: {encoding}")
                
                file_handle = open(self.file_path, 'r', encoding=encoding, errors='strict')
                encoding_used = encoding
                
                if self.logger and encoding != self.encoding:
                    self.logger.warning(f"Using fallback encoding: {encoding} for {self.file_path}")
                
                break
                
            except UnicodeDecodeError:
                if file_handle:
                    file_handle.close()
                continue
            except Exception as e:
                if file_handle:
                    file_handle.close()
                continue
        
        if not file_handle:
            # Final fallback with error replacement
            try:
                if self.logger:
                    self.logger.warning(f"All encodings failed, using error replacement for {self.file_path}")
                file_handle = open(self.file_path, 'r', encoding=self.encoding, errors='replace')
                encoding_used = self.encoding
            except Exception as e:
                raise Exception(f"Unable to open file {self.file_path}: {str(e)}")
        
        try:
            # Stream process the file
            current_statement = ""
            in_string = False
            escape_next = False
            line_count = 0
            
            for line in file_handle:
                line_count += 1
                
                # Log progress for very large files
                if self.logger and line_count % 100000 == 0:
                    self.logger.debug(f"Processed {line_count} lines from {self.file_path}")
                
                for char in line:
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
                            yield current_statement.strip()
                        current_statement = ""
            
            # Yield remaining statement if any
            if current_statement.strip():
                yield current_statement.strip()
            
            if self.logger:
                self.logger.info(f"Streamed {line_count} lines from {self.file_path} using {encoding_used}")
                
        finally:
            if file_handle:
                file_handle.close()
    
    def _read_file_with_fallback(self) -> str:
        """Read file with encoding fallback mechanism."""
        encodings_to_try = [
            self.encoding,
            'utf-8',
            'gbk',
            'gb2312',
            'gb18030',
            'latin-1',
            'cp1252',
            'iso-8859-1'
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_encodings = []
        for enc in encodings_to_try:
            if enc not in seen:
                seen.add(enc)
                unique_encodings.append(enc)
        
        last_error = None
        
        for encoding in unique_encodings:
            try:
                if self.logger:
                    self.logger.debug(f"Trying to read {self.file_path} with encoding: {encoding}")
                
                with open(self.file_path, 'r', encoding=encoding, errors='strict') as f:
                    content = f.read()
                
                if self.logger and encoding != self.encoding:
                    self.logger.warning(f"Successfully read {self.file_path} with fallback encoding: {encoding}")
                
                return content
                
            except UnicodeDecodeError as e:
                last_error = e
                continue
            except Exception as e:
                last_error = e
                continue
        
        # Final fallback with error replacement
        try:
            if self.logger:
                self.logger.warning(f"All encodings failed for {self.file_path}, using error replacement")
            
            with open(self.file_path, 'r', encoding=self.encoding, errors='replace') as f:
                content = f.read()
            
            return content
            
        except Exception as e:
            raise Exception(f"Unable to read file {self.file_path}: {last_error}")
    
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


class ChunkProcessor:
    """Processes chunks of SQL statements."""
    
    def __init__(self, db_manager, sql_rewriter, logger=None):
        """
        Initialize chunk processor.
        
        Args:
            db_manager: Database manager
            sql_rewriter: SQL rewriter
            logger: Optional logger
        """
        self.db_manager = db_manager
        self.sql_rewriter = sql_rewriter
        self.logger = logger
    
    def process_chunk(self, chunk: ImportChunk) -> ChunkResult:
        """
        Process a chunk of SQL statements.
        
        Args:
            chunk: ImportChunk to process
            
        Returns:
            ChunkResult with processing details
        """
        start_time = time.time()
        processed = 0
        failed = 0
        warnings = []
        
        try:
            if self.logger:
                self.logger.debug(f"Processing chunk {chunk.chunk_id} with {len(chunk.statements)} statements")
            
            for statement in chunk.statements:
                if self._is_valid_sql_statement(statement):
                    try:
                        # Rewrite the statement
                        rewritten = self.sql_rewriter.rewrite_insert_statement(statement)
                        
                        # Execute the statement
                        with self.db_manager.get_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(rewritten)
                                conn.commit()
                                processed += 1
                                
                    except Exception as e:
                        failed += 1
                        warning = f"Failed to execute statement: {str(e)[:100]}..."
                        warnings.append(warning)
                        if self.logger:
                            self.logger.warning(warning)
                else:
                    # Skip non-INSERT statements
                    if self.logger:
                        self.logger.debug(f"Skipping non-INSERT statement: {statement[:50]}...")
            
            processing_time = time.time() - start_time
            
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                success=failed == 0,
                processed_statements=processed,
                failed_statements=failed,
                processing_time=processing_time,
                warnings=warnings
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Error processing chunk {chunk.chunk_id}: {str(e)}"
            
            if self.logger:
                self.logger.error(error_msg)
            
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                success=False,
                processed_statements=processed,
                failed_statements=len(chunk.statements) - processed,
                processing_time=processing_time,
                error_message=error_msg,
                warnings=warnings
            )
    
    def _is_valid_sql_statement(self, statement: str) -> bool:
        """Check if statement is a valid INSERT statement."""
        statement_lower = statement.lower().strip()
        
        if not statement_lower:
            return False
        
        # Skip Oracle-specific commands
        oracle_commands = [
            'prompt ', 'set feedback', 'set define', 'set echo',
            'set pagesize', 'set linesize', 'set timing', 'set serveroutput',
            'set verify', 'set heading', 'spool ', 'exit', 'quit',
            'connect ', 'disconnect', 'commit;', 'rollback;',
            'alter session', 'whenever sqlerror', 'whenever oserror'
        ]
        
        for oracle_cmd in oracle_commands:
            if statement_lower.startswith(oracle_cmd):
                return False
        
        # Skip comments
        if (statement_lower.startswith('--') or 
            statement_lower.startswith('/*') or 
            statement_lower.startswith('rem ')):
            return False
        
        # Only allow INSERT statements
        return statement_lower.startswith('insert')


class StreamingParallelImporter:
    """Streaming parallel importer for large SQL files."""
    
    def __init__(self, db_manager, sql_rewriter, max_workers: int = 4, 
                 chunk_size: int = 10000, queue_size: int = 100, logger=None):
        """
        Initialize streaming parallel importer.
        
        Args:
            db_manager: Database manager
            sql_rewriter: SQL rewriter
            max_workers: Number of worker threads
            chunk_size: Number of statements per chunk
            queue_size: Maximum queue size
            logger: Optional logger
        """
        self.db_manager = db_manager
        self.sql_rewriter = sql_rewriter
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.queue_size = queue_size
        self.logger = logger
        
        # Statistics
        self.import_stats = {
            'total_chunks': 0,
            'successful_chunks': 0,
            'failed_chunks': 0,
            'total_statements': 0,
            'successful_statements': 0,
            'failed_statements': 0,
            'total_time': 0.0
        }
    
    def import_file(self, file_path: str, encoding: str, 
                   progress_callback: Optional[Callable[[StreamingProgress], None]] = None) -> List[ChunkResult]:
        """
        Import a large SQL file using streaming parallel processing.
        
        Args:
            file_path: Path to SQL file
            encoding: File encoding
            progress_callback: Optional progress callback
            
        Returns:
            List of chunk results
        """
        if self.logger:
            self.logger.info(f"Starting streaming import of {file_path} with {self.max_workers} workers")
        
        start_time = time.time()
        results = []
        
        # Create file reader
        reader = SQLFileReader(file_path, encoding, self.chunk_size, self.logger)
        
        # Create chunk processor
        processor = ChunkProcessor(self.db_manager, self.sql_rewriter, self.logger)
        
        # Create chunk queue
        chunk_queue = queue.Queue(maxsize=self.queue_size)
        
        # Progress tracking
        progress = StreamingProgress(
            file_path=file_path,
            total_chunks=0,
            completed_chunks=0,
            total_statements=0,
            processed_statements=0,
            failed_statements=0,
            start_time=start_time
        )
        
        # Producer thread - reads chunks from file
        def producer():
            try:
                chunk_count = 0
                total_statements = 0
                
                if self.logger:
                    self.logger.info(f"Producer started for {file_path}")
                
                for chunk in reader.read_chunks():
                    # Put chunk in queue (this will block if queue is full)
                    chunk_queue.put(chunk)
                    chunk_count += 1
                    total_statements += len(chunk.statements)
                    
                    # Update progress (estimate total based on current progress)
                    progress.total_chunks = chunk_count  # Will be updated as we go
                    progress.total_statements = total_statements
                    
                    if self.logger:
                        self.logger.debug(f"Queued chunk {chunk.chunk_id} with {len(chunk.statements)} statements")
                
                # Update final totals
                progress.total_chunks = chunk_count
                progress.total_statements = total_statements
                
                # Signal end of chunks to all workers
                for _ in range(self.max_workers):
                    chunk_queue.put(None)
                
                if self.logger:
                    self.logger.info(f"Producer finished: {chunk_count} chunks, {total_statements} statements")
                    
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Producer error: {str(e)}")
                # Signal workers to stop on error
                for _ in range(self.max_workers):
                    try:
                        chunk_queue.put(None, timeout=1)
                    except:
                        pass
        
        # Start producer thread
        producer_thread = threading.Thread(target=producer, daemon=True)
        producer_thread.start()
        
        # Consumer threads - process chunks in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit worker tasks
            futures = []
            for worker_id in range(self.max_workers):
                future = executor.submit(self._worker, worker_id, chunk_queue, processor)
                futures.append(future)
            
            # Collect results and update progress
            for future in as_completed(futures):
                try:
                    worker_results = future.result()
                    results.extend(worker_results)
                    
                    # Update progress
                    for result in worker_results:
                        progress.completed_chunks += 1
                        progress.processed_statements += result.processed_statements
                        progress.failed_statements += result.failed_statements
                        
                        if progress_callback:
                            progress_callback(progress)
                            
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Worker error: {str(e)}")
        
        # Wait for producer to finish
        producer_thread.join()
        
        # Update final statistics
        self._update_final_statistics(results)
        self.import_stats['total_time'] = time.time() - start_time
        
        if self.logger:
            self.logger.info(f"Streaming import completed: {len(results)} chunks processed")
        
        return sorted(results, key=lambda x: x.chunk_id)
    
    def _worker(self, worker_id: int, chunk_queue: queue.Queue, processor: ChunkProcessor) -> List[ChunkResult]:
        """
        Worker thread that processes chunks from the queue.
        
        Args:
            worker_id: Worker identifier
            chunk_queue: Queue containing chunks to process
            processor: Chunk processor
            
        Returns:
            List of chunk results
        """
        results = []
        
        if self.logger:
            self.logger.debug(f"Worker {worker_id} started")
        
        try:
            while True:
                try:
                    # Get chunk from queue (with longer timeout for large files)
                    chunk = chunk_queue.get(timeout=120)  # Increased timeout
                    
                    # None signals end of work
                    if chunk is None:
                        break
                    
                    # Process the chunk
                    result = processor.process_chunk(chunk)
                    results.append(result)
                    
                    if self.logger:
                        self.logger.debug(f"Worker {worker_id} processed chunk {chunk.chunk_id}: "
                                        f"{result.processed_statements} success, {result.failed_statements} failed")
                    
                    # Mark task as done
                    chunk_queue.task_done()
                    
                except queue.Empty:
                    # Timeout waiting for chunk
                    if self.logger:
                        self.logger.warning(f"Worker {worker_id} timeout waiting for chunks")
                    break
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"Worker {worker_id} error: {str(e)}")
        
        if self.logger:
            self.logger.debug(f"Worker {worker_id} finished with {len(results)} chunks")
        
        return results
    
    def _update_final_statistics(self, results: List[ChunkResult]):
        """Update final import statistics."""
        for result in results:
            self.import_stats['total_chunks'] += 1
            if result.success:
                self.import_stats['successful_chunks'] += 1
            else:
                self.import_stats['failed_chunks'] += 1
            
            self.import_stats['successful_statements'] += result.processed_statements
            self.import_stats['failed_statements'] += result.failed_statements
            self.import_stats['total_statements'] += (
                result.processed_statements + result.failed_statements
            )