"""
Optimized streaming parallel importer with proper workload distribution.
- Lightweight producer: only reads raw text chunks
- Heavy consumers: handle encoding, parsing, SQL rewriting, and DB operations
- Uses multiprocessing to bypass Python GIL limitations
"""

import os
import queue
import multiprocessing as mp
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any, Iterator
from pathlib import Path


@dataclass
class RawChunk:
    """Raw text chunk from file (before processing)."""
    chunk_id: int
    file_path: str
    raw_content: str
    encoding: str
    start_position: int
    end_position: int


@dataclass
class ProcessedChunk:
    """Processed chunk result."""
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


class LightweightFileReader:
    """Lightweight file reader - only reads raw text chunks."""
    
    def __init__(self, file_path: str, encoding: str, chunk_size_bytes: int = 1024*1024, logger=None):
        """
        Initialize lightweight file reader.
        
        Args:
            file_path: Path to SQL file
            encoding: Primary encoding to try
            chunk_size_bytes: Size of raw text chunks in bytes
            logger: Optional logger
        """
        self.file_path = file_path
        self.encoding = encoding
        self.chunk_size_bytes = chunk_size_bytes
        self.logger = logger
        
    def read_raw_chunks(self) -> Iterator[RawChunk]:
        """
        Read file in raw text chunks.
        
        Yields:
            RawChunk objects containing raw text
        """
        chunk_id = 0
        position = 0
        
        # Try different encodings for file opening
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
        
        file_handle = None
        encoding_used = None
        
        # Find working encoding
        for encoding in encodings_to_try:
            try:
                if self.logger:
                    self.logger.debug(f"Trying encoding {encoding} for {self.file_path}")
                
                # Test read a small portion first
                with open(self.file_path, 'r', encoding=encoding, errors='strict') as test_f:
                    test_f.read(1024)  # Test read
                
                file_handle = open(self.file_path, 'r', encoding=encoding, errors='replace')
                encoding_used = encoding
                
                if self.logger and encoding != self.encoding:
                    self.logger.warning(f"Using fallback encoding {encoding} for {self.file_path}")
                
                break
                
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                if self.logger:
                    self.logger.debug(f"Failed to open with {encoding}: {e}")
                continue
        
        if not file_handle:
            raise Exception(f"Unable to open file {self.file_path} with any encoding")
        
        try:
            if self.logger:
                self.logger.info(f"Reading {self.file_path} in chunks using {encoding_used}")
            
            while True:
                chunk_content = file_handle.read(self.chunk_size_bytes)
                
                if not chunk_content:
                    break  # End of file
                
                # Find a good break point (end of line or statement)
                if len(chunk_content) == self.chunk_size_bytes:
                    # Look for a good break point
                    last_semicolon = chunk_content.rfind(';')
                    last_newline = chunk_content.rfind('\n')
                    
                    break_point = max(last_semicolon, last_newline)
                    
                    if break_point > 0:
                        # Put back the extra content
                        extra_content = chunk_content[break_point + 1:]
                        chunk_content = chunk_content[:break_point + 1]
                        
                        # Seek back
                        file_handle.seek(file_handle.tell() - len(extra_content))
                
                yield RawChunk(
                    chunk_id=chunk_id,
                    file_path=self.file_path,
                    raw_content=chunk_content,
                    encoding=encoding_used,
                    start_position=position,
                    end_position=position + len(chunk_content)
                )
                
                chunk_id += 1
                position += len(chunk_content)
                
                if self.logger and chunk_id % 100 == 0:
                    self.logger.debug(f"Read {chunk_id} chunks from {self.file_path}")
        
        finally:
            if file_handle:
                file_handle.close()
        
        if self.logger:
            self.logger.info(f"Completed reading {self.file_path}: {chunk_id} chunks")


def process_raw_chunk(raw_chunk: RawChunk, db_config: dict, sql_rewriter_config: dict) -> ProcessedChunk:
    """
    Process a raw chunk in a separate process.
    This function will be executed by worker processes.
    
    Args:
        raw_chunk: Raw text chunk to process
        db_config: Database configuration
        sql_rewriter_config: SQL rewriter configuration
        
    Returns:
        ProcessedChunk with results
    """
    start_time = time.time()
    processed = 0
    failed = 0
    warnings = []
    
    try:
        # Import modules inside the function (required for multiprocessing)
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        
        from oracle_to_postgres.common.database import DatabaseManager, ConnectionInfo
        from oracle_to_postgres.common.sql_rewriter import SQLRewriter
        
        # Initialize components in worker process
        connection_info = ConnectionInfo(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            username=db_config['username'],
            password=db_config['password'],
            schema=db_config['schema']
        )
        
        db_manager = DatabaseManager(connection_info=connection_info, pool_size=1)
        
        sql_rewriter = SQLRewriter(
            source_db=sql_rewriter_config['source_db'],
            target_db=sql_rewriter_config['target_db'],
            target_schema=sql_rewriter_config['target_schema']
        )
        
        # Parse SQL statements from raw content
        statements = _parse_sql_statements(raw_chunk.raw_content)
        
        # Process each statement
        for statement in statements:
            if _is_valid_insert_statement(statement):
                try:
                    # Rewrite SQL
                    rewritten = sql_rewriter.rewrite_insert_statement(statement)
                    
                    # Execute in database
                    with db_manager.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(rewritten)
                            conn.commit()
                            processed += 1
                            
                except Exception as e:
                    failed += 1
                    warning = f"Failed to execute statement: {str(e)[:100]}..."
                    warnings.append(warning)
        
        processing_time = time.time() - start_time
        
        return ProcessedChunk(
            chunk_id=raw_chunk.chunk_id,
            success=failed == 0,
            processed_statements=processed,
            failed_statements=failed,
            processing_time=processing_time,
            warnings=warnings
        )
        
    except Exception as e:
        processing_time = time.time() - start_time
        return ProcessedChunk(
            chunk_id=raw_chunk.chunk_id,
            success=False,
            processed_statements=processed,
            failed_statements=len(_parse_sql_statements(raw_chunk.raw_content)) - processed,
            processing_time=processing_time,
            error_message=str(e),
            warnings=warnings
        )


def _parse_sql_statements(content: str) -> List[str]:
    """Parse SQL statements from raw content."""
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


def _is_valid_insert_statement(statement: str) -> bool:
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


class OptimizedStreamingImporter:
    """Optimized streaming importer with proper workload distribution."""
    
    def __init__(self, db_manager, sql_rewriter, max_workers: int = 4, 
                 chunk_size_bytes: int = 1024*1024, use_multiprocessing: bool = True, logger=None):
        """
        Initialize optimized streaming importer.
        
        Args:
            db_manager: Database manager (for config extraction)
            sql_rewriter: SQL rewriter (for config extraction)
            max_workers: Number of worker processes/threads
            chunk_size_bytes: Size of raw chunks in bytes
            use_multiprocessing: Whether to use multiprocessing (recommended)
            logger: Optional logger
        """
        self.db_manager = db_manager
        self.sql_rewriter = sql_rewriter
        self.max_workers = max_workers
        self.chunk_size_bytes = chunk_size_bytes
        self.use_multiprocessing = use_multiprocessing
        self.logger = logger
        
        # Extract configurations for worker processes
        self.db_config = {
            'host': db_manager.connection_info.host,
            'port': db_manager.connection_info.port,
            'database': db_manager.connection_info.database,
            'username': db_manager.connection_info.username,
            'password': db_manager.connection_info.password,
            'schema': db_manager.connection_info.schema
        }
        
        self.sql_rewriter_config = {
            'source_db': sql_rewriter.source_db,
            'target_db': sql_rewriter.target_db,
            'target_schema': sql_rewriter.target_schema
        }
        
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
                   progress_callback: Optional[Callable] = None) -> List[ProcessedChunk]:
        """
        Import a large SQL file using optimized streaming processing.
        
        Args:
            file_path: Path to SQL file
            encoding: File encoding
            progress_callback: Optional progress callback
            
        Returns:
            List of processed chunk results
        """
        if self.logger:
            self.logger.info(f"Starting optimized streaming import of {file_path} "
                           f"with {self.max_workers} {'processes' if self.use_multiprocessing else 'threads'}")
        
        start_time = time.time()
        results = []
        
        # Create lightweight file reader
        reader = LightweightFileReader(file_path, encoding, self.chunk_size_bytes, self.logger)
        
        if self.use_multiprocessing:
            # Use multiprocessing to bypass GIL
            results = self._process_with_multiprocessing(reader, progress_callback)
        else:
            # Use threading (fallback)
            results = self._process_with_threading(reader, progress_callback)
        
        # Update final statistics
        self._update_final_statistics(results)
        self.import_stats['total_time'] = time.time() - start_time
        
        if self.logger:
            self.logger.info(f"Optimized streaming import completed: {len(results)} chunks processed")
        
        return sorted(results, key=lambda x: x.chunk_id)
    
    def _process_with_multiprocessing(self, reader: LightweightFileReader, 
                                    progress_callback: Optional[Callable] = None) -> List[ProcessedChunk]:
        """Process using multiprocessing for better CPU utilization."""
        results = []
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for processing
            futures = {}
            
            for raw_chunk in reader.read_raw_chunks():
                future = executor.submit(
                    process_raw_chunk, 
                    raw_chunk, 
                    self.db_config, 
                    self.sql_rewriter_config
                )
                futures[future] = raw_chunk.chunk_id
                
                if self.logger:
                    self.logger.debug(f"Submitted chunk {raw_chunk.chunk_id} for processing")
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    
                    if self.logger:
                        self.logger.debug(f"Completed chunk {result.chunk_id}: "
                                        f"{result.processed_statements} processed, "
                                        f"{result.failed_statements} failed")
                    
                    if progress_callback:
                        # Simple progress callback
                        progress_callback(len(results), len(futures))
                        
                except Exception as e:
                    chunk_id = futures[future]
                    if self.logger:
                        self.logger.error(f"Chunk {chunk_id} processing failed: {e}")
                    
                    # Create error result
                    error_result = ProcessedChunk(
                        chunk_id=chunk_id,
                        success=False,
                        processed_statements=0,
                        failed_statements=0,
                        processing_time=0.0,
                        error_message=str(e)
                    )
                    results.append(error_result)
        
        return results
    
    def _process_with_threading(self, reader: LightweightFileReader, 
                              progress_callback: Optional[Callable] = None) -> List[ProcessedChunk]:
        """Process using threading (fallback method)."""
        results = []
        chunk_queue = queue.Queue(maxsize=self.max_workers * 2)
        
        # Producer thread
        def producer():
            try:
                for raw_chunk in reader.read_raw_chunks():
                    chunk_queue.put(raw_chunk)
                
                # Signal end
                for _ in range(self.max_workers):
                    chunk_queue.put(None)
                    
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Producer error: {e}")
                for _ in range(self.max_workers):
                    chunk_queue.put(None)
        
        # Start producer
        producer_thread = threading.Thread(target=producer, daemon=True)
        producer_thread.start()
        
        # Consumer threads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for worker_id in range(self.max_workers):
                future = executor.submit(self._thread_worker, worker_id, chunk_queue)
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    worker_results = future.result()
                    results.extend(worker_results)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Worker error: {e}")
        
        producer_thread.join()
        return results
    
    def _thread_worker(self, worker_id: int, chunk_queue: queue.Queue) -> List[ProcessedChunk]:
        """Thread worker for processing chunks."""
        results = []
        
        while True:
            try:
                raw_chunk = chunk_queue.get(timeout=60)
                
                if raw_chunk is None:
                    break
                
                # Process chunk in thread
                result = process_raw_chunk(raw_chunk, self.db_config, self.sql_rewriter_config)
                results.append(result)
                
                chunk_queue.task_done()
                
            except queue.Empty:
                break
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Thread worker {worker_id} error: {e}")
                break
        
        return results
    
    def _update_final_statistics(self, results: List[ProcessedChunk]):
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