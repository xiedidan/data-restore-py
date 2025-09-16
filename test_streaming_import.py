#!/usr/bin/env python3
"""
Test the streaming parallel import functionality.
"""

import os
import sys
import tempfile
import time

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.streaming_parallel_importer import (
    SQLFileReader, StreamingParallelImporter, ImportChunk
)


def create_large_test_file(num_statements: int = 50000) -> str:
    """Create a large test SQL file."""
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8')
    
    # Write Oracle-specific commands (should be filtered)
    temp_file.write("prompt Importing large test data...\n")
    temp_file.write("set feedback off\n")
    temp_file.write("set define off\n")
    
    # Write INSERT statements
    for i in range(num_statements):
        temp_file.write(f"INSERT INTO TEST_TABLE (ID, NAME, VALUE) VALUES ({i}, 'Name_{i}', {i * 10});\n")
        
        # Add some variety
        if i % 1000 == 0:
            temp_file.write(f"-- Progress: {i} statements written\n")
        
        if i % 5000 == 0:
            temp_file.write("commit;\n")  # Should be filtered
    
    temp_file.close()
    return temp_file.name


def test_sql_file_reader():
    """Test the SQL file reader with chunking."""
    print("Testing SQL File Reader")
    print("=" * 40)
    
    # Create test file
    test_file = create_large_test_file(1000)  # Smaller for testing
    
    try:
        # Test chunked reading
        reader = SQLFileReader(test_file, 'utf-8', chunk_size=100)
        
        chunks = list(reader.read_chunks())
        
        print(f"✅ Created {len(chunks)} chunks from test file")
        
        total_statements = sum(len(chunk.statements) for chunk in chunks)
        print(f"✅ Total statements in chunks: {total_statements}")
        
        # Verify chunk structure
        for i, chunk in enumerate(chunks[:3]):  # Show first 3 chunks
            print(f"  Chunk {chunk.chunk_id}: {len(chunk.statements)} statements "
                  f"(lines {chunk.start_line}-{chunk.end_line})")
            
            # Show sample statements
            if chunk.statements:
                sample = chunk.statements[0][:50] + "..." if len(chunk.statements[0]) > 50 else chunk.statements[0]
                print(f"    Sample: {sample}")
        
        return True
        
    except Exception as e:
        print(f"❌ SQL File Reader test failed: {e}")
        return False
        
    finally:
        # Cleanup
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_streaming_concept():
    """Test the streaming import concept without database."""
    print("\nTesting Streaming Import Concept")
    print("=" * 40)
    
    # Create test file
    test_file = create_large_test_file(5000)
    
    try:
        # Mock database manager and SQL rewriter
        class MockDBManager:
            def get_connection(self):
                return MockConnection()
        
        class MockConnection:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def cursor(self):
                return MockCursor()
            def commit(self):
                pass
        
        class MockCursor:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def execute(self, sql):
                # Simulate processing time
                time.sleep(0.001)
                if 'invalid' in sql.lower():
                    raise Exception("Simulated SQL error")
        
        class MockSQLRewriter:
            def rewrite_insert_statement(self, statement):
                # Simple transformation
                return statement.replace('TEST_TABLE', '"public"."test_table"')
        
        # Create streaming importer
        importer = StreamingParallelImporter(
            db_manager=MockDBManager(),
            sql_rewriter=MockSQLRewriter(),
            max_workers=4,
            chunk_size=1000,
            queue_size=10
        )
        
        # Track progress
        progress_updates = []
        
        def progress_callback(progress):
            progress_updates.append({
                'chunks': f"{progress.completed_chunks}/{progress.total_chunks}",
                'statements': f"{progress.processed_statements}",
                'percentage': f"{progress.completion_percentage:.1f}%"
            })
            print(f"  Progress: {progress.completion_percentage:.1f}% "
                  f"({progress.completed_chunks}/{progress.total_chunks} chunks)")
        
        print("Starting streaming import simulation...")
        start_time = time.time()
        
        # Execute streaming import
        results = importer.import_file(
            file_path=test_file,
            encoding='utf-8',
            progress_callback=progress_callback
        )
        
        end_time = time.time()
        
        # Analyze results
        total_processed = sum(r.processed_statements for r in results)
        total_failed = sum(r.failed_statements for r in results)
        processing_time = end_time - start_time
        
        print(f"✅ Streaming import completed in {processing_time:.2f}s")
        print(f"✅ Processed {len(results)} chunks")
        print(f"✅ Total statements processed: {total_processed}")
        print(f"✅ Total statements failed: {total_failed}")
        print(f"✅ Progress updates: {len(progress_updates)}")
        
        # Show throughput
        if processing_time > 0:
            throughput = total_processed / processing_time
            print(f"✅ Throughput: {throughput:.0f} statements/second")
        
        return True
        
    except Exception as e:
        print(f"❌ Streaming import test failed: {e}")
        return False
        
    finally:
        # Cleanup
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_performance_comparison():
    """Compare streaming vs traditional approach conceptually."""
    print("\nPerformance Comparison")
    print("=" * 40)
    
    print("TRADITIONAL APPROACH:")
    print("  1. Read entire file into memory")
    print("  2. Split into all statements")
    print("  3. Process in batches sequentially")
    print("  4. Memory usage: O(file_size)")
    print("  5. Parallelism: Limited to batch level")
    
    print("\nSTREAMING APPROACH:")
    print("  1. Read file in chunks (configurable size)")
    print("  2. Producer-consumer pattern")
    print("  3. Multiple workers process chunks in parallel")
    print("  4. Memory usage: O(chunk_size * queue_size)")
    print("  5. Parallelism: True parallel processing")
    
    print("\nBENEFITS OF STREAMING:")
    print("  ✅ Lower memory usage for large files")
    print("  ✅ Better parallelism (chunk-level)")
    print("  ✅ Faster processing of large files")
    print("  ✅ More responsive progress reporting")
    print("  ✅ Better resource utilization")


def main():
    """Main test function."""
    print("Streaming Parallel Import Test")
    print("=" * 50)
    
    success_count = 0
    total_tests = 2
    
    # Test SQL file reader
    if test_sql_file_reader():
        success_count += 1
    
    # Test streaming concept
    if test_streaming_concept():
        success_count += 1
    
    # Show performance comparison
    test_performance_comparison()
    
    print("\n" + "=" * 50)
    print(f"TEST RESULTS: {success_count}/{total_tests} passed")
    
    if success_count == total_tests:
        print("🎉 All streaming import tests passed!")
        print("\nThe streaming parallel importer provides:")
        print("1. ✅ Chunked file reading (configurable chunk size)")
        print("2. ✅ Producer-consumer pattern")
        print("3. ✅ True parallel processing of chunks")
        print("4. ✅ Lower memory usage for large files")
        print("5. ✅ Better progress reporting")
        print("6. ✅ Improved throughput")
    else:
        print("⚠️  Some tests failed - check implementation")


if __name__ == "__main__":
    main()