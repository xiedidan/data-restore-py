"""
Report generation utilities for Oracle to PostgreSQL migration tool.
"""

import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import asdict


class ReportGenerator:
    """Generate various types of reports for migration results."""
    
    def __init__(self, output_directory: str = "./reports"):
        """Initialize report generator with output directory."""
        self.output_directory = output_directory
        os.makedirs(output_directory, exist_ok=True)
    
    def generate_csv_report(self, data: List[Dict[str, Any]], output_path: str, 
                          headers: Optional[List[str]] = None) -> str:
        """Generate CSV report from list of dictionaries."""
        full_path = os.path.join(self.output_directory, output_path)
        
        if not data:
            # Create empty file with headers if no data
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                if headers:
                    writer = csv.writer(csvfile)
                    writer.writerow(headers)
            return full_path
        
        # Determine headers from data if not provided
        if headers is None:
            headers = list(data[0].keys())
        
        with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in data:
                # Convert dataclass objects to dictionaries if needed
                if hasattr(row, '__dataclass_fields__'):
                    row = asdict(row)
                writer.writerow(row)
        
        return full_path
    
    def generate_json_report(self, data: Any, output_path: str) -> str:
        """Generate JSON report from any serializable data."""
        full_path = os.path.join(self.output_directory, output_path)
        
        # Convert dataclass objects to dictionaries if needed
        if isinstance(data, list):
            serializable_data = []
            for item in data:
                if hasattr(item, '__dataclass_fields__'):
                    serializable_data.append(asdict(item))
                else:
                    serializable_data.append(item)
        elif hasattr(data, '__dataclass_fields__'):
            serializable_data = asdict(data)
        else:
            serializable_data = data
        
        with open(full_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(serializable_data, jsonfile, indent=2, ensure_ascii=False, default=str)
        
        return full_path
    
    def generate_summary_report(self, results: List[Any], output_path: str, 
                              title: str = "Migration Summary") -> str:
        """Generate human-readable summary report."""
        full_path = os.path.join(self.output_directory, output_path)
        
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(f"{title}\n")
            f.write("=" * len(title) + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            if not results:
                f.write("No results to report.\n")
                return full_path
            
            # Calculate statistics
            total_items = len(results)
            successful_items = 0
            failed_items = 0
            
            for result in results:
                if hasattr(result, 'success'):
                    if result.success:
                        successful_items += 1
                    else:
                        failed_items += 1
                elif hasattr(result, 'ddl_generated'):
                    if result.ddl_generated:
                        successful_items += 1
                    else:
                        failed_items += 1
            
            # Write summary statistics
            f.write("Summary Statistics:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Total Items: {total_items}\n")
            if successful_items > 0 or failed_items > 0:
                f.write(f"Successful: {successful_items}\n")
                f.write(f"Failed: {failed_items}\n")
                f.write(f"Success Rate: {(successful_items / total_items * 100):.1f}%\n")
            f.write("\n")
            
            # Write detailed results
            f.write("Detailed Results:\n")
            f.write("-" * 20 + "\n")
            
            for i, result in enumerate(results, 1):
                f.write(f"\n{i}. ")
                
                if hasattr(result, 'file_name'):
                    f.write(f"File: {result.file_name}\n")
                elif hasattr(result, 'table_name'):
                    f.write(f"Table: {result.table_name}\n")
                else:
                    f.write(f"Item {i}\n")
                
                # Write result-specific information
                if hasattr(result, 'success'):
                    status = "SUCCESS" if result.success else "FAILED"
                    f.write(f"   Status: {status}\n")
                elif hasattr(result, 'ddl_generated'):
                    status = "SUCCESS" if result.ddl_generated else "FAILED"
                    f.write(f"   DDL Generated: {status}\n")
                
                if hasattr(result, 'error_message') and result.error_message:
                    f.write(f"   Error: {result.error_message}\n")
                
                if hasattr(result, 'encoding'):
                    f.write(f"   Encoding: {result.encoding}\n")
                
                if hasattr(result, 'records_processed'):
                    f.write(f"   Records Processed: {result.records_processed}\n")
                
                if hasattr(result, 'execution_time'):
                    f.write(f"   Execution Time: {result.execution_time:.2f}s\n")
                
                if hasattr(result, 'processing_time'):
                    f.write(f"   Processing Time: {result.processing_time:.2f}s\n")
        
        return full_path
    
    def generate_encoding_report(self, analysis_results: List[Any]) -> str:
        """Generate encoding analysis report in CSV format."""
        headers = [
            'file_name', 'table_name', 'encoding', 'file_size_mb', 
            'ddl_generated', 'error_message'
        ]
        
        csv_data = []
        for result in analysis_results:
            # Get file size
            file_size_mb = 0.0
            if hasattr(result, 'file_name') and result.file_name:
                try:
                    file_size = os.path.getsize(result.file_name)
                    file_size_mb = file_size / (1024 * 1024)
                except (OSError, AttributeError):
                    pass
            
            csv_data.append({
                'file_name': getattr(result, 'file_name', ''),
                'table_name': getattr(result, 'table_name', ''),
                'encoding': getattr(result, 'encoding', ''),
                'file_size_mb': f"{file_size_mb:.2f}",
                'ddl_generated': getattr(result, 'ddl_generated', False),
                'error_message': getattr(result, 'error_message', '') or ''
            })
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"encoding_analysis_{timestamp}.csv"
        return self.generate_csv_report(csv_data, filename, headers)
    
    def generate_execution_report(self, execution_results: List[Any], 
                                operation_type: str = "execution") -> str:
        """Generate execution results report."""
        headers = [
            'table_name', 'success', 'execution_time', 'error_message'
        ]
        
        csv_data = []
        for result in execution_results:
            csv_data.append({
                'table_name': getattr(result, 'table_name', ''),
                'success': getattr(result, 'success', False),
                'execution_time': f"{getattr(result, 'execution_time', 0.0):.2f}",
                'error_message': getattr(result, 'error_message', '') or ''
            })
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{operation_type}_results_{timestamp}.csv"
        return self.generate_csv_report(csv_data, filename, headers)
    
    def generate_import_report(self, import_results: List[Any]) -> str:
        """Generate data import results report."""
        headers = [
            'file_name', 'records_processed', 'success', 
            'processing_time', 'error_message'
        ]
        
        csv_data = []
        for result in import_results:
            csv_data.append({
                'file_name': getattr(result, 'file_name', ''),
                'records_processed': getattr(result, 'records_processed', 0),
                'success': getattr(result, 'success', False),
                'processing_time': f"{getattr(result, 'processing_time', 0.0):.2f}",
                'error_message': getattr(result, 'error_message', '') or ''
            })
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"import_results_{timestamp}.csv"
        return self.generate_csv_report(csv_data, filename, headers)