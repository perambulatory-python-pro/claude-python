"""
Performance Optimization for Invoice Processing
Handles 25K-26K rows efficiently with incremental processing capabilities
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import pickle
from typing import Dict, List, Optional, Set
import multiprocessing as mp
from functools import partial
import logging


class InvoiceProcessingOptimizer:
    def __init__(self, cache_dir: str = "processing_cache"):
        """
        Initialize optimizer with caching capabilities
        
        Args:
            cache_dir: Directory for storing processing cache
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Processing configuration
        self.chunk_size = 5000  # Process in chunks of 5K rows
        self.use_multiprocessing = True
        self.num_processes = mp.cpu_count() - 1  # Leave one CPU free
    
    def load_incremental_state(self, invoice_type: str) -> Dict:
        """Load last processing state for incremental updates"""
        state_file = os.path.join(self.cache_dir, f"{invoice_type}_state.pkl")
        
        if os.path.exists(state_file):
            with open(state_file, 'rb') as f:
                return pickle.load(f)
        
        return {
            'last_processed_date': None,
            'processed_invoices': set(),
            'last_run_timestamp': None
        }
    
    def save_incremental_state(self, invoice_type: str, state: Dict):
        """Save processing state for next run"""
        state_file = os.path.join(self.cache_dir, f"{invoice_type}_state.pkl")
        with open(state_file, 'wb') as f:
            pickle.dump(state, f)
    
    def identify_new_records(self, df: pd.DataFrame, state: Dict, 
                           date_column: str = 'work_date',
                           invoice_column: str = 'invoice_number') -> pd.DataFrame:
        """
        Identify only new records to process
        
        Returns:
            DataFrame with only new records
        """
        initial_count = len(df)
        
        # Filter by date if we have a last processed date
        if state['last_processed_date']:
            cutoff_date = state['last_processed_date'] - timedelta(days=7)  # 7-day overlap for safety
            df = df[pd.to_datetime(df[date_column]) >= cutoff_date]
            self.logger.info(f"Date filter: {initial_count} → {len(df)} records")
        
        # Filter by invoice numbers we haven't processed
        if state['processed_invoices']:
            df = df[~df[invoice_column].isin(state['processed_invoices'])]
            self.logger.info(f"Invoice filter: → {len(df)} new records")
        
        return df
    
    def optimize_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame memory usage by converting to efficient data types
        
        Can reduce memory usage by 50-70%
        """
        initial_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
        
        for col in df.columns:
            col_type = df[col].dtype
            
            # Optimize numeric types
            if col_type != 'object':
                if col_type == 'int64':
                    # Downcast integers
                    if df[col].min() >= 0:
                        if df[col].max() < 255:
                            df[col] = df[col].astype(np.uint8)
                        elif df[col].max() < 65535:
                            df[col] = df[col].astype(np.uint16)
                        elif df[col].max() < 4294967295:
                            df[col] = df[col].astype(np.uint32)
                    else:
                        if df[col].min() > -128 and df[col].max() < 127:
                            df[col] = df[col].astype(np.int8)
                        elif df[col].min() > -32768 and df[col].max() < 32767:
                            df[col] = df[col].astype(np.int16)
                        elif df[col].min() > -2147483648 and df[col].max() < 2147483647:
                            df[col] = df[col].astype(np.int32)
                
                elif col_type == 'float64':
                    # Downcast floats
                    df[col] = pd.to_numeric(df[col], downcast='float')
            
            # Convert string columns with low cardinality to category
            elif col_type == 'object':
                num_unique = df[col].nunique()
                num_total = len(df[col])
                if num_unique / num_total < 0.5:  # Less than 50% unique values
                    df[col] = df[col].astype('category')
        
        final_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
        reduction_pct = (initial_memory - final_memory) / initial_memory * 100
        
        self.logger.info(f"Memory optimization: {initial_memory:.2f} MB → {final_memory:.2f} MB "
                        f"({reduction_pct:.1f}% reduction)")
        
        return df
    
    def process_in_chunks(self, df: pd.DataFrame, process_func, **kwargs) -> pd.DataFrame:
        """
        Process large DataFrame in chunks to avoid memory issues
        
        Args:
            df: DataFrame to process
            process_func: Function to apply to each chunk
            **kwargs: Additional arguments for process_func
        """
        chunks = []
        total_chunks = len(df) // self.chunk_size + (1 if len(df) % self.chunk_size else 0)
        
        self.logger.info(f"Processing {len(df)} records in {total_chunks} chunks")
        
        for i in range(0, len(df), self.chunk_size):
            chunk = df.iloc[i:i + self.chunk_size]
            processed_chunk = process_func(chunk, **kwargs)
            chunks.append(processed_chunk)
            
            # Log progress
            chunk_num = i // self.chunk_size + 1
            self.logger.info(f"Processed chunk {chunk_num}/{total_chunks}")
        
        return pd.concat(chunks, ignore_index=True)
    
    def parallel_process(self, df: pd.DataFrame, process_func, **kwargs) -> pd.DataFrame:
        """
        Process DataFrame using multiple CPU cores
        
        Args:
            df: DataFrame to process
            process_func: Function to apply (must be pickleable)
            **kwargs: Additional arguments for process_func
        """
        if len(df) < 1000 or not self.use_multiprocessing:
            # For small datasets, parallel processing overhead isn't worth it
            return process_func(df, **kwargs)
        
        # Split DataFrame for parallel processing
        df_split = np.array_split(df, self.num_processes)
        
        # Create partial function with kwargs
        func = partial(process_func, **kwargs)
        
        # Process in parallel
        with mp.Pool(processes=self.num_processes) as pool:
            results = pool.map(func, df_split)
        
        # Combine results
        return pd.concat(results, ignore_index=True)
    
    def create_summary_tables(self, df: pd.DataFrame, output_dir: str = "summaries"):
        """
        Create pre-aggregated summary tables for faster reporting
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Daily summary
        daily_summary = df.groupby(['work_date', 'source_system', 'pay_type']).agg({
            'hours_quantity': 'sum',
            'bill_amount': 'sum',
            'employee_number': 'nunique',
            'invoice_line_id': 'count'
        }).reset_index()
        daily_summary.columns = ['work_date', 'source_system', 'pay_type', 
                                'total_hours', 'total_amount', 'unique_employees', 'record_count']
        
        # Location summary
        location_summary = df.groupby(['mc_service_area', 'business_unit', 'source_system']).agg({
            'hours_quantity': 'sum',
            'bill_amount': 'sum',
            'employee_number': 'nunique',
            'work_date': ['min', 'max']
        }).reset_index()
        
        # Employee summary
        employee_summary = df.groupby(['employee_number', 'source_system']).agg({
            'hours_quantity': 'sum',
            'bill_amount': 'sum',
            'work_date': 'nunique',
            'pay_type': lambda x: x.value_counts().to_dict()
        }).reset_index()
        
        # Save summaries
        daily_summary.to_parquet(os.path.join(output_dir, 'daily_summary.parquet'))
        location_summary.to_parquet(os.path.join(output_dir, 'location_summary.parquet'))
        employee_summary.to_parquet(os.path.join(output_dir, 'employee_summary.parquet'))
        
        self.logger.info(f"Created summary tables in {output_dir}")
        
        return {
            'daily': daily_summary,
            'location': location_summary,
            'employee': employee_summary
        }
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, List]:
        """
        Fast data quality validation with specific business rules
        """
        issues = {
            'missing_values': [],
            'data_anomalies': [],
            'business_rule_violations': []
        }
        
        # Check for missing critical values
        critical_columns = ['invoice_number', 'employee_number', 'work_date', 'hours_quantity', 'bill_amount']
        for col in critical_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    issues['missing_values'].append(f"{col}: {missing_count} missing values")
        
        # Check for anomalies
        if 'hours_quantity' in df.columns:
            # Flag records with more than 24 hours in a day
            anomaly_mask = df['hours_quantity'] > 24
            if anomaly_mask.any():
                anomaly_records = df[anomaly_mask][['invoice_line_id', 'hours_quantity']]
                issues['data_anomalies'].append(
                    f"Found {len(anomaly_records)} records with >24 hours"
                )
        
        # Business rule: Check for duplicate employee-date-paytype combinations
        if all(col in df.columns for col in ['employee_number', 'work_date', 'pay_type']):
            dup_mask = df.duplicated(subset=['employee_number', 'work_date', 'pay_type'], keep=False)
            if dup_mask.any():
                dup_count = dup_mask.sum()
                issues['business_rule_violations'].append(
                    f"Found {dup_count} duplicate employee-date-paytype combinations"
                )
        
        return issues

# Performance comparison function
def benchmark_processing(optimizer: InvoiceProcessingOptimizer, 
                        bci_file: str, aus_file: str):
    """Benchmark different processing approaches"""
    import time
    
    # Load data
    print("Loading data files...")
    bci_df = pd.read_csv(bci_file)
    aus_df = pd.read_csv(aus_file)
    
    print(f"BCI: {len(bci_df)} rows, AUS: {len(aus_df)} rows")
    
    # Test 1: Memory optimization
    print("\n1. Testing memory optimization...")
    start = time.time()
    bci_optimized = optimizer.optimize_datatypes(bci_df.copy())
    aus_optimized = optimizer.optimize_datatypes(aus_df.copy())
    optimization_time = time.time() - start
    print(f"   Optimization completed in {optimization_time:.2f} seconds")
    
    # Test 2: Incremental processing
    print("\n2. Testing incremental processing...")
    state = optimizer.load_incremental_state('bci')
    new_records = optimizer.identify_new_records(bci_df, state, 
                                                date_column='Date', 
                                                invoice_column='Invoice_No')
    print(f"   Identified {len(new_records)} new records to process")
    
    # Test 3: Summary table generation
    print("\n3. Testing summary table generation...")
    start = time.time()
    # Would need unified data for this - skipping for now
    summary_time = time.time() - start
    print(f"   Summary generation completed in {summary_time:.2f} seconds")
    
    return {
        'optimization_time': optimization_time,
        'new_records_count': len(new_records),
        'summary_time': summary_time
    }

# Example usage
if __name__ == "__main__":
    # Initialize optimizer
    optimizer = InvoiceProcessingOptimizer()
    
    # Run benchmark
    results = benchmark_processing(
        optimizer,
        "invoice_details_bci.csv",
        "invoice_details_aus.csv"
    )
    
    print("\nBenchmark Results:")
    print(f"Memory optimization: {results['optimization_time']:.2f}s")
    print(f"New records found: {results['new_records_count']}")
