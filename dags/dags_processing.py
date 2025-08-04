from pre_checks import PreChecks
from extraction import MinIODataExtractor
from push import discover_batches_for_processing, process_symbol_batch, collect_processing_results

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import json

import logging

from minio import Minio
from minio.error import S3Error

from io import BytesIO

BUCKET_NAME = 'crypto-raw-data'


# Keep your existing imports and constants
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'XRPUSDT', 'DOTUSDT', 'AVAXUSDT', 'MATICUSDT', 'LINKUSDT']
BASE_URL = "https://api.binance.com/api/v3/klines"

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 'minio:9000',  # Adjust based on your Helm setup
    'access_key': 'admin',  # Change these in production!
    'secret_key': 'admin123',
    'secure': False  # Set to True if using HTTPS
}

def execute_planning_phase(**context):
    # Initialize your existing PreChecks class
    checker = PreChecks(
        base_url=BASE_URL,
        headers={"Content-Type": "application/json"}
    )
    
    # Your existing methods work as-is:
    results = checker.run_all_checks()

    return results

def extract_symbol_data(symbol, **context):
    """Main extraction function for a single symbol"""
    extractor = MinIODataExtractor(
        symbol,
        base_url=BASE_URL
    )
    
    try:
        # Fetch data
        raw_data = extractor.fetch_symbol_data()
        
        # Save data to MinIO
        metadata = extractor.save_raw_data(raw_data)
        
        # Return metadata for downstream tasks
        return {
            'symbol': symbol,
            'status': 'success',
            'metadata': metadata
        }
        
    except Exception as e:
        logging.error(f"Extraction failed for {symbol}: {str(e)}")
        raise {
            'symbol': symbol,
            'status': 'failed',
            'error': str(e)
        }
        

def collect_extraction_results(**context):
    """Collect results from all symbol extractions and save summary to MinIO"""
    task_instance = context['task_instance']
    
    results = []
    for symbol in SYMBOLS:
        # Get result from upstream task
        # FIX: Include the TaskGroup prefix in the task_id
        task_id = f'extraction_group.extract_{symbol.lower()}'
        result = task_instance.xcom_pull(task_ids=task_id)
        
        # Handle case where result might be None
        if result is None:
            logging.warning(f"No XCom data found for {task_id}")
            result = {
                'symbol': symbol,
                'status': 'failed',
                'error': 'No XCom data found'
            }

        results.append(result)
        
    successful_extractions = [r for r in results if r['status'] == 'success']
    failed_extractions = [r for r in results if r['status'] == 'failed']
    
    summary = {
        'total_symbols': len(SYMBOLS),
        'successful': len(successful_extractions),
        'failed': len(failed_extractions),
        'extraction_time': datetime.now().isoformat(),
        'results': results
    }
    
    logging.info(f"Extraction Summary: {summary['successful']}/{summary['total_symbols']} successful")
    
    # Save summary to MinIO
    try:
        minio_client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=MINIO_CONFIG['secure']
        )
        
        # Create summary object key
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_key = f"extraction_summaries/summary_{timestamp}.json"
        
        # Upload summary
        json_bytes = json.dumps(summary, indent=2).encode('utf-8')
        json_stream = BytesIO(json_bytes)
        
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=summary_key,
            data=json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )
        
        logging.info(f"Extraction summary saved to MinIO: {summary_key}")
        
    except Exception as e:
        logging.error(f"Failed to save summary to MinIO: {str(e)}")
        raise
        # Don't fail the task if summary save fails
        
    return summary

# Your existing default_args
default_args = {
    'owner': 'varunrajput',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'crypto_data_planning',
  description='Complete crypto data pipeline',
  default_args=default_args,
  schedule='@hourly',
  catchup=False,
) 

planning_task = PythonOperator(
    task_id = "etl_prechecks",
    python_callable=execute_planning_phase,
    dag=dag,
  )

# Stage 2: Data Extraction
with TaskGroup("extraction_group", dag=dag) as extraction_group:
    extraction_tasks = []
    
    for symbol in SYMBOLS:
        task = PythonOperator(
            task_id=f'extract_{symbol.lower()}',
            python_callable=extract_symbol_data,
            op_args=[symbol],
            dag=dag,
        )
        extraction_tasks.append(task)
    
    # Summary task for extractions
    extraction_summary = PythonOperator(
        task_id='collect_extraction_results',
        python_callable=collect_extraction_results,
        dag=dag,
    )
    
    # Set extraction dependencies
    extraction_tasks >> extraction_summary

# Stage 3: PostgreSQL Processing
with TaskGroup("processing_group", dag=dag) as processing_group:
    
    # Discover batches to process
    discover_task = PythonOperator(
        task_id='discover_batches',
        python_callable=discover_batches_for_processing,
        dag=dag,
    )
    
    # This will be dynamically populated based on discovered batches
    # For now, create processing tasks for each symbol (assuming 1 batch per symbol)
    processing_tasks = []
    
    for symbol in SYMBOLS:
        # Create processing task for each symbol's batches
        # In a more advanced setup, you'd dynamically create these based on discovery
        task = PythonOperator(
            task_id=f'process_batch_{symbol.lower()}_1',
            python_callable=process_symbol_batch,
            op_args=[symbol, 1],  # symbol and batch_number
            dag=dag,
        )
        processing_tasks.append(task)
    
    # Collect processing results
    processing_summary = PythonOperator(
        task_id='collect_processing_results',
        python_callable=collect_processing_results,
        dag=dag,
    )
    
    # Set processing dependencies
    discover_task >> processing_tasks >> processing_summary

# Set overall pipeline dependencies
planning_task >> extraction_group >> processing_group