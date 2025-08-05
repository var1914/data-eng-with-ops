import json
import logging
from datetime import datetime
from io import BytesIO
import psycopg2
from psycopg2.extras import execute_batch
from minio import Minio
from minio.error import S3Error

BUCKET_NAME = 'crypto-raw-data'

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'admin',
    'secret_key': 'admin123',
    'secure': False
}

# Database Configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "varunrajput", 
    "password": "yourpassword",
    "host": "host.docker.internal",
    "port": "5432"
}

class PostgreSQLProcessor:
    """Process batch data from Minio and push it to PostgreSQL"""

    def __init__(self, symbol, batch_number):
        self.symbol = symbol
        self.batch_number = batch_number
        self.logger = logging.getLogger(f"process_{symbol}_batch_{batch_number}")
        self.minio_client = self._get_minio_client()

    def _get_minio_client(self):
        """Initialize MinIO client"""
        try:
            client = Minio(
                MINIO_CONFIG['endpoint'],
                access_key=MINIO_CONFIG['access_key'],
                secret_key=MINIO_CONFIG['secret_key'],
                secure=MINIO_CONFIG['secure']
            )
            self.logger.info(f"MinIO client initialized for {self.symbol}")
            return client
        except Exception as e:
            self.logger.error(f"Failed to initialize MinIO client: {str(e)}")
            raise

    def _get_db_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except psycopg2.Error as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise

    def _create_table_if_not_exists(self, conn):
        """
        Creates a simple table to store Binance kline data
        Fixed to handle concurrent table creation properly
        """
        cursor = conn.cursor()

        try:
            # First check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_tables 
                    WHERE schemaname = 'public' AND tablename = 'crypto_data'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                # Use IF NOT EXISTS to prevent concurrent creation issues
                create_table_query = """
                CREATE TABLE IF NOT EXISTS crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    open_time BIGINT NOT NULL,
                    close_time BIGINT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume REAL NOT NULL,
                    quote_volume REAL NOT NULL,
                    trades_count INTEGER NOT NULL,
                    buy_ratio REAL,
                    batch_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, open_time)
                )
                """
                cursor.execute(create_table_query)
                conn.commit()
                self.logger.info("Table 'crypto_data' created or already exists")
            else:
                self.logger.info("Table 'crypto_data' already exists")
                
                # Check for missing columns and add them if needed
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = 'crypto_data'
                """)
                existing_columns = {row[0] for row in cursor.fetchall()}
                
                required_columns = {
                    'batch_id': 'TEXT',
                    'buy_ratio': 'REAL'
                }
                
                for column_name, column_type in required_columns.items():
                    if column_name not in existing_columns:
                        try:
                            cursor.execute(f"""
                                ALTER TABLE crypto_data 
                                ADD COLUMN IF NOT EXISTS {column_name} {column_type}
                            """)
                            conn.commit()
                            self.logger.info(f"Added column {column_name} to crypto_data table")
                        except psycopg2.Error as e:
                            # Log but don't fail if column already exists
                            self.logger.warning(f"Could not add column {column_name}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error creating/checking table: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    def read_batch_from_minio(self):
        """Read batch data from MinIO"""
        date_str = datetime.now().strftime("%Y-%m-%d")
        object_key = f"date={date_str}/symbol={self.symbol}/batch_{self.batch_number:03d}.json"
        
        try:
            response = self.minio_client.get_object(BUCKET_NAME, object_key)
            data = json.loads(response.read().decode('utf-8'))
            
            self.logger.info(f"Read batch from MinIO: {object_key}")
            return data
            
        except S3Error as e:
            self.logger.error(f"Failed to read from MinIO: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error reading batch: {str(e)}")
            raise

    def transform_data(self, raw_data):
        """Transform raw Binance kline data for database insertion"""
        transformed_records = []
        
        for kline in raw_data['data']:
            # Binance kline format: [open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore]
            
            # Calculate buy ratio
            volume = float(kline[5])
            taker_buy_volume = float(kline[9])
            buy_ratio = round(taker_buy_volume / volume * 100, 4) if volume > 0 else 0.0
            
            record = {
                'symbol': self.symbol,
                'open_time': int(kline[0]),
                'close_time': int(kline[6]),
                'open_price': float(kline[1]),
                'high_price': float(kline[2]),
                'low_price': float(kline[3]),
                'close_price': float(kline[4]),
                'volume': float(kline[5]),
                'quote_volume': float(kline[7]),
                'trades_count': int(kline[8]),
                'buy_ratio': buy_ratio,
                'batch_id': f"{self.symbol}_{self.batch_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
            
            transformed_records.append(record)
        
        self.logger.info(f"Transformed {len(transformed_records)} records for {self.symbol}")
        return transformed_records
    
    def bulk_insert_records(self, records):
        """Bulk insert records into PostgreSQL using execute_batch"""
        insert_sql = """
        INSERT INTO crypto_data (
            symbol, open_time, close_time, open_price, high_price, low_price,
            close_price, volume, quote_volume, trades_count, buy_ratio, batch_id
        ) VALUES (
            %(symbol)s, %(open_time)s, %(close_time)s, %(open_price)s, %(high_price)s, %(low_price)s,
            %(close_price)s, %(volume)s, %(quote_volume)s, %(trades_count)s, %(buy_ratio)s, %(batch_id)s
        ) ON CONFLICT (symbol, open_time) DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            trades_count = EXCLUDED.trades_count,
            buy_ratio = EXCLUDED.buy_ratio,
            batch_id = EXCLUDED.batch_id,
            created_at = CURRENT_TIMESTAMP
        """
        
        conn = None
        try:
            conn = self._get_db_connection()
            # Create table first, before any operations
            self._create_table_if_not_exists(conn)
            
            with conn.cursor() as cursor:
                # Use execute_batch for better performance
                execute_batch(
                    cursor, 
                    insert_sql, 
                    records, 
                    page_size=1000  # Process in chunks of 1000
                )
                
                conn.commit()
                self.logger.info(f"Successfully inserted {len(records)} records for {self.symbol}")
                
                # Return insertion stats
                return {
                    'symbol': self.symbol,
                    'batch_number': self.batch_number,
                    'records_processed': len(records),
                    'status': 'success',
                    'processing_time': datetime.now().isoformat()
                }
                
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error during bulk insert: {str(e)}")
            raise
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Unexpected error during bulk insert: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def cleanup_processed_batch(self):
        """Remove processed batch file from MinIO"""
        date_str = datetime.now().strftime("%Y-%m-%d")
        object_key = f"date={date_str}/symbol={self.symbol}/batch_{self.batch_number:03d}.json"
        
        try:
            self.minio_client.remove_object(BUCKET_NAME, object_key)
            self.logger.info(f"Cleaned up batch file: {object_key}")
        except S3Error as e:
            self.logger.warning(f"Failed to cleanup batch file {object_key}: {str(e)}")
            # Don't raise - cleanup failure shouldn't fail the task

    def process_batch(self):
        """Main method to process a batch from MinIO to PostgreSQL"""
        try:
            # Read data from MinIO
            raw_data = self.read_batch_from_minio()
            
            # Transform data
            transformed_records = self.transform_data(raw_data)
            
            # Insert into PostgreSQL
            result = self.bulk_insert_records(transformed_records)
            
            # Cleanup MinIO (optional - comment out if you want to keep files)
            # self.cleanup_processed_batch()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Batch processing failed for {self.symbol} batch {self.batch_number}: {str(e)}")
            return {
                'symbol': self.symbol,
                'batch_number': self.batch_number,
                'status': 'failed',
                'error': str(e),
                'processing_time': datetime.now().isoformat()
            }

def process_symbol_batches(symbol, **context):
    """Process all batches for a specific symbol"""
    task_instance = context['task_instance']
    
    # Get discovered batches from upstream task
    all_discovered_batches = task_instance.xcom_pull(task_ids='processing_group.discover_batches')
    
    if not all_discovered_batches:
        logging.warning(f"No batches discovered for processing")
        return {
            'symbol': symbol,
            'total_batches': 0,
            'processed_batches': 0,
            'failed_batches': 0,
            'batch_results': []
        }
    
    # Filter batches for this specific symbol
    symbol_batches = [batch for batch in all_discovered_batches if batch['symbol'] == symbol]
    
    if not symbol_batches:
        logging.warning(f"No batches found for symbol: {symbol}")
        return {
            'symbol': symbol,
            'total_batches': 0,
            'processed_batches': 0,
            'failed_batches': 0,
            'batch_results': []
        }
    
    logging.info(f"Processing {len(symbol_batches)} batches for symbol: {symbol}")
    
    all_results = []
    successful_count = 0
    failed_count = 0
    total_records = 0
    
    # Sort batches by batch number to process in order
    symbol_batches.sort(key=lambda x: x['batch_number'])
    
    for batch_info in symbol_batches:
        batch_number = batch_info['batch_number']
        
        try:
            logging.info(f"Processing {symbol} batch {batch_number}")
            
            # Process individual batch
            result = process_symbol_batch(symbol, batch_number, **context)
            
            if result['status'] == 'success':
                successful_count += 1
                total_records += result.get('records_processed', 0)
                logging.info(f"Successfully processed: {symbol} batch {batch_number}")
            else:
                failed_count += 1
                logging.error(f"Failed to process: {symbol} batch {batch_number}")
            
            all_results.append(result)
            
        except Exception as e:
            failed_count += 1
            error_result = {
                'symbol': symbol,
                'batch_number': batch_number,
                'status': 'failed',
                'error': str(e),
                'processing_time': datetime.now().isoformat()
            }
            all_results.append(error_result)
            logging.error(f"Exception processing {symbol} batch {batch_number}: {str(e)}")
    
    # Summary for this symbol
    symbol_summary = {
        'symbol': symbol,
        'total_batches': len(symbol_batches),
        'processed_batches': successful_count,
        'failed_batches': failed_count,
        'total_records_processed': total_records,
        'batch_results': all_results,
        'processing_time': datetime.now().isoformat()
    }
    
    logging.info(f"Symbol {symbol} processing complete: {successful_count}/{len(symbol_batches)} batches successful, {total_records} total records")
    
    return symbol_summary

def process_symbol_batch(symbol, batch_number, **context):
    """Airflow task function to process a single batch"""
    processor = PostgreSQLProcessor(symbol, batch_number)
    result = processor.process_batch()
    
    # Log result
    if result['status'] == 'success':
        logging.info(f"Batch processing completed: {symbol} batch {batch_number}")
    else:
        logging.error(f"Batch processing failed: {symbol} batch {batch_number}")
        raise Exception(f"Processing failed: {result.get('error', 'Unknown error')}")
    
    return result

def discover_batches_for_processing(**context):
    """Discover all batch files that need processing"""
    minio_client = Minio(
        MINIO_CONFIG['endpoint'],
        access_key=MINIO_CONFIG['access_key'],
        secret_key=MINIO_CONFIG['secret_key'],
        secure=MINIO_CONFIG['secure']
    )
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    batches_to_process = []
    
    try:
        # List all objects for today
        objects = minio_client.list_objects(
            BUCKET_NAME, 
            prefix=f"date={date_str}/",
            recursive=True
        )
        
        for obj in objects:
            if obj.object_name.endswith('.json') and 'batch_' in obj.object_name:
                # Extract symbol and batch number from object key
                # Format: date=2025-08-02/symbol=BTCUSDT/batch_001.json
                parts = obj.object_name.split('/')
                if len(parts) >= 3:
                    symbol_part = parts[1]  # symbol=BTCUSDT
                    batch_part = parts[2]   # batch_001.json
                    
                    symbol = symbol_part.split('=')[1]
                    batch_number = int(batch_part.split('_')[1].split('.')[0])
                    
                    batches_to_process.append({
                        'symbol': symbol,
                        'batch_number': batch_number,
                        'object_key': obj.object_name
                    })
        
        logging.info(f"Discovered {len(batches_to_process)} batches to process")
        return batches_to_process
        
    except Exception as e:
        logging.error(f"Failed to discover batches: {str(e)}")
        raise

def collect_processing_results(SYMBOLS, **context):
    """Collect results from all symbol processing tasks"""
    task_instance = context['task_instance']
    
    all_symbol_results = []
    overall_stats = {
        'total_symbols': len(SYMBOLS),
        'total_batches': 0,
        'successful_batches': 0,
        'failed_batches': 0,
        'total_records_processed': 0
    }
    
    # Collect results from each symbol processing task
    for symbol in SYMBOLS:
        task_id = f'processing_group.process_symbol_{symbol.lower()}'
        symbol_result = task_instance.xcom_pull(task_ids=task_id)
        
        if symbol_result:
            all_symbol_results.append(symbol_result)
            
            # Aggregate stats
            overall_stats['total_batches'] += symbol_result.get('total_batches', 0)
            overall_stats['successful_batches'] += symbol_result.get('processed_batches', 0)
            overall_stats['failed_batches'] += symbol_result.get('failed_batches', 0)
            overall_stats['total_records_processed'] += symbol_result.get('total_records_processed', 0)
        else:
            logging.warning(f"No result found for symbol: {symbol}")
    
    # Create final summary
    final_summary = {
        'processing_time': datetime.now().isoformat(),
        'overall_stats': overall_stats,
        'symbol_results': all_symbol_results
    }
    
    logging.info(f"Overall Processing Summary: {overall_stats['successful_batches']}/{overall_stats['total_batches']} batches successful across {len(SYMBOLS)} symbols")
    
    # Save summary to MinIO (same as before)
    try:
        minio_client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=MINIO_CONFIG['secure']
        )
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_key = f"processing_summaries/processing_summary_{timestamp}.json"
        
        json_bytes = json.dumps(final_summary, indent=2).encode('utf-8')
        json_stream = BytesIO(json_bytes)
        
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=summary_key,
            data=json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )
        
        logging.info(f"Processing summary saved to MinIO: {summary_key}")
        
    except Exception as e:
        logging.error(f"Failed to save processing summary: {str(e)}")
    
    return final_summary