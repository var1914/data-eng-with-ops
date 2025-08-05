import requests
import json
import os
from datetime import datetime, timedelta
import time

import logging

from minio import Minio
from minio.error import S3Error

from io import BytesIO

BUCKET_NAME = 'crypto-raw-data'

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 'minio:9000',  # Adjust based on your Helm setup
    'access_key': 'admin',  # Change these in production!
    'secret_key': 'admin123',
    'secure': False  # Set to True if using HTTPS
}

class MinIODataExtractor:
    """ Data Extractor with MinIO Storage"""

    def __init__(self, symbol, base_url):
        self.symbol = symbol
        self.base_url = base_url
        self.logger = logging.getLogger(f"extractor_{symbol}")
        self.minio_client = self._get_minio_client()
        self._ensure_bucket_exists()
        
        # ADD THIS: Initialize session with connection management
        self.session = self._create_session()
        self.request_count = 0

    def _create_session(self):
        """Create session with connection pooling and retry logic"""
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        
        session = requests.Session()
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=5,
            pool_maxsize=10,
            pool_block=True
        )
        
        session.mount("https://", adapter)
        return session

    def _get_minio_client(self):
        try:
            client = Minio(
                MINIO_CONFIG['endpoint'],
                access_key=MINIO_CONFIG['access_key'],
                secret_key=MINIO_CONFIG['secret_key'],
                secure=MINIO_CONFIG['secure']
            )
            self.logger.info(f"MinIO Client Initialised for symbol : {self.symbol}")
            return client
        except Exception as e:
            self.logger.error(f"Failed to initialise MinIO Client: {str(e)}")
            raise

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist"""
        try:
            if not self.minio_client.bucket_exists(BUCKET_NAME):
                self.minio_client.make_bucket(BUCKET_NAME)
                self.logger.info(f"Created bucket: {BUCKET_NAME}")
            else:
                self.logger.info(f"Bucket {BUCKET_NAME} already exists")
        except S3Error as e:
            self.logger.error(f"Error with bucket operations: {str(e)}")
            raise

    def fetch_symbol_data_in_batches(self, interval='1h', total_limit=100000, batch_size=1000):
        """Generator that yields batches one at a time - memory efficient"""
        try:
            num_batches = (total_limit + batch_size - 1) // batch_size  # Ceiling division: rounds up to ensure all items fit in batchesRetryClaude can make mistakes. Please double-check responses.
            self.logger.info(f"Fetching {total_limit} records for {self.symbol} in {num_batches} batches")
            
            records_fetched = 0
            last_timestamp = None
            
            for batch_num in range(num_batches):
                current_batch_limit = min(batch_size, total_limit - records_fetched)
                
                if current_batch_limit <= 0:
                    break
                    
                params = {
                    'symbol': self.symbol,
                    'interval': interval,
                    'limit': current_batch_limit
                }
                
                # Use last timestamp for pagination
                if last_timestamp:
                    params['endTime'] = last_timestamp - 1
                
                # Rate limiting for heavy processing
                if self.request_count % 100 == 0 and self.request_count > 0:
                    time.sleep(1)
                    self.logger.info(f"Rate limiting pause after {self.request_count} requests")

                self.request_count += 1

                try:
                    response = self.session.get(self.base_url, params=params, timeout=(10, 30))
                except Exception as e:
                    # Handle connection issues
                    if "Failed to resolve" in str(e) or "Max retries exceeded" in str(e):
                        self.logger.warning(f"Connection issue detected, resetting session: {e}")
                        self.session.close()
                        self.session = self._create_session()
                        time.sleep(2)
                        # Retry once with new session
                        response = self.session.get(self.base_url, params=params, timeout=(10, 30))
                    else:
                        raise
                
                if response.status_code == 200:
                    batch_data = response.json()
                    
                    if not batch_data:
                        break
                        
                    # Update last timestamp for next iteration
                    last_timestamp = min(int(record[0]) for record in batch_data)
                    records_fetched += len(batch_data)
                    
                    self.logger.info(f"Batch {batch_num + 1}: Fetched {len(batch_data)} records")
                    
                    # Yield the batch data
                    yield batch_data
                    
                    if len(batch_data) < current_batch_limit:
                        break
                else:
                    raise Exception(f"API returned status code: {response.status_code}")
                    
        except Exception as e:
            self.logger.error(f"Failed to fetch data for {self.symbol}: {str(e)}")
            raise

    def save_raw_data(self, batch_data, batch_number, total_batches=None):
        """Save raw JSON data to MinIO with proper batch identification"""
        
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create more descriptive batch_id
        if total_batches:
            batch_id = f"{self.symbol}_{timestamp}_{batch_number:03d}_of_{total_batches:03d}"
        else:
            batch_id = f"{self.symbol}_{timestamp}_{batch_number:03d}"
        
        # Create object key following your desired structure
        object_key = f"date={date_str}/symbol={self.symbol}/batch_{batch_number:03d}.json"

        # Create metadata
        metadata = {
            'symbol': self.symbol,
            'extraction_time': datetime.now().isoformat(),
            'record_count': len(batch_data),
            'batch_id': batch_id,
            'batch_number': batch_number,
            'total_batches': total_batches,
            'object_key': object_key,
            'bucket': BUCKET_NAME
        }
        
        # Prepare data for upload
        upload_data = {
            'metadata': metadata,
            'data': batch_data
        }

        try:
            # Convert to JSON bytes
            json_bytes = json.dumps(upload_data, indent=2).encode('utf-8')
            json_stream = BytesIO(json_bytes)
            
            # Upload to MinIO
            self.minio_client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=object_key,
                data=json_stream,
                length=len(json_bytes),
                content_type='application/json'
            )
            
            batch_info = f"batch {batch_number}"
            if total_batches:
                batch_info += f" of {total_batches}"
                
            self.logger.info(f"Saved {len(batch_data)} records to MinIO: {object_key} ({batch_info})")
            return metadata
            
        except S3Error as e:
            self.logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error saving to MinIO: {str(e)}")
            raise

    def __del__(self):
        """Cleanup session when object is destroyed"""
        if hasattr(self, 'session'):
            self.session.close()