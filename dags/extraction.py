import requests
import json
import os
from datetime import datetime, timedelta

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
        
    def fetch_symbol_data(self, interval='1h', limit=10000):
        """fetch data for specific symbol"""
        try:
            params = {
                'symbol': self.symbol,
                'interval': interval,  # 1 day interval
                'limit': limit 
            }
            self.logger.info(f"Fetching data for {self.symbol}")
            response = requests.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Successfully fetched {len(data)} records for {self.symbol}")
                return data
            else:
                raise Exception(f"API returned status code: {response.status_code}")
                    
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch data for {self.symbol}: {str(e)}")
            raise

    def save_raw_data(self, data, batch_id=1):
        """Save raw JSON data to MinIO"""
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Create object key following your desired structure
        object_key = f"date={date_str}/symbol={self.symbol}/batch_{batch_id:03d}.json"

        # Create metadata
        metadata = {
            'symbol': self.symbol,
            'extraction_time': datetime.now().isoformat(),
            'record_count': len(data),
            'batch_id': batch_id,
            'object_key': object_key,
            'bucket': BUCKET_NAME
        }
        
        # Prepare data for upload
        upload_data = {
            'metadata': metadata,
            'data': data
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
            
            self.logger.info(f"Saved {len(data)} records to MinIO: {object_key}")
            return metadata
            
        except S3Error as e:
            self.logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error saving to MinIO: {str(e)}")
            raise
