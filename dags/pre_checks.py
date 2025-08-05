import requests
import psutil
import psycopg2
from psycopg2 import pool
import threading

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import requests.auth
import time

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'XRPUSDT', 'DOTUSDT', 'AVAXUSDT', 'MATICUSDT', 'LINKUSDT']
BASE_URL = "https://api.binance.com/api/v3/klines"

class DatabasePool:
  """Singelton Connection Pool Manager"""
  _instance = None
  _lock = threading.Lock()

  def __new__(cls):
    if cls._instance is None:
      with cls._lock:
        if cls._instance is None:
          cls._instance = super(DatabasePool, cls).__new__(cls)
          cls._instance.pool = None
    return cls._instance

  def create_pool(self, db_config, min_conn=2, max_conn=10):
    if self.pool is None:
      try:
        self.pool = psycopg2.pool.ThreadedConnectionPool(
          min_conn, max_conn, **db_config
        )
      except psycopg2.Error as e:
        raise f"Failed to created connection pool: {str(e)}"
    
  def get_connection(self):
      """Get connection from pool"""
      if self.pool:
          return self.pool.getconn()
      return None
  
  def put_connection(self, conn):
      """Return connection to pool"""
      if self.pool and conn:
          self.pool.putconn(conn)
  
  def close_pool(self):
      """Close all connections in pool"""
      if self.pool:
          self.pool.closeall()
          self.pool = None
          return "Connection pool closed"
      return "No pool to close"
  
  def get_pool_status(self):
      """Get pool connection stats"""
      if self.pool:
          return {
              "pool_exists": True,
              "pool_type": "ThreadedConnectionPool"
          }
      return {"pool_exists": False}

class PreChecks:
  def __init__(self, base_url, headers = None, params = None, db_config = None):
    self.base_url = base_url
    self.headers = headers or {}
    self.params = params or {}
    self.db_config = db_config or {
      "dbname": "postgres",
      "user": "varunrajput", 
      "password": "yourpassword",
      "host": "host.docker.internal",
      "port": "5432"
    }
    self.db_pool = DatabasePool()

  def get_api_status(self):
    
    try:
      response = requests.get(
        self.base_url, 
        headers = self.headers, 
        params = self.params,
        timeout = 10
      )

      if response.status_code == 200:
        return f"API {self.base_url} is healthy with status code {response.status_code}"
      else:
        return f"API {self.base_url} return status {response.status_code}"
      
    except requests.exceptions.RequestException as e:
      raise f"API {self.base_url} Failed with {str(e)}"

  def get_system_resources(self):
    try:
      # Memory info
      memory = psutil.virtual_memory()
      available_ram_gb = memory.available / (1024**3)
      total_ram_gb = memory.total / (1024**3)
      
      # CPU info
      cpu_cores = psutil.cpu_count(logical=False)
      logical_cores = psutil.cpu_count(logical=True)
      cpu_usage = psutil.cpu_percent(interval=1)

      return {
          "available_ram_gb": round(available_ram_gb, 2),
          "total_ram_gb": round(total_ram_gb, 2),
          "ram_usage_percent": memory.percent,
          "physical_cpu_cores": cpu_cores,
          "logical_cpu_cores": logical_cores,
          "cpu_usage_percent": cpu_usage
      }
    
    except Exception as e:
      return f"Failed to get system resources: {str(e)}"

  def initialize_db_pool(self, min_conn=2, max_conn=10):
      """Initialize the global database connection pool"""
      return self.db_pool.create_pool(self.db_config, min_conn, max_conn)
  
  def get_db_conn_stats(self):
    try:
      conn = psycopg2.connect(**self.db_config)
      conn.close()
      return f"Connected to database successfully" 
    except psycopg2.Error as e:
      raise f"Database connection failed: {str(e)}"
    except Exception as e:
      raise f"Unexpected Database Error : {str(e)}"
    
  def get_pool_status(self):
      """Get connection pool status"""
      return self.db_pool.get_pool_status()

  def run_all_checks(self):
      """Run all health checks and return a summary"""
      results = {
          "api_status": self.get_api_status(),
          "system_resources": self.get_system_resources(),
          "database_status": self.get_db_conn_stats(),
          "pool_status": self.get_pool_status(),
          # "connection_pool_test": self.test_connection_pool()
      }
      return results 
