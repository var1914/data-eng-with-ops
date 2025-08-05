# Cryptocurrency Data Pipeline with Airflow

A complete ETL pipeline that extracts cryptocurrency data from Binance API, stores raw data in MinIO object storage, and processes it into PostgreSQL for analysis.
![alt text](image.png)

## Architecture Overview

```
Binance API → Airflow DAG → MinIO (Raw Storage) → PostgreSQL (Processed Data)
```

The pipeline consists of three main stages:
1. **Planning Phase**: Pre-checks and validation
2. **Data Extraction**: Fetch crypto data and store in MinIO
3. **Data Processing**: Transform and load data into PostgreSQL

## Features

- **Multi-symbol extraction**: Supports 10 major cryptocurrencies (BTC, ETH, BNB, ADA, SOL, XRP, DOT, AVAX, MATIC, LINK)
- **Scalable storage**: Uses MinIO for raw data lake storage
- **Batch processing**: Processes data in configurable batches
- **Error handling**: Comprehensive logging and error recovery
- **Data deduplication**: Handles duplicate records with UPSERT operations
- **Monitoring**: Built-in summary reports and processing statistics

## Prerequisites

- Kubernetes cluster
- Helm 3.x
- kubectl configured
- Docker (for custom image builds)

## Quick Start

### 1. Install MinIO

```bash
# Add MinIO Helm repository
helm repo add minio https://charts.min.io/
helm repo update

# Install MinIO with custom values
helm install minio minio/minio -f minio/values.yaml
```

### 2. Install Airflow

```bash
# Add Apache Airflow Helm repository
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install Airflow with custom values
helm install airflow apache-airflow/airflow -f airflow/values.yaml
```

### 3. Build and Deploy Custom Airflow Image

```bash
# Build custom Airflow image with dependencies
cd dags/
docker build -t your-registry/custom-airflow:latest .

# Push to your container registry
docker push your-registry/custom-airflow:latest
```

### 4. Deploy DAGs

The DAGs are automatically part of Dockerfiles

## Configuration

### Environment Variables

Update the following configurations in your Python files:

**MinIO Configuration** (`MINIO_CONFIG`):
```python
MINIO_CONFIG = {
    'endpoint': 'minio:9000',  # Update if different
    'access_key': 'admin',     # Change in production!
    'secret_key': 'admin123',  # Change in production!
    'secure': False            # Set to True for HTTPS
}
```

**Database Configuration** (`DB_CONFIG`):
```python
DB_CONFIG = {
    "dbname": "postgres",
    "user": "your_username",
    "password": "your_password",
    "host": "host.docker.internal",  # Update for your setup
    "port": "5432"
}
```

### Supported Cryptocurrencies

The pipeline currently supports these symbols:
- BTCUSDT (Bitcoin)
- ETHUSDT (Ethereum)
- BNBUSDT (Binance Coin)
- ADAUSDT (Cardano)
- SOLUSDT (Solana)
- XRPUSDT (Ripple)
- DOTUSDT (Polkadot)
- AVAXUSDT (Avalanche)
- MATICUSDT (Polygon)
- LINKUSDT (Chainlink)

## Pipeline Details

### Stage 1: Planning Phase
- Validates API connectivity
- Performs pre-flight checks
- Ensures system readiness

### Stage 2: Data Extraction
- Fetches kline/candlestick data from Binance API
- Stores raw JSON data in MinIO with organized structure:
  ```
  date=YYYY-MM-DD/symbol=BTCUSDT/batch_001.json
  ```
- Includes metadata and batch tracking

### Stage 3: Data Processing
- Discovers batch files in MinIO
- Transforms raw data into structured format
- Calculates additional metrics (buy ratio, etc.)
- Bulk inserts into PostgreSQL with conflict resolution
- Generates processing summaries

## Database Schema

The pipeline creates a `crypto_data` table with the following structure:

```sql
CREATE TABLE crypto_data (
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
);
```

## Monitoring and Logging

### Airflow UI
- Access the Airflow web interface to monitor DAG runs
- View task logs and execution history
- Monitor resource usage and performance

### MinIO Console
- Browse stored data files
- Monitor storage usage
- Access processing summaries

### Logs Location
- Extraction summaries: `extraction_summaries/`
- Processing summaries: `processing_summaries/`
- Individual task logs available in Airflow UI

## Customization

### Adding New Cryptocurrencies
Update the `SYMBOLS` list in `dags_processing.py`:
```python
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'YOUR_NEW_SYMBOL']
```

### Modifying Data Interval
Change the interval parameter in the extraction:
```python
# In extraction.py, fetch_symbol_data method
params = {
    'symbol': self.symbol,
    'interval': '1h',  # Change to '5m', '15m', '1d', etc.
    'limit': 10000
}
```

### Adjusting Schedule
Modify the DAG schedule in `dags_processing.py`:
```python
dag = DAG(
    'crypto_data_planning',
    schedule='@hourly',  # Change to '@daily', '0 */4 * * *', etc.
    # ...
)
```

## Troubleshooting

### Common Issues

1. **MinIO Connection Failed**
   - Verify MinIO service is running: `kubectl get pods`
   - Check endpoint configuration in `MINIO_CONFIG`
   - Ensure firewall/network policies allow connections

2. **Database Connection Failed**
   - Verify PostgreSQL is accessible from Airflow pods
   - Check database credentials and host configuration
   - Test connection manually from Airflow worker pod

3. **API Rate Limits**
   - Binance has rate limits (1200 requests per minute)
   - Consider adding delays between requests
   - Monitor API response codes in logs

4. **DAG Import Errors**
   - Check Python dependencies in custom Airflow image
   - Verify all imports are available
   - Review Airflow logs for specific error messages

### Debugging Steps

1. Check pod status:
```bash
kubectl get pods
kubectl logs <airflow-pod-name>
```

2. Access Airflow scheduler logs:
```bash
kubectl logs deployment/airflow-scheduler
```

3. Test MinIO connectivity:
```bash
kubectl port-forward svc/minio 9000:9000
# Access MinIO console at http://localhost:9000
```

## Production Considerations

### Security
- Change default MinIO credentials
- Use Kubernetes secrets for sensitive data
- Enable HTTPS/TLS for MinIO
- Implement proper RBAC

### Performance
- Consider partitioning PostgreSQL tables by date/symbol
- Implement data retention policies
- Monitor resource usage and scale as needed
- Use connection pooling for database connections

### Backup and Recovery
- Set up MinIO data replication
- Implement PostgreSQL backup strategy
- Document recovery procedures

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section above
- Review Airflow and MinIO documentation
- Open an issue in the project repository