import pymysql
import psycopg2
import logging
from pymysql.cursors import DictCursor
from psycopg2.extras import DictCursor as PgDictCursor
from datetime import datetime

# Configuration for Boundless Pay's source (MySQL) and target (PostgreSQL) databases
SOURCE_DB_CONFIG = {
    'host': 'C:\Users\Uche\Documents\BPay\MSQLDatabase\BPayCustomerDatabase.csv',
    'user': 'bp_mysql_user',
    'password': 'bp_mysql_password',
    'db': 'boundless_pay_customers',
    'port': 3306
}

TARGET_DB_CONFIG = {
    'host': 'C:\Users\Uche\Documents\BPay\PSGDatabase\BPayCustomerDatabaseClean.csv,
    'user': 'bp_pg_user',
    'password': 'bp_pg_password',
    'dbname': 'boundless_pay_customer_analytics',
    'port': 5432
}

# Table to process (e.g., customers table)
SOURCE_TABLE = 'customers'
TARGET_TABLE = 'customers_cleaned'

# Batch size for fetching and processing data
BATCH_SIZE = 1000

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='boundless_pay_customer_pipeline.log'
)
logger = logging.getLogger(__name__)

def extract_data():
    """Extract data from the MySQL database in batches."""
    try:
        conn = pymysql.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor(DictCursor)
        cursor.execute(f"SELECT * FROM {SOURCE_TABLE}")
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            yield rows
    except Exception as e:
        logger.error(f"Error extracting data from MySQL: {e}")
        raise
    finally:
        if conn:
            conn.close()

def transform_data(rows):
    """Transform data by cleaning, converting types, and applying business logic."""
    transformed_rows = []
    for row in rows:
        try:
            # Example transformations:
            # 1. Convert date_of_birth to ISO format
            if 'date_of_birth' in row and row['date_of_birth']:
                row['date_of_birth'] = row['date_of_birth'].strftime('%Y-%m-%d')
            
            # 2. Convert phone_number to a standardized format
            if 'phone_number' in row and row['phone_number']:
                row['phone_number'] = row['phone_number'].replace('-', '').replace(' ', '')
            
            # 3. Add a new column for customer segment based on age
            if 'date_of_birth' in row and row['date_of_birth']:
                birth_year = int(row['date_of_birth'][:4])
                current_year = datetime.now().year
                age = current_year - birth_year
                if age < 30:
                    row['segment'] = 'young'
                elif 30 <= age < 50:
                    row['segment'] = 'middle-aged'
                else:
                    row['segment'] = 'senior'
            
            transformed_rows.append(row)
        except Exception as e:
            logger.error(f"Error transforming row: {row}. Error: {e}")
    return transformed_rows

def load_data(rows):
    """Load transformed data into the target PostgreSQL database."""
    try:
        conn = psycopg2.connect(**TARGET_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=PgDictCursor)
        for row in rows:
            columns = ', '.join(row.keys())
            placeholders = ', '.join(['%s'] * len(row))
            sql = f"INSERT INTO {TARGET_TABLE} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(row.values()))
        conn.commit()
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise
    finally:
        if conn:
            conn.close()

def run_pipeline():
    """Run the data pipeline: extract, transform, and load data."""
    logger.info("Starting customer data pipeline...")
    try:
        for batch in extract_data():
            transformed_batch = transform_data(batch)
            load_data(transformed_batch)
            logger.info(f"Processed {len(transformed_batch)} rows.")
        logger.info("Customer data pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Customer data pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
