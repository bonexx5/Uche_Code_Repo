import psycopg2
import pymysql
import logging
from psycopg2.extras import DictCursor
from pymysql.cursors import DictCursor as MySQLDictCursor
from datetime import datetime

# Configuration for Boundless Pay's source (PostgreSQL) and target (MySQL) databases
SOURCE_DB_CONFIG = {
    'dbname': 'BPay_Database',
    'user': 'bp_source_user',
    'password': 'bp_source_password',
    'host': 'C:\Users\Uche\Documents\BPay\PSGDatabase\BPay_Database.csv',
    'port': 5432
}

TARGET_DB_CONFIG = {
    'dbname': 'BPay_Database_Migrated',
    'user': 'bp_target_user',
    'password': 'bp_target_password',
    'host': 'C:\Users\Uche\Documents\BPay\MSQLDatabase\BPay_Database_Migrated.csv',
    'port': 3306
}

# Table to migrate (Transactions table)
TABLE_NAME = 'transactions'

# Batch size for fetching and inserting data
BATCH_SIZE = 5000

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='boundless_pay_migration.log'
)
logger = logging.getLogger(__name__)

def validate_schemas():
    """Validate that the source and target tables have compatible schemas."""
    try:
        # Connect to source (PostgreSQL)
        source_conn = psycopg2.connect(**SOURCE_DB_CONFIG)
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{TABLE_NAME}'
        """)
        source_schema = {row[0]: row[1] for row in source_cursor.fetchall()}

        # Connect to target (MySQL)
        target_conn = pymysql.connect(**TARGET_DB_CONFIG)
        target_cursor = target_conn.cursor()
        target_cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{TABLE_NAME}'
        """)
        target_schema = {row[0]: row[1] for row in target_cursor.fetchall()}

        # Compare schemas
        if source_schema.keys() != target_schema.keys():
            raise ValueError("Source and target table columns do not match.")
        logger.info("Schema validation successful.")
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        raise
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()

def transform_data(row):
    """Transform data types if necessary (e.g., PostgreSQL timestamps to MySQL datetime)."""
    transformed_row = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            transformed_row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            transformed_row[key] = value
    return transformed_row

def fetch_data_from_source():
    """Fetch data from the source PostgreSQL database in batches."""
    try:
        conn = psycopg2.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute(f"SELECT * FROM {TABLE_NAME}")
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            yield rows
    except Exception as e:
        logger.error(f"Error fetching data from source: {e}")
        raise
    finally:
        if conn:
            conn.close()

def insert_data_into_target(rows):
    """Insert data into the target MySQL database."""
    try:
        conn = pymysql.connect(**TARGET_DB_CONFIG)
        cursor = conn.cursor(MySQLDictCursor)
        for row in rows:
            transformed_row = transform_data(row)
            columns = ', '.join(transformed_row.keys())
            placeholders = ', '.join(['%s'] * len(transformed_row))
            sql = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(transformed_row.values()))
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting data into target: {e}")
        raise
    finally:
        if conn:
            conn.close()

def migrate_data():
    """Migrate data from source to target database."""
    logger.info("Starting data migration...")
    try:
        validate_schemas()
        for batch in fetch_data_from_source():
            insert_data_into_target(batch)
            logger.info(f"Inserted {len(batch)} rows into target.")
        logger.info("Data migration completed successfully.")
    except Exception as e:
        logger.error(f"Data migration failed: {e}")
        raise

if __name__ == "__main__":
    migrate_data()
