import pymysql
import logging
from datetime import datetime
from hfc.fabric import Client

# Configuration for Boundless Pay's MySQL database
MYSQL_DB_CONFIG = {
    'host': 'C:\Users\Uche\Documents\BPay\BPayDatabase.csv', # Path to Boundless Pay's MySQL database
    'user': 'bp_mysql_user',                                # MySQL username
    'password': 'bp_mysql_password',                        # MySQL password
    'db': 'boundless_pay_db',                               # MySQL database name
    'port': 3306
}

# Configuration for Hyperledger Fabric
HYPERLEDGER_CONFIG = {
    'network_config': '/path/to/network/config.yaml',  # Path to Hyperledger network config
    'channel_name': 'mychannel',                      # Channel name
    'chaincode_name': 'transactions_cc',              # Chaincode name
    'org_name': 'Org1',                               # Organization name
    'user_name': 'Admin'                              # User name
}

# Table to process (e.g., transactions table)
SOURCE_TABLE = 'transactions'

# Batch size for fetching and processing data
BATCH_SIZE = 1000

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='boundless_pay_blockchain_pipeline.log'
)
logger = logging.getLogger(__name__)

def extract_data():
    """Extract data from the MySQL database in batches."""
    try:
        conn = pymysql.connect(**MYSQL_DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
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
            # 1. Convert amount to float (if stored as string in source)
            if 'amount' in row:
                row['amount'] = float(row['amount'])
            
            # 2. Add a new column for transaction status based on amount
            if 'amount' in row:
                row['status'] = 'high' if row['amount'] > 1000 else 'low'
            
            # 3. Convert transaction_date to ISO format for blockchain
            if 'transaction_date' in row:
                row['transaction_date'] = row['transaction_date'].isoformat()
            
            transformed_rows.append(row)
        except Exception as e:
            logger.error(f"Error transforming row: {row}. Error: {e}")
    return transformed_rows

def load_data_to_blockchain(rows):
    """Load transformed data into the Hyperledger Fabric blockchain."""
    try:
        # Initialize Hyperledger Fabric client
        c = Client(net_profile=HYPERLEDGER_CONFIG['network_config'])
        c.new_channel(HYPERLEDGER_CONFIG['channel_name'])

        # Invoke chaincode to add transactions
        for row in rows:
            args = [str(row['id']), row['transaction_date'], str(row['amount']), row['status']]
            response = c.chaincode_invoke(
                requestor=HYPERLEDGER_CONFIG['user_name'],
                channel_name=HYPERLEDGER_CONFIG['channel_name'],
                peers=['peer0.org1.example.com'],
                args=args,
                cc_name=HYPERLEDGER_CONFIG['chaincode_name'],
                fcn='createTransaction'  # Chaincode function name
            )
            logger.info(f"Transaction {row['id']} added to blockchain. Response: {response}")
    except Exception as e:
        logger.error(f"Error loading data into blockchain: {e}")
        raise

def run_pipeline():
    """Run the data pipeline: extract, transform, and load data."""
    logger.info("Starting blockchain data pipeline...")
    try:
        for batch in extract_data():
            transformed_batch = transform_data(batch)
            load_data_to_blockchain(transformed_batch)
            logger.info(f"Processed {len(transformed_batch)} rows.")
        logger.info("Blockchain data pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Blockchain data pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
