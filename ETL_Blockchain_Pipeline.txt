import pandas as pd
from sqlalchemy import create_engine
from web3 import Web3
import time

# Step 1: Extract - Pull data from a traditional database
def extract_data(database_url, query, chunk_size=100000):
    """
    Extract financial transactions from a traditional database in chunks.
    """
    engine = create_engine(database_url)
    chunks = pd.read_sql_query(query, engine, chunksize=chunk_size)
    return chunks

# Step 2: Transform - Clean and prepare data for blockchain
def transform_data(chunk):
    """
    Transform the data to fit blockchain requirements.
    """
    # Example transformations:
    # 1. Drop null values
    chunk = chunk.dropna()
    # 2. Convert amounts to wei (smallest unit of Ethereum)
    chunk['amount_wei'] = chunk['amount'].apply(lambda x: Web3.toWei(x, 'ether'))
    # 3. Add a timestamp
    chunk['timestamp'] = int(time.time())
    return chunk

# Step 3: Load - Push data to the blockchain
def load_data_to_blockchain(chunk, contract_address, abi, private_key, infura_url):
    """
    Load transformed data into a smart contract on the blockchain.
    """
    # Connect to Ethereum blockchain
    w3 = Web3(Web3.HTTPProvider(infura_url))
    w3.eth.default_account = w3.eth.account.privateKeyToAccount(private_key).address

    # Load the smart contract
    contract = w3.eth.contract(address=contract_address, abi=abi)

    # Iterate over rows and send transactions
    for _, row in chunk.iterrows():
        # Prepare transaction
        transaction = contract.functions.addTransaction(
            row['from_address'],
            row['to_address'],
            row['amount_wei'],
            row['timestamp']
        ).buildTransaction({
            'chainId': 1,  # Mainnet
            'gas': 200000,
            'gasPrice': w3.toWei('50', 'gwei'),
            'nonce': w3.eth.getTransactionCount(w3.eth.default_account),
        })

        # Sign and send transaction
        signed_txn = w3.eth.account.signTransaction(transaction, private_key=private_key)
        txn_hash = w3.eth.sendRawTransaction(signed_txn.rawTransaction)
        print(f"Transaction sent with hash: {txn_hash.hex()}")

# Main ETL Pipeline
def etl_pipeline(database_url, query, contract_address, abi, private_key, infura_url):
    """
    Run the ETL pipeline to extract, transform, and load financial transactions.
    """
    # Extract data in chunks
    chunks = extract_data(database_url, query)

    for chunk in chunks:
        # Transform data
        transformed_chunk = transform_data(chunk)

        # Load data to blockchain
        load_data_to_blockchain(transformed_chunk, contract_address, abi, private_key, infura_url)

# Example usage
if __name__ == "__main__":
    # Database connection
    DATABASE_URL = "postgresql://user:password@localhost:5432/financial_db"
    QUERY = "SELECT * FROM transactions WHERE status = 'pending'"

    # Blockchain connection
    CONTRACT_ADDRESS = "0xYourContractAddress"
    ABI = [...]  # ABI of your smart contract
    PRIVATE_KEY = "your_private_key"
    INFURA_URL = "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID"

    # Run the ETL pipeline
    etl_pipeline(DATABASE_URL, QUERY, CONTRACT_ADDRESS, ABI, PRIVATE_KEY, INFURA_URL)