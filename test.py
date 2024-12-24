from multiprocessing import Pool, cpu_count
from bip_utils import (
    Bip39MnemonicGenerator,
    Bip39SeedGenerator,
    Bip39Languages,
    Bip44,
    Bip44Coins,
    Bip44Changes,
)
from eth_utils import to_checksum_address
from pymongo import MongoClient

# MongoDB configuration (Railway MongoDB URL)
MONGO_URI = "your_mongo_connection_string"
DATABASE_NAME = "seed_db"
COLLECTION_NAME = "seed_addresses"

# Batch size for database inserts
BATCH_SIZE = 500000

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def generate_valid_address():
    """
    Generate a valid mnemonic and corresponding Ethereum address.
    Returns:
        tuple: (mnemonic, Ethereum address)
    """
    lang = Bip39Languages.ENGLISH
    mnemonic = Bip39MnemonicGenerator(lang).FromWordsNumber(12)  # Generate 12-word mnemonic
    seed_bytes = Bip39SeedGenerator(mnemonic).Generate()  # Generate seed from mnemonic

    bip44_mst = Bip44.FromSeed(seed_bytes, Bip44Coins.ETHEREUM)
    bip44_acc = bip44_mst.Purpose().Coin().Account(0)  # m/44'/60'/0'
    bip44_change = bip44_acc.Change(Bip44Changes.CHAIN_EXT)  # m/44'/60'/0'/0
    bip44_addr = bip44_change.AddressIndex(0)  # m/44'/60'/0'/0/0

    return mnemonic, to_checksum_address(bip44_addr.PublicKey().ToAddress())

def worker(_):
    """
    Worker function for multiprocessing. Generates a mnemonic and address.
    Returns:
        tuple: (mnemonic, Ethereum address)
    """
    return generate_valid_address()

def save_to_db(batch):
    """
    Save a batch of seed phrases and addresses to MongoDB.
    Args:
        batch (list): List of (mnemonic, address) tuples.
    """
    try:
        # Convert batch to documents for MongoDB
        documents = [{"mnemonic": mnemonic, "address": address} for mnemonic, address in batch]
        collection.insert_many(documents)
        print(f"Saved {len(batch)} records to the database.")
    except Exception as e:
        print(f"Error saving to database: {e}")

if __name__ == "__main__":
    total_saved = 0

    while True:
        batch = []

        # Use multiprocessing to parallelize address generation
        with Pool(cpu_count()) as pool:
            for result in pool.imap(worker, range(BATCH_SIZE)):
                batch.append(result)

                # When batch size is reached, save to database
                if len(batch) >= BATCH_SIZE:
                    save_to_db(batch)
                    total_saved += len(batch)
                    batch.clear()  # Clear the batch to free memory

        # Save any remaining records in the batch
        if batch:
            save_to_db(batch)
            total_saved += len(batch)

        print(f"Total records saved so far: {total_saved}")
