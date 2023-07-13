import json
import sqlite3
import requests
import time
from datetime import datetime

# Replace the following values with your Novo node's RPC settings
NODE_URL = "http://127.0.0.1:8332"
RPC_USER = "NovoDockerUser"
RPC_PASSWORD = "NovoDockerPassword"

def rpc_request(method, params):
    headers = {"content-type": "text/plain"}
    rpc_data = {
        "jsonrpc": "1.0",
        "id": "curltest",
        "method": method,
        "params": params
    }

    response = requests.post(NODE_URL, headers=headers, data=json.dumps(rpc_data), auth=(RPC_USER, RPC_PASSWORD))
    return response.json()["result"]

def create_database():
    conn = sqlite3.connect("novo_blocks.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blocks (
            hash TEXT PRIMARY KEY,
            confirmations INTEGER,
            size INTEGER,
            height INTEGER,
            version INTEGER,
            versionHex TEXT,
            merkleroot TEXT,
            time TEXT,
            mediantime INTEGER,
            nonce INTEGER,
            bits TEXT,
            difficulty REAL,
            chainwork TEXT,
            previousblockhash TEXT,
            nextblockhash TEXT,
            last_synced_height INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            hex TEXT,
            txid TEXT PRIMARY KEY,
            hash TEXT,
            size INTEGER,
            version INTEGER,
            locktime INTEGER,
            vin TEXT,
            vout TEXT,
            blockhash TEXT,
            blockheight INTEGER,
            confirmations INTEGER,
            time TEXT,
            blocktime TEXT,
            FOREIGN KEY(blockhash) REFERENCES blocks(hash)
        )
    """)

    return conn

import json
import sqlite3
import requests

# Replace the following values with your Novo node's RPC settings
NODE_URL = "http://127.0.0.1:8332"
RPC_USER = "NovoDockerUser"
RPC_PASSWORD = "NovoDockerPassword"

def rpc_request(method, params):
    headers = {"content-type": "text/plain"}
    rpc_data = {
        "jsonrpc": "1.0",
        "id": "curltest",
        "method": method,
        "params": params
    }

    response = requests.post(NODE_URL, headers=headers, data=json.dumps(rpc_data), auth=(RPC_USER, RPC_PASSWORD))
    print(response.json())
    return response.json()["result"]

    # Convert the UNIX timestamps to datetime strings before saving them to the database
def save_block_data(conn, block_data):
    cursor = conn.cursor()

    # Convert UNIX timestamps to datetime strings
    block_time = datetime.fromtimestamp(block_data["time"]).strftime('%Y-%m-%d %H:%M:%S')

    # Save block data into the 'blocks' table
    cursor.execute("""
        INSERT OR REPLACE INTO blocks (
            hash, confirmations, size, height, version, versionHex,
            merkleroot, time, mediantime, nonce, bits, difficulty,
            chainwork, previousblockhash, nextblockhash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        block_data["hash"],
        block_data["confirmations"],
        block_data["size"],
        block_data["height"],
        block_data["version"],
        block_data["versionHex"],
        block_data["merkleroot"],
        block_time,
        block_data["mediantime"],
        block_data["nonce"],
        block_data["bits"],
        block_data["difficulty"],
        block_data["chainwork"],
        block_data.get("previousblockhash", ""),
        block_data.get("nextblockhash", "")
    ))

    # Save transaction data into the 'transactions' table
    for txid in block_data["tx"]:
        tx_data = rpc_request("getrawtransaction", [txid, True])

        # Convert UNIX timestamps to datetime strings
        tx_time = datetime.fromtimestamp(tx_data["time"]).strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
            INSERT OR REPLACE INTO transactions (
                hex, txid, hash, size, version, locktime, vin, vout,
                blockhash, blockheight,confirmations, time, blocktime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tx_data["hex"],
            tx_data["txid"],
            tx_data["hash"],
            tx_data["size"],
            tx_data["version"],
            tx_data["locktime"],
            json.dumps(tx_data["vin"]),
            json.dumps(tx_data["vout"]),
            block_data["hash"],
            block_data["height"],
            tx_data["confirmations"],
            tx_time,
            block_time
        ))

    conn.commit()

   
    # Get the last synced block height
def get_last_synced_height(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(height) FROM blocks")
    result = cursor.fetchone()
    return result[0] if result[0] else 0

# Update the last synced block height after syncing the block
def update_last_synced_height(conn, block_data):
    cursor = conn.cursor()
    cursor.execute("UPDATE blocks SET last_synced_height = ? WHERE height = ?", (block_data["height"], block_data["height"]))
    conn.commit()


def main():
    conn = create_database()
    
    while True:
        block_count = rpc_request("getblockcount", [])
        last_synced_height = get_last_synced_height(conn)

        if last_synced_height < block_count:
            print(f"Started syncing blocks from height {last_synced_height + 1} to {block_count}...")

            for block_height in range(last_synced_height + 1, block_count + 1):
                block_hash = rpc_request("getblockhash", [block_height])
                block_data = rpc_request("getblock", [block_hash])
                try:
                    print(f"Saving block {block_height} of {block_count}")
                    save_block_data(conn, block_data)
                    update_last_synced_height(conn, block_data)
                except KeyError as e:
                    print(f"Error saving block {block_height}: {e}. Block data: {block_data}")
                except Exception as e:
                    print(f"Unknown error saving block {block_height}: {e}")
            print("Sync completed.")
        else:
            print("No new blocks found. Waiting for 60 seconds before checking again.")
            time.sleep(60)

    conn.close()

if __name__ == "__main__":
    main()
