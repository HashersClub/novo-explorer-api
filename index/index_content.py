import sqlite3
import json
import binascii
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def create_content_database():
    conn = sqlite3.connect("content.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS content (
            txid TEXT UNIQUE,
            blockheight INTEGER, 
            vout TEXT,
            time INTEGER,
            op_return TEXT,
            text TEXT,
            json TEXT,
            standard TEXT
        )
    """)

    # Add a unique constraint on the 'id' column
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_content_txid ON content (txid)")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inscriptions (
            id TEXT UNIQUE,
            number INTEGER,
            address TEXT,
            genesis_address TEXT,
            genesis_block_height INTEGER,
            genesis_block_hash TEXT,
            genesis_tx_id TEXT,
            genesis_fee TEXT,
            genesis_timestamp TEXT,
            tx_id TEXT,
            chunk_txids TEXT,
            location TEXT,
            output TEXT,
            value TEXT,
            offset TEXT,
            mime_type TEXT,
            content_type TEXT,
            content_length INTEGER,
            timestamp INTEGER,
            curse_type TEXT,
            encrypted INTEGER,
            licence TEXT,
            max_claims INTEGER,
            whitelist TEXT
        )
    """)

    # Add a unique constraint on the 'id' column
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inscriptions_id ON inscriptions (id)")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id TEXT,
            block_height INTEGER,
            block_hash TEXT,
            address TEXT,
            tx_id TEXT,
            location TEXT,
            output TEXT,
            value INTEGER,
            offset INTEGER,
            timestamp INTEGER
        )
    """)

    return conn

def unix_to_datetime(unix_timestamp):
    return datetime.utcfromtimestamp(int(unix_timestamp)).strftime('%Y-%m-%d %H:%M:%S')

def extract_op_return_hex(vout):
    vout_json = json.loads(vout)

    for entry in vout_json:
        script_asm = entry.get("scriptPubKey", {}).get("asm", "")
        if "OP_RETURN" in script_asm:
            op_return_hex = script_asm.split("OP_RETURN ")[-1]
            return op_return_hex

    return ""

def hex_to_text(hex_string):
    try:
        byte_array = bytes.fromhex(hex_string)
        text = byte_array.decode("utf-8")
        return text
    except ValueError:
        return ""

def is_valid_json(text):
    try:
        json.loads(text)
        return "Yes"
    except json.JSONDecodeError:
        return "No"

def is_standard_json(text):
    try:
        data = json.loads(text)
        if "genesis_address" in data and "genesis_fee" in data and "genesis_timestamp" in data and "mime_type" in data and "content_type" in data and "content_length" in data and "encrypted" in data and "licence" in data and "max_claims" in data and "whitelist" in data and "chunk_txids" in data:
            return "Yes"
        else:
            return "No"
    except json.JSONDecodeError:
        return "No"
        
def get_transactions_with_any_content(conn):
    cursor = conn.cursor()

    query = "SELECT txid, vout, time, blockheight FROM transactions"
    cursor.execute(query)
    transactions = cursor.fetchall()

    filtered_transactions = []
    for tx in transactions:
        txid, vout, time, blockheight = tx
        op_return_hex = extract_op_return_hex(vout)
        if op_return_hex:
            text = hex_to_text(op_return_hex)
            json_status = is_valid_json(text)
            standard_status=is_standard_json(text)
            filtered_transactions.append((txid, vout, time, blockheight, op_return_hex, text, json_status, standard_status))

    return filtered_transactions


def process_transactions(conn, transactions):
    cursor = conn.cursor()

    for tx in transactions:
        txid, vout, time, blockheight, op_return_hex, text, json_status, standard_status = tx

        cursor.execute("""
            INSERT OR IGNORE INTO content (txid, vout, time, blockheight, op_return, text, json, standard)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (txid, vout, time, blockheight, op_return_hex, text, json_status, standard_status))

    conn.commit()


def extract_json_data(text):
    try:
        data = json.loads(text)
        chunk_txids = data.get("chunk_txids", [])
        mime_type = data.get("mime_type", "")
        content_length = data.get("content_length", 0)
        content_type = data.get("content_type", "")
        genesis_address = data.get("genesis_address", "")
        genesis_timestamp = unix_to_datetime(data.get("genesis_timestamp", ""))  # Conversion here
        genesis_fee=data.get("genesis_fee", 0)
        unique_identifier = data.get("unique_identifier", "")
        encrypted = data.get("encrypted", False)
        licence = data.get("licence", "")
        max_claims = data.get("max_claims", 0)
        whitelist = data.get("whitelist", [])
        return chunk_txids, mime_type, content_length, content_type, genesis_address, genesis_timestamp, genesis_fee, unique_identifier, encrypted, licence, max_claims, whitelist
    except json.JSONDecodeError:
        return [], "", 0, "", "", "", 0, "", False, "", 0, []


def get_valid_json_entries(conn):
    cursor = conn.cursor()

    query = "SELECT txid, text, time, blockheight FROM content WHERE standard='Yes' ORDER BY time ASC"
    cursor.execute(query)
    entries = cursor.fetchall()

    valid_entries = []
    number = 1
    for entry in entries:
        txid, text, time, blockheight = entry
        chunk_txids, mime_type, content_length, content_type, genesis_address, genesis_timestamp, genesis_fee, unique_identifier, encrypted, licence, max_claims, whitelist = extract_json_data(text)
        if chunk_txids:
            valid_entries.append((number, txid, json.dumps(chunk_txids), mime_type, content_length, content_type, genesis_address, genesis_timestamp, genesis_fee, unique_identifier, encrypted, licence, max_claims, whitelist, blockheight, time))
            number += 1

    return valid_entries

def process_valid_json_entries(conn, valid_entries):
    cursor = conn.cursor()

    for entry in valid_entries:
        number, txid, chunk_txids, mime_type, content_length, content_type, genesis_address, genesis_timestamp, genesis_fee, unique_identifier, encrypted, licence, max_claims, whitelist, blockheight, time = entry

        cursor.execute("""
            INSERT OR IGNORE INTO inscriptions (number, id, address, genesis_tx_id, chunk_txids, mime_type, content_length, content_type, genesis_address, genesis_timestamp, genesis_fee, encrypted, licence, max_claims, whitelist, genesis_block_height, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (number, txid , genesis_address, txid, chunk_txids, mime_type, content_length, content_type, genesis_address, genesis_timestamp, genesis_fee, encrypted, licence, max_claims, json.dumps(whitelist), blockheight, time))

    conn.commit()


def main():
    novo_blocks_conn = sqlite3.connect("novo_blocks.db")
    conn = create_content_database()

    transactions = get_transactions_with_any_content(novo_blocks_conn)
    process_transactions(conn, transactions)

    valid_entries = get_valid_json_entries(conn)
    process_valid_json_entries(conn, valid_entries)

    conn.close()
    novo_blocks_conn.close()

if __name__ == "__main__":
    update_interval = 60  # Update every minute
    while True:
        try:
            main()
            logger.info("Sleeping for %d seconds", update_interval)
            time.sleep(update_interval)
        except Exception as e:
            logger.error("Error encountered: %s", e)
            logger.error("Retrying in %d seconds", update_interval)
            time.sleep(update_interval)
