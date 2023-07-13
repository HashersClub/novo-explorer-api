import sqlite3
import json
import subprocess
import time
import datetime
import math
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_contracts_database():
    conn = sqlite3.connect('contracts.db')
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS token_balances (
            address TEXT,
            token_contract_id TEXT,
            contract_type TEXT,
            balance REAL,
            last_updated INTEGER,
            token_name TEXT,
            token_symbol TEXT,
            token_decimals INTEGER,
            token_icon TEXT,
            PRIMARY KEY (address, token_contract_id, contract_type)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS token_interactions (
            transaction_id TEXT,
            address TEXT,
            contract_id TEXT,
            transaction_data TEXT,
            max_supply INTEGER,
            token_name TEXT,
            token_symbol TEXT,
            token_decimals INTEGER,
            token_icon TEXT,
            genesis_price TEXT,
            limit_mint TEXT,
            limit_wallet TEXT,
            interaction_time INTEGER,
            n INTEGER,
            type TEXT,
            value REAL,
            direction TEXT,
            UNIQUE(transaction_id, address, contract_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS defi (
            contract_id TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            decimals INTEGER,
            max_supply INTEGER,
            minted_amount REAL,
            percentage_minted REAL,
            num_holders INTEGER,
            tx_volume_24h REAL,
            tx_volume_7d REAL,
            tx_volume_all_time REAL,
            tx_volume_evolution_24h REAL,
            tx_volume_evolution_7d REAL,
            last_updated TEXT,
            genesis_date TEXT,
            token_icon TEXT,
            genesis_price INTEGER,
            limit_mint INTEGER,
            limit_wallet INTEGER, 
            minter INTEGER, 
            deployer INTEGER
        )
    """)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS imported_addresses (
            address TEXT PRIMARY KEY,
            novo_balance REAL
        )
    """)

    return conn


def get_transactions_with_any_contract_id():
    conn_novo_blocks = sqlite3.connect("novo_blocks.db")
    cursor_novo_blocks = conn_novo_blocks.cursor()

    query = "SELECT txid, vout, time FROM transactions WHERE vout LIKE '%\"contractID\":%'"
    cursor_novo_blocks.execute(query)
    results = cursor_novo_blocks.fetchall()

    transactions = []
    addresses = set()
    for tx in results:
        txid = tx[0]
        vout = json.loads(tx[1])
        for entry in vout:
            if 'contractID' in entry:
                address_list = entry.get('scriptPubKey', {}).get('addresses', [])
                transaction_data = json.dumps(entry)
                transactions.extend([(txid, address, transaction_data, tx[2]) for address in address_list])
                addresses.update(address_list)

    conn_novo_blocks.close()

    return transactions, addresses


def import_address(address):
    try:
        command = f"novo-cli importaddress {address}"
        output = subprocess.check_output(command, shell=True)
        logger.info(f"Imported address {address}: {output}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error importing address {address}: {e}")


def add_imported_address(conn, address, novo_balance):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO imported_addresses (address, novo_balance)
        VALUES (?, ?)
    """, (address, novo_balance))
    conn.commit()


def list_address_groupings():
    command = "novo-cli listaddressgroupings"
    output = subprocess.check_output(command, shell=True)
    return json.loads(output)


def update_novo_balances(conn, address, balance):
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE imported_addresses
        SET novo_balance = ?
        WHERE address = ?
    """, (balance, address))

    conn.commit()


def get_all_addresses_from_token_interactions():
    conn = sqlite3.connect('contracts.db')
    cursor = conn.cursor()

    query = "SELECT DISTINCT address FROM token_interactions"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    # Unpack the tuple results into a set of unique addresses
    addresses = set(address[0] for address in results)

    return addresses


def is_address_imported(conn, address):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM imported_addresses WHERE address = ?
    """, (address,))
    return cursor.fetchone() is not None



def list_all_contract_unspent():
    command = "novo-cli listcontractunspent"
    output = subprocess.check_output(command, shell=True)
#    logger.info("listcontractunspent output: %s", output)
    return json.loads(output)

def update_token_balances(conn, address, contract_id, contract_type, balance, metadata):
    cursor = conn.cursor()

    # Compute the token balance
    cursor.execute("""
        SELECT SUM(CASE WHEN direction = 'received' THEN value ELSE 0 END) - SUM(CASE WHEN direction = 'sent' THEN value ELSE 0 END) AS balance
        FROM token_interactions
        WHERE address = ? AND contract_id = ?
    """, (address, contract_id))
    result = cursor.fetchone()
    existing_balance = result[0] if result[0] is not None else 0

    # Fetch contract unspent for the given address and contract_id
    contract_unspent_list = list_all_contract_unspent()
    new_balance = existing_balance + balance

    for contract_unspent in contract_unspent_list:
        unspent_address = contract_unspent.get('address')
        unspent_contract_id = contract_unspent.get('contractID')
        unspent_balance = contract_unspent.get('contractValue')
        
        if unspent_address == address and unspent_contract_id == contract_id:
            # Update the balance with the unspent balance
            try:
                new_balance += int(unspent_balance)
            except ValueError:
                logger.error(f"Invalid contractValue: {unspent_balance}")

    # Retrieve token symbol, name, and decimals from token_interactions table
    cursor.execute("""
        SELECT token_symbol, token_name, token_decimals
        FROM token_interactions
        WHERE address = ? AND contract_id = ?
        LIMIT 1
    """, (address, contract_id))
    token_data = cursor.fetchone()
    if token_data:
        token_symbol, token_name, token_decimals = token_data
    else:
        token_symbol = None
        token_name = None
        token_decimals = None

    cursor.execute("""
        INSERT OR REPLACE INTO token_balances (address, token_contract_id, contract_type, balance, last_updated, token_name, token_symbol, token_decimals)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (address, contract_id, contract_type, new_balance, int(time.time()), token_name, token_symbol, token_decimals))

    conn.commit()




def process_transactions(conn, transactions):
    cursor = conn.cursor()
    for tx in transactions:
        txid, address, transaction_data, interaction_time = tx
        contract_id = json.loads(transaction_data).get('contractID', None)
        max_supply = json.loads(transaction_data).get('contractMaxSupply', None)
        token_name = None
        token_symbol = None
        n = json.loads(transaction_data).get('n', None)
        contract_type = json.loads(transaction_data).get('contractType', None)
        value = json.loads(transaction_data).get('contractValue', None)
        token_decimals = None
        token_icon = None
        genesis_price = None
        limit_mint = None
        limit_wallet = None

        try:
            metadata = json.loads(json.loads(transaction_data).get('contractMetadata', ''))
            if isinstance(metadata, dict):
                token_name = metadata.get('name', None)
                token_symbol = metadata.get('symbol', None)
                token_decimals = metadata.get('decimal', None)
                token_icon = metadata.get('icon', None)
                genesis_price = metadata.get('genesis_price', None)
                limit_mint = metadata.get('limit_mint', None)
                limit_wallet = metadata.get('limit_wallet', None)
        except (json.JSONDecodeError, TypeError):
            # Handle case where metadata is not valid JSON or is not a dictionary
            pass

        type_value = None
        if contract_type == 'FT_MINT':
            type_value = 'token mint'
        elif contract_type == 'FT':
            type_value = 'token transfer'
        elif contract_type == 'NFT':
            type_value = 'NFT transfer'
        elif contract_type == 'NFT_MINT':
            type_value = 'NFT mint'

        cursor.execute("""
            INSERT OR IGNORE INTO token_interactions (transaction_id, address, contract_id, transaction_data, max_supply, token_name, token_symbol, interaction_time, n, type, value, token_decimals,  token_icon, genesis_price, limit_mint, limit_wallet, direction)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (txid, address, contract_id, transaction_data, max_supply, token_name, token_symbol, interaction_time, n, type_value, value, token_decimals, token_icon, genesis_price, limit_mint, limit_wallet, None))

    conn.commit()


def populate_direction_column(conn):
    cursor = conn.cursor()

    # Set 'mint' in 'direction' column for mint transactions
    cursor.execute("""
        UPDATE token_interactions
        SET direction = 'mint'
        WHERE type LIKE '%mint%'
    """)

    # Get transfers with 'transfer' in 'type' column
    cursor.execute("""
        SELECT transaction_id, n
        FROM token_interactions
        WHERE type LIKE '%transfer%'
    """)
    transfers = cursor.fetchall()

    for txid, n in transfers:
        # Check if there is a lower 'n' value for the same txid
        cursor.execute("""
            SELECT COUNT(*)
            FROM token_interactions
            WHERE transaction_id = ? AND n < ?
        """, (txid, n))
        lower_n_count = cursor.fetchone()[0]

        if lower_n_count == 0:
            # If there are no lower 'n' values, set 'sent' as direction
            cursor.execute("""
                UPDATE token_interactions
                SET direction = 'sent'
                WHERE transaction_id = ? AND n = ?
            """, (txid, n))
        else:
            # Check if the lower 'n' value transaction is of 'mint' type
            cursor.execute("""
                SELECT COUNT(*)
                FROM token_interactions
                WHERE transaction_id = ? AND n = ? AND type LIKE '%mint%'
            """, (txid, n - 1))
            mint_count = cursor.fetchone()[0]

            if mint_count > 0:
                # If the lower 'n' value transaction is of 'mint' type, set 'received from mint' as direction
                cursor.execute("""
                    UPDATE token_interactions
                    SET direction = 'received from mint'
                    WHERE transaction_id = ? AND n = ?
                """, (txid, n))
            else:
                # If the lower 'n' value transaction is of 'transfer' type, set 'received from wallet' as direction
                cursor.execute("""
                    UPDATE token_interactions
                    SET direction = 'received from wallet'
                    WHERE transaction_id = ? AND n = ?
                """, (txid, n))

    conn.commit()


def populate_defi_table(conn):
    cursor = conn.cursor()

    blacklist = ['0000000000000000000000000000000000000000000000000000000000000000:4294967295', 'b569a13bd81f44b9a12d480284825b5b7f25b00c02d619667b935a6f6d5c794b:0', '5c6d3b6d84488722a38c9bc04ddeee6125c01f37b3871ef62bb62b0b9854bc34:0', '92b12fb42983cbe6223596c9c3d29b84e7cb61142ad0dd8f64cb9ed08809df4c:0', 'bed33d6620aae8f39374b5a8e57586d2d6cef12db56ebfa871201f5ee463c070:0', 'c6966c060a4ae1c39a2538851bc7e8089efbe7d1185da9908cffee4a7382ca06:0', '486537a65eb8dde94d11e8e8e7ebb0f00ea1f7aa1b0f2d8d11867f5e1b97eea7:0', '89da903a405ac63beb3493fab18c7b3186204d936c4599ea613e3dd41434f6d1:0', '1bde3f789e2eb2e36ff41ad57234a5f543614da1e2d8e76be7080596f2091581:0', 'e8650a2e1315592f116b4da9c02d16a4e3517f816588678a811aba040b4e1142:0', 'e8650a2e1315592f116b4da9c02d16a4e3517f816588678a811aba040b4e1142:0', '0d53293ece64b238f99bfa7c5dd1b239097678c636b12a25cd5a16a4715874d4:0', '1646cd7ea830304b1e3bfb3e96479ae2088e8e6adb5350425e6186ffa3b754c7:0', 'b29a8f95dd14bb56e4d19e34f086d0335203b53159ffe764c6fba6b3cbed94b8:0', '60b76f41a657b6f57db2bcddbbffa3d2420bb87245339d8cc5b6f6132f4cbe36:0', 'b5f07afa105578d4178286c4dc3465d24453f1d198a0acb604336a4cc7e1ccde:0', '22fbbef587d99b38badc41a06f64cc1a848bc3a628ec5bb3377fc424d449d26b:0', 'a388d26f561d1bc7156c9a465e03c96522c65d31e4e45a5b4759a634b4537e8a:0', '1da1bfc617c5617cd40bb05dea8674003029c0acd87618359ab975c7d30d5ab5:0', '4b118c5c6ed585c4c374779f30e63421201a5acff472fdf906b3550eacf17830:0']  # Define the list of blacklisted contract_id values

    # Retrieve distinct contract IDs associated with tokens that have a non-null token_symbol
    cursor.execute("""
        SELECT DISTINCT contract_id
        FROM token_interactions
        WHERE token_symbol IS NOT NULL
    """)
    contract_ids = cursor.fetchall()

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for contract_id in contract_ids:
        contract_id = contract_id[0]  # Extract the contract_id value from the tuple

        # Skip if the contract_id is in the blacklist
        if contract_id in blacklist:
            continue

        # Retrieve the genesis_date from the oldest "token mint" interaction for the contract_id
        cursor.execute("""
            SELECT MIN(interaction_time) AS genesis_date
            FROM token_interactions
            WHERE contract_id = ? AND type = 'token mint'
        """, (contract_id,))
        result = cursor.fetchone()
        if result:
            genesis_date = result[0]
        else:
            continue

        # Retrieve the minted_amount from the highest "value" in "token mint" interactions for the contract_id
        cursor.execute("""
            SELECT MAX(value) AS minted_amount
            FROM token_interactions
            WHERE contract_id = ? AND type = 'token mint'
        """, (contract_id,))
        result = cursor.fetchone()
        if result:
            minted_amount = result[0]
        else:
            continue

        # Retrieve token details from token_interactions table
        cursor.execute("""
            SELECT contract_id, token_name, token_symbol, token_decimals, max_supply, token_icon, genesis_price, limit_mint, limit_wallet
            FROM token_interactions
            WHERE contract_id = ? AND token_symbol IS NOT NULL
            LIMIT 1
        """, (contract_id,))
        token_data = cursor.fetchone()

        if token_data:
            contract_id, token_name, token_symbol, token_decimals, max_supply, token_icon, genesis_price, limit_mint, limit_wallet = token_data
        else:
            continue

        # Calculate the percentage_minted
        if max_supply and minted_amount is not None:
            percentage_minted = (minted_amount / max_supply) * 100
        else:
            percentage_minted = None

        # Retrieve the deployer address from the first interaction with a "received" direction
        cursor.execute("""
            SELECT address AS deployer
            FROM token_interactions
            WHERE contract_id = ? AND direction = 'received from mint'
            ORDER BY interaction_time ASC
            LIMIT 1
        """, (contract_id,))
        result = cursor.fetchone()
        if result:
            deployer = result[0]
        else:
            deployer = None

        # Retrieve the minter address from the first "token mint" interaction
        cursor.execute("""
            SELECT address AS minter
            FROM token_interactions
            WHERE contract_id = ? AND type = 'token mint'
            ORDER BY interaction_time ASC
            LIMIT 1
        """, (contract_id,))
        result = cursor.fetchone()
        if result:
            minter = result[0]
        else:
            minter = None

        # Count the number of addresses with a non-null balance for the contract_id
        cursor.execute("""
            SELECT COUNT(DISTINCT address)
            FROM token_balances
            WHERE token_contract_id = ?
        """, (contract_id,))
        result = cursor.fetchone()
        if result:
            num_holders = result[0]
        else:
            continue

        # Calculate tx_volume_24h
        interaction_time_24h_start = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        interaction_time_24h_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
            SELECT COALESCE(SUM(value), 0) AS tx_volume_24h
            FROM token_interactions
            WHERE contract_id = ? AND direction LIKE '%received%' AND interaction_time > ? AND interaction_time <= ?
        """, (contract_id, interaction_time_24h_start, interaction_time_24h_end))

        result = cursor.fetchone()
        if result:
            tx_volume_24h = result[0]
        else:
            continue

        # Calculate tx_volume for the previous 24h period
        interaction_time_48h_start = (datetime.datetime.now() - datetime.timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')
        interaction_time_48h_end = interaction_time_24h_start

        cursor.execute("""
            SELECT COALESCE(SUM(value), 0) AS tx_volume_48h
            FROM token_interactions
            WHERE contract_id = ? AND direction LIKE '%received%' AND interaction_time > ? AND interaction_time <= ?
        """, (contract_id, interaction_time_48h_start, interaction_time_48h_end))

        result = cursor.fetchone()
        if result:
            tx_volume_48h = result[0]
        else:
            continue

        # Calculate the tx_volume_7d
        interaction_time_7d_start = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        interaction_time_7d_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
            SELECT COALESCE(SUM(value), 0) AS tx_volume_7d
            FROM token_interactions
            WHERE contract_id = ? AND direction LIKE '%received%' AND interaction_time > ? AND interaction_time <= ?
        """, (contract_id, interaction_time_7d_start, interaction_time_7d_end))

        result = cursor.fetchone()
        if result:
            tx_volume_7d = result[0]
        else:
            continue

        # Calculate tx_volume for the previous 7d period
        interaction_time_14d_start = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')
        interaction_time_14d_end = interaction_time_7d_start

        cursor.execute("""
            SELECT COALESCE(SUM(value), 0) AS tx_volume_14d
            FROM token_interactions
            WHERE contract_id = ? AND direction LIKE '%received%' AND interaction_time > ? AND interaction_time <= ?
        """, (contract_id, interaction_time_14d_start, interaction_time_14d_end))

        result = cursor.fetchone()
        if result:
            tx_volume_14d = result[0]
        else:
            continue

        # Calculate the tx_volume_all_time
        cursor.execute("""
            SELECT COALESCE(SUM(value), 0) AS tx_volume_all_time
            FROM token_interactions
            WHERE contract_id = ? AND direction LIKE '%received%'
        """, (contract_id,))

        result = cursor.fetchone()
        if result:
            tx_volume_all_time = result[0]
        else:
            continue

        # Calculate the evolution for the past 24h and 7d periods
        tx_volume_evolution_24h = ((tx_volume_24h - tx_volume_48h) / tx_volume_48h) * 100 if tx_volume_48h else None
        tx_volume_evolution_7d = ((tx_volume_7d - tx_volume_14d) / tx_volume_14d) * 100 if tx_volume_14d else None

	# Insert a new record if it doesn't exist
        cursor.execute("""
	    INSERT OR IGNORE INTO defi (
		contract_id, 
		name, 
		symbol, 
		decimals, 
		max_supply, 
		minted_amount, 
		percentage_minted, 
		num_holders, 
		tx_volume_24h, 
		tx_volume_7d, 
		tx_volume_all_time, 
		tx_volume_evolution_24h, 
		tx_volume_evolution_7d, 
		last_updated, 
		genesis_date, 
		deployer, 
		minter,
		token_icon, 
		genesis_price, 
		limit_mint, 
		limit_wallet
	    ) 
	    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	""", (
	    contract_id, 
	    token_name, 
	    token_symbol, 
	    token_decimals, 
	    max_supply, 
	    minted_amount, 
	    percentage_minted, 
	    num_holders, 
	    tx_volume_24h, 
	    tx_volume_7d, 
	    tx_volume_all_time, 
	    tx_volume_evolution_24h, 
	    tx_volume_evolution_7d, 
	    current_time, 
	    genesis_date, 
	    deployer, 
	    minter,
	    token_icon, 
	    genesis_price, 
	    limit_mint, 
	    limit_wallet
	))

	# Update the existing record
        cursor.execute("""
	    UPDATE defi 
	    SET 
		name = ?, 
		symbol = ?, 
		decimals = ?, 
		max_supply = ?, 
		minted_amount = ?, 
		percentage_minted = ?, 
		num_holders = ?, 
		tx_volume_24h = ?, 
		tx_volume_7d = ?, 
		tx_volume_all_time = ?, 
		tx_volume_evolution_24h = ?, 
		tx_volume_evolution_7d = ?, 
		last_updated = ?, 
		genesis_date = ?, 
		deployer = COALESCE(deployer, ?), 
		minter = ?,
		token_icon = COALESCE(token_icon, ?), 
		genesis_price = COALESCE(genesis_price, ?), 
		limit_mint = COALESCE(limit_mint, ?), 
		limit_wallet = COALESCE(limit_wallet, ?)
	    WHERE contract_id = ?
	""", (
	    token_name, 
	    token_symbol, 
	    token_decimals, 
	    max_supply, 
	    minted_amount, 
	    percentage_minted, 
	    num_holders, 
	    tx_volume_24h, 
	    tx_volume_7d, 
	    tx_volume_all_time, 
	    tx_volume_evolution_24h, 
	    tx_volume_evolution_7d, 
	    current_time, 
	    genesis_date, 
	    deployer, 
	    minter, 
	    token_icon, 
	    genesis_price, 
	    limit_mint, 
	    limit_wallet, 
	    contract_id
	))

        conn.commit()




    conn.commit()



def main():
    contracts_conn = create_contracts_database()
    transactions, addresses = get_transactions_with_any_contract_id()
    process_transactions(contracts_conn, transactions)
    populate_direction_column(contracts_conn)
    populate_defi_table(contracts_conn)

    print('Addresses:')

    for address in addresses:
        logger.info("Importing address: %s", address)
        if not is_address_imported(contracts_conn, address):
            import_address(address)
            # Add a default NOVO balance of 0 when adding a new imported address
            add_imported_address(contracts_conn, address, 0)

    contract_unspent_list = list_all_contract_unspent()
    for contract_unspent in contract_unspent_list:
        print(f"contract_unspent: {contract_unspent}")
        address = contract_unspent.get('address')
        contract_id = contract_unspent.get('contractID')
        contract_type = contract_unspent.get('contractType')
        try:
            balance = int(contract_unspent.get('contractValue'))
        except ValueError:
            logger.error(f"Invalid contractValue: {contract_unspent.get('contractValue')}")
            continue
        metadata = contract_unspent.get('contractMetadata')

        # Import address only if it hasn't been imported before
        if not is_address_imported(contracts_conn, address):
            import_address(address)
            add_imported_address(contracts_conn, address, 0)

        update_token_balances(contracts_conn, address, contract_id, contract_type, balance, metadata)

    # Update NOVO balances
    address_groupings = list_address_groupings()
    for group in address_groupings:
        for entry in group:
            address, novo_balance = entry[:2]
            if is_address_imported(contracts_conn, address):
                update_novo_balances(contracts_conn, address, novo_balance)
 


    contracts_conn.close()


if __name__ == '__main__':
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
