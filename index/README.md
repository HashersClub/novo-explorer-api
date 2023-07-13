# Novo-Explorer-API
Novo-Explorer API indexes the Novo chain and provides an API for exploring the chain.

This repository contains scripts for extracting data from the Novo chain and for running an API server that provides access to this data.

## Indexing the Novo Chain

To index the Novo chain, you need to run the `extract.py` script. This script creates and updates the `novo_blocks.db` database, which stores the block data.

To start the extraction process, open a terminal and execute the following command:

```bash
python extract.py
```

Leave this script running as it continually updates the database with new blocks from the Novo chain.

## Extracting Inscriptions Related Data

To extract data related to inscriptions, you need to run the `index_content.py` script. This script creates and updates the `contents.db` database, which stores inscription-related data.

To start the extraction process, open a new terminal and execute the following command:

```bash
python index_content.py
```

Leave this script running as it continually updates the database with new inscriptions from the Novo chain.

## Starting the Novo Explorer API

To start the Novo Explorer API, you need to run the `explorer_api.py` script.

To start the API, open a new terminal and execute the following command:

```bash
python explorer_api.py
```

The API will run on `http://localhost:5000/`.

## Optional: Extracting Contracts Related Data

If you wish to extract data related to contracts, you can run the `contracts.py` script. This script creates and updates the `contracts.db` database, which stores contract-related data.

To start the extraction process, open a new terminal and execute the following command:

```bash
python contracts.py
```

Leave this script running as it continually updates the database with new contracts from the Novo chain.

This script is optional and is primarily used by the Hashers.Club API.
