# novo-explorer-api
Novo-explorer Index and API


# Index Novo chain
Execute python extract.py in a terminal and let it run.
It will create and update the novo_blocks.db database.

# Extract inscriptions related data
Execute python index_content.py in a new terminal and let it run.
It will create and update the contents.db database needed for Novo Explorer API.

# Start Novo Explorer API
Execute python explorer_api.py in a new terminal and let it run.
API will run on http://localhost:5000/


# OPTIONAL: Extract contracts related data
Execute python contracts.py in a new terminal and let it run.
It will create and update the contracts.db database used by Hashers.Club API.


