neo4j-admin import --database addresses.db --id-type string \
             --nodes:Address addresses_import.csv \
             --relationships:Transaction transactions_import.csv 
