import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth
from celery import Celery
import os

IN_ADDRESS = 'from'
OUT_ADDRESS = 'to'
AMOUNT = 'value'

app = Celery('tasks', broker='redis://')

@app.task
def load_chunk(path):
    chunk = pd.read_csv(path)
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "blockchain"))
    session = driver.session()
    
    for index, row in chunk.iterrows():
        session.run("MERGE (address1:Address {address:{address1}})"
                    "MERGE (address2:Address {address:{address2}})"
                    "CREATE UNIQUE (address1)-[t:Transactions]->(address2)"
                    "SET t.amount = coalesce(t.amount, 0) + {amount}"
                    "SET t.number = coalesce(t.number, 0) + 1",
                    {"address1": row[IN_ADDRESS], "address2": row[OUT_ADDRESS], 'amount': row[AMOUNT]}
    del chunk
    os.remove(path)
    session.close()

print(__name__)
if __name__ == "__main__":
    ethereum = pd.read_csv("./transactions.csv", chunksize=10000, iterator=True, sep=";", header=0)
    index = 0
    for chunk in ethereum:
        index += 1
        path = './chunks/chunk' + str(index)
        chunk.to_csv(path)
        load_chunk.delay(path)
        del chunk
