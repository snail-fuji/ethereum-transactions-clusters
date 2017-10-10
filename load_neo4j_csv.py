import pandas as pd

ethereum = pd.read_csv("./transactions.csv", header=0, sep=";", nrows=1000) # Use desired number of rows

addresses = open("./addresses_import.csv", "wb")
addresses.write("address:ID(Address)\n".encode())

all_addresses = set(ethereum["from"]).union(set(ethereum["to"]))

for address in all_addresses:
  addresses.write("{}\n".format(address).encode())

addresses.close()

transactions = open("./transactions_import.csv", "wb")
transactions.write(":START_ID(Address), amount, :END_ID(Address)\n".encode())

for index, transaction in ethereum.iterrows():
  from_address = transaction["from"]
  to_address = transaction["to"]
  amount = transaction["value"]
  transactions.write("{}, {}, {}\n".format(from_address, amount, to_address).encode())

transactions.close()
