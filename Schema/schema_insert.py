from grakn.client import GraknClient
import csv 
import os

def insertSchema(uri, keyspace):
	client = GraknClient(uri=uri)
	session = client.session(keyspace=keyspace)
	print('.....')
	print('Inserting schema...')
	print('.....')
	with open("../biograkn-covid/Schema/biograkn-covid-1.8.gql", "r") as graql_file:
		schema = graql_file.read()
	with session.transaction().write() as write_transaction:
		write_transaction.query(schema)
		write_transaction.commit() 
	print('.....')
	print('Success inserting schema!')
	print('.....')