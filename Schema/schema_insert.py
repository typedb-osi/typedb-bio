from grakn.client import GraknClient, SessionType, TransactionType
import csv 
import os

def insertSchema(uri, keyspace):
	client = GraknClient(uri)
	client.databases().create(keyspace)
	session = client.session(keyspace, SessionType.SCHEMA)

	print('.....')
	print('Inserting schema...')
	print('.....')
	with open("./Schema/biograkn-covid.gql", "r") as graql_file:
		schema = graql_file.read()
	with session.transaction(TransactionType.WRITE) as write_transaction:
		write_transaction.query().define(schema)
		write_transaction.commit() 
	print('.....')
	print('Success inserting schema!')
	print('.....')
	session.close()