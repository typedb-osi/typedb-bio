from grakn.client import *


def insertSchema(uri, database):
	client = GraknClient.core(uri)
	client.databases().create(database)
	session = client.session(database, SessionType.SCHEMA)
	print('.....')
	print('Inserting schema...')
	print('.....')
	with open("Schema/biograkn-covid.gql", "r") as graql_file:
		schema = graql_file.read()
	with session.transaction(TransactionType.WRITE) as write_transaction:
		write_transaction.query().define(schema)
		write_transaction.commit() 
	print('.....')
	print('Success inserting schema!')
	print('.....')
	session.close()