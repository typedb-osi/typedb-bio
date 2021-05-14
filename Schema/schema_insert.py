from grakn.client import *


def insertSchema(uri, database, force=False):
	client = Grakn.core_client(uri)
	if database in [db.name() for db in client.databases().all()]:
    	if force:
    		client.databases().get(database).delete()
    	else:
			raise ValueError("database {} already exists, use --force True to overwrite")
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
	client.close()