from grakn.client import GraknClient


def insertSchema(uri, keyspace):
	client = GraknClient(uri=uri)
	session = client.session(keyspace=keyspace)
	print('.....')
	print('Inserting schema...')
	print('.....')
	with open("Schema/biograkn-covid.gql", "r") as graql_file:
		schema = graql_file.read()
	with session.transaction().write() as write_transaction:
		write_transaction.query(schema)
		write_transaction.commit() 
	print('.....')
	print('Success inserting schema!')
	print('.....')