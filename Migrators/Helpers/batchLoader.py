from typedb.client import TransactionType

# toggle to turn query printing on
global verbose 
verbose = True

def batch_job(session, batch):
	tx = session.transaction(TransactionType.WRITE)
	for query in batch:
		if verbose == True:
			print(query)
		tx.query().insert(query)
	tx.commit()
	tx.close()