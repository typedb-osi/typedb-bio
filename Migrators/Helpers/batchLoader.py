from grakn.client import TransactionType

def batch_job(session, batch):
	tx = session.transaction(TransactionType.WRITE)
	for query in batch:
		print(query)
		tx.query().insert(query)
	tx.commit()
	tx.close()