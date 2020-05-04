
def batch_job(session, batch):
	tx = session.transaction().write()
	for query in batch:
		print(query)
		tx.query(query)
	tx.commit()