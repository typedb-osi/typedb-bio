from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

from grakn.client import Grakn, SessionType, TransactionType

from Migrators.Helpers.batchLoader import batch_job
from Migrators.Helpers.open_file import openFile
from Migrators.Helpers.get_file import get_file

import ssl, wget, os

def dgidbMigrator(uri, database, num_dr, num_int, num_threads, ctn):
	client = Grakn.core_client(uri)
	session = client.session(database, SessionType.DATA)
	insertDrugs(uri, database, num_dr, num_threads, ctn, session)
	insertInteractions(uri, database, num_int, num_threads, ctn, session)
	session.close()
	client.close()

def insertDrugs(uri, database, num_dr, num_threads, ctn, session): 

	#from Migrators.Helpers.get_file import get_file
	get_file("https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv", "Dataset/DGIdb/")
	file = 'Dataset/DGIdb/drugs.tsv'
	
	print('  ')
	print('Opening DGIdb...')
	print('  ')
	raw_file = openFile(file, num_dr)
	drugs = []
	for i in raw_file[:num_dr]:
		data = {}
		data['drug-claim-name'] = i[0].strip('"')
		data['drug-name'] = i[1].strip('"')
		data['chembl-id'] = i[2]
		data['drug-claim-source'] = i[3]
		drugs.append(data)
	os.remove('Dataset/DGIdb/drugs.tsv')
	counter = 0
	drugs_list = drugs
	batches = []
	batches2 = []

	pool = ThreadPool(num_threads)
	for d in drugs_list:
		counter = counter + 1
		graql = f'''insert $d isa drug, has drug-claim-name "{d['drug-claim-name']}", has drug-name "{d['drug-name']}", has chembl-id "{d['chembl-id']}", has drug-claim-source "{d['drug-claim-source']}";'''
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Drugs committed!')


def insertInteractions(uri, database, num_int, num_threads, ctn, session):
	batches_pr = []
	ssl._create_default_https_context = ssl._create_unverified_context
	url = "https://www.dgidb.org/data/monthly_tsvs/2021-Jan/interactions.tsv"
	wget.download(url, 'Dataset/DGIdb/')
	
	
	file = 'Dataset/DGIdb/interactions.tsv'
	print('  ')
	print('Opening DGIdb-Interactions...')
	print('  ')
	raw_file = openFile(file, num_int)

	interactions = []
	for i in raw_file[:num_int]:
		data = {}
		data['gene-name'] = i[0]
		data['entrez-id'] = i[2]
		data['interaction-type'] = i[4]
		data['drug-claim-name'] = i[5]
		data['drug-name'] = i[7]
		data['chembl-id'] = i[8]
		interactions.append(data)
	os.remove('Dataset/DGIdb/interactions.tsv')
	counter = 0
	pool = ThreadPool(num_threads)
	batches = []
	for q in interactions: 
		if q['entrez-id'] is not "":
			counter = counter + 1
			graql = f"""match $g isa gene, has entrez-id "{q['entrez-id']}"; $d isa drug, has drug-claim-name "{q['drug-claim-name']}";"""
			# TODO Insert interaction type as a role
			if q['interaction-type'] == "":
				graql = graql + f"insert $r (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction;"
			else: 
				graql = graql + f"""insert $r (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction, has interaction-type "{q['interaction-type']}";"""
			batches.append(graql)
			del graql
			if counter % ctn == 0:
				batches_pr.append(batches)
				batches = []
	batches_pr.append(batches)
	pool.map(partial(batch_job, session), batches_pr)
	pool.close()
	pool.join()
	print('.....')
	print('Finished migrating Drug Interactions.')
	print('.....')