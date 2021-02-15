from grakn.client import GraknClient, SessionType, TransactionType
from inspect import cleandoc
import ssl, gzip, wget, csv, os, itertools

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from Migrators.Helpers.batchLoader import batch_job


def disgenetMigrator(uri, database, num, num_threads, ctn):
	client = GraknClient.core(uri)
	session = client.session(database, SessionType.DATA)
	batches_pr = []

	if num != 0:
		print('  ')
		print('Opening Disgenet dataset...')
		print('  ')

		ssl._create_default_https_context = ssl._create_unverified_context
		url = "https://www.disgenet.org/static/disgenet_ap1/files/downloads/all_gene_disease_associations.tsv.gz"
		wget.download(url, 'Dataset/Disgenet/')
		

		with gzip.open('Dataset/Disgenet/all_gene_disease_associations.tsv.gz', 'rt') as f:
			csvreader = csv.reader(f, delimiter='	')
			raw_file = []
			n = 0
			for row in csvreader: 
				n = n + 1
				if n != 1:
					raw_file.append(row)

		disgenet = []
		for i in raw_file[:num]:
			data = {}
			data['entrez-id'] = i[0].strip()
			data['gene-symbol'] = i[1]
			data['disease-id'] = i[4]
			data['disease-name'] = i[5]
			data['disgenet-score'] = float(i[9])
			disgenet.append(data)
		os.remove('Dataset/Disgenet/all_gene_disease_associations.tsv.gz')
		insertDiseases(disgenet, session, num_threads, ctn)

		counter = 0
		pool = ThreadPool(num_threads)
		batches = []
		for q in disgenet: 
			counter = counter + 1
			graql = f"""
match $g isa gene, has gene-symbol "{q['gene-symbol']}", has entrez-id "{q['entrez-id']}";
$d isa disease, has disease-id "{q['disease-id']}", has disease-name "{q['disease-name']}";
insert $r (associated-gene: $g, associated-disease: $d) isa gene-disease-association, has disgenet-score {q['disgenet-score']};"""
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
		print('Finished migrating Disgenet.')
		print('.....')
		session.close()
		client.close()


def insertDiseases(disgenet, session, num_threads, ctn): 
	counter = 0
	disease_list = []
	batches = []
	batches2 = []
	for q in disgenet: 
		disease_list.append([q['disease-name'], q['disease-id']]) 
	disease_list.sort()
	disease_list = list(disease_list for disease_list,_ in itertools.groupby(disease_list)) # Remove duplicates

	pool = ThreadPool(num_threads)
	for d in disease_list:
		counter = counter + 1
		graql = f'insert $d isa disease, has disease-name "{d[0]}", has disease-id "{d[1]}";'
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Diseases committed!')



	