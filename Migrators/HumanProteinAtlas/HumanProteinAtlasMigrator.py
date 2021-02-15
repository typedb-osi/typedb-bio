import csv
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
import wget, ssl, os
from grakn.client import GraknClient, SessionType, TransactionType
from zipfile import ZipFile
from Migrators.Helpers.batchLoader import batch_job


def proteinAtlasMigrator(uri, database, num, num_threads, ctn):
	client = GraknClient.core(uri)
	session = client.session(database, SessionType.DATA)
	batches_pr = []

	if num is not 0:
		print('  ')
		print('Opening HPA dataset...')
		print('  ')

		ssl._create_default_https_context = ssl._create_unverified_context
		url = 'https://www.proteinatlas.org/download/normal_tissue.tsv.zip'
		wget.download(url, 'Dataset/HumanProteinAtlas/')

		with ZipFile('Dataset/HumanProteinAtlas/normal_tissue.tsv.zip', 'r') as f:
			f.extractall('Dataset/HumanProteinAtlas')

		with open('Dataset/HumanProteinAtlas/normal_tissue.tsv', 'rt', encoding='utf-8') as csvfile:
			csvreader = csv.reader(csvfile, delimiter='	')
			raw_file = []
			n = 0
			for row in csvreader: 
				n = n + 1
				if n is not 1:
					d = {}
					d['ensembl-gene-id'] = row[0]
					d['gene-symbol'] = row[1]
					d['tissue'] = row[2]
					d['expression-value'] = row[4]
					d['expression-value-reliability'] = row[5]
					raw_file.append(d)
			os.remove('Dataset/HumanProteinAtlas/normal_tissue.tsv.zip')
			os.remove('Dataset/HumanProteinAtlas/normal_tissue.tsv')

		tissue = []
		for r in raw_file[:num]:
			tissue.append(r['tissue'])
		tissue = (list(set(tissue)))

		insertTissue(tissue, session, num_threads)
		insertEnsemblId(raw_file, session, num_threads, ctn)
		insertGeneTissue(raw_file, session, num_threads, ctn)

def insertTissue(tissue, session, num_threads):
	tx = session.transaction(TransactionType.WRITE)
	for t in tissue: 
		q = 'insert $t isa tissue, has tissue-name "' + t + '";'
		a = tx.query().insert(q)
	tx.commit()

def insertGeneTissue(raw_file, session, num_threads, ctn):
	pool = ThreadPool(num_threads)
	counter = 0
	batches = []
	batches2 = []
	for g in raw_file:
		counter = counter + 1
		graql = f"""
		match $g isa gene, has gene-symbol '{g['gene-symbol']}'; 
		$t isa tissue, has tissue-name '{g['tissue']}';
		insert
		(expressing-gene: $g, expressed-tissue: $t) isa expression, 
		has expression-value '{g['expression-value']}',
		has expression-value-reliability '{g['expression-value-reliability']}'; 
		"""
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Genes <> Tissues committed!')


def insertEnsemblId(raw_file, session, num_threads, ctn):
	list_of_tuples = []
	for r in raw_file:
		list_of_tuples.append((r['ensembl-gene-id'], r['gene-symbol']))
	list_of_tuples = [t for t in (set(tuple(i) for i in list_of_tuples))]
	pool = ThreadPool(num_threads)
	counter = 0
	batches = []
	batches2 = []

	for g in list_of_tuples: 
		counter = counter + 1
		graql = f"""
		match $g isa gene, has gene-symbol '{g[1]}'; 
		insert
		$g has ensembl-gene-stable-id '{g[0]}'; 
		"""
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('EnsemblIDs committed!')













