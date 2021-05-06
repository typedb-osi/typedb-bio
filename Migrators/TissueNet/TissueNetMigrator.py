import csv

from grakn.client import Grakn


def tissueNetMigrator(uri, keyspace, num, num_threads, ctn):
	client = Grakn(uri=uri)
	session = client.session(keyspace=keyspace)
	batches_pr = []

	if num is not 0:
		print('  ')
		print('Opening TissueNet dataset...')
		print('  ')

		with open('Dataset/TissueNet/HPA-Protein.tsv', 'rt', encoding='utf-8') as csvfile:
			csvreader = csv.reader(csvfile, delimiter='	')
			raw_file = []
			n = 0
			for row in csvreader: 
				n = n + 1
				if n is not 1:
					raw_file.append(row)

