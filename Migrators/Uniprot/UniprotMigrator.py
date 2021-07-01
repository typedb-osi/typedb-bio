from typedb.client import TypeDB, SessionType, TransactionType
import csv 
import os
from inspect import cleandoc

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from Migrators.Helpers.batchLoader import batch_job

def uniprotMigrate(uri, database, num, num_threads, ctn):
	client = TypeDB.core_client(uri)
	session = client.session(database, SessionType.DATA)
	batches_pr = []

	if num != 0:
		print('  ')
		print('Opening Uniprot dataset...')
		print('  ')

		tx = session.transaction(TransactionType.WRITE)
		org = "insert $h isa organism, has organism-name 'Homo sapiens (Human)', has organism-name 'Human'; $o2 isa organism, has organism-name 'Avian';"
		tx.query().insert(org)
		tx.commit()
		tx.close()


		with open('Dataset/Uniprot/uniprot-reviewed_yes+AND+proteome.tsv', 'rt', encoding='utf-8') as csvfile:
			csvreader = csv.reader(csvfile, delimiter='	')
			raw_file = []
			n = 0
			for row in csvreader: 
				n = n + 1
				if n != 1:
					raw_file.append(row)

		uniprotdb = []
		for i in raw_file[:num]:
			data = {}
			data['uniprot-id'] = i[0]
			data['uniprot-entry-name'] = i[1]
			data['protein-name'] = i[3]
			data['gene-symbol'] = i[4]
			data['organism'] = i[5]
			data['function-description'] = i[7]
			data['ensembl-transcript'] = i[11]
			data['entrez-id'] = i[12]
			uniprotdb.append(data)

		insertGenes(uniprotdb, session, num_threads, ctn)
		insertTranscripts(uniprotdb, session, num_threads, ctn)

		# Insert proteins 
		counter = 0
		pool = ThreadPool(num_threads)
		batches = []
		for q in uniprotdb: 
			counter = counter + 1
			transcripts = transcriptHelper(q)
			gene = geneHelper(q)[0]
			if transcripts != None: 
				variables = []
				tvariable = 1
				typeql = "match "
				for t in transcripts:
					variables.append(tvariable)
					typeql = typeql + "$" + str(tvariable) + " isa transcript, has ensembl-transcript-stable-id '" + t + "'; "
					tvariable = tvariable + 1
			if gene != None: 
				try: 
					typeql = typeql + "$g isa gene, has gene-symbol '" + gene + "';"
				except Exception: 
					typeql = "match $g isa gene, has gene-symbol '" + gene + "';"
			try: 
				typeql = typeql + "$h isa organism, has organism-name '" + q['organism'] + "';"
			except Exception:
				typeql = "match $h isa organism, has organism-name '" + q['organism'] + "';"

			typeql = f"""{ typeql } insert $a isa protein, has uniprot-id "{q['uniprot-id']}", has uniprot-name
"{q['protein-name']}", has function-description "{q['function-description']}", 
has uniprot-entry-name "{q['uniprot-entry-name']}";
$r (associated-organism: $h, associating: $a) isa organism-association;"""
			if gene != None: 
				typeql = typeql + "$gpe (encoding-gene: $g, encoded-protein: $a) isa gene-protein-encoding;"
			if transcripts != None: 
				for v in variables:
					typeql = f"""{ typeql } $r{str(v)}(translating-transcript: ${str(v)}, translated-protein: $a) isa translation; """
			if gene and transcripts != None:
				for v in variables: 
					typeql = typeql + "$trans" + str(v) + "(transcribing-gene: $g, encoded-transcript: $" + str(v) + ") isa transcription;"

			batches.append(typeql)
			del typeql
			if counter % ctn == 0:
				batches_pr.append(batches)
				batches = []
		batches_pr.append(batches)
		pool.map(partial(batch_job, session), batches_pr)
		pool.close()
		pool.join()
		print('.....')
		print('Finished migrating Uniprot file.')
		print('.....')
		session.close()
		client.close()


def transcriptHelper(q):
	import re
	list = []
	n = q['ensembl-transcript']
	nt = n.count(';')
	if nt != 0:
		while nt != 0:
			nt = nt - 1
			pos = [m.start() for m in re.finditer(r";",n)]
			if nt == 0: 
				n2 = n[0:pos[0]]
			else: 
				try: 
					pos_st = pos[nt - 1] +1
					pos_end = pos[nt]
					n2 = n[pos_st:pos_end]
				except: 
					pass
			estr = n2.find('[') - 1
			n2 = n2[:estr]
			list.append(n2)
		return list

# Returns: a list of [gene, entrez-id]
# Method removes synonyms from genes and only takes the first one
def geneHelper(q): 
	gene_name = q['gene-symbol']
	entrez_id = q['entrez-id'][0:-1]
	if gene_name.find(' ') != -1:
		gene_name = gene_name[0:gene_name.find(' ')]
	list = [gene_name, entrez_id]
	return list

# Insert genes from gene-symbol. 
# NB: We only insert the first name, if there are synonyms, we ignore them. 
def insertGenes(uniprotdb, session, num_threads, ctn): 
	counter = 0
	gene_list = []
	batches = []
	batches2 = []
	for q in uniprotdb: 
		if q['gene-symbol'] != "":
			gene_list.append(geneHelper(q)) 
			
	# gene_list = list(dict.fromkeys(gene_list)) # TO DO: Remove duplicate gene-symbols

	pool = ThreadPool(num_threads)
	for g in gene_list:
		counter = counter + 1
		typeql = f"insert $g isa gene, has gene-symbol '{g[0]}', has entrez-id '{g[1]}';"
		batches.append(typeql)
		del typeql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Genes committed!')

# Insert transcripts 
def insertTranscripts(uniprotdb, session, num_threads, ctn):
	transcript_list = []
	batches = []
	batches2 = []
	for q in uniprotdb: 
		tr = transcriptHelper(q)
		if tr != None: 
			transcript_list = transcript_list + tr

	transcript_list = list(dict.fromkeys(transcript_list)) # Remove duplicate transcripts

	pool = ThreadPool(num_threads)
	counter = 0
	for q in transcript_list: 
		counter = counter + 1
		typeql = "insert $t isa transcript, has ensembl-transcript-stable-id '" + q + "' ;"
		batches.append(typeql)
		del typeql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Transcripts committed!')
