from timeit import default_timer as timer
import json
from pprint import pprint
from grakn.client import GraknClient
import os

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from Migrators.Helpers.batchLoader import batch_job

def cord_ner_migrator(uri, keyspace, num_ner, num_threads, ctn):
	client = GraknClient(uri=uri)
	session = client.session(keyspace=keyspace)
	tx = session.transaction().write()
	print('.....')
	print('Opening CORD NER file.')
	print('.....')
	with open('../biograkn-covid/Dataset/CORD_NER/CORD-NER-full.json', "r") as f:
	    data = json.loads("[" + 
	        f.read().replace("}\n{", "},\n{") + 
	    "]")
	data = data[:num_ner]
	insert_authors(data, num_threads, ctn, session)	
	insert_journals(data, num_threads, ctn, session)
	insert_publications_journals(data, num_threads, ctn, session)
	insert_publications_with_authors(data, num_threads, 5, session)
	insert_entities_pub(data, num_threads, ctn, session)



# Input: a string of authors
# Return: List of authors
def author_names(author_string):
	author_list = []
	if author_string.find("',") != -1:
		ind = author_string.find("',")
	else:
		ind = author_string.find(';')
	author_string = author_string.replace("]","").replace("[","")
	first_name = author_string[:ind].replace("'", "")
	rest_name = author_string[ind + 2:]
	author_list.append(first_name)
	while ind > 2:
		if author_string.find("',") != -1:
			ind = rest_name.find("',") + 1
		else:
			ind = rest_name.find(';')
		if ind == -1:
			author_list.append(rest_name.replace("'", ""))
		elif ind == 0:
			ind = -1
			author_list.append(rest_name[:ind].replace("'", ""))
		else: 
			author_list.append(rest_name[:ind].replace("'", ""))
		rest_name = rest_name[ind + 2:] 
	return author_list

def insert_authors(data, num_threads, ctn, session):
	counter = 0
	pool = ThreadPool(num_threads)
	batches = []
	batches_pr = []
	list_of_list_of_authors = []
	for d in data:
		if d['authors'] is not 0:
			list_of_list_of_authors = list_of_list_of_authors + author_names(d['authors'])

	list_of_list_of_authors = set(list_of_list_of_authors)
	for l in list_of_list_of_authors: 
		l = l.replace('"', "'")
		graql = f"""
		insert
		$p isa person, has published-name "{l}";
		"""
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches_pr.append(batches)
			batches = []
	if counter % ctn == 0:
		batches_pr.append(batches)
		batches = []	
	batches_pr.append(batches)
	pool.map(partial(batch_job, session), batches_pr)
	pool.close()
	pool.join()
	print('.....')
	print('Finished inserting authors.')
	print('.....')

def insert_journals(data, num_threads, ctn, session):	
	counter = 0
	pool = ThreadPool(num_threads)
	batches = []
	batches_pr = []
	list_of_journals = []
	for d in data:
		list_of_journals.append(d['journal'])
	list_of_journals = set(list_of_journals)
	for l in list_of_journals:
		try: 
			l = l.replace('"', "'").replace(".", "")
		except Exception: 
			pass
		graql = f"""
		insert
		$p isa journal, has journal-name "{l}";
		"""
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches_pr.append(batches)
			batches = []
	if counter % ctn == 0:
		batches_pr.append(batches)
		batches = []	
	batches_pr.append(batches)
	pool.map(partial(batch_job, session), batches_pr)
	pool.close()
	pool.join()
	print(len(list_of_journals))
	print('.....')
	print('Finished inserting journals.')
	print('.....')

def insert_publications_journals(data, num_threads, ctn, session):	
	counter = 0
	pool = ThreadPool(num_threads)
	batches = []
	batches_pr = []
	list_of_pubs = []
	for d in data:
		pub = {}
		pub['title'] = d['title'].replace('"', "'")
		pub['doi'] = d['doi']
		pub['paper-id'] = d['pubmed_id']
		pub['publish_time'] = d['publish_time']
		pub['journal'] = d['journal']
		list_of_pubs.append(pub)

	for l in list_of_pubs:
		graql = f"""
		match 
		$j isa journal, has journal-name "{l['journal']}"; 
		insert
		$pu isa publication, has title "{l['title']}", has doi "{l['doi']}", 
		has paper-id "{l['paper-id']}", has publish-time "{l['publish_time']}";
		(published-publication: $pu, publishing-journal: $j) isa publishing;
		"""
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches_pr.append(batches)
			batches = []
	if counter % ctn == 0:
		batches_pr.append(batches)
		batches = []	
	batches_pr.append(batches)
	pool.map(partial(batch_job, session), batches_pr)
	pool.close()
	pool.join()
	print(len(list_of_pubs))
	print('.....')
	print('Finished inserting publications and connecting them with journals.')
	print('.....')


def insert_publications_with_authors(data, num_threads, ctn, session):	
	start = timer()
	counter = 0
	pool = ThreadPool(num_threads)
	batches = []
	batches_pr = []
	for d in data: 
		if d['authors'] is not 0:
			authors = author_names(d['authors'])
			author_graql = ""
			counter = 0
			relations_authors = ""
			for a in authors: 
				author_graql = author_graql + "$p" + str(counter) + ' isa person, has published-name "' + a + '"; '
				relations_authors = relations_authors + "(authored-publication: $pu, author: $p" + str(counter) + ") isa authorship; "
				counter = counter + 1
			d['title'] = d['title'].replace('"', "'")
			graql = f"""
			match
			$pu isa publication, has title "{d['title']}"; 
			{author_graql}
			insert 
			{relations_authors}
			"""
			batches.append(graql)
			del graql
		if counter % ctn == 0:
			batches_pr.append(batches)
			batches = []
	if counter % ctn == 0:
		batches_pr.append(batches)
		batches = []	
	batches_pr.append(batches)
	pool.map(partial(batch_job, session), batches_pr)
	pool.close()
	pool.join()
	end = timer()
	time_in_sec = end - start
	print("Elapsed time: " + str(time_in_sec) + " seconds.")
	print('.....')
	print('Finished inserting journals <> pub <> authors.')
	print('.....')
		
def insert_entities_pub(data, num_threads, ctn, session):
	start = timer()
	counter = 0
	pool = ThreadPool(num_threads)
	batches = []
	batches_pr = []
	for d in data: 
		d['title'] = d['title'].replace('"', "'")
		for e in d['entities']:
			ent = {}
			ent['type'] = classify_type(e['type'])
			ent['text'] = e['text'].replace('"',"").replace("'", '')
			ent['start'] = e['start']
			ent['end'] = e['end']
			if ent['type'] != None:
				if ent['type'] != "gene or protein":
					graql = f"""
					match $1 isa {ent['type']}, has {ent['type']}-name "{ent['text']}";
					$p isa publication, has title "{d['title']}"; 
					insert 
					(mentioning: $p, mentioned: $1) isa mention, has start "{e['start']}", has end "{e['end']}";
					"""
				else: 
					graql = f"""
					match $1 isa protein, has uniprot-entry-name "{ent['text']}";
					$p isa publication, has title "{d['title']}"; 
					insert 
					(mentioning: $p, mentioned: $1) isa mention, has start "{e['start']}", has end "{e['end']}";
					"""
					graql2 = f"""
					match 
					$p isa publication, has title "{d['title']}"; 
					$2 isa gene, has gene-symbol "{ent['text']}";
					insert 
					(mentioning: $p, mentioned: $2) isa mention, has start "{e['start']}", has end "{e['end']}";
					"""
					batches.append(graql2)

				counter = counter + 1
				batches.append(graql)
				del graql
				if counter % ctn == 0:
					batches_pr.append(batches)
					batches = []
		if counter % ctn == 0:
			batches_pr.append(batches)
			batches = []
	batches_pr.append(batches)
	pool.map(partial(batch_job, session), batches_pr)
	pool.close()
	pool.join()
	end = timer()
	time_in_sec = end - start
	print("Elapsed time: " + str(time_in_sec) + " seconds.")
	print('.....')
	print('Finished inserting.')
	print('.....')


def classify_type(type):
	if type == "CORONAVIRUS":
		return "virus" 
	if type == "VIRUS":
		return "virus" 
	# if type == "TISSUE":
	# 	return "tissue"
	# if type == "CHEMICAL":
	# 	return "chemical"
	if type == "GENE_OR_GENOME":
		return "gene or protein"
	if type == "DISEASE_OR_SYNDROME":
		return "disease"
	# if type == "CELL_FUNCTION":
	# 	return "cell-function"


