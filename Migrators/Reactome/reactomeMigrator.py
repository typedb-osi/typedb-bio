import itertools
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

from grakn.client import GraknClient

from Migrators.Helpers.batchLoader import batch_job
from Migrators.Helpers.open_file import openFile


def reactomeMigrator(uri, keyspace, num_path, num_threads, ctn):
	client = GraknClient(uri=uri)
	session = client.session(keyspace=keyspace)
	pathway_associations = filterHomoSapiens(num_path)
	insertPathways(uri, keyspace, num_threads, ctn, session, pathway_associations)
	insertPathwayInteractions(uri, keyspace, num_threads, ctn, session, pathway_associations)
	session.close()
	client.close()
			
def insertPathways(uri, keyspace, num_threads, ctn, session, pathway_associations): 
	pathway_list = []
	for p in pathway_associations: 
		pathway_list.append([p['pathway-id'], p['pathway-name']]) 
	pathway_list.sort()
	pathway_list = list(pathway_list for pathway_list,_ in itertools.groupby(pathway_list)) # Remove duplicates

	counter = 0
	batches = []
	batches2 = []

	tx = session.transaction().write()
	pool = ThreadPool(num_threads)
	for d in pathway_list:
		counter = counter + 1
		graql = f'''insert $p isa pathway, has pathway-name "{d[1]}", has pathway-id "{d[0]}";'''
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Pathways committed!')

def insertPathwayInteractions(uri, keyspace, num_threads, ctn, session, pathway_associations): 
	counter = 0
	batches = []
	batches2 = []

	print(len(pathway_associations))
	tx = session.transaction().write()
	pool = ThreadPool(num_threads)
	for d in pathway_associations:
		counter = counter + 1
		graql = f'''match $p isa pathway, has pathway-id "{d['pathway-id']}"; $pr isa protein, has uniprot-id "{d['uniprot-id']}"; insert (participated-pathway: $p, participating-protein: $pr) isa pathway-participation;'''
		batches.append(graql)
		del graql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
		print(counter)
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Pathways committed!')


def filterHomoSapiens(num_path):
	file = 'Dataset/Reactome/UniProt2Reactome_All_Levels.tsv'
	print('  ')
	print('Opening Reactome...')
	print('  ')
	raw_file = openFile(file, num_path)
	pathway_associations = []
	for i in raw_file[:num_path]:
		if i[5] == "Homo sapiens":
			data = {}
			data['uniprot-id'] = i[0].strip('"')
			data['pathway-id'] = i[1].strip('"')
			data['pathway-name'] = i[3]
			data['organism'] = i[5]
			pathway_associations.append(data)
	return pathway_associations