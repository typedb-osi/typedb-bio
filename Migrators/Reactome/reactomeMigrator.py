import itertools
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
import wget
from typedb.client import TypeDB, SessionType, TransactionType
import ssl, os

from Migrators.Helpers.batchLoader import batch_job
from Migrators.Helpers.open_file import openFile



def reactomeMigrator(uri, database, num_path, num_threads, ctn):
	client = TypeDB.core_client(uri)
	session = client.session(database, SessionType.DATA)
	pathway_associations = filterHomoSapiens(num_path)
	insertPathways(uri, database, num_threads, ctn, session, pathway_associations)
	insertPathwayInteractions(uri, database, num_threads, ctn, session, pathway_associations)
	session.close()
	client.close()
			
def insertPathways(uri, database, num_threads, ctn, session, pathway_associations): 
	pathway_list = []
	for p in pathway_associations: 
		pathway_list.append([p['pathway-id'], p['pathway-name']]) 
	pathway_list.sort()
	pathway_list = list(pathway_list for pathway_list,_ in itertools.groupby(pathway_list)) # Remove duplicates

	counter = 0
	batches = []
	batches2 = []

	pool = ThreadPool(num_threads)
	for d in pathway_list:
		counter = counter + 1
		typeql = f'''insert $p isa pathway, has pathway-name "{d[1]}", has pathway-id "{d[0]}";'''
		batches.append(typeql)
		del typeql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Pathways committed!')

def insertPathwayInteractions(uri, database, num_threads, ctn, session, pathway_associations, verbose=False): 
	counter = 0
	batches = []
	batches2 = []

	print(len(pathway_associations))
	pool = ThreadPool(num_threads)
	for d in pathway_associations:
		counter = counter + 1
		typeql = f'''match $p isa pathway, has pathway-id "{d['pathway-id']}"; $pr isa protein, has uniprot-id "{d['uniprot-id']}"; insert (participated-pathway: $p, participating-protein: $pr) isa pathway-participation;'''
		batches.append(typeql)
		del typeql
		if counter % ctn == 0:
			batches2.append(batches)
			batches = []
		if verbose == True:
			print(counter)
	batches2.append(batches)
	pool.map(partial(batch_job, session), batches2)
	pool.close()
	pool.join()
	print('Pathways committed!')


def filterHomoSapiens(num_path):
	ssl._create_default_https_context = ssl._create_unverified_context
	url = "https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt"
	wget.download(url, 'Dataset/Reactome/')
	file = 'Dataset/Reactome/UniProt2Reactome_All_Levels.txt'
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
	os.remove('Dataset/Reactome/UniProt2Reactome_All_Levels.txt')
	return pathway_associations