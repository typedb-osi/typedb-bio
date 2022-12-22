import itertools
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
import wget
import ssl, os

from Migrators.Helpers.batchLoader import write_batch
from Migrators.Helpers.open_file import openFile


def migrate_reactome(session, num_path, num_threads, batch_size):
    print('.....')
    print('Starting with reactome.')
    print('.....')

    pathway_associations = filter_homo_sapiens(num_path)
    insert_pathways(session, num_threads, batch_size, pathway_associations)
    insert_pathway_interactions(session, num_threads, batch_size, pathway_associations)

    print('.....')
    print('Finished with reactome.')
    print('.....')


def insert_pathways(session, num_threads, batch_size, pathway_associations):
    print('  Starting with reactome pathways.')
    pathway_list = []
    for p in pathway_associations:
        pathway_list.append([p['pathway-id'], p['pathway-name']])
    pathway_list.sort()
    pathway_list = list(pathway_list for pathway_list, _ in itertools.groupby(pathway_list))  # Remove duplicates

    batch = []
    batches = []
    total = 0
    for d in pathway_list:
        typeql = f'''insert $p isa pathway, has pathway-name "{d[1]}", has pathway-id "{d[0]}";'''
        batch.append(typeql)
        total += 1
        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print(f'  Reactome Pathways inserted! ({total} entries)')


def insert_pathway_interactions(session, num_threads, batch_size, pathway_associations):
    print('  Starting with reactome pathway interactions.')
    batch = []
    batches = []
    total = 0
    for d in pathway_associations:
        typeql = f'''match $p isa pathway, has pathway-id "{d['pathway-id']}"; $pr isa protein, has uniprot-id "{d['uniprot-id']}"; insert (participated-pathway: $p, participating-protein: $pr) isa pathway-participation;'''
        batch.append(typeql)
        total += 1
        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print(f' Reactome pathway interactions inserted! ({total} entries)')


def filter_homo_sapiens(num_path):
    print("Downloading reactome dataset")
    ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt"
    wget.download(url, 'Dataset/Reactome/')
    print("\nFinished downloading reactome dataset")
    file = 'Dataset/Reactome/UniProt2Reactome_All_Levels.txt'
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
