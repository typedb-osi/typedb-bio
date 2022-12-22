import csv
import os
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from zipfile import ZipFile

from typedb.client import TransactionType

from Migrators.Helpers.batchLoader import write_batch
from Migrators.Helpers.get_file import get_file


def migrate_protein_atlas(session, num, num_threads, batch_size):
    if num == 0: return;

    print('  ')
    print('Opening HPA dataset...')
    print('  ')

    tissue, raw_data = get_tissue_data(num)
    insert_tissue(tissue, session)
    insert_ensemble_id(raw_data, num, session, num_threads, batch_size)
    insert_gene_tissue(raw_data, num, session, num_threads, batch_size)

    print('.....')
    print('Finished migrating HPA.')
    print('.....')


def get_tissue_data(num):
    print('  Downloading protein atlas data')
    get_file('https://www.proteinatlas.org/download/normal_tissue.tsv.zip', 'Dataset/HumanProteinAtlas/')
    print('  Finished downloading')

    with ZipFile('Dataset/HumanProteinAtlas/normal_tissue.tsv.zip', 'r') as f:
        f.extractall('Dataset/HumanProteinAtlas')

    with open('Dataset/HumanProteinAtlas/normal_tissue.tsv', 'rt', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile, delimiter='\t')
        raw_file = []
        n = 0
        for row in csvreader:
            n = n + 1
            if n != 1:
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
    return (tissue, raw_file)


def insert_tissue(tissue, session):
    print('  Starting with tissue.')
    with session.transaction(TransactionType.WRITE) as tx:
        for t in tissue:
            q = 'insert $t isa tissue, has tissue-name "' + t + '";'
            tx.query().insert(q)
        tx.commit()
    print(f'  Finished tissue. ({len(tissue)} entries)')


def insert_ensemble_id(raw_file, num, session, num_threads, batch_size):
    list_of_tuples = []
    for r in raw_file:
        list_of_tuples.append((r['ensembl-gene-id'], r['gene-symbol']))
    list_of_tuples = [t for t in (set(tuple(i) for i in list_of_tuples))]
    batch = []
    batches = []
    total = 0
    print('  Starting ensemble id.')
    for g in list_of_tuples:
        typeql = f"""
        match $g isa gene, has gene-symbol '{g[1]}'; 
        insert
        $g has ensembl-gene-stable-id '{g[0]}'; 
        """
        batch.append(typeql)
        total += 1
        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []
        if total == num:
            break
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.imap_unordered(partial(write_batch, session), batches, 1000)
    pool.close()
    pool.join()
    print(f'  Finished ensemble id! ({total} entries)')


def insert_gene_tissue(raw_file, num, session, num_threads, batch_size):
    batches = []
    batch = []
    total = 0
    print('  Starting expression.')
    for g in raw_file:
        typeql = f"""
        match $g isa gene, has gene-symbol '{g['gene-symbol']}'; 
        $t isa tissue, has tissue-name '{g['tissue']}';
        insert
        (expressing-gene: $g, expressed-tissue: $t) isa expression, 
        has expression-value '{g['expression-value']}',
        has expression-value-reliability '{g['expression-value-reliability']}'; 
        """
        batch.append(typeql)
        total += 1
        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []
        if total == num:
            break
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.imap_unordered(partial(write_batch, session), batches, 1000)
    pool.close()
    pool.join()
    print(f'  Finished Genes <> Tissues expression. ({total} entries)')
