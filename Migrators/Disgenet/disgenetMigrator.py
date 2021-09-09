import gzip, csv, os, itertools

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from Migrators.Helpers.batchLoader import write_batch
from Migrators.Helpers.get_file import get_file


def migrate_disgenet(session, num, num_threads, batch_size):
    if num == 0: return
    print('  ')
    print('Opening Disgenet dataset...')
    print('  ')
    insert_gene_disease(session, num, num_threads, batch_size)
    print('.....')
    print('Finished migrating Disgenet.')
    print('.....')


def insert_gene_disease(session, num, num_threads, batch_size):
    print("  Downloading Disgenet dataset")
    url = "https://www.disgenet.org/static/disgenet_ap1/files/downloads/all_gene_disease_associations.tsv.gz"
    get_file(url, 'Dataset/Disgenet/')
    print("\n Finished downloading dataset")

    with gzip.open('Dataset/Disgenet/all_gene_disease_associations.tsv.gz', 'rt') as f:
        csvreader = csv.reader(f, delimiter='\t')
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
    insert_diseases(disgenet, session, num_threads, batch_size)

    batches = []
    batch = []
    total = 0
    print("  Starting with gene disease associations.")
    for q in disgenet:
        typeql = f"""
match $g isa gene, has gene-symbol "{q['gene-symbol']}", has entrez-id "{q['entrez-id']}";
$d isa disease, has disease-id "{q['disease-id']}", has disease-name "{q['disease-name']}";
insert $r (associated-gene: $g, associated-disease: $d) isa gene-disease-association, has disgenet-score {q['disgenet-score']};"""
        batch.append(typeql)
        total += 1
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print(f' gene-disease associations inserted! ({total} entries)')


def insert_diseases(disgenet, session, num_threads, batch_size):
    print('  Starting with diseases.')
    disease_list = []
    batch = []
    batches = []
    for q in disgenet:
        disease_list.append([q['disease-name'], q['disease-id']])
    disease_list.sort()
    disease_list = list(disease_list for disease_list, _ in itertools.groupby(disease_list))  # Remove duplicates

    total = 0
    for d in disease_list:
        typeql = f'insert $d isa disease, has disease-name "{d[0]}", has disease-id "{d[1]}";'
        batch.append(typeql)
        total += 1
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print(f' Diseases inserted! ({total} entries)')
