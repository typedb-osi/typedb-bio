import os
import ssl

import wget

from Migrators.Helpers.batchLoader import write_batches
from Migrators.Helpers.get_file import get_file
from Migrators.Helpers.read_csv import read_tsv


def migrate_dgibd(session, num_dr, num_int, num_threads, batch_size):
    print('  ')
    print('Opening DGIdb...')
    print('  ')
    insert_drugs(session, num_dr, num_threads, batch_size)
    insert_interactions(session, num_int, num_threads, batch_size)
    print('.....')
    print('Finished migrating DGIdb.')
    print('.....')


def insert_drugs(session, num_dr, num_threads, batch_size):
    # from Migrators.Helpers.get_file import get_file
    print('  Downloading dataset')
    get_file("https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv", "Dataset/DGIdb/")
    print('  Finished downloading')
    file = 'Dataset/DGIdb/drugs.tsv'

    rows = read_tsv(file, num_dr)
    drugs = []
    for row in rows[:num_dr]:
        data = {}
        data['drug-claim-name'] = row[0].strip('"')
        data['drug-name'] = row[1].strip('"')
        data['chembl-id'] = row[2]
        data['drug-claim-source'] = row[3]
        drugs.append(data)
    os.remove('Dataset/DGIdb/drugs.tsv')
    drugs_list = drugs
    print('  Starting with drugs.')
    queries = []
    total = 0
    for d in drugs_list:
        typeql = f'''insert $d isa drug, has drug-claim-name "{d['drug-claim-name']}", has drug-name "{d['drug-name']}", has chembl-id "{d['chembl-id']}", has drug-claim-source "{d['drug-claim-source']}";'''
        queries.append(typeql)
        total += 1
    write_batches(session, queries, batch_size, num_threads)
    print(f'  Drugs inserted! ({total} entries)')


def insert_interactions(session, num_int, num_threads, batch_size):
    print('  Downloading drug-gene interactions dataset')
    ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://www.dgidb.org/data/monthly_tsvs/2021-Jan/interactions.tsv"
    wget.download(url, 'Dataset/DGIdb/')
    print('  Finished downloading')
    file = 'Dataset/DGIdb/interactions.tsv'
    rows = read_tsv(file, num_int)

    interactions = []
    for row in rows[:num_int]:
        data = {}
        data['gene-name'] = row[0]
        data['entrez-id'] = row[2]
        data['interaction-type'] = row[4]
        data['drug-claim-name'] = row[5]
        data['drug-name'] = row[7]
        data['chembl-id'] = row[8]
        interactions.append(data)
    os.remove('Dataset/DGIdb/interactions.tsv')
    print('  Starting with drug-gene interactions.')
    queries = []
    total = 0
    for q in interactions:
        if q['entrez-id'] != "":
            typeql = f"""match $g isa gene, has entrez-id "{q['entrez-id']}"; $d isa drug, has drug-claim-name "{q['drug-claim-name']}";"""
            # TODO Insert interaction type as a role
            if q['interaction-type'] == "":
                typeql = typeql + f"insert $r (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction;"
            else:
                typeql = typeql + f"""insert $r (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction, has interaction-type "{q['interaction-type']}";"""
            queries.append(typeql)
            total += 1
    write_batches(session, queries, batch_size, num_threads)
    print(f'  Finished drug-gene interactions. ({total} entries) ')
