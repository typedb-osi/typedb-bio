import csv

from typedb.client import TypeDB, SessionType, TransactionType


def tissueNetMigrator(uri, database, num, num_threads, batch_size):
    client = TypeDB.core_client(uri)
    session = client.session(database, SessionType.DATA)
    batches_pr = []

    if num != 0:
        print('  ')
        print('Opening TissueNet dataset...')
        print('  ')

        with open('Dataset/TissueNet/HPA-Protein.tsv', 'rt', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile, delimiter='\t')
            raw_file = []
            n = 0
            for row in csvreader:
                n = n + 1
                if n != 1:
                    raw_file.append(row)

    # // TODO this seems unfinished?

    session.close()
    client.close()