import csv
from typedb.client import TypeDB, SessionType, TransactionType
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from Migrators.Helpers.batchLoader import write_batch
import glob

def openFileVariant(path, tissue_name):
    with open(path, 'rt', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile, delimiter='\t')
        raw_file = []
        n = 0
        for row in csvreader:
            n = n + 1
            if n != 1:
                row.append(tissue_name)
                raw_file.append(row)
        return raw_file

def migrate_tissuenet(session, num_threads, batch_size):
    print('  ')
    print('Reading in the tissue nodes already present in bio-covid')
    print('  ')
    
    existing_tissues = set()
    with session.transaction(TransactionType.READ) as read_transaction:
        read_query_iterator = read_transaction.query().match('''match $t isa tissue, has tissue-name $tn;''')
        for read_result in read_query_iterator:
            tissue_name = read_result.get("tn")
            existing_tissues.add(tissue_name.get_value())
            
    print('Reading in candidate tissues from TissueNet')
            
    path_to_data = "Dataset/TissueNet/HPA-protein/*.tsv"
    candidate_tissues = set()
    for filepath in glob.iglob(path_to_data):
        tissue = filepath.split("/")[-1].split(".")[0]
        candidate_tissues.add(tissue)
        
    print('Finalising Tissues to be uploaded to bio-covid')
    tissue_staging = candidate_tissues.difference(existing_tissues)
    
    print('Inserting Tissues to bio-covid')
    
    if len(tissue_staging): 
        batch = []
        batches = []
        total = 0 
        for tissue in tissue_staging:
            typeql = f'''insert $t isa tissue, has tissue-name "{tissue}";'''
            batch.append(typeql)
            total += 1
            if len(batch) >= batch_size:
                batches.append(batch)
                batch = []
        print(batches)
        
        pool = ThreadPool(num_threads)
        pool.imap_unordered(partial(write_batch, session), batches, 1000)
        pool.join()
        pool.close()
        print(f'  Tissues inserted! ({total} entries)')

        #session.close()
    else:
        print("There were no tissues to upload!!")
        #session.close()

def migrate_tissuenet_ppi(session, num_threads, batch_size):
    
    path_to_data = "Dataset/TissueNet/HPA-protein/*.tsv"
    file_contents = []
    for filepath in glob.iglob(path_to_data):
        tissue = filepath.split("/")[-1].split(".")[0]
        data = openFileVariant(filepath, tissue_name=tissue)
        file_contents.append(data)

    for file_content in file_contents:
        for file_row in file_content:
            ENSP1 = file_row[0]
            ENSP2 = file_row[1]
            #tissue = file_row[-1]
            print(ENSP2)

    # // TODO this seems unfinished?

    #session.close()
    #client.close()
