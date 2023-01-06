import csv
from zipfile import ZipFile
from typedb.client import TypeDB, SessionType, TransactionType
from Migrators.Helpers.batchLoader import write_batches
from Migrators.Helpers.get_file import get_file
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

    print("Downloading HPA - Protein dataset from TissueNet")
    url = "https://netbio.bgu.ac.il/tissuenet2-interactomes/TissueNet2.0/HPA-Protein.zip"
    get_file(url, 'Dataset/TissueNet/HPA-Protein')
    print("\n Finished downloading dataset")

    with ZipFile('Dataset/TissueNet/HPA-Protein/HPA-Protein.zip', 'r') as f:
        f.extractall('Dataset/TissueNet/HPA-Protein/')

    print('  ')
    print('Reading in the tissue nodes already present in typedb-bio')
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

    print('Finalising Tissues to be uploaded to typedb-bio')
    tissue_staging = candidate_tissues.difference(existing_tissues)
    
    print('Inserting Tissues to typedb-bio')
    
    if len(tissue_staging): 
        queries = []
        total = 0 
        for tissue in tissue_staging:
            typeql = f'''insert $t isa tissue, has tissue-name "{tissue}";'''
            queries.append(typeql)
            total += 1
        
        write_batches(session, queries, batch_size, num_threads)
        print(f'  Tissues inserted! ({total} entries)')

    else:
        print("There were no tissues to upload!!")

def migrate_tissuenet_ppi(session, num_threads, batch_size):

    # TODO: Protein interactions in tissues

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
