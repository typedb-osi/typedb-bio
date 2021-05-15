import csv
# from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing
import pandas as pd
import untangle
from grakn.client import Grakn, SessionType, TransactionType

from Migrators.Helpers.batchLoader import batch_job

def migrate_semmed(uri, database, num_semmed, num_threads, ctn):
    
    print("Migrate 'Subject_CORD_NER.csv'")

    file_path = "Dataset/SemMed/Subject_CORD_NER.csv"
    raw_file = openFile(file_path, 1)[:num_semmed]
    pmids_set = list(set([tupple[3] for tupple in raw_file]))        #get set of pmids

    ###Fetch articles metadata from pubmed
    xml_articles_data = fetch_articles_metadata(pmids_set)
    journal_names = get_journal_names(xml_articles_data)
    author_names = get_authors_names(xml_articles_data)
    publications_list = get_publication_data(xml_articles_data)
    relationship_data = get_relationship_data('Dataset/SemMed/Subject_CORD_NER.csv')[:num_semmed]

    print("--------Loading journals---------")
    load_in_parallel(migrate_journals, journal_names, num_threads, ctn, uri, database)
    print("--------Loading authors---------")
    load_in_parallel(migrate_authors, author_names, num_threads, ctn, uri, database)
    print("--------Loading publications--------")
    load_in_parallel(migrate_publications, publications_list, num_threads, ctn, uri, database)
    print("--------Loading relations----------")
    load_in_parallel(migrate_relationships, relationship_data, num_threads, ctn, uri, database)

    print("Migrate 'Object_CORD_NER.csv'")

    file_path = "Dataset/SemMed/Object_CORD_NER.csv"
    raw_file = openFile(file_path, 1)[:num_semmed]
    pmids_set = list(set([tupple[3] for tupple in raw_file]))        #get set of pmids

    ###Fetch articles metadata from pubmed
    xml_articles_data = fetch_articles_metadata(pmids_set)
    journal_names = get_journal_names(xml_articles_data)
    author_names = get_authors_names(xml_articles_data)
    publications_list = get_publication_data(xml_articles_data)
    relationship_data = get_relationship_data('Dataset/SemMed/Object_CORD_NER.csv')[:num_semmed]

    print("--------Loading journals---------")
    load_in_parallel(migrate_journals, journal_names, num_threads, ctn, uri, database)
    print("--------Loading authors---------")
    load_in_parallel(migrate_authors, author_names, num_threads, ctn, uri, database)
    print("--------Loading publications--------")
    load_in_parallel(migrate_publications, publications_list, num_threads, ctn, uri, database)
    print("--------Loading relations----------")
    load_in_parallel(migrate_relationships, relationship_data, num_threads, ctn, uri, database)
    
def get_journal_names(xml_response):

    journals_set = set()

    xml_object = untangle.parse(xml_response)

    for document in xml_object.eSummaryResult.DocSum:
        is_published_in_journal = False

        journal_name = ""
        for item in document.Item:
            if item["Name"] == "PubTypeList":
                try:
                    pubtypes = item.Item
                except:
                    continue
                for pubtype in pubtypes:
                    if pubtype.cdata == "Journal Article": is_published_in_journal = True
            if item["Name"] == "FullJournalName":
                journal_name = item.cdata
        if is_published_in_journal:
            journals_set.add(journal_name)

    return list(journals_set)

def migrate_journals(uri, database, journal_names: list, ctn, process_id = 0):
    '''
    Migrate journals to Grakn \n
    journal_names - list of journal names (strings) \n
    process_id - process id while running on multiple cores, by process_id = 0
    '''
    with Grakn.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            counter = 0
            transaction = session.transaction(TransactionType.WRITE)
            for journal_name in journal_names:
                ##Check if journal already in Knowledge Base
                try:
                    match_query = 'match $j isa journal, has journal-name "{}";'.format(journal_name)
                    next(transaction.query().match(match_query))
                except StopIteration:
                    insert_query = 'insert $j isa journal, has journal-name "{}";'.format(journal_name)
                    transaction.query().insert(insert_query)
                if counter % ctn == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    print("Process {} COMMITED".format(process_id))
                print("Process {} ----- {} journals added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()

def get_authors_names(xml_response):
    '''
    Retrieve unique author names from PubMed API's xml response in a form of list of strings\n
    xml_response - xml object returned from PubMed API
    '''

    authors_set = set()

    xml_object = untangle.parse(xml_response)

    for document in xml_object.eSummaryResult.DocSum:

        for item in document.Item:
            if item["Name"] == "AuthorList":
                try:
                    authors = item.Item
                except:
                    continue
                for author in authors:
                    authors_set.add(author.cdata)

    return list(authors_set)


def migrate_authors(uri, database, author_names: list, ctn, process_id = 0):
    '''
    Migrate authors to Grakn\n
    author_names - list of author names (strings)\n
    process_id - process id while running on multiple cores, by process_id = 0
    '''
    with Grakn.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            counter = 0
            transaction = session.transaction(TransactionType.WRITE)
            for author_name in author_names:
                ##Check if journal already in Knowledge Base
                try:
                    match_query = 'match $a isa person, has published-name "{}";'.format(author_name)
                    next(transaction.query().match(match_query))
                except StopIteration:
                    insert_query = 'insert $a isa person, has published-name "{}";'.format(author_name)
                    transaction.query().insert(insert_query)
                if counter % ctn == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    print("Process {} COMMITED".format(process_id))
                print("Process {} ----- {} authors added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()

def get_publication_data(xml_response):
    '''
    Retrieve publication data from PubMed API's xml response in a form of list of dictionaries\n
    xml_response - xml object returned from PubMed API\n
    '''
    publications = []
    pmid_set = set()

    xml_object = untangle.parse(xml_response)

    for document in xml_object.eSummaryResult.DocSum:

        publication = {}
        publication["paper-id"] = document.Id.cdata
        publication["pmid"] = document.Id.cdata
        publication["doi"] = ""
        publication["authors"] = []
        publication["issn"] = ""
        publication["volume"] = ""
        publication["journal-name"] = ""
        publication["publish-time"] = ""
        authors_list = []
        
        for item in document.Item:
            if item["Name"] == "Title":
                publication["title"] = item.cdata.replace('"',"'")
            elif item["Name"] == "PubDate":
                publication["publish-time"] = item.cdata
            elif item["Name"] == "Volume":
                publication["volume"] = item.cdata
            elif item["Name"] == "ISSN":
                publication["issn"] = item.cdata
            elif item["Name"] == "FullJournalName":
                publication["journal-name"] = item.cdata.replace('"',"'")
            elif item["Name"] == "DOI":
                publication["doi"] = item.cdata
            elif item["Name"] == "AuthorList":
                try:
                    authors = item.Item
                except:
                    continue
                for author in authors:
                    authors_list.append(author.cdata)
                publication["authors"] = authors_list
            if publication["paper-id"] not in pmid_set: 
                publications.append(publication)
                pmid_set.add(publication["paper-id"])

    return list(publications)

def migrate_publications(uri, database, publications_list: list, ctn, process_id = 0):
    '''
    Migrate publiations to Grakn\n
    publications_list - list of dictionaries with publication data\n
    process_id - process id while running on multiple cores, by process_id = 0
    '''
    with Grakn.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            counter = 0
            transaction = session.transaction(TransactionType.WRITE)
            for publication_dict in publications_list:
                authors = publication_dict["authors"]   #list of authors - list of strings
                ##Check if publication already in Knowledge Base
                try:
                    match_query = 'match $p isa publication, has paper-id "{}";'.format(publication_dict["paper-id"])
                    next(transaction.query().match(match_query))
                except StopIteration:
                    match_query = 'match $j isa journal, has journal-name "{}"; '.format(publication_dict["journal-name"])
                    match_query = match_query + create_authorship_query(authors)[0]
                    insert_query = 'insert $p isa publication, has paper-id "{}", has title "{}", has doi "{}", has publish-time "{}", has volume "{}", has issn "{}", has pmid "{}"; '.format(publication_dict["paper-id"], publication_dict["title"], publication_dict["doi"],publication_dict["publish-time"],publication_dict["volume"],publication_dict["issn"],publication_dict["pmid"])
                    insert_query = insert_query + create_authorship_query(authors)[1]
                    insert_query = insert_query + '(publishing-journal: $j, published-publication: $p) isa publishing;'
                    # print(match_query+insert_query)
                    transaction.query().insert(match_query + insert_query)
                if counter % ctn == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    print("Process {} COMMITED".format(process_id))
                print("Process {} ----- {} publications added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()

def create_authorship_query(authors_list):
    match_query = ''
    insert_query = ''
    for counter, author in enumerate(authors_list):
        match_query = match_query + '$pe{} isa person, has published-name "{}"; '.format(counter, author)
        insert_query = insert_query + '(author: $pe{}, authored-publication: $p) isa authorship; '.format(counter)

    return [match_query, insert_query]

def get_relationship_data(data_path):
    '''returns unique relations in a form of list of lists'''
    data_df = pd.read_csv(data_path, sep = ';', usecols=["P_PMID", "P_PREDICATE", "P_SUBJECT_NAME", "P_OBJECT_NAME", "S_SENTENCE"])
    data_df = data_df.drop_duplicates()
    return data_df.to_numpy().tolist()

def migrate_relationships(uri, database, data: list, ctn, process_id = 0):
    '''
    Migrate relations to Grakn\n
    data - table in a form of list of lists \n
    process_id - process id while running on multiple cores, by process_id = 0
    '''
    with Grakn.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            counter = 0
            transaction = session.transaction(TransactionType.WRITE)
            for data_entity in data:
                predicate_name = data_entity[1]
                subject_name = data_entity[2]
                object_name = data_entity[3]
                relation = relationship_mapper(predicate_name)  #add handler for situation when there is no relation implemented in a mapper
                pmid = data_entity[0]
                sentence_text = data_entity[4].replace('"',"'")

                match_query = 'match $p isa publication, has paper-id "{}"; $g1 isa gene, has gene-symbol "{}"; $g2 isa gene, has gene-symbol "{}"; '.format(data_entity[0], data_entity[2], data_entity[3])
                insert_query = 'insert $r ({}: $g1, {}: $g2) isa {}, has sentence-text "{}"; $m (mentioned-genes-relation: $r, mentioning: $p) isa mention, has source "SemMed";'.format(relation["active-role"], relation["passive-role"], relation["relation-name"], sentence_text)
                transaction.query().insert(match_query + insert_query)
                print(match_query + insert_query)
                if counter % ctn == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    print("Process {} COMMITED".format(process_id))
                print("Process {} ----- {} relations added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()

def fetch_articles_metadata(pmids):
    '''
    function - function name to run in paralell\n
    data - data to load by function running in parallel
    '''
    import requests

    ids_param = ""

    for pmid in pmids:
        ids_param = ids_param + "," + str(pmid)
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'

    params = {
        'db':'pubmed',
        'id':ids_param,
        'retmode':'xml'
        }

    print('Sending API call to fetch articles Metadata')

    response = requests.post(url, data=params)
    xml_response = response.text
    return xml_response

def load_in_parallel(function, data, num_threads, ctn, uri, database):
    '''
    Runs a specific function to load data to Grakn several time in parallel\n
    function - function name to run in paralell\n
    data - data to load by function running in parallel
    '''
    # start_time = datetime.datetime.now()

    chunk_size = int(len(data)/num_threads)
    processes = []

    for i in range(num_threads):

        if i == num_threads - 1:
            chunk = data[i*chunk_size:]
            
        else:
            chunk = data[i*chunk_size:(i+1)*chunk_size]
    
        process = multiprocessing.Process(target = function, args = (uri, database, chunk, ctn, i))

        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    # end_time = datetime.datetime.now()
    # print("-------------\nTime taken: {}".format(end_time - start_time))
    
def relationship_mapper(relationship: str):
    '''
    Input: predicate name from SemMed db (string)\n
    Output: dictionary with relation name and roles for specific predicate
    '''
    mapper = {
            "INHIBITS" : {"relation-name" : "inhibition", "passive-role" : "inhibited", "active-role" : "inhibiting"},
            "INTERACTS_WITH" : {"relation-name" : "interaction", "passive-role" : "interacted", "active-role" : "interacting"},
            "COEXISTS_WITH" : {"relation-name" : "coexistance", "passive-role" : "coexisting", "active-role" : "coexisting"},
            "compared_with" : {"relation-name" : "comparison", "passive-role" : "compared", "active-role" : "comparing"},
            "higher_than" : {"relation-name" : "comparison", "passive-role" : "lower", "active-role" : "higher"},
            "STIMULATES" : {"relation-name" : "stimulation", "passive-role" : "stimulated", "active-role" : "stimulating"},
            "CONVERTS_TO" : {"relation-name" : "conversion", "passive-role" : "converted", "active-role" : "converting"},
            "PRODUCES" : {"relation-name" : "production", "passive-role" : "produced", "active-role" : "producing"},
            "NEG_COEXISTS_WITH" : {"relation-name" : "neg-coexistance", "passive-role" : "neg-coexisting", "active-role" : "neg-coexisting"},
            "NEG_INHIBITS" : {"relation-name" : "neg-inhibition", "passive-role" : "neg-inhibited", "active-role" : "neg-inhibiting"},
            "NEG_INTERACTS_WITH" : {"relation-name" : " neg-interaction", "passive-role" : "neg-interacted", "active-role" : "neg-interacting"},
            "NEG_STIMULATES" : {"relation-name" : "neg-stimulation", "passive-role" : "neg-stimulating", "active-role" : "neg-stimulated"},
            "NEG_PRODUCES" : {"relation-name" : "neg-production", "passive-role" : "neg-producing", "active-role" : "neg-produced"},
            "lower_than" : {"relation-name" : "comparison", "passive-role" : "higher", "active-role" : "lower"},
            "NEG_PART_OF" : {"relation-name" : "neg-constitution", "passive-role" : "neg-constituting", "active-role" : "neg-constituted"},
            "same_as" : {"relation-name" : "similarity", "passive-role" : "similar", "active-role" : "similar"},
            "NEG_same_as" : {"relation-name" : "neg-similarity", "passive-role" : "neg-similar", "active-role" : "neg-similar"},
            "LOCATION_OF" : {"relation-name" : "location", "passive-role" : "locating", "active-role" : "located"},
            "PART_OF" : {"relation-name" : "constitution", "passive-role" : "constituting", "active-role" : "constituted"},
            "NEG_higher_than" : {"relation-name" : "neg-comparison", "passive-role" : "neg-higher", "active-role" : "neg-lower"},
            "NEG_CONVERTS_TO" : {"relation-name" : "neg-conversion", "passive-role" : "neg-converting", "active-role" : "neg-converted"},
            "DISRUPTS" : {"relation-name" : "disruption", "passive-role" : "disrupting", "active-role" : "disrupted"},
            "AUGMENTS" : {"relation-name" : "augmentation", "passive-role" : "augmenting", "active-role" : "augmented"},
            "AFFECTS" : {"relation-name" : "affection", "passive-role" : "affecting", "active-role" : "affected"},
            "ASSOCIATED_WITH" : {"relation-name" : "association", "passive-role" : "associated", "active-role" : "associating"}
    }
    mapping = mapper.get(relationship, {})

    return mapping

def openFile(filePath, num):
    if num != 0:
        with open(filePath) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=';')
            raw_file = []
            n = 0
            for row in csvreader: 
                n = n + 1
                if n != 1:
                    raw_file.append(row)
    return raw_file
