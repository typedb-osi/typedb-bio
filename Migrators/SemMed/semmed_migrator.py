import csv
import multiprocessing
import os

import pandas as pd
import requests
import untangle
from tqdm import tqdm
from typedb.client import TypeDB, SessionType, TransactionType


def migrate_semmed(session, uri, num_semmed, num_threads, batch_size):
    """
    Import SemMed data into the database.
    :param session: The session
    :param uri: The SemMed uri
    :param num_semmed: The number of SemMed data to import
    :param num_threads: The number of threads to use for importing
    :param batch_size: The batch size for adding into the database.
    """
    print("Migrate 'Subject_CORD_NER.csv'")

    file_path = "Dataset/SemMed/Subject_CORD_NER.csv"
    author_names, journal_names, publications_list, relationship_data = load_data_from_file(file_path, num_semmed)

    load_dataset_in_parallel(author_names, batch_size, journal_names, num_threads, publications_list, relationship_data,
                             session, uri)

    print("Migrate 'Object_CORD_NER.csv'")

    file_path = "Dataset/SemMed/Object_CORD_NER.csv"
    author_names, journal_names, publications_list, relationship_data = load_data_from_file(file_path, num_semmed)

    load_dataset_in_parallel(author_names, batch_size, journal_names, num_threads, publications_list, relationship_data,
                             session, uri)


def load_data_from_file(file_path, num_semmed):
    """
    Load data from file. This function is used to load data from the file and return the data in a list.
    :param file_path: The path to the file
    :param num_semmed: The number of SemMed data to import
    :return: A list of data
    """
    raw_file = openFile(file_path, 1)[:num_semmed]
    pmids_set = list(set([tupple[3] for tupple in raw_file]))  # get set of pmids
    # Fetch articles metadata from pubmed
    xml_articles_data, failed_articles = fetch_articles_metadata(pmids_set)
    journal_names = get_journal_names(xml_articles_data)
    author_names = get_authors_names(xml_articles_data)
    publications_list = get_publication_data(xml_articles_data)
    relationship_data = get_relationship_data(file_path)[:num_semmed]
    return author_names, journal_names, publications_list, relationship_data


def load_dataset_in_parallel(author_names, batch_size, journal_names, num_threads, publications_list, relationship_data,
                             session, uri):
    """
    Load dataset in parallel
    :param author_names: The list of author names
    :param batch_size: The number of data to be added into the database at once
    :param journal_names: The list of journal names
    :param num_threads:  The number of threads to use for importing
    :param publications_list: The list of publications
    :param relationship_data: The list of relationship data
    :param session: Database session
    :param uri: SemMed uri
    """
    print("--------Loading journals---------")
    load_in_parallel(session, uri, migrate_journals, journal_names, num_threads, batch_size)
    print("--------Loading authors---------")
    load_in_parallel(session, uri, migrate_authors, author_names, num_threads, batch_size)
    print("--------Loading publications--------")
    load_in_parallel(session, uri, migrate_publications, publications_list, num_threads, batch_size)
    print("--------Loading relations----------")
    load_in_parallel(session, uri, migrate_relationships, relationship_data, num_threads, batch_size)


def get_journal_names(xml_objects):
    """
    Get journal names from xml objects
    :param xml_objects:
    :return: A list of journal names
    """
    journals_set = set()

    # xml_object = untangle.parse(xml_response)
    for xml_object in xml_objects:
        for document in xml_object:
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


def migrate_journals(uri, database, journal_names: list, batch_size, process_id=0):
    """
    Migrate journals to TypeDB \n
    journal_names - list of journal names (strings) \n
    process_id - process id while running on multiple cores, by process_id = 0
    """
    counter = 0
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for journal_name in journal_names:
                # Check if journal already in Knowledge Base
                try:
                    match_query = 'match $j isa journal, has journal-name "{}";'.format(journal_name)
                    next(transaction.query().match(match_query))
                except StopIteration:
                    insert_query = 'insert $j isa journal, has journal-name "{}";'.format(journal_name)
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} journals added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()


def get_authors_names(xml_response):
    """
    Retrieve unique author names from PubMed API's xml response in a form of list of strings\n
    xml_response - xml object returned from PubMed API
    :param xml_response: The xml response from PubMed API
    :return: List of author names
    """

    authors_set = set()

    # xml_object = untangle.parse(xml_response)
    for xml_object in xml_response:
        for document in xml_object:

            for item in document.Item:
                if item["Name"] == "AuthorList":
                    try:
                        authors = item.Item
                    except:
                        continue
                    for author in authors:
                        authors_set.add(author.cdata)

    return list(authors_set)


def clean_string_for_database(string):
    """
    Clean string from special characters for database
    :param string: Raw string
    :return: Return string without special characters
    """
    string = string.replace("(", "")
    string = string.replace(")", "")
    string = string.replace("[", "")
    string = string.replace("]", "")
    string = string.replace("{", "")
    string = string.replace("}", "")
    string = string.replace(":", "")
    string = string.replace(";", "")
    string = string.replace(",", "")
    string = string.replace(".", "")
    string = string.replace("!", "")
    string = string.replace("?", "")
    string = string.replace("=", "")
    string = string.replace("+", "")
    string = string.replace("-", "")
    string = string.replace("*", "")
    string = string.replace("/", "")
    string = string.replace("\\", "")
    string = string.replace("|", "")
    string = string.replace(">", "")
    string = string.replace("<", "")
    string = string.replace("@", "")
    string = string.replace("#", "")
    string = string.replace("$", "")
    string = string.replace("%", "")
    string = string.replace("^", "")
    string = string.replace("&", "")
    string = string.replace("~", "")
    string = string.replace("_", "")
    string = string.replace('"', "'")
    return string


def migrate_authors(uri, database, author_names: list, batch_size, process_id=0):
    """
    Migrate authors to TypeDB\n
    :param uri: The uri of the TypeDB server
    :param database: The name of the database
    :param author_names: The list of author names
    :param batch_size: The batch size for commiting
    :param process_id: Unused. The process id while running on multiple cores, by default 0
    """
    counter = 0
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for author_name in author_names:
                ##Check if journal already in Knowledge Base
                author_name = clean_string_for_database(author_name)
                try:
                    match_query = 'match $a isa person, has published-name "{}";'.format(author_name)
                    next(transaction.query().match(match_query))
                except StopIteration:
                    insert_query = 'insert $a isa person, has published-name "{}";'.format(author_name)
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} authors added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()


def get_publication_data(xml_response):
    """
    Retrieve publication data from PubMed API's xml response in a form of list of dictionaries\n
    :param xml_response: The xml response from PubMed API
    :return: The list of dictionaries with publication data
    """
    publications = []
    pmid_set = set()
    for xml_object in xml_response:
        for document in xml_object:

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
                    publication["title"] = item.cdata.replace('"', "'")
                elif item["Name"] == "PubDate":
                    publication["publish-time"] = item.cdata
                elif item["Name"] == "Volume":
                    publication["volume"] = item.cdata
                elif item["Name"] == "ISSN":
                    publication["issn"] = item.cdata
                elif item["Name"] == "FullJournalName":
                    publication["journal-name"] = item.cdata.replace('"', "'")
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


def migrate_publications(uri, database, publications_list: list, batch_size, process_id=0):
    """
    Migrate publiations to TypeDB\n
    publications_list - list of dictionaries with publication data\n
    process_id - process id while running on multiple cores, by process_id = 0
    :param uri: The uri of the TypeDB server
    :param database: The name of the database
    :param publications_list: List of dictionaries with publication data
    :param batch_size: The batch size for commiting
    :param process_id: The process id while running on multiple cores, by default 0
    """
    counter = 0
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for publication_dict in publications_list:
                authors = publication_dict["authors"]  # list of authors - list of strings
                ##Check if publication already in Knowledge Base
                try:
                    match_query = 'match $p isa publication, has paper-id "{}";'.format(publication_dict["paper-id"])
                    next(transaction.query().match(match_query))
                except StopIteration:
                    match_query = 'match $j isa journal, has journal-name "{}"; '.format(
                        publication_dict["journal-name"])
                    match_query = match_query + create_authorship_query(authors)[0]
                    insert_query = 'insert $p isa publication, has paper-id "{}", has title "{}", has doi "{}", has publish-time "{}", has volume "{}", has issn "{}", has pmid "{}"; '.format(
                        publication_dict["paper-id"], publication_dict["title"], publication_dict["doi"],
                        publication_dict["publish-time"], publication_dict["volume"], publication_dict["issn"],
                        publication_dict["pmid"])
                    insert_query = insert_query + create_authorship_query(authors)[1]
                    insert_query = insert_query + '(publishing-journal: $j, published-publication: $p) isa publishing;'
                    # print(match_query+insert_query)
                    transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} publications added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()


def create_authorship_query(authors_list):
    """
    Create authorship query for inserting authors and authorship relations\n
    :param authors_list:
    :return:
    """
    match_query = ''
    insert_query = ''
    for counter, author in enumerate(authors_list):
        match_query = match_query + '$pe{} isa person, has published-name "{}"; '.format(counter, author)
        insert_query = insert_query + '(author: $pe{}, authored-publication: $p) isa authorship; '.format(counter)

    return [match_query, insert_query]


def get_relationship_data(data_path):
    """
    Returns unique relations in a form of list of lists
    :param data_path: The path to the file with relations
    :return: A list of lists with relations
    """
    data_df = pd.read_csv(data_path, sep=';',
                          usecols=["P_PMID", "P_PREDICATE", "P_SUBJECT_NAME", "P_OBJECT_NAME", "S_SENTENCE"])
    data_df = data_df.drop_duplicates()
    return data_df.to_numpy().tolist()


def migrate_relationships(uri: object, database: object, data: list, batch_size: object, process_id: object = 0) -> object:
    """
    Migrate relations to TypeDB\n
    data - table in a form of list of lists \n
    process_id - process id while running on multiple cores, by process_id = 0
    :param uri: The uri of the TypeDB server
    :param database: The database name
    :param data: Data in a form of list of lists
    :param batch_size: The batch size for commiting
    :param process_id: The process id while running on multiple cores, by default 0
    """
    counter = 0
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for data_entity in data:
                predicate_name = clean_string_for_database(data_entity[1])
                subject_name = clean_string_for_database(data_entity[2])
                object_name = clean_string_for_database(data_entity[3])
                relation = relationship_mapper(
                    predicate_name)  # add handler for situation when there is no relation implemented in a mapper
                pmid = clean_string_for_database(data_entity[0])
                sentence_text = clean_string_for_database(data_entity[4])

                match_query = 'match $p isa publication, has paper-id "{}"; $g1 isa gene, has gene-symbol "{}"; $g2 isa gene, has gene-symbol "{}"; '.format(
                    pmid, subject_name, object_name)
                insert_query = 'insert $r ({}: $g1, {}: $g2) isa {}, has sentence-text "{}"; $m (mentioned-genes-relation: $r, mentioning: $p) isa mention, has source "SemMed";'.format(
                    relation["active-role"], relation["passive-role"], relation["relation-name"], sentence_text)
                transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} relations added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()


def create_cache_folder():
    """
    Creates a cache folder for storing article metadata
    """
    if not os.path.exists(".cache"):
        os.makedirs(".cache")


def __fetch_article_metadata_backend(pm_ids):
    """
    Fetches article metadata from PubMed API\n
    pm_ids - PubMed IDs in a form of list of strings
    :param pm_ids:
    :return:
    """

    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={}&retmode=xml".format(pm_ids)
    response = requests.get(url)
    data = response if response.status_code == 200 else None
    return data, response.status_code


def check_in_local_cache_folder(pm_id):
    '''
    Checks if article metadata is in local cache folder\n
    pm_id - PubMed ID in a form of string
    '''
    file_path = ".cache/{}.txt".format(pm_id)
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            data = file.read()
            return data, 200
    else:
        return None, 404


def fetch_article_metadata(pmids, batch_size=100):
    successful_data = []
    failed_data = []
    for i in tqdm(range(0, len(pmids), batch_size)):
        pm_ids = set(pmids[i:i + batch_size])
        for pm_id in pmids[i:i + batch_size]:
            data, status_code = check_in_local_cache_folder(pm_id)
            if status_code == 200 and data is not None:
                successful_data.extend(untangle.parse(data))
                pm_ids.remove(pm_id)
                continue
        if len(pm_ids) > 0:
            data, status_code = __fetch_article_metadata_backend(",".join(list(pm_ids)))
            if status_code == 200 and data is not None:
                try:
                    parsed_data = untangle.parse(data.text)
                    successful_data.extend(parsed_data.eSummaryResult.DocSum)
                    for pm_data in parsed_data.eSummaryResult.DocSum:
                        pm_id = pm_data.Id.cdata
                        with open(".cache/{}.txt".format(pm_id), "w") as file:
                            file.write(data.text)
                except KeyError:
                    print("Failed to fetch data for pmids: {}".format(",".join(pmids[i:i + batch_size])))
                    failed_data.extend(pmids[i:i + batch_size])
            else:
                print("Failed to fetch data for pmids: {}".format(",".join(pmids[i:i + batch_size])))
                failed_data.append(pmids[i:i + batch_size])
    return successful_data, failed_data


def fetch_articles_metadata(pmids, batch_factor=400):
    '''
    function - function name to run in paralell\n
    data - data to load by function running in parallel
    '''
    create_cache_folder()
    pubmed_data, failed_pmids = fetch_article_metadata(pmids, batch_size=batch_factor)
    retried_data, retried_failed_pmids = fetch_article_metadata(failed_pmids, batch_size=1)
    pubmed_data.extend(retried_data)
    return pubmed_data, retried_failed_pmids


def load_in_parallel(session, uri, function, data, num_threads, batch_size):
    '''
    Runs a specific function to load data to TypeDB several time in parallel\n
    function - function name to run in paralell\n
    data - data to load by function running in parallel
    '''
    # start_time = datetime.datetime.now()
    chunk_size = int(len(data) / num_threads)
    processes = []

    for i in range(num_threads):

        if i == num_threads - 1:
            chunk = data[i * chunk_size:]

        else:
            chunk = data[i * chunk_size:(i + 1) * chunk_size]

        process = multiprocessing.Process(target=function, args=(uri, session.database().name(), chunk, batch_size, i))

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
        "INHIBITS": {"relation-name": "inhibition", "passive-role": "inhibited", "active-role": "inhibiting"},
        "INTERACTS_WITH": {"relation-name": "interaction", "passive-role": "interacted", "active-role": "interacting"},
        "COEXISTS_WITH": {"relation-name": "coexistance", "passive-role": "coexisting", "active-role": "coexisting"},
        "compared_with": {"relation-name": "comparison", "passive-role": "compared", "active-role": "comparing"},
        "higher_than": {"relation-name": "comparison", "passive-role": "lower", "active-role": "higher"},
        "STIMULATES": {"relation-name": "stimulation", "passive-role": "stimulated", "active-role": "stimulating"},
        "CONVERTS_TO": {"relation-name": "conversion", "passive-role": "converted", "active-role": "converting"},
        "PRODUCES": {"relation-name": "production", "passive-role": "produced", "active-role": "producing"},
        "NEG_COEXISTS_WITH": {"relation-name": "neg-coexistance", "passive-role": "neg-coexisting",
                              "active-role": "neg-coexisting"},
        "NEG_INHIBITS": {"relation-name": "neg-inhibition", "passive-role": "neg-inhibited",
                         "active-role": "neg-inhibiting"},
        "NEG_INTERACTS_WITH": {"relation-name": " neg-interaction", "passive-role": "neg-interacted",
                               "active-role": "neg-interacting"},
        "NEG_STIMULATES": {"relation-name": "neg-stimulation", "passive-role": "neg-stimulating",
                           "active-role": "neg-stimulated"},
        "NEG_PRODUCES": {"relation-name": "neg-production", "passive-role": "neg-producing",
                         "active-role": "neg-produced"},
        "lower_than": {"relation-name": "comparison", "passive-role": "higher", "active-role": "lower"},
        "NEG_PART_OF": {"relation-name": "neg-constitution", "passive-role": "neg-constituting",
                        "active-role": "neg-constituted"},
        "same_as": {"relation-name": "similarity", "passive-role": "similar", "active-role": "similar"},
        "NEG_same_as": {"relation-name": "neg-similarity", "passive-role": "neg-similar", "active-role": "neg-similar"},
        "LOCATION_OF": {"relation-name": "location", "passive-role": "locating", "active-role": "located"},
        "PART_OF": {"relation-name": "constitution", "passive-role": "constituting", "active-role": "constituted"},
        "NEG_higher_than": {"relation-name": "neg-comparison", "passive-role": "neg-higher",
                            "active-role": "neg-lower"},
        "NEG_CONVERTS_TO": {"relation-name": "neg-conversion", "passive-role": "neg-converting",
                            "active-role": "neg-converted"},
        "DISRUPTS": {"relation-name": "disruption", "passive-role": "disrupting", "active-role": "disrupted"},
        "AUGMENTS": {"relation-name": "augmentation", "passive-role": "augmenting", "active-role": "augmented"},
        "AFFECTS": {"relation-name": "affection", "passive-role": "affecting", "active-role": "affected"},
        "ASSOCIATED_WITH": {"relation-name": "association", "passive-role": "associated", "active-role": "associating"}
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
