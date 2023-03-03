# -*- coding: utf-8 -*-
import json
import os
import re
import unicodedata

import joblib
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from typedb.client import SessionType, TransactionType, TypeDB


def migrate_semmed(session, uri, num_semmed, num_threads, batch_size):
    """Import SemMed data into the database.

    :param session: The session
    :param uri: The SemMed uri
    :param num_semmed: The number of SemMed data to import
    :param num_threads: The number of threads to use for importing
    :param batch_size: The batch size for adding into the database.
    """
    print("Migrate 'Subject_CORD_NER.csv'")

    file_path = "Dataset/SemMed/Subject_CORD_NER.csv"
    (
        author_names,
        journal_names,
        publications_list,
        relationship_data,
    ) = load_data_from_file(file_path, num_semmed)

    load_dataset_in_parallel(
        author_names,
        batch_size,
        journal_names,
        num_threads,
        publications_list,
        relationship_data,
        session,
        uri,
    )

    print("Migrate 'Object_CORD_NER.csv'")

    file_path = "Dataset/SemMed/Object_CORD_NER.csv"
    (
        author_names,
        journal_names,
        publications_list,
        relationship_data,
    ) = load_data_from_file(file_path, num_semmed)

    load_dataset_in_parallel(
        author_names,
        batch_size,
        journal_names,
        num_threads,
        publications_list,
        relationship_data,
        session,
        uri,
    )


def load_data_from_file(file_path, num_semmed):
    """Load data from file. This function is used to load data from the file and return the data in a list.

    :param file_path: The path to the file
    :param num_semmed: The number of SemMed data to import
    :return: A list of data
    """
    df = pd.read_csv(file_path, sep=";")[:num_semmed]
    pmids = df["P_PMID"].unique().astype(str)
    # Fetch articles metadata from pubmed
    json_articles_data, failed_articles = fetch_articles_metadata(pmids)
    journal_names = get_journal_names(json_articles_data)
    author_names = get_authors_names(json_articles_data)
    publications_list = get_publication_data(json_articles_data)
    relationship_data = get_relationship_data(file_path)[:num_semmed]
    return author_names, journal_names, publications_list, relationship_data


def load_dataset_in_parallel(
    author_names,
    batch_size,
    journal_names,
    num_threads,
    publications_list,
    relationship_data,
    session,
    uri,
):
    """Load dataset in parallel.

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
    load_in_parallel(
        session, uri, migrate_journals, journal_names, num_threads, batch_size
    )
    print("--------Loading authors---------")
    load_in_parallel(session, uri, migrate_authors, author_names, num_threads, batch_size)
    print("--------Loading publications--------")
    load_in_parallel(
        session, uri, migrate_publications, publications_list, num_threads, batch_size
    )
    print("--------Loading relations----------")
    load_in_parallel(
        session, uri, migrate_relationships, relationship_data, num_threads, batch_size
    )


def get_journal_names(json_objects):
    """Get journal names from xml objects.

    :param json_objects:
    :return: A list of journal names
    """
    journals_set = set()

    for document in json_objects:
        if "Journal Article" in document["pubtype"]:
            journals_set.add(document["fulljournalname"])

    return list(journals_set)


def migrate_journals(uri, database, journal_names: list, batch_size, process_id=0):
    """Migrate journals to TypeDB \n.

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
                    match_query = (
                        f'match $j isa journal, has journal-name "{journal_name}";'
                    )
                    next(transaction.query().match(match_query))
                except StopIteration:
                    insert_query = (
                        f'insert $j isa journal, has journal-name "{journal_name}";'
                    )
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} journals added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()


def get_authors_names(json_objects):
    """Retrieve unique author names from PubMed API's xml response in a form of list of strings\n.

    :param json_objects: The xml response from PubMed API
    :return: List of author names
    """

    authors_set = set()

    for document in json_objects:
        for author in document["authors"]:
            authors_set.add(author["name"])

    return list(authors_set)


def clean_string(string):
    """Clean string from special characters for database.

    :param string: Raw string
    :return: Return string without special characters
    """
    return "".join(re.findall(r"\w+", unicodedata.normalize("NFC", string), re.UNICODE))


def migrate_authors(uri, database, author_names: list, batch_size, process_id=0):
    """Migrate authors to TypeDB\n.

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
                author_name = clean_string(author_name)
                try:
                    match_query = (
                        f'match $a isa person, has published-name "{author_name}";'
                    )
                    next(transaction.query().match(match_query))
                except StopIteration:
                    insert_query = (
                        f'insert $a isa person, has published-name "{author_name}";'
                    )
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} authors added".format(process_id, counter))
                counter = counter + 1
            transaction.commit()
            transaction.close()


def get_publication_data(json_objects):
    """Retrieve publication data from PubMed API's xml response in a form of list of dictionaries\n.

    :param json_objects: The xml response from PubMed API
    :return: The list of dictionaries with publication data
    """
    publications = []
    pmid_set = set()
    for document in json_objects:
        if document["uid"] not in pmid_set:
            publication = {}
            publication["paper-id"] = publication["pmid"] = document["uid"]

            for article_id in document["articleids"]:
                if article_id["idtype"] == "doi":
                    publication["doi"] = article_id["value"]

            publication["authors"] = []
            for author in document["authors"]:
                publication["authors"].append(author["name"])

            publication["issn"] = document["issn"]
            publication["volume"] = document["volume"]

            publication["journal-name"] = ""
            if "Journal Article" in document["pubtype"]:
                publication["journal-name"] = document["fulljournalname"]

            publication["publish-time"] = document["pubdate"]

            publication["title"] = document["title"].replace('"', "'")

            publications.append(publication)
            pmid_set.add(publication["paper-id"])

    return list(publications)


def migrate_publications(
    uri, database, publications_list: list, batch_size, process_id=0
):
    """Migrate publiations to TypeDB\n.

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
                    match_query = f'match $p isa publication, has paper-id "{publication_dict["paper-id"]}";'
                    next(transaction.query().match(match_query))
                except StopIteration:
                    match_query = f'match $j isa journal, has journal-name "{publication_dict["journal-name"]}"; '
                    match_query = match_query + create_authorship_query(authors)[0]
                    insert_query = (
                        f'insert $p isa publication, has paper-id "{publication_dict["paper-id"]}", '
                        f'has title "{publication_dict["title"]}", has doi "{publication_dict["doi"]}", '
                        f'has publish-time "{publication_dict["publish-time"]}", '
                        f'has volume "{publication_dict["volume"]}", '
                        f'has issn "{publication_dict["issn"]}", '
                        f'has pmid "{publication_dict["pmid"]}";'
                    )

                    insert_query = insert_query + create_authorship_query(authors)[1]
                    insert_query = (
                        insert_query
                        + "(publishing-journal: $j, published-publication: $p) isa publishing;"
                    )
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
    """Create authorship query for inserting authors and authorship relations\n.

    :param authors_list:
    :return:
    """
    match_query = ""
    insert_query = ""
    for counter, author in enumerate(authors_list):
        match_query = (
            match_query + f'$pe{counter} isa person, has published-name "{author}"; '
        )
        insert_query = (
            insert_query
            + f"(author: $pe{counter}, authored-publication: $p) isa authorship; "
        )

    return [match_query, insert_query]


def get_relationship_data(data_path):
    """Returns unique relations in a form of list of lists.

    :param data_path: The path to the file with relations
    :return: A list of lists with relations
    """
    data_df = pd.read_csv(
        data_path,
        sep=";",
        usecols=[
            "P_PMID",
            "P_PREDICATE",
            "P_SUBJECT_NAME",
            "P_OBJECT_NAME",
            "S_SENTENCE",
        ],
    )
    data_df = data_df.drop_duplicates()
    return data_df.values.tolist()


def migrate_relationships(
    uri: object, database: object, data: list, batch_size: object, process_id: object = 0
) -> object:
    """Migrate relations to TypeDB\n.

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
                predicate_name = clean_string(data_entity[1])
                subject_name = clean_string(data_entity[2])
                object_name = clean_string(data_entity[3])
                relation = relationship_mapper(
                    predicate_name
                )  # add handler for situation when there is no relation implemented in a mapper
                pmid = clean_string(data_entity[0])
                sentence_text = clean_string(data_entity[4])

                match_query = (
                    f'match $p isa publication, has paper-id "{pmid}"; '
                    f'$g1 isa gene, has gene-symbol "{subject_name}"; '
                    f'$g2 isa gene, has gene-symbol "{object_name}"; '
                )
                insert_query = (
                    f'insert $r ({relation["active-role"]}: $g1, '
                    f'{relation["passive-role"]}: $g2) isa {relation["relation-name"]}, '
                    f'has sentence-text "{sentence_text}"; '
                    f'$m (mentioned-genes-relation: $r, mentioning: $p) isa mention, has source "SemMed";'
                )
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
    """Creates a cache folder for storing article metadata."""
    if not os.path.exists(".cache"):
        os.makedirs(".cache")


def __fetch_article_metadata_backend(pm_ids):
    """Fetches article metadata from PubMed API\n.

    pm_ids - PubMed IDs in a form of list of strings
    :param pm_ids:
    :return:
    """

    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={pm_ids}&retmode=json"
    response = requests.get(url)
    data = response.json() if response.status_code == 200 else None
    return data, response.status_code


def check_in_local_cache_folder(pm_id):
    """Checks if article metadata is in local cache folder\n.

    pm_id - PubMed ID in a form of string
    """
    file_path = f".cache/{pm_id}.json"
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
            return data, 200
    else:
        return None, 404


def fetch_article_metadata(pmids, batch_size=100):
    successful_data = []
    failed_data = []
    for i in tqdm(range(0, len(pmids), batch_size)):
        pm_ids = set(pmids[i : i + batch_size])
        uncached_pm_ids = []
        for pm_id in pm_ids:
            data, status_code = check_in_local_cache_folder(pm_id)
            if status_code == 200 and data is not None:
                successful_data.append(data)
            else:
                uncached_pm_ids.append(pm_id)
        if len(uncached_pm_ids) > 0:
            data, status_code = __fetch_article_metadata_backend(
                ",".join(uncached_pm_ids)
            )
            if status_code == 200 and data is not None:
                for uid in data["result"]:
                    if uid != "uids":
                        successful_data.append(data["result"][uid])
                        with open(f".cache/{uid}.json", "w") as file:
                            file.write(json.dumps(data["result"][uid], indent=4))
            else:
                print(f'Failed to fetch data for pm_ids: {",".join(uncached_pm_ids)}')
                failed_data.extend(uncached_pm_ids)
    return successful_data, failed_data


def fetch_articles_metadata(pmids, batch_factor=400):
    """
    function - function name to run in parallel\n
    data - data to load by function running in parallel
    """
    create_cache_folder()
    pubmed_data, failed_pmids = fetch_article_metadata(pmids, batch_size=batch_factor)
    retried_data, retried_failed_pmids = fetch_article_metadata(
        failed_pmids, batch_size=1
    )
    pubmed_data.extend(retried_data)
    return pubmed_data, retried_failed_pmids


def load_in_parallel(session, uri, function, data, num_threads, batch_size):
    """Runs a specific function to load data to TypeDB several time in parallel\n.

    function - function name to run in paralell\n
    data - data to load by function running in parallel
    """
    # start_time = datetime.datetime.now()

    joblib.Parallel(n_jobs=num_threads)(
        joblib.delayed(function)(uri, session.database().name(), chunk, batch_size, i)
        for i, chunk in enumerate(np.array_split(data, num_threads))
    )

    # end_time = datetime.datetime.now()
    # print("-------------\nTime taken: {}".format(end_time - start_time))


def relationship_mapper(relationship: str):
    """
    Input: predicate name from SemMed db (string)\n
    Output: dictionary with relation name and roles for specific predicate
    """
    mapper = {
        "INHIBITS": {
            "relation-name": "inhibition",
            "passive-role": "inhibited",
            "active-role": "inhibiting",
        },
        "INTERACTS_WITH": {
            "relation-name": "interaction",
            "passive-role": "interacted",
            "active-role": "interacting",
        },
        "COEXISTS_WITH": {
            "relation-name": "coexistance",
            "passive-role": "coexisting",
            "active-role": "coexisting",
        },
        "compared_with": {
            "relation-name": "comparison",
            "passive-role": "compared",
            "active-role": "comparing",
        },
        "higher_than": {
            "relation-name": "comparison",
            "passive-role": "lower",
            "active-role": "higher",
        },
        "STIMULATES": {
            "relation-name": "stimulation",
            "passive-role": "stimulated",
            "active-role": "stimulating",
        },
        "CONVERTS_TO": {
            "relation-name": "conversion",
            "passive-role": "converted",
            "active-role": "converting",
        },
        "PRODUCES": {
            "relation-name": "production",
            "passive-role": "produced",
            "active-role": "producing",
        },
        "NEG_COEXISTS_WITH": {
            "relation-name": "neg-coexistance",
            "passive-role": "neg-coexisting",
            "active-role": "neg-coexisting",
        },
        "NEG_INHIBITS": {
            "relation-name": "neg-inhibition",
            "passive-role": "neg-inhibited",
            "active-role": "neg-inhibiting",
        },
        "NEG_INTERACTS_WITH": {
            "relation-name": " neg-interaction",
            "passive-role": "neg-interacted",
            "active-role": "neg-interacting",
        },
        "NEG_STIMULATES": {
            "relation-name": "neg-stimulation",
            "passive-role": "neg-stimulating",
            "active-role": "neg-stimulated",
        },
        "NEG_PRODUCES": {
            "relation-name": "neg-production",
            "passive-role": "neg-producing",
            "active-role": "neg-produced",
        },
        "lower_than": {
            "relation-name": "comparison",
            "passive-role": "higher",
            "active-role": "lower",
        },
        "NEG_PART_OF": {
            "relation-name": "neg-constitution",
            "passive-role": "neg-constituting",
            "active-role": "neg-constituted",
        },
        "same_as": {
            "relation-name": "similarity",
            "passive-role": "similar",
            "active-role": "similar",
        },
        "NEG_same_as": {
            "relation-name": "neg-similarity",
            "passive-role": "neg-similar",
            "active-role": "neg-similar",
        },
        "LOCATION_OF": {
            "relation-name": "location",
            "passive-role": "locating",
            "active-role": "located",
        },
        "PART_OF": {
            "relation-name": "constitution",
            "passive-role": "constituting",
            "active-role": "constituted",
        },
        "NEG_higher_than": {
            "relation-name": "neg-comparison",
            "passive-role": "neg-higher",
            "active-role": "neg-lower",
        },
        "NEG_CONVERTS_TO": {
            "relation-name": "neg-conversion",
            "passive-role": "neg-converting",
            "active-role": "neg-converted",
        },
        "DISRUPTS": {
            "relation-name": "disruption",
            "passive-role": "disrupting",
            "active-role": "disrupted",
        },
        "AUGMENTS": {
            "relation-name": "augmentation",
            "passive-role": "augmenting",
            "active-role": "augmented",
        },
        "AFFECTS": {
            "relation-name": "affection",
            "passive-role": "affecting",
            "active-role": "affected",
        },
        "ASSOCIATED_WITH": {
            "relation-name": "association",
            "passive-role": "associated",
            "active-role": "associating",
        },
    }
    mapping = mapper.get(relationship, {})

    return mapping
