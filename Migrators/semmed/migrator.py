# -*- coding: utf-8 -*-
from typing import Any, Callable

import joblib
import numpy as np
import pandas as pd
import typedb.api.connection.session
from typedb.client import SessionType, TransactionType, TypeDB

from Migrators.Helpers.utils import clean_string
from Migrators.semmed.fetch import fetch_articles_metadata
from Migrators.semmed.mapper import relationship_mapper
from Migrators.semmed.parse import (
    get_author_names,
    get_journal_names,
    get_publication_data,
)


def migrate_semmed(session, uri, num_semmed, num_threads, batch_size):
    """Import SemMed data into the database.

    :param session: The session
    :param uri: The semmed uri
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
    """Load data from file. This func is used to load data from the file and return the data in a list.

    :param file_path: The path to the file
    :param num_semmed: The number of SemMed data to import
    :return: A list of data
    """
    data_df = pd.read_csv(
        file_path,
        sep=";",
        usecols=[
            "P_PMID",
            "P_PREDICATE",
            "P_SUBJECT_NAME",
            "P_OBJECT_NAME",
            "S_SENTENCE",
        ],
    )
    data_df = data_df.drop_duplicates(subset=["P_PMID"])[:num_semmed]
    pmids = data_df["P_PMID"].astype(str)
    # Fetch articles metadata from pubmed
    publications, failed_ids = fetch_articles_metadata(pmids, batch_size=400, retries=1)
    journal_names = get_journal_names(publications)
    author_names = get_author_names(publications)
    publications_list = get_publication_data(publications)
    relationship_data = data_df.values.tolist()
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
    :param uri: semmed uri
    """

    # load_in_parallel = partial()
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


def migrate_journals(uri, database, journal_names: list, batch_size, process_id=0):
    """Migrate journals to TypeDB.

    journal_names - list of journal names (strings) \n
    process_id - process id while running on multiple cores, by process_id = 0
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, journal_name in enumerate(journal_names, start=1):
                # Check if journal already in Knowledge Base
                match_query = (
                    "match $j isa journal, " f'has journal-name "{journal_name}";'
                )
                if len(list(transaction.query().match(match_query))) == 0:
                    insert_query = (
                        "insert $j isa journal, " f'has journal-name "{journal_name}";'
                    )
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} journals added".format(process_id, counter))
            transaction.commit()
            transaction.close()


def migrate_authors(uri, database, author_names: list, batch_size, process_id=0):
    """Migrate authors to TypeDB\n.

    :param uri: The uri of the TypeDB server
    :param database: The name of the database
    :param author_names: The list of author names
    :param batch_size: The batch size for commiting
    :param process_id: Unused. The process id while running on multiple cores, by default 0
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, author_name in enumerate(author_names, start=1):
                ##Check if journal already in Knowledge Base
                author_name = clean_string(author_name)
                match_query = (
                    "match $a isa person, " f'has published-name "{author_name}";'
                )
                if len(list(transaction.query().match(match_query))) == 0:
                    insert_query = (
                        "insert $a isa person, " f'has published-name "{author_name}";'
                    )
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} authors added".format(process_id, counter))
            transaction.commit()
            transaction.close()


def migrate_publications(uri, database, publications: list, batch_size, process_id=0):
    """Migrate publiations to TypeDB\n.

    publications - list of dictionaries with publication data\n
    process_id - process id while running on multiple cores, by process_id = 0
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, publication in enumerate(publications):
                authors = publication["authors"]  # list of authors - list of strings
                ##Check if publication already in Knowledge Base
                match_query = (
                    f'match $p isa publication, has paper-id "{publication["paper-id"]}";'
                )
                if len(list(transaction.query().match(match_query))) == 0:
                    match_query = f'match $j isa journal, has journal-name "{publication["journal-name"]}"; '
                    match_query += create_authorship_query(authors)[0]
                    insert_query = (
                        "insert $p isa publication, "
                        f'has paper-id "{publication["paper-id"]}", '
                        f'has title "{publication["title"]}", '
                        f'has doi "{publication["doi"]}", '
                        f'has publish-time "{publication["publish-time"]}", '
                        f'has volume "{publication["volume"]}", '
                        f'has issn "{publication["issn"]}", '
                        f'has pmid "{publication["pmid"]}"; '
                    )
                    insert_query += create_authorship_query(authors)[1]
                    insert_query += "(publishing-journal: $j, published-publication: $p) isa publishing;"
                    # print(match_query+insert_query)
                    transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} publications added".format(process_id, counter))
            transaction.commit()
            transaction.close()


def create_authorship_query(authors_list):
    match_query = ""
    insert_query = ""
    for counter, author in enumerate(authors_list):
        match_query += f'$pe{counter} isa person, has published-name "{author}"; '
        insert_query += (
            f"(author: $pe{counter}, authored-publication: $p) isa authorship; "
        )

    return [match_query, insert_query]


def migrate_relationships(
    uri, database, data, batch_size, process_id: object = 0
) -> None:
    """Migrate relations to TypeDB\n.

    data - table in a form of list of lists \n
    process_id - process id while running on multiple cores, by process_id = 0
    :param uri: The uri of the TypeDB server
    :param database: The database name
    :param data: Data in a form of list of lists
    :param batch_size: The batch size for commiting
    :param process_id: The process id while running on multiple cores, by default 0
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, data_entity in enumerate(data, start=1):
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
                    f'{relation["passive-role"]}: $g2) '
                    f'isa {relation["relation-name"]}, '
                    f'has sentence-text "{sentence_text}"; '
                    "$m (mentioned-genes-relation: $r, mentioning: $p) "
                    'isa mention, has source "semmed";'
                )

                transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
                    # print("Process {} COMMITED ----- {} relations added".format(process_id, counter))
            transaction.commit()
            transaction.close()


def load_in_parallel(
    session: typedb.api.connection.session.TypeDBSession,
    uri: str,
    func: Callable,
    data: Any,
    n_jobs: int,
    batch_size: int,
):
    """Run the function `func` to load `data` to TypeDB in parallel.

    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param func: The function to run for loading data
    :type func: Callable
    :param data: The data to load
    :type data: Any
    :param n_jobs: The number of jobs to run in parallel
    :type n_jobs: int
    :param batch_size: The batch size for each job
    :type batch_size: int
    """
    joblib.Parallel(n_jobs=n_jobs)(
        joblib.delayed(func)(uri, session.database().name(), chunk, batch_size, i)
        for i, chunk in enumerate(np.array_split(data, n_jobs))
    )
