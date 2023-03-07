# -*- coding: utf-8 -*-
from functools import partial
from typing import Any, Callable

import joblib
import numpy as np
import typedb.api.connection.session
from typedb.client import SessionType, TransactionType, TypeDB

from Migrators.Helpers.utils import clean_string
from Migrators.semmed.fetch import _get_data
from Migrators.semmed.mapper import relationship_mapper
from Migrators.semmed.parse import (
    get_author_names,
    get_journal_names,
    get_publication_data,
)


def migrate_semmed(session, uri, num_semmed, n_jobs, batch_size):
    """Import SemMed data into the database.

    :param session: The session
    :param uri: The semmed uri
    :param num_semmed: The number of SemMed data to import
    :param n_jobs: The number of threads to use for importing
    :param batch_size: The batch size for adding into the database.
    """

    __load_dataset = partial(
        _migrate_dataset,
        num_semmed=num_semmed,
        session=session,
        uri=uri,
        batch_size=batch_size,
        n_jobs=n_jobs,
    )

    __load_dataset(file_path="Dataset/SemMed/Subject_CORD_NER.csv")
    __load_dataset(file_path="Dataset/SemMed/Object_CORD_NER.csv")


def _migrate_dataset(
    file_path,
    num_semmed,
    session,
    uri,
    batch_size,
    n_jobs,
):
    """Load dataset in parallel.

    :param author_names: The list of author names
    :param batch_size: The number of data to be added into the database at once
    :param journal_names: The list of journal names
    :param n_jobs:  The number of threads to use for importing
    :param publications_list: The list of publications
    :param relationship_data: The list of relationship data
    :param session: Database session
    :param uri: semmed uri
    """

    print(f"Migrate {file_path}")
    relations, publications = _get_data(file_path, num_semmed)

    __load_data = partial(
        _load_data, session=session, uri=uri, batch_size=batch_size, n_jobs=n_jobs
    )

    print("--------Loading journals---------")
    __load_data(func=_load_journals, data=get_journal_names(publications))

    print("--------Loading authors---------")
    __load_data(func=_load_authors, data=get_author_names(publications))

    print("--------Loading publications--------")
    __load_data(func=_load_publications, data=get_publication_data(publications))

    print("--------Loading relations----------")
    __load_data(func=_load_relations, data=relations)


def _load_journals(uri, database, journal_names: list, batch_size):
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
            transaction.commit()
            transaction.close()


def _load_authors(uri, database, author_names: list, batch_size):
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
            transaction.commit()
            transaction.close()


def _load_publications(uri, database, publications: list, batch_size):
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
                    match_query += _create_authorship_query(authors)[0]
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
                    insert_query += _create_authorship_query(authors)[1]
                    insert_query += "(publishing-journal: $j, published-publication: $p) isa publishing;"
                    transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
            transaction.commit()
            transaction.close()


def _create_authorship_query(authors_list):
    match_query = ""
    insert_query = ""
    for counter, author in enumerate(authors_list):
        match_query += f'$pe{counter} isa person, has published-name "{author}"; '
        insert_query += (
            f"(author: $pe{counter}, authored-publication: $p) isa authorship; "
        )

    return [match_query, insert_query]


def _load_relations(uri, database, data, batch_size) -> None:
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
            for counter, row in enumerate(data.itertuples(), start=1):
                # Add handler for situation when there is no relation implemented in a mapper
                relation = relationship_mapper(row.predicate)

                match_query = (
                    f'match $p isa publication, has paper-id "{row.pmid}"; '
                    f'$g1 isa gene, has gene-symbol "{row.subject}"; '
                    f'$g2 isa gene, has gene-symbol "{row.object}"; '
                )
                insert_query = (
                    f'insert $r ({relation["active-role"]}: $g1, '
                    f'{relation["passive-role"]}: $g2) '
                    f'isa {relation["relation-name"]}, '
                    f'has sentence-text "{row.sentence}"; '
                    "$m (mentioned-genes-relation: $r, mentioning: $p) "
                    'isa mention, has source "semmed";'
                )

                transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
            transaction.commit()
            transaction.close()


def _load_data(
    session: typedb.api.connection.session.TypeDBSession,
    uri: str,
    func: Callable,
    data: Any,
    n_jobs: int,
    batch_size: int,
):
    """Load `data` to TypeDB using `func` and `n_jobs` in parallel.

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
        joblib.delayed(func)(uri, session.database().name(), chunk, batch_size)
        for chunk in np.array_split(data, n_jobs)
    )
