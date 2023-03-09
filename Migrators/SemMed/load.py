# -*- coding: utf-8 -*-
"""Define functions for loading data into TypeDB."""
import pandas as pd
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType
from typedb.client import TypeDB

from Migrators.SemMed.mapper import relationship_mapper


def load_journals(
    journal_names: list[str], database: str, uri: str, batch_size: int
) -> None:
    """Load journals into TypeDB.

    :param journal_names: The list of journal names
    :type journal_names: list[str]
    :param database: The name of the database
    :type database: str
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, journal_name in enumerate(journal_names, start=1):
                match_query = f'match $j isa journal, has journal-name "{journal_name}";'
                if len(list(transaction.query().match(match_query))) == 0:
                    insert_query = (
                        f'insert $j isa journal, has journal-name "{journal_name}";'
                    )
                    transaction.query().insert(insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
            transaction.commit()
            transaction.close()


def load_authors(
    author_names: list[str], database: str, uri: str, batch_size: int
) -> None:
    """Load authors into TypeDB.

    :param author_names: The list of author names
    :type author_names: list[str]
    :param database: The name of the database
    :type database: str
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, author_name in enumerate(author_names, start=1):
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


def load_publications(
    publications: list[dict], database: str, uri: str, batch_size: int
) -> None:
    """Load publications into TypeDB.

    :param publications: The list of publications
    :type publications: list[dict]
    :param database: The name of the database
    :type database: str
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, publication in enumerate(publications):
                match_query = (
                    f'match $p isa publication, has paper-id "{publication["paper-id"]}";'
                )
                if len(list(transaction.query().match(match_query))) == 0:
                    match_query = (
                        "match $j isa journal, "
                        f'has journal-name "{publication["journal-name"]}"; '
                    )
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
                    for i, author in enumerate(publication["authors"]):
                        match_query += (
                            f'$pe{i} isa person, has published-name "{author}"; '
                        )
                        insert_query += (
                            f"(author: $pe{i}, authored-publication: $p) isa authorship; "
                        )
                    insert_query += (
                        "(publishing-journal: $j, "
                        "published-publication: $p) isa publishing;"
                    )
                    transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
            transaction.commit()
            transaction.close()


def load_relations(data: pd.DataFrame, database: str, uri: str, batch_size: int) -> None:
    """Load relations into TypeDB.

    :param data: The data to load
    :type data: pd.DataFrame
    :param database: The name of the database
    :type database: str
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, row in enumerate(data.itertuples(), 1):
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
                    'isa mention, has source "SemMed";'
                )

                transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
            transaction.commit()
            transaction.close()
