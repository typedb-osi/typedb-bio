# -*- coding: utf-8 -*-
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType
from typedb.client import TypeDB

from Migrators.Helpers.utils import clean_string
from Migrators.semmed.mapper import relationship_mapper


def load_journals(uri, database, journal_names: list, batch_size):
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


def load_authors(uri, database, author_names: list, batch_size):
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


def load_publications(uri, database, publications: list, batch_size):
    """Migrate publiations to TypeDB\n.

    publications - list of dictionaries with publication data\n
    process_id - process id while running on multiple cores, by process_id = 0
    """
    with TypeDB.core_client(uri) as client:
        with client.session(database, SessionType.DATA) as session:
            transaction = session.transaction(TransactionType.WRITE)
            for counter, publication in enumerate(publications):
                ##Check if publication already in Knowledge Base
                match_query = (
                    f'match $p isa publication, has paper-id "{publication["paper-id"]}";'
                )
                if len(list(transaction.query().match(match_query))) == 0:
                    match_query = f'match $j isa journal, has journal-name "{publication["journal-name"]}"; '
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
                    insert_query += "(publishing-journal: $j, published-publication: $p) isa publishing;"
                    transaction.query().insert(match_query + insert_query)
                if counter % batch_size == 0:
                    transaction.commit()
                    transaction.close()
                    transaction = session.transaction(TransactionType.WRITE)
            transaction.commit()
            transaction.close()


def load_relations(uri, database, data, batch_size) -> None:
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
