# -*- coding: utf-8 -*-
"""Define functions for loading data into TypeDB."""
import pandas as pd
from typedb.api.connection.session import TypeDBSession
from Migrators.Helpers.batchLoader import write_batches
from Migrators.SemMed.mapper import relationship_mapper


def load_journals(
    journal_names: list[str], session: TypeDBSession, num_jobs: int, batch_size: int
) -> None:
    """Load journals into TypeDB.

    :param journal_names: The list of journal names
    :type journal_names: list[str]
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    queries = list()

    for journal_name in journal_names:
        query = " ".join([
            "match",
            "?jn = \"{}\";",
            "not {{ $j isa journal, has journal-name ?jn; }};",
            "insert",
            "$j isa journal,",
            "has journal-name ?jn;",
        ]).format(
            journal_name,
        )
        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)


def load_authors(
    author_names: list[str], session: TypeDBSession, num_jobs: int, batch_size: int
) -> None:
    """Load authors into TypeDB.

    :param author_names: The list of author names
    :type author_names: list[str]
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    queries = list()

    for author_name in author_names:
        query = " ".join([
            "match",
            "?pn = \"{}\";",
            "not {{ $p isa person, has published-name ?pn; }};",
            "insert",
            "$p isa person,",
            "has published-name ?pn;",
        ]).format(
            author_name,
        )
        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)


def load_publications(
    publications: list[dict], session: TypeDBSession, num_jobs: int, batch_size: int
) -> None:
    """Load publications into TypeDB.

    :param publications: The list of publications
    :type publications: list[dict]
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    queries = list()

    for publication in publications:
        match_clause = " ".join([
            "match",
            "$publication type publication;",
            "not {{ $p isa $publication, has paper-id \"{}\"; }};",
        ]).format(
            publication["paper-id"],
        )

        insert_clause = " ".join([
            "insert",
            "$p isa publication,",
            "has paper-id \"{}\",",
            "has pmid \"{}\"",
        ]).format(
            publication["paper-id"],
            publication["pmid"],
        )

        for attribute in ("title", "doi", "issn", "volume"):
            if attribute in publication.keys():
                insert_clause += ", has {} \"{}\"".format(attribute, publication[attribute])

        for attribute in ("year",):
            if attribute in publication.keys():
                insert_clause += ", has {} {}".format(attribute, publication[attribute])

        insert_clause += ";"

        for i, author in enumerate(publication["authors"]):
            match_clause += " $pe{} isa person, has published-name \"{}\";".format(i, author)
            insert_clause += " (author: $pe{}, authored-publication: $p) isa authorship;".format(i)

        if "journal-name" in publication.keys():
            match_clause += " " + " ".join([
                "$j isa journal,",
                "has journal-name \"{}\";",
            ]).format(
                publication["journal-name"],
            )

            insert_clause += " (publishing-journal: $j, published-publication: $p) isa publishing;"

        query = match_clause + " " + insert_clause
        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)


def load_relations(data: pd.DataFrame, session: TypeDBSession, num_jobs: int, batch_size: int) -> None:
    """Load relations into TypeDB.

    :param data: The data to load
    :type data: pd.DataFrame
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    load_gene_relations(data, session, num_jobs, batch_size)
    load_gene_mentions(data, session, num_jobs, batch_size)


def load_gene_relations(data: pd.DataFrame, session: TypeDBSession, num_jobs: int, batch_size: int) -> None:
    """Load gene relations into TypeDB.

    :param data: The data to load
    :type data: pd.DataFrame
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    queries = list()

    for counter, row in enumerate(data.itertuples(), 1):
        relation = relationship_mapper(row.predicate)

        if relation["negated"]:
            continue

        match_clause = " ".join([
            "match",
            "$g1 isa gene,",
            "has official-gene-symbol \"{}\";",
            "$g2 isa gene,",
            "has official-gene-symbol \"{}\";",
            "not {{ ({}: $g1, {}: $g2) isa {}; }};"
        ]).format(
            row.subject,
            row.object,
            relation["active-role"],
            relation["passive-role"],
            relation["relation-name"],
        )

        insert_clause = " ".join([
            "insert",
            "({}: $g1, {}: $g2) isa {};",
        ]).format(
            relation["active-role"],
            relation["passive-role"],
            relation["relation-name"],
        )

        query = match_clause + " " + insert_clause
        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)


def load_gene_mentions(data: pd.DataFrame, session: TypeDBSession, num_jobs: int, batch_size: int) -> None:
    """Load gene mentions into TypeDB.

    :param data: The data to load
    :type data: pd.DataFrame
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    queries = list()

    for counter, row in enumerate(data.itertuples(), 1):
        relation = relationship_mapper(row.predicate)

        if relation["negated"]:
            continue

        match_clause = " ".join([
            "match",
            "$p isa publication,",
            "has paper-id \"{}\";",
            "$g1 isa gene,",
            "has official-gene-symbol \"{}\";",
            "$g2 isa gene,",
            "has official-gene-symbol \"{}\";",
            "$r ({}: $g1, {}: $g2) isa {};"
        ]).format(
            row.pmid,
            row.subject,
            row.object,
            relation["active-role"],
            relation["passive-role"],
            relation["relation-name"],
        )

        insert_clause = " ".join([
            "insert",
            "$m (mentioned-genes-relation: $r, mentioning: $p) isa mention,",
            "has sentence-text \"{}\",",
            "has source \"SemMed\";",
        ]).format(
            row.sentence,
        )

        query = match_clause + " " + insert_clause
        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)
