# -*- coding: utf-8 -*-
"""Define the pipeline for migrating SemMed data to TypeDB."""
import os
from pathlib import Path
import typedb.api.connection.session
from loader.semmed.fetch import fetch_data
from loader.semmed.load import load_authors, load_journals, load_publications, load_relations
from loader.semmed.parse import get_author_names, get_journal_names, get_publication_data


def load_semmed(
    session: typedb.api.connection.session.TypeDBSession,
    max_publications: int | None,
    num_jobs: int,
    batch_size: int,
    cache_dir: str | os.PathLike = ".cache/semmed",
) -> None:
    """Migrate SemMed data to TypeDB.

    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param max_publications: The maximum number of publications to be migrated
    :type max_publications: int | None
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    :param cache_dir: The directory to store the cache files
    :type cache_dir: str | os.PathLike
    """
    if max_publications is None or max_publications > 0:
        print("Loading SemMed dataset...")
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        print("Loading SemMed subject dataset...")

        load_dataset(
            file_path="dataset/semmed/Subject_CORD_NER.csv",
            session=session,
            max_publications=max_publications,
            num_jobs=num_jobs,
            batch_size=batch_size,
            cache_dir=cache_path
        )

        print("Loading SemMed object dataset...")

        load_dataset(
            file_path="dataset/semmed/Object_CORD_NER.csv",
            session=session,
            max_publications=max_publications,
            num_jobs=num_jobs,
            batch_size=batch_size,
            cache_dir=cache_path
        )

        print("Dataset load complete.")
        print("--------------------------------------------------")


def load_dataset(
    file_path: str,
    session: typedb.api.connection.session.TypeDBSession,
    max_publications: int | None,
    num_jobs: int,
    batch_size: int,
    cache_dir: Path,
) -> None:
    """Migrate different components of SemMed data to TypeDB.

    :param file_path: The path to the file specifying the SemMed data
    :type file_path: str
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param max_publications: The maximum number of publications to be migrated
    :type max_publications: int | None
    :param num_jobs: The number of jobs to be run in parallel
    :type num_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    :param cache_dir: The directory to store the cache files
    :type cache_dir: Path
    """

    relations, publications = fetch_data(file_path, max_publications, cache_dir)
    load_journals(journal_names=get_journal_names(publications), session=session, num_jobs=num_jobs, batch_size=batch_size)
    load_authors(author_names=get_author_names(publications), session=session, num_jobs=num_jobs, batch_size=batch_size)
    load_publications(publications=get_publication_data(publications), session=session, num_jobs=num_jobs, batch_size=batch_size)
    load_relations(data=relations, session=session, num_jobs=num_jobs, batch_size=batch_size)
