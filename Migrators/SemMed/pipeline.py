# -*- coding: utf-8 -*-
"""Define the pipeline for migrating SemMed data to TypeDB."""
from functools import partial
from typing import Any, Callable

import joblib
import numpy as np
import typedb.api.connection.session

from Migrators.SemMed.fetch import fetch_data
from Migrators.SemMed.load import (
    load_authors,
    load_journals,
    load_publications,
    load_relations,
)
from Migrators.SemMed.parse import (
    get_author_names,
    get_journal_names,
    get_publication_data,
)


def migrate_semmed(
    session: typedb.api.connection.session.TypeDBSession,
    uri: str,
    num_semmed: int,
    n_jobs: int,
    batch_size: int,
) -> None:
    """Migrate SemMed data to TypeDB.

    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param num_semmed: The number of publications to be migrated
    :type num_semmed: int
    :param n_jobs: The number of jobs to be run in parallel
    :type n_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    __load_dataset = partial(
        _migrate_dataset,
        session=session,
        uri=uri,
        num_semmed=num_semmed,
        n_jobs=n_jobs,
        batch_size=batch_size,
    )

    __load_dataset(file_path="Dataset/SemMed/Subject_CORD_NER.csv")
    __load_dataset(file_path="Dataset/SemMed/Object_CORD_NER.csv")


def _migrate_dataset(
    file_path: str,
    session: typedb.api.connection.session.TypeDBSession,
    uri: str,
    num_semmed: int,
    n_jobs: int,
    batch_size: int,
) -> None:
    """Migrate different components of SemMed data to TypeDB.

    :param file_path: The path to the file specifying the SemMed data
    :type file_path: str
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param num_semmed: The number of publications to be migrated
    :type num_semmed: int
    :param n_jobs: The number of jobs to be run in parallel
    :type n_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    print(f"Migrate {file_path}")
    relations, publications = fetch_data(file_path, num_semmed)

    __load_data = partial(
        _load_data, session=session, uri=uri, batch_size=batch_size, n_jobs=n_jobs
    )

    print("--------Loading journals---------")
    __load_data(func=load_journals, data=get_journal_names(publications))

    print("--------Loading authors---------")
    __load_data(func=load_authors, data=get_author_names(publications))

    print("--------Loading publications--------")
    __load_data(func=load_publications, data=get_publication_data(publications))

    print("--------Loading relations----------")
    __load_data(func=load_relations, data=relations)


def _load_data(
    func: Callable,
    data: Any,
    session: typedb.api.connection.session.TypeDBSession,
    uri: str,
    n_jobs: int,
    batch_size: int,
):
    """Load `data` to TypeDB using `func` and `n_jobs` in parallel.

    :param func: The function to run for loading data
    :type func: Callable
    :param data: The data to load
    :type data: Any
    :param session: The TypeDB session
    :type session: typedb.api.connection.session.TypeDBSession
    :param uri: The uri of the TypeDB server
    :type uri: str
    :param n_jobs: The number of jobs to run in parallel
    :type n_jobs: int
    :param batch_size: The batch size for committing
    :type batch_size: int
    """
    joblib.Parallel(n_jobs=n_jobs)(
        joblib.delayed(func)(data_chunk, session.database().name(), uri, batch_size)
        for data_chunk in np.array_split(data, n_jobs)
    )
