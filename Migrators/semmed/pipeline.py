# -*- coding: utf-8 -*-
from functools import partial
from typing import Any, Callable

import joblib
import numpy as np
import typedb.api.connection.session

from Migrators.semmed.fetch import fetch_data
from Migrators.semmed.load import (
    load_authors,
    load_journals,
    load_publications,
    load_relations,
)
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
        joblib.delayed(func)(data_chunk, session.database().name(), uri, batch_size)
        for data_chunk in np.array_split(data, n_jobs)
    )
