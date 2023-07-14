# -*- coding: utf-8 -*-
"""Module containing the tests for the SemMed migrator."""
from functools import partial
from pathlib import Path

import pytest
import requests
from typedb.client import SessionType, TypeDBClient

from loader.semmed.pipeline import load_semmed
from schema.initialise import initialise_database


def _raise_exception(*args, **kwargs):
    raise Exception


def test_migrate_semmed(
    client: TypeDBClient,
    database: str,
    cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test the migrate_semmed() function.

    :param client: The TypeDB client.
    :type client: TypeDBClient
    :param database: The database name.
    :type database: str
    :param cache_dir: The cache directory.
    :type cache_dir: Path
    :param monkeypatch: The pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """

    # Check migrate_semmed() works when pulling data
    # from the NCBI API

    with client.session(database, SessionType.DATA) as session:
        assert load_semmed(
            session=session,
            max_publications=None,
            num_jobs=4,
            batch_size=50,
            cache_dir=cache_dir,
        ) is None

    # Check migrate_semmed() works when pulling data
    # from the cache

    initialise_database(client, database, True)

    with client.session(database, SessionType.DATA) as session:
        with monkeypatch.context() as m:
            # Monkeypatch the requests.get() function to raise an exception.
            # This will not cause a problem if the data is read from the cache,
            # before trying to be pulled from the NCBI API.
            m.setattr(requests, "get", _raise_exception)

            assert load_semmed(
                session=session,
                max_publications=None,
                num_jobs=4,
                batch_size=50,
                cache_dir=cache_dir,
            ) is None
