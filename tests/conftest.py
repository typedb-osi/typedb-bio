# -*- coding: utf-8 -*-
"""Define pytest fixtures that could be used to run tests in parallel."""
import uuid
from pathlib import Path

import pytest
from typedb.client import TypeDB, TypeDBClient

from schema.initialise import initialise_database


@pytest.fixture(scope="session")
def db_uri() -> str:
    """Return the database uri.

    :return: The database uri.
    :rtype: str
    """
    return "localhost:1729"


@pytest.fixture(scope="session")
def client(db_uri: str) -> TypeDBClient:
    """Return a TypeDB client.

    :param db_uri: The database uri.
    :type db_uri: str
    :return: The TypeDB client.
    :rtype: TypeDBClient
    """
    with TypeDB.core_client(db_uri) as _client:
        yield _client


@pytest.fixture
def identifier() -> str:
    """Return an identifier.

    :return: The identifier.
    :rtype: str
    """
    return uuid.uuid4().hex


@pytest.fixture
def cache_dir(tmp_path_factory: pytest.TempPathFactory, identifier: str) -> Path:
    """Create and return the cache directory.

    :param tmp_path_factory: The pytest tmp_path_factory fixture.
    :type tmp_path_factory: pytest.TempPathFactory
    :param identifier: The identifier.
    :type identifier: str
    :return: The cache directory.
    :rtype: Path
    """
    _cache_dir = tmp_path_factory.mktemp(f"cache_{identifier}").expanduser().resolve()
    return _cache_dir


@pytest.fixture
def database(client: TypeDBClient, identifier: str) -> str:  # type: ignore
    """Create and return a database.

    :param client: The TypeDB client.
    :type client: TypeDBClient
    :param identifier: The identifier.
    :type identifier: str
    :return: The database name.
    :rtype: str
    """
    _database = f"test_{identifier}"
    initialise_database(client, _database, False)
    yield _database
    client.databases().get(_database).delete()
