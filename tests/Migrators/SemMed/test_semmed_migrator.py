# -*- coding: utf-8 -*-
from functools import partial

from typedb.client import SessionType

from Migrators.SemMed.pipeline import migrate_semmed


def test_migrate_semmed(client, database, db_uri, cache_dir):
    with client.session(database, SessionType.SCHEMA) as session:
        _migrate_semmed = partial(
            migrate_semmed,
            session=session,
            uri=db_uri,
            num_semmed=4,
            n_jobs=2,
            batch_size=2,
            cache_dir=cache_dir,
        )
        assert _migrate_semmed() is None
