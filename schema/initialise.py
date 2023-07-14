# -*- coding: utf-8 -*-
"""Module containing the function for initialising the database."""
from typedb.client import SessionType, TransactionType


def initialise_database(client, database, force):
    """Initialise the database."""
    if client.databases().contains(database):
        if force:
            client.databases().get(database).delete()
        else:
            raise ValueError("database {} already exists, use --force True to overwrite")

    client.databases().create(database)
    print("Defining schema...")

    with open("schema/schema.tql", "r", encoding="utf-8") as file:
        schema = file.read()

    with client.session(database, SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.WRITE) as transaction:
            transaction.query().define(schema)
            transaction.commit()

    print("Schema definition complete.")
    print("--------------------------------------------------")
