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

    with client.session(database, SessionType.SCHEMA) as session:
        print(".....")
        print("Inserting schema...")
        print(".....")
        with open("Schema/schema.tql", "r", encoding="utf-8") as typeql_file:
            schema = typeql_file.read()
        with session.transaction(TransactionType.WRITE) as write_transaction:
            write_transaction.query().define(schema)
            write_transaction.commit()
        print(".....")
        print("Successfully committed schema!")
        print(".....")
