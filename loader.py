# -*- coding: utf-8 -*-
"""Script for migrating data from various sources into the database."""
import argparse
from functools import partial
from typedb.api.connection.credential import TypeDBCredential
from typedb.client import SessionType, TypeDB
from loader.coronavirus.coronavirus_loader import load_coronavirus
from loader.dgidb.dgidb_loader import load_dgibd
from loader.disgenet.disgenet_loader import load_disgenet
from loader.hpa.hpa_loader import load_hpa
from loader.reactome.reactome_loader import load_reactome
from loader.semmed.pipeline import load_semmed
from loader.tissuenet.tissuenet_loader import load_tissuenet
from loader.uniprot.uniprot_loader import load_uniprot
from schema.initialise import initialise_database


def loader_parser():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser(
        description="Defines typedb-bio database and inserts data by calling separate loader scripts.",
    )
    parser.add_argument(
        "-n",
        "--num_jobs",
        type=int,
        help="The maximum number of concurrently running jobs (default: 8)",
        default=8,
    )
    parser.add_argument(
        "-b",
        "--commit_batch",
        help="Sets the number of queries made per commit (default: 50)",
        default=50,
    )
    parser.add_argument(
        "-d",
        "--database",
        help="Database name (default: typedb-bio)",
        default="typedb-bio",
    )
    parser.add_argument(
        "-f",
        "--force",
        help="Force overwrite the database even if a database by this name already exists (default: False)",
        default=False,
    )
    parser.add_argument(
        "-s",
        "--server_type",
        help="TypeDB server product in use, \"CORE\" or \"CLUSTER\" (default: \"CORE\")",
        default="CORE",
    )
    parser.add_argument(
        "-a",
        "--address",
        help="Server host address (default: \"localhost:1729\")",
        default="localhost:1729",
    )
    parser.add_argument(
        "-u",
        "--username",
        help="Username for TypeDB Cluster (default: \"admin\")",
        default="admin",
    )
    parser.add_argument(
        "-p",
        "--password",
        help="Password for TypeDB Cluster (default: \"password\")",
        default="password",
    )
    parser.add_argument(
        "-c",
        "--cert_path",
        help="TLS certificate path for TypeDB Cluster (default: \"tls_cert\")",
        default="tls_cert",
    )
    return parser


# Set a max constant to None to migrate all entries.
# Set a max constant to 0 to migrate no entries.
MAX_PROTEINS = None  # Maximum number of proteins to migrate.
LOAD_CORONAVIRUS = True  # Load coronavirus dataset or not.
MAX_PATHWAYS = None  # Maximum number of pathway associations to migrate.
MAX_DISEASES = None  # Maximum number of diseases to migrate.
MAX_DRUGS = None  # Maximum number of drugs to migrate.
MAX_DRUG_INTERACTIONS = None  # Maximum number of drug-gene interactions to migrate.
MAX_TISSUES = None  # Maximum number of tissues to migrate.
MAX_PUBLICATIONS = None  # Maximum number of publications to migrate.
MAX_PROTEIN_INTERACTIONS = None  # Maximum number of protein-protein interactions per tissue to migrate.

if __name__ == "__main__":
    parser = loader_parser()
    args = parser.parse_args()

    if args.server_type.lower() == "core":
        client_partial = partial(TypeDB.core_client, address=args.address)
    elif args.server_type.lower() == "cluster":
        credential = TypeDBCredential(username=args.username, password=args.password, tls_root_ca_path=args.cert_path)
        client_partial = partial(TypeDB.cluster_client, addresses=args.address, credential=credential)
    else:
        raise ValueError("Unknown server type. Must be \"CORE\" or \"CLUSTER\".")

    print("Welcome to the TypeDB Bio database loader!")
    print("--------------------------------------------------")

    with client_partial() as client:
        initialise_database(client, args.database, args.force)

        with client.session(args.database, SessionType.DATA) as session:
            load_uniprot(session, MAX_PROTEINS, args.num_jobs, args.commit_batch)
            load_coronavirus(session, LOAD_CORONAVIRUS, args.num_jobs, args.commit_batch)
            load_reactome(session, MAX_PATHWAYS, args.num_jobs, args.commit_batch)
            load_disgenet(session, MAX_DISEASES, args.num_jobs, args.commit_batch)
            load_dgibd(session, MAX_DRUGS, MAX_DRUG_INTERACTIONS, args.num_jobs, args.commit_batch)
            load_hpa(session, MAX_TISSUES, args.num_jobs, args.commit_batch)
            load_semmed(session, MAX_PUBLICATIONS, args.num_jobs, args.commit_batch)
            load_tissuenet(session, MAX_PROTEIN_INTERACTIONS, args.num_jobs, args.commit_batch)

    print("All data loaded.")
    print("Goodbye!")
