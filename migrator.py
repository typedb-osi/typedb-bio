# -*- coding: utf-8 -*-
"""Script for migrating data from various sources into the database."""
import argparse
from timeit import default_timer as timer

from typedb.client import SessionType, TypeDB

from Migrators.Coronaviruses.CoronavirusMigrator import migrate_coronavirus
from Migrators.DGIdb.DGIdbMigrator import migrate_dgibd
from Migrators.Disgenet.disgenetMigrator import migrate_disgenet
from Migrators.HumanProteinAtlas.HumanProteinAtlasMigrator import migrate_protein_atlas
from Migrators.Reactome.reactomeMigrator import migrate_reactome
from Migrators.SemMed.pipeline import migrate_semmed
from Migrators.TissueNet.TissueNetMigrator import migrate_tissuenet
from Migrators.Uniprot.UniprotMigrator import load_uniprot
from Schema.initialise import initialise_database


def migrator_parser():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser(
        description="Defines bio database and insert data by "
        "calling separate migrate scripts."
    )
    parser.add_argument(
        "-n",
        "--n_jobs",
        type=int,
        help="The maximum number of concurrently running jobs (default: 8)",
        default=8,
    )
    parser.add_argument(
        "-c",
        "--commit_batch",
        help="Sets the number of queries made per commit (default: 50)",
        default=50,
    )
    parser.add_argument(
        "-d", "--database", help="Database name (default: bio)", default="bio"
    )
    parser.add_argument(
        "-f",
        "--force",
        help="Force overwrite the database even if "
        "a database by this name already exists (default: False)",
        default=False,
    )
    parser.add_argument(
        "-a",
        "--address",
        help="Server host address (default: localhost)",
        default="localhost",
    )
    parser.add_argument(
        "-p", "--port", help="Server port (default: 1729)", default="1729"
    )
    return parser


# Set a constant to None to migrate all entries.
# Set a constant to 0 to migrate no entries.
MAX_PROTEINS = None  # Maximum number of proteins to migrate.
MAX_DISEASES = 0  # Maximum number of diseases to migrate.
MAX_DRUGS = 0  # Maximum number of drugs to migrate.
MAX_INTERACTIONS = 0  # Maximum number of drug-gene interactions to migrate.
MAX_PATHWAYS = 0  # Maximum number of pathway associations to migrate.
MAX_TISSUENET_ROWS = 0  # Maximum number of TissueNet rows to migrate.
MAX_TISSUES = 0  # Maximum number of Tissues to migrate.
MAX_SEMMED_ROWS = 0  # Maximum number of SemMed rows to migrate.

start = timer()
if __name__ == "__main__":
    parser = migrator_parser()
    args = parser.parse_args()

    uri = args.address + ":" + args.port
    with TypeDB.core_client(uri) as client:
        initialise_database(client, args.database, args.force)
        with client.session(args.database, SessionType.DATA) as session:
            load_uniprot(session, MAX_PROTEINS, args.n_jobs, args.commit_batch)
            migrate_coronavirus(session)
            migrate_reactome(session, MAX_PATHWAYS, args.n_jobs, args.commit_batch)
            migrate_disgenet(session, MAX_DISEASES, args.n_jobs, args.commit_batch)
            migrate_dgibd(session, MAX_DRUGS, MAX_INTERACTIONS, args.n_jobs, args.commit_batch)
            migrate_protein_atlas(session, MAX_TISSUES, args.n_jobs, args.commit_batch)
            migrate_semmed(session, uri, MAX_SEMMED_ROWS, args.n_jobs, args.commit_batch)

            # TODO: add protein interaction relations in tissues
            migrate_tissuenet(session, MAX_TISSUENET_ROWS, args.n_jobs, args.commit_batch)
end = timer()
time_in_sec = end - start
print("Elapsed time: " + str(time_in_sec) + " seconds.")
