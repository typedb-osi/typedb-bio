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
from Migrators.Uniprot.UniprotMigrator import migrate_uniprot
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
    parser.add_argument(
        "-v", "--verbose", help="Verbosity (default: False)", default=False
    )
    return parser


NUM_PROTEINS = 10000000  # Total proteins to migrate (There are total of 20350 proteins)
NUM_DIS = 10000000  # Total diseases to migrate
NUM_DR = 10000000  # Total drug to migrate (32k total)
NUM_INT = 10000000  # Total drug-gene interactions to migrate (42k total)
NUM_PATH = 10000000  # Total pathway associations to migrate
NUM_TN = 10000000  # Total TissueNet being migrated
NUM_PA = 10000000  # Total Tissues <> Genes to migrate
NUM_SEM = 10000000  # Total number of rows from SemMed to migrate

start = timer()
if __name__ == "__main__":
    parser = migrator_parser()
    args = parser.parse_args()

    # This is a global flag toggling counter and query printouts
    # when we want to see less detail
    verbose = args.verbose

    uri = args.address + ":" + args.port
    with TypeDB.core_client(uri) as client:
        initialise_database(client, args.database, args.force)
        with client.session(args.database, SessionType.DATA) as session:
            migrate_uniprot(session, NUM_PROTEINS, args.n_jobs, args.commit_batch)
            migrate_coronavirus(session)
            migrate_reactome(session, NUM_PATH, args.n_jobs, args.commit_batch)
            migrate_disgenet(session, NUM_DIS, args.n_jobs, args.commit_batch)
            migrate_dgibd(session, NUM_DR, NUM_INT, args.n_jobs, args.commit_batch)
            migrate_protein_atlas(session, NUM_PA, args.n_jobs, args.commit_batch)
            migrate_semmed(session, uri, NUM_SEM, args.n_jobs, args.commit_batch)

            # TODO: add protein interaction relations in tissues
            migrate_tissuenet(session, args.n_jobs, args.commit_batch)
end = timer()
time_in_sec = end - start
print("Elapsed time: " + str(time_in_sec) + " seconds.")
