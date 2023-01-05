from typedb.client import *
from schema.initialise import initialise_database
from Migrators.Uniprot.UniprotMigrator import migrate_uniprot
from Migrators.CORD_NER.cord_ner_migrator import migrate_cord_ner
from Migrators.Coronaviruses.CoronavirusMigrator import migrate_coronavirus
from Migrators.Disgenet.disgenetMigrator import migrate_disgenet
from Migrators.DGIdb.DGIdbMigrator import migrate_dgibd
from Migrators.Reactome.reactomeMigrator import migrate_reactome
from Migrators.HumanProteinAtlas.HumanProteinAtlasMigrator import migrate_protein_atlas
from Migrators.SemMed.semmed_migrator import migrate_semmed
from Migrators.TissueNet.TissueNetMigrator import migrate_tissuenet
from timeit import default_timer as timer
import argparse


def migrator_parser():
    parser = argparse.ArgumentParser(
        description='Define bio_covid database and insert data by calling separate migrate scripts.')
    parser.add_argument("-n", "--num_threads", type=int,
                        help="Number of threads to enable multi-threading (default: %(default)s)", default=8)
    parser.add_argument("-c", "--commit_batch", type=int,
                        help="Sets the number of queries made per commit (default: %(default)s)", default=50)
    parser.add_argument("-d", "--database", help="Database name (default: %(default)s)", default="bio_covid")
    parser.add_argument("-f", "--force",
                        help="Force overwrite the database even if a database by this name already exists",
                        action='store_true')
    parser.add_argument("-a", "--address", help="Server host address (default: %(default)s)", default="localhost")
    parser.add_argument("-p", "--port", help="Server port (default: %(default)s)", default="1729")
    parser.add_argument("-v", "--verbose", help="Use verbose output", action='store_true')
    return parser


NUM_PROTEINS = 10000000  # Total proteins to migrate (There are total of 20350 proteins)
NUM_DIS = 10000000  # Total diseases to migrate
NUM_DR = 10000000  # Total drug to migrate (32k total)
NUM_INT = 10000000  # Total drug-gene interactions to migrate (42k total)
NUM_PATH = 10000000  # Total pathway associations to migrate
NUM_TN = 10000000  # Total TissueNet being migrated
NUM_PA = 10000000  # Total Tissues <> Genes to migrate
NUM_NER = 10000000  # Total number of publications (authors: 110k; journals: 1.7k; publications: 29k)
NUM_SEM = 10000000  # Total number of rows from Semmed to migrate

start = timer()
if __name__ == "__main__":
    parser = migrator_parser()
    args = parser.parse_args()

    # This is a global flag toggling counter and query printouts when we want to see less detail
    verbose = args.verbose

    uri = args.address + ":" + args.port
    client = TypeDB.core_client(uri)

    initialise_database(client, args.database, args.force)

    with client.session(args.database, SessionType.DATA) as session:
        migrate_uniprot(session, NUM_PROTEINS, args.num_threads, args.commit_batch)
        migrate_coronavirus(session)
        migrate_reactome(session, NUM_PATH, args.num_threads, args.commit_batch)
        migrate_disgenet(session, NUM_DIS, args.num_threads, args.commit_batch)  # > FAILS WITH TOO MANY OPEN FILES #
        migrate_dgibd(session, NUM_DR, NUM_INT, args.num_threads, args.commit_batch)
        migrate_protein_atlas(session, NUM_PA, args.num_threads, args.commit_batch)
        migrate_semmed(session, uri, NUM_SEM, args.num_threads, args.commit_batch)
        migrate_tissuenet(session, args.num_threads, args.commit_batch)
    migrate_cord_ner(client, args.database, SessionType.DATA, NUM_NER, args.num_threads, args.commit_batch)  # DOWNLOAD THE CORD-NER-FULL.json (ADD TO DATASET/CORD_NER): https://uofi.app.box.com/s/k8pw7d5kozzpoum2jwfaqdaey1oij93x/file/651148518303
# add tissueNet?
# add CORD-19?
end = timer()
time_in_sec = end - start
print("Elapsed time: " + str(time_in_sec) + " seconds.")
