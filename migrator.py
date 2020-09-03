from Schema.schema_insert import insertSchema
from Migrators.Uniprot.UniprotMigrator import uniprotMigrate
from Migrators.CORD_NER.cord_ner_migrator import cord_ner_migrator
from Migrators.Coronaviruses.CoronavirusMigrator import coronavirusMigrator
from Migrators.Disgenet.disgenetMigrator import disgenetMigrator
from Migrators.DGIdb.DGIdbMigrator import dgidbMigrator
from Migrators.Reactome.reactomeMigrator import reactomeMigrator
from Migrators.HumanProteinAtlas.HumanProteinAtlasMigrator import proteinAtlasMigrator
from Migrators.SemMed.semmed_migrator import migrate_semmed
from timeit import default_timer as timer

# for Windows URI = IP:port (127.0.0.1:48555)
URI = "insert IP or localhost"
KEYSPACE = "biograkn_covid"

NUM_PROTEINS = 1000000 # Total proteins to migrate (There are total of 20350 proteins)
NUM_DIS = 1000000 # Total diseases to migrate 
NUM_DR = 1000000 # Total drug to migrate (32k total)
NUM_INT = 1000000 # Total drug-gene interactions to migrate (42k total)
NUM_PATH = 1000000 # Total pathway associations to migrate
NUM_TN = 1000000 # Total TissueNet being migrated
NUM_PA = 1000000 # Total Tissues <> Genes to migrate
NUM_NER = 1000000 # Total number of publications (authors: 110k; journals: 1.7k; publications: 29k)
NUM_SEM = 1000000 # Total number of rows from Semmed to migrate
num_threads = 8 # Number of threads to enable multi threading
ctn = 500 # This sets the number of queries are made before we commit


start = timer()
insertSchema(URI, KEYSPACE)
uniprotMigrate(URI, KEYSPACE, NUM_PROTEINS, num_threads, ctn)
coronavirusMigrator(URI, KEYSPACE)
reactomeMigrator(URI,KEYSPACE, NUM_PATH, num_threads, ctn)
disgenetMigrator(URI,KEYSPACE, NUM_DIS, num_threads, ctn)
dgidbMigrator(URI,KEYSPACE, NUM_DR, NUM_INT, num_threads, ctn)
proteinAtlasMigrator(URI,KEYSPACE, NUM_PA, num_threads, ctn)
cord_ner_migrator(URI,KEYSPACE, NUM_NER, num_threads, ctn)
migrate_semmed(URI,KEYSPACE, NUM_SEM, num_threads, ctn)
end = timer()
time_in_sec = end - start
print("Elapsed time: " + str(time_in_sec) + " seconds.")