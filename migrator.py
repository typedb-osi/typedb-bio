from Schema.schema_insert import insertSchema
from Migrators.Uniprot.UniprotMigrator import uniprotMigrate
from Migrators.Coronaviruses.CoronavirusMigrator import coronavirusMigrator
from Migrators.Disgenet.disgenetMigrator import disgenetMigrator
from Migrators.DGIdb.DGIdbMigrator import dgidbMigrator
from Migrators.Reactome.reactomeMigrator import reactomeMigrator

URI = "localhost:48555"
KEYSPACE = "biograkn_covid"

NUM_PROTEINS = 22000 # Total proteins to migrate (There are total of 20350 proteins)
NUM_DIS = 200000 # Total diseases to migrate 
NUM_DR = 40000 # Total drug to migrate (32k total)
NUM_INT = 50000 # Total drug-gene interactions to migrate (42k total)
NUM_PATH = 5000000 # Total pathway associations to migrate
num_threads = 8 # Number of threads to enable multi threading
ctn = 400 # This sets the number of queries are made before we commit


insertSchema(URI, KEYSPACE)
uniprotMigrate(URI, KEYSPACE, NUM_PROTEINS, num_threads, ctn)
coronavirusMigrator(URI, KEYSPACE)
disgenetMigrator(URI,KEYSPACE, NUM_DIS, num_threads, ctn)
dgidbMigrator(URI,KEYSPACE, NUM_DR, NUM_INT, num_threads, ctn)
reactomeMigrator(URI,KEYSPACE, NUM_PATH, num_threads, ctn)