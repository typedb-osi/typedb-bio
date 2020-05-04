from Schema.schema_insert import insertSchema
from Migrators.Uniprot.UniprotMigrator import uniprotMigrate
from Migrators.Coronaviruses.CoronavirusMigrator import coronavirusMigrator
from Migrators.Disgenet.disgenetMigrator import disgenetMigrator

URI = "localhost:48555"
KEYSPACE = "biograkn_covid"

NUM_PROTEINS = 5000 # Total proteins to migrate (There are total of 20350 proteins)
NUM_DIS = 20000 # Total diseases to migrate 
num_threads = 8 # Number of threads to enable multi threading
ctn = 500 # This sets the number of queries are made before we commit

insertSchema(URI, KEYSPACE)
uniprotMigrate(URI, KEYSPACE, NUM_PROTEINS, num_threads, ctn)
coronavirusMigrator(URI, KEYSPACE)
disgenetMigrator(URI,KEYSPACE, NUM_DIS, num_threads, ctn)