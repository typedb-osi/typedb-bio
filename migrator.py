from Schema.schema_insert import insertSchema
from Migrators.Uniprot.UniprotMigrator import uniprotMigrate
from Migrators.Coronaviruses.CoronavirusMigrator import coronavirusMigrator

URI = "localhost:48555"
KEYSPACE = "biograkn_covid"

NUM_PROTEINS = 20350 # There are total of 20350 proteins

insertSchema(URI, KEYSPACE)
uniprotMigrate(URI, KEYSPACE, NUM_PROTEINS)
coronavirusMigrator(URI, KEYSPACE)