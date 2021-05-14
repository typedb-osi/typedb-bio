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
import argparse 
 
# Initialize parser
parser = argparse.ArgumentParser(description='Define biograkn_covid database and insert data by calling separate migrate scripts.')

# Add optional arguments
parser.add_argument("-n", "--num_threads", type="int", help = "Number of threads to enable multi-threading (default: 8)", default=8)
parser.add_argument("-c", "--commit_rate", help = "Sets the number of queries made before we commit (default: 50)", default=50)
parser.add_argument("-d", "--database", help = "Database name (default: biograkn_covid)", default="biograkn_covid")
parser.add_argument("-f", "--force", help = "Force creation of new database even if a database by this name already exists (default: False)", default=False)
parser.add_argument("-h", "--host", help = "Server host address (default: localhost)", default="localhost")
parser.add_argument("-p", "--port", help = "Server port (default: 1729)", default="1729")
parser.add_argument("-v", "--verbose", help = "Verbosity (default: True)", default=True)

# Read arguments from command line
args = parser.parse_args()

# This is a global flag toggling counter and query printouts when we want to see less detail
# there is another flag in the batch loader module
global verbose 
verbose = args.verbose

# for Windows URI = IP:port (127.0.0.1:1729)
URI = args.host+":"+args.port

NUM_PROTEINS = 1000000 # Total proteins to migrate (There are total of 20350 proteins)
NUM_DIS = 1000000 # Total diseases to migrate 
NUM_DR = 1000000 # Total drug to migrate (32k total)
NUM_INT = 1000000 # Total drug-gene interactions to migrate (42k total)
NUM_PATH = 1000000 # Total pathway associations to migrate
NUM_TN = 1000000 # Total TissueNet being migrated
NUM_PA = 1000000 # Total Tissues <> Genes to migrate
NUM_NER = 30000 # Total number of publications (authors: 110k; journals: 1.7k; publications: 29k)
NUM_SEM = 10000000 # Total number of rows from Semmed to migrate

start = timer()
if __name__ == "__main__":
	insertSchema(URI, args.database, args.force)
	uniprotMigrate(URI, args.database, NUM_PROTEINS, args.num_threads, args.commit_rate)
	coronavirusMigrator(URI, args.database)
	reactomeMigrator(URI,args.database, NUM_PATH, args.num_threads, args.commit_rate)
	disgenetMigrator(URI,args.database, NUM_DIS, args.num_threads, args.commit_rate) #> FAILS WITH TOO MANY OPEN FILES # 
	dgidbMigrator(URI,args.database, NUM_DR, NUM_INT, args.num_threads, args.commit_rate)
	proteinAtlasMigrator(URI,args.database, NUM_PA, args.num_threads, args.commit_rate)
	cord_ner_migrator(URI,args.database, NUM_NER, args.num_threads, args.commit_rate) # DOWNLOAD THE CORD-NER-FULL.json (ADD TO DATASET/CORD_NER): https://uofi.app.box.com/s/k8pw7d5kozzpoum2jwfaqdaey1oij93x/file/651148518303
	migrate_semmed(URI,args.database, NUM_SEM, args.num_threads, args.commit_rate)
	# add tissueNet?
	# add CORD-19?
end = timer()
time_in_sec = end - start
print("Elapsed time: " + str(time_in_sec) + " seconds.")