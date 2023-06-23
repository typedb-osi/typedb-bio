import os
import wget
from Migrators.Helpers.batchLoader import write_batches
from Migrators.Helpers.get_file import get_file
from Migrators.Helpers.read_csv import read_tsv


def load_dgibd(session, max_drugs, max_interactions, num_threads, batch_size):
    if max_drugs is None or max_drugs > 0:
        print("  ")
        print("Opening DGIdb...")
        print("  ")
        insert_drugs(session, max_drugs, num_threads, batch_size)

        if max_interactions is None or max_interactions > 0:
            insert_interactions(session, max_interactions, num_threads, batch_size)

        print(".....")
        print("Finished migrating DGIdb.")
        print(".....")


def insert_drugs(session, max_rows, num_threads, batch_size):
    print("  Downloading dataset")
    get_file("https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv", "Dataset/DGIdb/")
    print("  Finished downloading")
    rows = read_tsv("Dataset/DGIdb/drugs.tsv")
    drugs = list()

    if max_rows is None:
        max_rows = len(rows)

    for row in rows[:max_rows]:
        data = {
            "drug-claim-name": row[0].strip("\"").strip(),
            "drug-name": row[1].strip("\"").strip(),
            "chembl-id": row[2].strip(),
            "drug-claim-source": row[3].strip(),
        }

        drugs.append(data)

    os.remove("Dataset/DGIdb/drugs.tsv")
    print("  Starting with drugs.")
    queries = list()

    for drug in drugs:
        query = " ".join([
            "insert",
            "$d isa drug,",
            "has drug-claim-name \"{}\",",
            "has drug-name \"{}\",",
            "has chembl-id \"{}\",",
            "has drug-claim-source \"{}\";",
        ]).format(
            drug["drug-claim-name"],
            drug["drug-name"],
            drug["chembl-id"],
            drug["drug-claim-source"]
        )

        queries.append(query)

    write_batches(session, queries, batch_size, num_threads)
    print("  Drugs inserted! ({} entries)".format(len(queries)))


def insert_interactions(session, max_rows, num_threads, batch_size):
    print("  Downloading drug-gene interactions dataset")
    #ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://www.dgidb.org/data/monthly_tsvs/2021-Jan/interactions.tsv"
    wget.download(url, "Dataset/DGIdb/")
    print("  Finished downloading")
    rows = read_tsv("Dataset/DGIdb/interactions.tsv")
    interactions = list()

    for row in rows[:max_rows]:
        data = {
            "gene-name": row[0],
            "entrez-id": row[2],
            "interaction-type": row[4],
            "drug-claim-name": row[5],
            "drug-name": row[7],
            "chembl-id": row[8],
        }

        interactions.append(data)

    os.remove("Dataset/DGIdb/interactions.tsv")
    print("  Starting with drug-gene interactions.")
    queries = list()

    for interaction in interactions:
        if interaction["entrez-id"] != "":
            query = " ".join([
                "match",
                "$g isa gene,",
                "has entrez-id \"{}\";",
                "$d isa drug,",
                "has drug-claim-name \"{}\";"
            ]).format(
                interaction["entrez-id"],
                interaction["drug-claim-name"],
            )

            # TODO Insert interaction type as a role

            query += " insert $r (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction"

            if interaction["interaction-type"] != "":
                query += ", has interaction-type \"{}\"".format(interaction["interaction-type"])

            query += ";"
            queries.append(query)

    write_batches(session, queries, batch_size, num_threads)
    print("  Finished drug-gene interactions. ({} entries) ".format(len(queries)))
