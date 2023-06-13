import os
import wget

from Migrators.Helpers.batchLoader import write_batches
from Migrators.Helpers.read_csv import read_tsv


def load_reactome(session, max_pathways, num_threads, batch_size):
    if max_pathways is None or max_pathways > 0:
        print(".....")
        print("Starting with reactome.")
        print(".....")
        dataset = get_reactome_dataset(max_pathways)
        insert_pathways(session, num_threads, batch_size, dataset)
        insert_pathway_interactions(session, num_threads, batch_size, dataset)
        print(".....")
        print("Finished with reactome.")
        print(".....")


def get_reactome_dataset(max_rows):
    print("Downloading reactome dataset")
    url = "https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt"
    wget.download(url, "Dataset/Reactome/")
    print("\nFinished downloading reactome dataset")
    path = "Dataset/Reactome/UniProt2Reactome_All_Levels.txt"
    rows = [row for row in read_tsv(path) if row[5] == "Homo sapiens"]
    dataset = list()

    if max_rows is None:
        max_rows = len(rows)

    for row in rows[:max_rows]:
        data = {
            "uniprot-id": row[0].strip("\""),
            "pathway-id": row[1].strip("\""),
            "pathway-name": row[3],
            "organism": row[5],
        }

        dataset.append(data)

    os.remove('Dataset/Reactome/UniProt2Reactome_All_Levels.txt')
    return dataset


def insert_pathways(session, num_threads, batch_size, dataset):
    print("  Starting with reactome pathways.")

    pathway_ids = {data["pathway-id"] for data in dataset}
    pathways = list()

    for pathway_id in pathway_ids:
        pathway = dict()
        pathway["pathway-id"] = pathway_id
        pathway["pathway-name"] = list()

        for data in dataset:
            if data["pathway-id"] == pathway_id:
                pathway["pathway-name"].append(data["pathway-name"])

        pathways.append(pathway)

    queries = list()

    for pathway in pathways:
        query = "insert $p isa pathway, has pathway-id \"{}\"".format(pathway["pathway-id"])

        for name in pathway["pathway-name"]:
            query += ", has pathway-name \"{}\"".format(name)

        query += ";"
        queries.append(query)

    write_batches(session, queries, batch_size, num_threads)
    print("  Reactome Pathways inserted! ({} entries)".format(len(queries)))


def insert_pathway_interactions(session, num_threads, batch_size, dataset):
    print("  Starting with reactome pathway interactions.")
    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$p isa pathway, has pathway-id \"{}\";",
            "$pr isa protein, has uniprot-id \"{}\";",
            "not {{ (participated-pathway: $p, participating-protein: $pr) isa pathway-participation; }};"
            "insert",
            "(participated-pathway: $p, participating-protein: $pr) isa pathway-participation;"
        ]).format(
            data["pathway-id"],
            data["uniprot-id"],
        )

        queries.append(query)

    write_batches(session, queries, batch_size, num_threads)
    print(" Reactome pathway interactions inserted! ({} entries)".format(len(queries)))
