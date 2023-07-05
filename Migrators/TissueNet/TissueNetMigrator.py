import glob
import csv
import os.path
from zipfile import ZipFile
from Migrators.Helpers.batchLoader import write_batches
from Migrators.Helpers.get_file import get_file
from Migrators.TissueNet.mapper import tissue_mapper


def load_tissuenet(session, max_interactions, num_jobs, batch_size):
    if max_interactions is None or max_interactions > 0:
        if not os.path.exists("Dataset/TissueNet/HPA-Protein/HPA-Protein.zip"):
            print("Downloading HPA - Protein dataset from TissueNet")
            get_file("https://netbio.bgu.ac.il/tissuenet2-interactomes/TissueNet2.0/HPA-Protein.zip", "Dataset/TissueNet/HPA-Protein")
            print("\n Finished downloading dataset")

        print("Extracting TissueNet data")

        with ZipFile("Dataset/TissueNet/HPA-Protein/HPA-Protein.zip", "r") as file:
            file.extractall("Dataset/TissueNet/HPA-Protein/")

        paths = glob.iglob("Dataset/TissueNet/HPA-protein/*.tsv")

        print("  ")
        print("Opening TissueNet dataset...")
        print("  ")

        for path in paths:
            file_name = path.split("/")[-1].split(".")[0]
            tissue = tissue_mapper(file_name)
            print("Opening dataset: {}".format(file_name))

            with open(path, "r", encoding="utf-8") as file:
                reader = csv.reader(file, delimiter="\t")
                next(reader)
                rows = list(reader)

            os.remove(path)
            interactions = list()

            if max_interactions is None:
                max_interactions = len(rows)

            for row in rows[:max_interactions]:
                interaction = {
                    "gene-id-1": row[0].strip(),
                    "gene-id-2": row[1].strip(),
                    "tissue-name": tissue["tissue-name"],
                    "cell-name": tissue["cell-name"],
                }

                interactions.append(interaction)

            load_interactions(interactions, session, num_jobs, batch_size)
            load_contexts(interactions, session, num_jobs, batch_size)

        print(".....")
        print("Finished migrating TissueNet file.")
        print(".....")


def load_interactions(interactions, session, num_jobs, batch_size):
    queries = list()

    for interaction in interactions:
        query = " ".join([
            "match",
            "$g1 isa gene,",
            "has ensembl-gene-stable-id \"{}\";",
            "$g2 isa gene,",
            "has ensembl-gene-stable-id \"{}\";",
            "not {{ (interacting-gene: $g1, interacting-gene: $g2) isa gene-gene-interaction; }};"
            "insert",
            "$i (interacting-gene: $g1, interacting-gene: $g2) isa gene-gene-interaction;",
        ]).format(
            interaction["gene-id-1"],
            interaction["gene-id-2"],
        )

        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)


def load_contexts(interactions, session, num_jobs, batch_size):
    queries = list()

    for interaction in interactions:
        if interaction["tissue-name"] is None and interaction["cell-name"] is None:
            continue

        query = " ".join([
            "match",
            "$g1 isa gene,",
            "has ensembl-gene-stable-id \"{}\";",
            "$g2 isa gene,",
            "has ensembl-gene-stable-id \"{}\";",
            "$i (interacting-gene: $g1, interacting-gene: $g2) isa gene-gene-interaction;",
            "$t isa tissue;",
            "$c isa cell;",
            "(composed-tissue: $t, composing-cell: $c) isa composition;",
            "not {{ (biomolecular-process: $i, cell-context: $c) isa process-localisation; }};",
        ]).format(
            interaction["gene-id-1"],
            interaction["gene-id-2"],
        )

        if interaction["tissue-name"] is not None:
            query += " $t has tissue-name \"{}\";".format(interaction["tissue-name"])

        if interaction["cell-name"] is not None:
            query += " $c has cell-name \"{}\";".format(interaction["cell-name"])

        query += " insert (biomolecular-process: $i, cell-context: $c) isa process-localisation;"

        queries.append(query)

    write_batches(session, queries, batch_size, num_jobs)
