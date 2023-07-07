import glob
import os.path
from zipfile import ZipFile
from loader.util import write_batches, get_file, read_tsv
from loader.tissuenet.mapper import tissue_mapper


def load_tissuenet(session, max_interactions, num_jobs, batch_size):
    if max_interactions is None or max_interactions > 0:
        print("Loading TissueNet dataset...")
        get_file("https://netbio.bgu.ac.il/tissuenet2-interactomes/TissueNet2.0/HPA-Protein.zip", "dataset/tissuenet")

        with ZipFile("dataset/tissuenet/HPA-Protein.zip", "r") as file:
            file.extractall("dataset/tissuenet/")

        paths = glob.iglob("dataset/tissuenet/*.tsv")

        for path in paths:
            file_name = path.split("/")[-1].split(".")[0]
            tissue = tissue_mapper(file_name)
            print("Loading TissueNet \"{}\" dataset...".format(file_name))
            rows = read_tsv(path)
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

        print("Dataset load complete.")
        print("--------------------------------------------------")


def load_interactions(interactions, session, num_jobs, batch_size):
    queries = list()

    for interaction in interactions:
        query = " ".join([
            "match",
            "$g1 isa gene,",
            "has ensembl-gene-id \"{}\";",
            "$g2 isa gene,",
            "has ensembl-gene-id \"{}\";",
            "not {{ (interacting-gene: $g1, interacting-gene: $g2) isa gene-gene-interaction; }};"
            "insert",
            "$i (interacting-gene: $g1, interacting-gene: $g2) isa gene-gene-interaction;",
        ]).format(
            interaction["gene-id-1"],
            interaction["gene-id-2"],
        )

        queries.append(query)

    print("Inserting protein-protein interactions:")
    write_batches(session, queries, num_jobs, batch_size)


def load_contexts(interactions, session, num_jobs, batch_size):
    queries = list()

    for interaction in interactions:
        if interaction["tissue-name"] is None and interaction["cell-name"] is None:
            continue

        query = " ".join([
            "match",
            "$g1 isa gene,",
            "has ensembl-gene-id \"{}\";",
            "$g2 isa gene,",
            "has ensembl-gene-id \"{}\";",
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

    print("Inserting tissue contexts:")
    write_batches(session, queries, num_jobs, batch_size)
