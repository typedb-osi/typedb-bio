import csv
import os
from zipfile import ZipFile
from typedb.client import TransactionType
from Migrators.Helpers.batchLoader import write_batches
from Migrators.Helpers.get_file import get_file


def load_protein_atlas(session, max_tissues, num_threads, batch_size):
    if max_tissues is None or max_tissues > 0:
        print("  ")
        print("Opening HPA dataset...")
        print("  ")
        dataset = get_tissue_dataset(max_tissues)
        insert_tissue(dataset, session)
        insert_ensemble_id(dataset, session, num_threads, batch_size)
        insert_gene_tissue(dataset, session, num_threads, batch_size)
        print(".....")
        print("Finished migrating HPA.")
        print(".....")


def get_tissue_dataset(max_rows):
    if not os.path.exists("Dataset/HumanProteinAtlas/normal_tissue.tsv.zip"):
        print("  Downloading protein atlas data")
        get_file("https://www.proteinatlas.org/download/normal_tissue.tsv.zip", "Dataset/HumanProteinAtlas/")
        print("  Finished downloading")

    with ZipFile("Dataset/HumanProteinAtlas/normal_tissue.tsv.zip", "r") as file:
        file.extractall("Dataset/HumanProteinAtlas")

    with open("Dataset/HumanProteinAtlas/normal_tissue.tsv", "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter="\t")
        next(reader)
        rows = list(reader)

    dataset = list()

    if max_rows is None:
        max_rows = len(rows)

    for row in rows[:max_rows]:
        data = {
            "ensembl-gene-id": row[0].strip(),
            "gene-symbol": row[1].strip(),
            "tissue": row[2].rstrip("1234567890").strip(),
            "cell-type": row[3].strip(),
            "expression-value": row[4].strip(),
            "expression-value-reliability": row[5].strip(),
        }

        if data["expression-value"].lower() in ("low", "medium", "high", "ascending", "descending"):
            dataset.append(data)
        elif data["expression-value"].lower() in ("not detected", "not representative", "n/a"):
            pass
        else:
            raise ValueError("Unhandled gene expression value: {}".format(data["expression-value"]))

    os.remove("Dataset/HumanProteinAtlas/normal_tissue.tsv")

    return dataset


def insert_tissue(dataset, session):
    print("  Starting with tissue.")

    cell_types = dict()

    for data in dataset:
        if data["tissue"] not in cell_types.keys():
            cell_types[data["tissue"]] = set()

        cell_types[data["tissue"]].add(data["cell-type"])

    for tissue in cell_types.keys():
        cell_types[tissue] = list(cell_types[tissue])

    with session.transaction(TransactionType.WRITE) as transaction:
        for tissue in cell_types.keys():
            query = "insert $t isa tissue, has tissue-name \"{}\";".format(tissue)

            for i in range(len(cell_types[tissue])):
                query += " $c{} isa cell, has cell-name \"{}\";".format(i, cell_types[tissue][i])
                query += " (composed-tissue: $t, composing-cell: $c{}) isa composition;".format(i)

            transaction.query().insert(query)

        transaction.commit()

    print("  Finished tissue. ({} entries)".format(len(cell_types)))


def insert_ensemble_id(dataset, session, num_threads, batch_size):
    ensembl_ids = dict()

    for data in dataset:
        if data["gene-symbol"] not in ensembl_ids.keys():
            ensembl_ids[data["gene-symbol"]] = set()

        ensembl_ids[data["gene-symbol"]].add(data["ensembl-gene-id"])

    queries = list()
    print("  Starting ensembl id.")

    for gene in ensembl_ids.keys():
        query = "match $g isa gene, has gene-symbol \"{}\"; insert".format(gene)

        for ensembl_id in ensembl_ids[gene]:
            query += " $g has ensembl-gene-stable-id \"{}\";".format(ensembl_id)

        queries.append(query)

    write_batches(session, queries, batch_size, num_threads)
    print("  Finished ensembl id! ({} entries)".format(len(ensembl_ids)))


def insert_gene_tissue(dataset, session, num_threads, batch_size):
    queries = list()
    print("  Starting expression.")

    for data in dataset:
        query = " ".join([
            "match",
            "$g isa gene,",
            "has gene-symbol \"{}\";",
            "$t isa tissue,",
            "has tissue-name \"{}\";",
            "$c isa cell,",
            "has cell-name \"{}\";",
            "(composed-tissue: $t, composing-cell: $c) isa composition;"
            "insert",
            "(expressed-gene: $g, expressing-cell: $c) isa expression,",
            "has expression-value \"{}\",",
            "has expression-value-reliability \"{}\";"
        ]).format(
            data["gene-symbol"],
            data["tissue"],
            data["cell-type"],
            data["expression-value"],
            data["expression-value-reliability"],
        )

        queries.append(query)

    write_batches(session, queries, batch_size, num_threads)
    print("  Finished Genes <> Tissues expression. ({} entries)".format(len(dataset)))
