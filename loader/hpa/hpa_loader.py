from loader.util import write_batches, get_file, read_tsv


def load_hpa(session, max_tissues, num_jobs, batch_size):
    if max_tissues is None or max_tissues > 0:
        print("Loading HPA dataset...")
        dataset = get_tissue_dataset(max_tissues)
        insert_tissue(dataset, session, num_jobs, batch_size)
        insert_ensemble_id(dataset, session, num_jobs, batch_size)
        insert_gene_tissue(dataset, session, num_jobs, batch_size)
        print("Dataset load complete.")
        print("--------------------------------------------------")


def get_tissue_dataset(max_rows):
    get_file("https://www.proteinatlas.org/download/normal_tissue.tsv.zip", "dataset/hpa")
    rows = read_tsv("dataset/hpa/normal_tissue.tsv.zip", archive="zip")
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

    return dataset


def insert_tissue(dataset, session, num_jobs, batch_size):
    cell_types = dict()

    for data in dataset:
        if data["tissue"] not in cell_types.keys():
            cell_types[data["tissue"]] = set()

        cell_types[data["tissue"]].add(data["cell-type"])

    for tissue in cell_types.keys():
        cell_types[tissue] = list(cell_types[tissue])

    queries = list()

    for tissue in cell_types.keys():
        query = "insert $t isa tissue, has tissue-name \"{}\";".format(tissue)

        for i, cell_name in enumerate(cell_types[tissue]):
            query += " $c{} isa cell, has cell-name \"{}\";".format(i, cell_name)
            query += " (composed-tissue: $t, composing-cell: $c{}) isa tissue-composition;".format(i)

        queries.append(query)

    print("Inserting tissues:")
    write_batches(session, queries, num_jobs, batch_size)


def insert_ensemble_id(dataset, session, num_jobs, batch_size):
    ensembl_ids = dict()

    for data in dataset:
        if data["gene-symbol"] not in ensembl_ids.keys():
            ensembl_ids[data["gene-symbol"]] = set()

        ensembl_ids[data["gene-symbol"]].add(data["ensembl-gene-id"])

    queries = list()

    for gene in ensembl_ids.keys():
        query = "match $g isa gene, has primary-gene-symbol \"{}\"; insert".format(gene)

        for ensembl_id in ensembl_ids[gene]:
            query += " $g has ensembl-gene-id \"{}\";".format(ensembl_id)

        queries.append(query)

    print("Inserting additional gene IDs:")
    write_batches(session, queries, num_jobs, batch_size)


def insert_gene_tissue(dataset, session, num_jobs, batch_size):
    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$g isa gene,",
            "has primary-gene-symbol \"{}\";",
            "$t isa tissue,",
            "has tissue-name \"{}\";",
            "$c isa cell,",
            "has cell-name \"{}\";",
            "(composed-tissue: $t, composing-cell: $c) isa tissue-composition;"
            "insert",
            "(expressed-gene: $g, expressing-cell: $c) isa cell-expression,",
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

    print("Inserting gene-tissue expressions:")
    write_batches(session, queries, num_jobs, batch_size)
