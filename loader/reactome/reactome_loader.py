from loader.util import write_batches, get_file, read_tsv


def load_reactome(session, max_pathways, num_jobs, batch_size):
    if max_pathways is None or max_pathways > 0:
        print("Loading Reactome dataset...")
        dataset = get_reactome_dataset(max_pathways)
        insert_pathways(session, num_jobs, batch_size, dataset)
        insert_pathway_interactions(session, num_jobs, batch_size, dataset)
        print("Dataset load complete.")
        print("--------------------------------------------------")


def get_reactome_dataset(max_rows):
    get_file("https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt", "dataset/reactome")
    rows = [row for row in read_tsv("dataset/reactome/UniProt2Reactome_All_Levels.txt", header=False) if row[5] == "Homo sapiens"]
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

    return dataset


def insert_pathways(session, num_jobs, batch_size, dataset):
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
        query = "insert $p isa pathway, has reactome-id \"{}\"".format(pathway["pathway-id"])

        for name in pathway["pathway-name"]:
            query += ", has pathway-name \"{}\"".format(name)

        query += ";"
        queries.append(query)

    print("Inserting pathways:")
    write_batches(session, queries, num_jobs, batch_size)


def insert_pathway_interactions(session, num_jobs, batch_size, dataset):
    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$p isa pathway, has reactome-id \"{}\";",
            "$pr isa protein, has uniprot-id \"{}\";",
            "not {{ (participated-pathway: $p, participating-protein: $pr) isa pathway-participation; }};"
            "insert",
            "(participated-pathway: $p, participating-protein: $pr) isa pathway-participation;"
        ]).format(
            data["pathway-id"],
            data["uniprot-id"],
        )

        queries.append(query)

    print("Inserting protein-pathway participations:")
    write_batches(session, queries, num_jobs, batch_size)
