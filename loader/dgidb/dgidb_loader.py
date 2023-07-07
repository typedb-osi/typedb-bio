from loader.util import write_batches, get_file, read_tsv


def load_dgibd(session, max_drugs, max_interactions, num_jobs, batch_size):
    if max_drugs is None or max_drugs > 0:
        print("Loading DGIdb dataset...")
        insert_drugs(session, max_drugs, num_jobs, batch_size)

        if max_interactions is None or max_interactions > 0:
            insert_interactions(session, max_interactions, num_jobs, batch_size)

        print("Dataset load complete.")
        print("--------------------------------------------------")


def insert_drugs(session, max_rows, num_jobs, batch_size):
    get_file("https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv", "dataset/dgidb")
    rows = read_tsv("dataset/dgidb/drugs.tsv")
    dataset = list()

    if max_rows is None:
        max_rows = len(rows)

    for row in rows[:max_rows]:
        data = {
            "drug-claim-name": row[0].strip("\"").strip(),
            "drug-name": row[1].strip("\"").strip(),
            "chembl-id": row[2].strip(),
            "drug-claim-source": row[3].strip(),
        }

        dataset.append(data)

    drugs = dict()

    for data in dataset:
        if data["chembl-id"] == "":
            continue

        if data["chembl-id"] not in drugs.keys():
            drugs[data["chembl-id"]] = {
                "drug-claim-name": list(),
                "drug-name": list(),
                "drug-claim-source": list(),
            }

        if data["drug-claim-name"] != "":
            drugs[data["chembl-id"]]["drug-claim-name"].append(data["drug-claim-name"])

        if data["drug-name"] != "":
            drugs[data["chembl-id"]]["drug-name"].append(data["drug-name"])

        if data["drug-claim-source"] != "":
            drugs[data["chembl-id"]]["drug-claim-source"].append(data["drug-claim-source"])

    queries = list()

    for chembl_id in drugs.keys():
        query = "insert $d isa drug, has chembl-id \"{}\"".format(chembl_id)

        for claim_name in drugs[chembl_id]["drug-claim-name"]:
            query += ", has dgidb-claim-name \"{}\"".format(claim_name)

        for name in drugs[chembl_id]["drug-name"]:
            query += ", has dgidb-name \"{}\"".format(name)

        for claim_source in drugs[chembl_id]["drug-claim-source"]:
            query += ", has drug-claim-source \"{}\"".format(claim_source)

        query += ";"

        queries.append(query)

    print("Inserting drugs:")
    write_batches(session, queries, num_jobs, batch_size)


def insert_interactions(session, max_rows, num_jobs, batch_size):
    get_file("https://www.dgidb.org/data/monthly_tsvs/2021-Jan/interactions.tsv", "dataset/dgidb")
    rows = read_tsv("dataset/dgidb/interactions.tsv")
    interactions = list()

    for row in rows[:max_rows]:
        data = {
            "gene-name": row[0].strip(),
            "entrez-id": row[2].strip(),
            "interaction-type": row[4].strip(),
            "drug-claim-name": row[5].strip(),
            "drug-name": row[7].strip(),
            "chembl-id": row[8].strip(),
        }

        interactions.append(data)

    queries = list()

    for interaction in interactions:
        if interaction["entrez-id"] != "":
            match_clause = " ".join([
                "match",
                "$g isa gene,",
                "has entrez-id \"{}\";",
                "$d isa drug,",
                "has chembl-id \"{}\";",
            ]).format(
                interaction["entrez-id"],
                interaction["chembl-id"],
            )

            match_clause += " not { (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction"
            insert_clause = "insert $r (target-gene: $g, interacting-drug: $d) isa drug-gene-interaction"

            if interaction["interaction-type"] != "":
                match_clause += ", has interaction-type \"{}\"".format(interaction["interaction-type"])
                insert_clause += ", has interaction-type \"{}\"".format(interaction["interaction-type"])

            match_clause += "; };"
            insert_clause += ";"
            query = match_clause + " " + insert_clause
            queries.append(query)

    print("Inserting drug-gene interactions:")
    write_batches(session, queries, num_jobs, batch_size)
