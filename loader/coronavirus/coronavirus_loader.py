import csv
from loader.util import write_batches, read_tsv


def load_coronavirus(session, max_coronaviruses, num_jobs, batch_size):
    if max_coronaviruses is None or max_coronaviruses > 0:
        print("Loading Coronavirus dataset...")
        virus_dataset = get_virus_dataset(max_coronaviruses)
        insert_extra_roleplayers(virus_dataset, session, num_jobs, batch_size)
        insert_viruses(virus_dataset, session, num_jobs, batch_size)
        insert_host_proteins(session, num_jobs, batch_size)
        print("Dataset load complete.")
        print("--------------------------------------------------")


def get_virus_dataset(max_rows):
    rows = read_tsv("dataset/coronavirus/Genome identity.csv", delimiter=",")
    dataset = list()

    if max_rows is None:
        max_rows = len(rows)

    for row in rows[:max_rows]:
        data = {
            "genbank-id": row["GenBank ID"],
            "identity-percent": row["Identity %"],
            "host": row["Host"],
            "location-discovered": row["Location discovered"],
            "names": [name for name in row["Coronavirus"].replace("]", "").split("[") if name != ""]
        }

        if len(row) > 5 and row[5] != "":
            data["names"].append(row[5])

        if len(row) > 6 and row[6] != "":
            data["names"].append(row[6])

        dataset.append(data)

    return dataset


def insert_extra_roleplayers(dataset, session, num_jobs, batch_size):
    organism_names = set()
    country_names = set()

    for data in dataset:
        organism_names.add(data["host"])
        country_names.add(data["location-discovered"])

    queries = list()

    for organism_name in organism_names:
        query = " ".join([
            "match",
            "?n = \"{}\";",
            "not {{ $o isa organism, has organism-name ?n; }};",
            "insert",
            "$o isa organism, has organism-name ?n;"
        ]).format(
            organism_name,
        )

        queries.append(query)

    for country_name in country_names:
        query = " ".join([
            "match",
            "?n = \"{}\";",
            "not {{ $c isa country, has country-name ?n; }};",
            "insert",
            "$c isa country, has country-name ?n;"
        ]).format(
            country_name,
        )

        queries.append(query)

    print("Inserting organisms and countries:")
    write_batches(session, queries, num_jobs, batch_size)


def insert_viruses(dataset, session, num_jobs, batch_size):
    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$c isa country, has country-name \"{}\";",
            "$o isa organism, has organism-name \"{}\";",
            "insert",
            "$v isa virus, has genbank-id \"{}\"",
        ]).format(
            data["location-discovered"],
            data["host"],
            data["genbank-id"],
        )

        for name in data["names"]:
            query += ", has virus-name \"{}\"".format(name)

        query += " ".join([
            ", has identity-percentage {};",
            "(discovering-location: $c, discovered-virus: $v) isa discovery;",
            "(host-organism: $o, hosted-virus: $v) isa virus-hosting;",
        ]).format(
            data["identity-percent"],
        )

        queries.append(query)

    print("Inserting viruses:")
    write_batches(session, queries, num_jobs, batch_size)


def insert_host_proteins(session, num_jobs, batch_size):
    rows = read_tsv("dataset/coronavirus/Host proteins (potential drug targets).csv", delimiter=",")
    dataset = list()

    for row in rows:
        data = {
            "coronavirus": row["Coronavirus"],
            "uniprot-id": row["UniProt ID"],
            "entrez-id": row["Host Gene Entrez ID"],
        }

        dataset.append(data)

    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$v isa virus, has virus-name \"{}\";",
            "$p isa protein, has uniprot-id \"{}\";",
            "not {{ (targeted-protein: $p, interacting-virus: $v) isa virus-protein-interaction; }};",
            "insert",
            "(targeted-protein: $p, interacting-virus: $v) isa virus-protein-interaction;",
        ]).format(
            data["coronavirus"],
            data["uniprot-id"],
            data["entrez-id"],
        )

        queries.append(query)

    print("Inserting protein-virus associations:")
    write_batches(session, queries, num_jobs, batch_size)
