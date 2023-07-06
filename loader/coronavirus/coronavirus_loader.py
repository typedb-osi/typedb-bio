import csv
from loader.util import write_batches, read_tsv


def load_coronavirus(session, load, num_jobs, batch_size):
    if load:
        print("Loading Coronavirus dataset...")
        virus_dataset = get_virus_dataset()
        insert_extra_roleplayers(virus_dataset, session, num_jobs, batch_size)
        insert_viruses(virus_dataset, session, num_jobs, batch_size)
        insert_host_proteins(session, num_jobs, batch_size)
        print("Dataset load complete.")
        print("--------------------------------------------------")


def get_virus_dataset():
    rows = read_tsv("dataset/coronavirus/Genome identity.csv", delimiter=",")
    dataset = list()

    for row in rows:
        data = {
            "genbank-id": row[0],
            "identity-percent": row[2],
            "host": row[3].strip(),
            "location-discovered": row[4].strip(),
            "names": [row[1].strip()]
        }

        if len(row) > 5 and row[5].strip != "":
            data["names"].append(row[5].strip())

        if len(row) > 6 and row[6].strip != "":
            data["names"].append(row[6].strip())

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
            "(hosting-organism: $o, hosted-virus: $v) isa organism-virus-hosting;",
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
            "coronavirus": row[0].strip(),
            "uniprot-id": row[3].strip(),
            "entrez-id": row[4].strip()
        }

        dataset.append(data)

    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$v isa virus, has virus-name \"{}\";",
            "$g isa gene, has entrez-id \"{}\";",
            "not {{ (associated-virus-gene: $g, associated-virus: $v) isa gene-virus-association; }};",
            "insert",
            "(associated-virus-gene: $g, associated-virus: $v) isa gene-virus-association;",
        ]).format(
            data["coronavirus"],
            data["uniprot-id"],
            data["entrez-id"],
        )

        queries.append(query)

        query = " ".join([
            "match",
            "$v isa virus, has virus-name \"{}\";",
            "$p isa protein, has uniprot-id \"{}\";",
            "not {{ (hosting-virus-protein: $p, associated-virus: $v) isa protein-virus-association; }};",
            "insert",
            "(hosting-virus-protein: $p, associated-virus: $v) isa protein-virus-association;",
        ]).format(
            data["coronavirus"],
            data["uniprot-id"],
            data["entrez-id"],
        )

        queries.append(query)

    print("Inserting protein-virus associations:")
    write_batches(session, queries, num_jobs, batch_size)
