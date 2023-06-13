import csv
from typedb.client import TransactionType


def load_coronavirus(session):
    print(".....")
    print("Starting with Coronavirus file.")
    print(".....")

    with session.transaction(TransactionType.WRITE) as transaction:
        # TODO: Move countries and organisms to a utility loader.
        query = " ".join([
            "insert",
            "$c isa country, has country-name \"China\";",
            "$c2 isa country, has country-name \"Kingdom of Saudi Arabia\";",
            "$c3 isa country, has country-name \"USA\";",
            "$c4 isa country, has country-name \"South Korea\";",
            "$o isa organism, has organism-name \"Mouse\";",
        ])

        transaction.query().insert(query)
        transaction.commit()

    insert_viruses(session)
    insert_host_proteins(session)
    print(".....")
    print("Finished with Coronavirus file.")
    print(".....")


def insert_viruses(session):
    with open("Dataset/Coronaviruses/Genome identity.csv", "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=",")
        next(reader)
        rows = list(reader)

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

    with session.transaction(TransactionType.WRITE) as transaction:
        for query in queries:
            transaction.query().insert(query)

        transaction.commit()


def insert_host_proteins(session):
    with open("Dataset/Coronaviruses/Host proteins (potential drug targets).csv", "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=",")
        next(reader)
        rows = list(reader)

    dataset = list()

    for row in rows:
        data = {
            'coronavirus': row[0].strip(),
            'uniprot-id': row[3].strip(),
            'entrez-id': row[4].strip()
        }

        dataset.append(data)

    queries = list()

    for data in dataset:
        query = " ".join([
            "match",
            "$v isa virus, has virus-name \"{}\";",
            "$p isa protein, has uniprot-id \"{}\";",
            "$g isa gene, has entrez-id \"{}\";",
            "insert",
            "$r2 (associated-virus-gene: $g, associated-virus: $v) isa gene-virus-association;",
            "$r3 (hosting-virus-protein: $p, associated-virus: $v) isa protein-virus-association;",
        ]).format(
            data['coronavirus'],
            data['uniprot-id'],
            data['entrez-id'],
        )

        queries.append(query)

    with session.transaction(TransactionType.WRITE) as transaction:
        for query in queries:
            transaction.query().insert(query)

        transaction.commit()
