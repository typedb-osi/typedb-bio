from loader.util import write_batches, read_tsv


def load_uniprot(session, max_proteins, num_jobs, batch_size):
    if max_proteins is None or max_proteins > 0:
        print("Loading UniProt dataset...")
        uniprot_dataset = get_uniprot_dataset(max_proteins)
        insert_organisms(uniprot_dataset, session, num_jobs, batch_size)
        insert_genes(uniprot_dataset, session, num_jobs, batch_size)
        insert_transcripts(uniprot_dataset, session, num_jobs, batch_size)
        insert_proteins(uniprot_dataset, session, num_jobs, batch_size)
        print("Dataset load complete.")
        print("--------------------------------------------------")


def get_uniprot_dataset(max_rows):
    rows = read_tsv("dataset/uniprot/uniprot-reviewed_yes+AND+proteome.tsv")
    dataset = list()

    if max_rows is None:
        max_rows = len(rows)

    for row in rows[:max_rows]:
        data = {
            "uniprot-id": row[0].strip(),
            "uniprot-entry-name": row[1].strip(),
            "protein-name": row[3].strip(),
            "gene-symbol": row[4].strip(),
            "organism": row[5].strip(),
            "function-description": row[7].strip(),
            "ensembl-transcript": row[11].strip(),
            "entrez-id": row[12][:-1].strip(),
        }

        dataset.append(data)

    return dataset


def insert_organisms(uniprot_dataset, session, num_jobs, batch_size):
    organism_names = set()

    for data in uniprot_dataset:
        organism_names.add(data["organism"])

    queries = list()

    for organism_name in organism_names:
        query = " ".join([
            "match",
            "?n = \"{}\";"
            "not {{ $o isa organism, has organism-name ?n; }};",
            "insert",
            "$o isa organism, has organism-name ?n;"
        ]).format(
            organism_name,
        )

        queries.append(query)

    print("Inserting organisms:")
    write_batches(session, queries, num_jobs, batch_size)


def extract_gene_entry(data):
    entry = dict()
    symbols = [symbol.strip() for symbol in data["gene-symbol"].split(" ")]
    entry["official-gene-symbol"] = symbols.pop(0)
    entry["alternative-gene-symbol"] = symbols

    if data["entrez-id"] == "":
        entry["entrez-id"] = list()
    else:
        entry["entrez-id"] = data["entrez-id"].split(";")

    return entry


def insert_genes(uniprot_dataset, session, num_jobs, batch_size):
    gene_entries = list()

    for data in uniprot_dataset:
        if data["gene-symbol"] != "":
            gene_entries.append(extract_gene_entry(data))

    symbols = {entry["official-gene-symbol"] for entry in gene_entries}
    genes = list()

    for symbol in symbols:
        gene = dict()
        gene["official-gene-symbol"] = symbol
        gene["alternative-gene-symbol"] = list()
        gene["entrez-id"] = list()

        for entry in gene_entries:
            if entry["official-gene-symbol"] == symbol:
                gene["alternative-gene-symbol"] += entry["alternative-gene-symbol"]
                gene["entrez-id"] += entry["entrez-id"]

        genes.append(gene)

    queries = list()

    for gene in genes:
        query = "insert $g isa gene, has primary-gene-symbol \"{}\"".format(gene["official-gene-symbol"])

        for symbol in gene["alternative-gene-symbol"]:
            query += ", has alternative-gene-symbol \"{}\"".format(symbol)

        for entrez_id in gene["entrez-id"]:
            query += ", has entrez-id \"{}\"".format(entrez_id)

        query += ";"
        queries.append(query)

    print("Inserting genes:")
    write_batches(session, queries, num_jobs, batch_size)


def extract_transcript_entries(data):
    entries = [entry.split("[")[0].strip() for entry in data["ensembl-transcript"][:-1].split(";")]
    return [entry for entry in entries if entry != ""]


def insert_transcripts(uniprot_dataset, session, num_jobs, batch_size):
    transcripts = dict()
    queries = list()

    for data in uniprot_dataset:
        entries = extract_transcript_entries(data)

        for entry in entries:
            if entry not in transcripts:
                transcripts[entry] = list()

            if data["gene-symbol"] != "":
                gene_symbol = extract_gene_entry(data)["official-gene-symbol"]
                transcripts[entry].append(gene_symbol)

    for transcript_id in transcripts.keys():
        match_clause = "match"
        insert_clause = "insert $t isa transcript, has ensembl-transcript-id \"{}\";".format(transcript_id)
        gene_symbols = transcripts[transcript_id]

        for i, gene_symbol in enumerate(gene_symbols):
            match_clause += " $g{} isa gene, has primary-gene-symbol \"{}\";".format(i, gene_symbol)
            insert_clause += " (transcribed-gene: $g{}, synthesised-transcript: $t) isa transcription;".format(i)

        if match_clause == "match":
            query = insert_clause
        else:
            query = match_clause + " " + insert_clause

        queries.append(query)

    print("Inserting transcripts:")
    write_batches(session, queries, num_jobs, batch_size)


def extract_protein_names(entry):
    protein_names = {
        "alternative-names": list()
    }

    if entry == "":
        return protein_names

    candidate_names = list()
    name = ""
    depth = 0

    for char in entry[::-1]:
        if char in (")", "]"):
            depth += 1
            name = char + name
        elif char in ("(", "["):
            depth -= 1
            name = char + name

            if depth == 0:
                candidate_names.append(name)
                name = ""
        else:
            name = char + name

    candidate_names.append(name)

    if len(candidate_names) == 0:
        return protein_names
    else:
        primary_name = ""
        alternative_names = list()
        in_primary_name = False

        for name in candidate_names:
            if name == "":
                continue

            if name.strip()[0] == "(" and name.strip()[-1] == ")":
                alternative_names.append(name.strip()[1:-1].strip())
            elif name.strip()[0] == "[" and name.strip()[-1] == "]":
                if in_primary_name:
                    primary_name = name + primary_name
                else:
                    continue
            else:
                in_primary_name = True
                primary_name = name + primary_name

    protein_names["primary-name"] = primary_name.strip()
    protein_names["alternative-names"] = alternative_names[::-1]
    return protein_names


def insert_proteins(uniprot_dataset, session, num_jobs, batch_size):
    queries = list()

    for data in uniprot_dataset:
        transcripts = extract_transcript_entries(data)
        match_clause = "match"
        insert_clause = "insert"

        insert_clause += " " + " ".join([
            "$p isa protein,",
            "has uniprot-id \"{}\",",
            "has uniprot-entry-name \"{}\"",
        ]).format(
            data["uniprot-id"],
            data["uniprot-entry-name"],
        )

        if data["function-description"] != "":
            insert_clause += ", has function-description \"{}\"".format(data["function-description"])

        names = extract_protein_names(data["protein-name"])

        if "primary-name" in names:
            insert_clause += ", has primary-uniprot-name \"{}\"".format(names["primary-name"])

        for name in names["alternative-names"]:
            insert_clause += ", has alternative-uniprot-name \"{}\"".format(name)

        insert_clause += ";"

        match_clause += " $o isa organism, has organism-name \"{}\";".format(data["organism"])
        insert_clause += " (associated-organism: $o, associated-protein: $p) isa organism-protein-association;"

        for i, transcript_id in enumerate(transcripts):
            match_clause += " $t{} isa transcript, has ensembl-transcript-id \"{}\";".format(i, transcript_id)
            insert_clause += " (translated-transcript: $t{}, synthesised-protein: $p) isa translation;".format(i)

        query = match_clause + " " + insert_clause
        queries.append(query)

    print("Inserting proteins:")
    write_batches(session, queries, num_jobs, batch_size)
