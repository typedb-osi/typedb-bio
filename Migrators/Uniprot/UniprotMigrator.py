from typedb.client import TransactionType
import csv
import re

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from Migrators.Helpers.batchLoader import write_batch


def migrate_uniprot(session, num, num_threads, batch_size):
    if num != 0:
        print('  ')
        print('Opening Uniprot dataset...')
        print('  ')

        with session.transaction(TransactionType.WRITE) as tx:
            org = "insert $h isa organism, has organism-name 'Homo sapiens (Human)', has organism-name 'Human'; $o2 isa organism, has organism-name 'Avian';"
            tx.query().insert(org)
            tx.commit()

        uniprotdb = get_uniprotdb(num_threads)
        insert_genes(uniprotdb, session, num_threads, batch_size)
        insert_transcripts(uniprotdb, session, num_threads, batch_size)
        insert_proteins(uniprotdb, session, num_threads, batch_size)
        print('.....')
        print('Finished migrating Uniprot file.')
        print('.....')


def get_uniprotdb(split_size):
    with open('Dataset/Uniprot/uniprot-reviewed_yes+AND+proteome.tsv', 'rt', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile, delimiter='\t')
        raw_file = []
        n = 0
        for row in csvreader:
            n = n + 1
            if n != 1:
                raw_file.append(row)

    uniprotdb = []
    for i in raw_file[:split_size]:
        data = {}
        data['uniprot-id'] = i[0]
        data['uniprot-entry-name'] = i[1]
        data['protein-name'] = i[3]
        data['gene-symbol'] = i[4]
        data['organism'] = i[5]
        data['function-description'] = i[7]
        data['ensembl-transcript'] = i[11]
        data['entrez-id'] = i[12]
        uniprotdb.append(data)
    return uniprotdb


def transcript_helper(q):
    list = []
    n = q['ensembl-transcript']
    nt = n.count(';')
    if nt != 0:
        while nt != 0:
            nt = nt - 1
            pos = [m.start() for m in re.finditer(r";", n)]
            if nt == 0:
                n2 = n[0:pos[0]]
            else:
                try:
                    pos_st = pos[nt - 1] + 1
                    pos_end = pos[nt]
                    n2 = n[pos_st:pos_end]
                except:
                    pass
            estr = n2.find('[') - 1
            n2 = n2[:estr]
            list.append(n2)
        return list


# Returns: a list of [gene, entrez-id]
# Method removes synonyms from genes and only takes the first one
def gene_helper(q):
    gene_name = q['gene-symbol']
    entrez_id = q['entrez-id'][0:-1]
    if gene_name.find(' ') != -1:
        gene_name = gene_name[0:gene_name.find(' ')]
    list = [gene_name, entrez_id]
    return list


# Insert genes from gene-symbol.
# NB: We only insert the first name, if there are synonyms, we ignore them. 
def insert_genes(uniprotdb, session, num_threads, batch_size):
    gene_list = []
    batch = []
    batches = []
    for q in uniprotdb:
        if q['gene-symbol'] != "":
            gene_list.append(gene_helper(q))

    # gene_list = list(dict.fromkeys(gene_list)) # TO DO: Remove duplicate gene-symbols

    for g in gene_list:
        typeql = f"insert $g isa gene, has gene-symbol '{g[0]}', has entrez-id '{g[1]}';"
        batch.append(typeql)
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('Genes committed!')


# Insert transcripts
def insert_transcripts(uniprotdb, session, num_threads, batch_size):
    transcript_list = []
    batch = []
    batches = []
    for q in uniprotdb:
        tr = transcript_helper(q)
        if tr != None:
            transcript_list = transcript_list + tr

    transcript_list = list(dict.fromkeys(transcript_list))  # Remove duplicate transcripts

    for q in transcript_list:
        typeql = "insert $t isa transcript, has ensembl-transcript-stable-id '" + q + "' ;"
        batch.append(typeql)
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('Transcripts committed!')


def get_batched_protein_queries(uniprotdb, batch_size):
    batches = []
    batch = []
    for q in uniprotdb:
        transcripts = transcript_helper(q)
        gene = gene_helper(q)[0]
        if transcripts != None:
            variables = []
            tvariable = 1
            typeql = "match "
            for t in transcripts:
                variables.append(tvariable)
                typeql = typeql + "$" + str(
                    tvariable) + " isa transcript, has ensembl-transcript-stable-id '" + t + "'; "
                tvariable = tvariable + 1
        if gene != None:
            try:
                typeql = typeql + "$g isa gene, has gene-symbol '" + gene + "';"
            except Exception:
                typeql = "match $g isa gene, has gene-symbol '" + gene + "';"
        try:
            typeql = typeql + "$h isa organism, has organism-name '" + q['organism'] + "';"
        except Exception:
            typeql = "match $h isa organism, has organism-name '" + q['organism'] + "';"

        typeql = f"""{typeql} insert $a isa protein, has uniprot-id "{q['uniprot-id']}", has uniprot-name
    "{q['protein-name']}", has function-description "{q['function-description']}", 
    has uniprot-entry-name "{q['uniprot-entry-name']}";
    $r (associated-organism: $h, associating: $a) isa organism-association;"""
        if gene != None:
            typeql = typeql + "$gpe (encoding-gene: $g, encoded-protein: $a) isa gene-protein-encoding;"
        if transcripts != None:
            for v in variables:
                typeql = f"""{typeql} $r{str(v)}(translating-transcript: ${str(v)}, translated-protein: $a) isa translation; """
        if gene and transcripts != None:
            for v in variables:
                typeql = typeql + "$trans" + str(v) + "(transcribing-gene: $g, encoded-transcript: $" + str(
                    v) + ") isa transcription;"

        batch.append(typeql)
        del typeql
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    return batches


def insert_proteins(uniprotdb, session, num_threads, batch_size):
    batched_protein_queries = get_batched_protein_queries(uniprotdb, batch_size)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batched_protein_queries)
    pool.close()
    pool.join()
