import json
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

from Migrators.Helpers.batchLoader import write_batch


def migrate_cord_ner(client, database, session_type, num_ner, num_threads, batch_size):
    print('.....')
    print('Opening CORD NER file.')
    print('.....')

    # FIRST DOWNLOAD THE CORD-NER-FULL.json FROM THIS WEBSITE:
    # https://uofi.app.box.com/s/k8pw7d5kozzpoum2jwfaqdaey1oij93x/file/651148518303

    # AND ADD IT TO THIS DIR: DATASET/CORD_NER/

    # TODO: Implement a JSON streamer
    with open('Dataset/CORD_NER/CORD-NER-full.json', "r") as f:
        data = json.loads("[" +
                          f.read().replace("}\n{", "},\n{") +
                          "]")

    # The session could time out if we open it BEFORE we load the file
    data = data[:num_ner]
    with client.session(database, session_type) as session:
        insert_authors(session, data, num_threads, batch_size)
        insert_journals(session, data, num_threads, batch_size)
        insert_publications_journals(session, data, num_threads, batch_size)
        insert_publications_with_authors(session, data, num_threads, batch_size)
        insert_entities_pub(session, data, num_threads, batch_size)  # fails with logic error # TODO check
    print('.....')
    print('Finished migrating CORD NER.')
    print('.....')


# Input: a string of authors
# Return: List of authors
def author_names(author_string):
    author_list = []

    ind = author_string.find('",')
    ind2 = author_string.find("',")
    if ind < ind2:
        ind = ind
    else:
        ind = ind2

    if author_string.find(';') != -1:
        ind = author_string.find(';')
    author_string = author_string.replace("]", "").replace("[", "")
    first_name = author_string[:ind].replace("'", "").replace('"', "")
    rest_name = author_string[ind + 2:]
    author_list.append(first_name)
    while ind > 2:

        ind1 = rest_name.find('",')
        ind2 = rest_name.find("',")
        if ind1 < ind2:
            ind = ind2 + 1
        elif ind1 < ind2 and ind1 == -1:
            ind = ind2 + 1
        elif ind1 > ind2 == -1:
            ind = ind1 + 1
        elif ind1 > ind2:
            ind = ind2 + 1
        else:
            ind = rest_name.find(';')

        # if author_string.find("',") != -1:
        #     ind = rest_name.find("',") + 1
        # if author_string.find(';') != -1:
        #     ind = rest_name.find(';')

        if ind == -1:
            author_list.append(rest_name.replace("'", "").replace('"', ""))
        elif ind == 0:
            ind = -1
            author_list.append(rest_name[:ind].replace("'", "").replace('"', ""))
        else:
            author_list.append(rest_name[:ind].replace("'", "").replace('"', ""))

        rest_name = rest_name[ind + 2:]
    return author_list


def insert_authors(session, data, num_threads, batch_size):
    batch = []
    batches = []
    list_of_list_of_authors = []
    for d in data:
        if d['authors'] != 0:
            list_of_list_of_authors = list_of_list_of_authors + author_names(d['authors'])

    list_of_list_of_authors = set(list_of_list_of_authors)
    for l in list_of_list_of_authors:
        l = l.replace('"', "'")
        typeql = f"""
        insert
        $p isa person, has published-name "{l}";
        """
        batch.append(typeql)
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('.....')
    print('Finished inserting authors.')
    print('.....')


def insert_journals(session, data, num_threads, batch_size):
    batch = []
    batches = []
    list_of_journals = []
    for d in data:
        list_of_journals.append(d['journal'])
    list_of_journals = set(list_of_journals)
    for l in list_of_journals:
        try:
            l = l.replace('"', "'").replace(".", "")
        except Exception:
            pass
        typeql = f"""
        insert
        $p isa journal, has journal-name "{l}";
        """
        batch.append(typeql)
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('.....')
    print('Finished inserting journals.')
    print('.....')


def insert_publications_journals(session, data, num_threads, batch_size):
    batch = []
    batches = []
    list_of_pubs = []
    for d in data:
        pub = {}
        pub['title'] = d['title'].replace('"', "'")
        pub['doi'] = d['doi']
        pub['paper-id'] = d['pubmed_id']
        pub['publish_time'] = d['publish_time']
        pub['journal'] = d['journal']
        list_of_pubs.append(pub)

    for l in list_of_pubs:
        typeql = f"""
        match 
        $j isa journal, has journal-name "{l['journal']}"; 
        insert
        $pu isa publication, has title "{l['title']}", has doi "{l['doi']}", 
        has paper-id "{l['paper-id']}", has publish-time "{l['publish_time']}";
        (published-publication: $pu, publishing-journal: $j) isa publishing;
        """
        batch.append(typeql)
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('.....')
    print('Finished inserting publications and connecting them with journals.')
    print('.....')


def insert_publications_with_authors(session, data, num_threads, batch_size):
    batches = []
    batch = []
    for d in data:
        if d['authors'] != 0:
            authors = author_names(d['authors'])
            author_typeql = ""
            counter = 0
            relations_authors = ""
            for a in authors:
                author_typeql = author_typeql + "$p" + str(counter) + ' isa person, has published-name "' + a + '"; '
                relations_authors = relations_authors + "(authored-publication: $pu, author: $p" + str(
                    counter) + ") isa authorship; "
                counter = counter + 1
            d['title'] = d['title'].replace('"', "'")
            typeql = f"""
            match
            $pu isa publication, has title "{d['title']}"; 
            {author_typeql}
            insert 
            {relations_authors}
            """
            batch.append(typeql)
        if len(batch) == batch_size:
            batches.append(batch)
            batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('.....')
    print('Finished inserting journals <> pub <> authors.')
    print('.....')


def insert_entities_pub(session, data, num_threads, batch_size):
    batches = []
    batch = []
    for d in data:
        d['title'] = d['title'].replace('"', "'")
        for e in d['entities']:
            ent = {}
            ent['type'] = classify_type(e['type'])
            ent['text'] = e['text'].replace('"', "").replace("'", '')
            ent['start'] = e['start']
            ent['end'] = e['end']
            if ent['type'] != None:
                if ent['type'] != "gene or protein":
                    typeql = f"""
                    match $1 isa {ent['type']}, has {ent['type']}-name "{ent['text']}";
                    $p isa publication, has title "{d['title']}"; 
                    insert 
                    (mentioning: $p, mentioned: $1) isa mention, has start "{e['start']}", has end "{e['end']}";
                    """
                else:
                    typeql = f"""
                    match $1 isa protein, has uniprot-entry-name "{ent['text']}";
                    $p isa publication, has title "{d['title']}"; 
                    insert 
                    (mentioning: $p, mentioned: $1) isa mention, has start "{e['start']}", has end "{e['end']}";
                    """
                    typeql2 = f"""
                    match 
                    $p isa publication, has title "{d['title']}"; 
                    $2 isa gene, has gene-symbol "{ent['text']}";
                    insert 
                    (mentioning: $p, mentioned: $2) isa mention, has start "{e['start']}", has end "{e['end']}";
                    """
                    batch.append(typeql2)

                batch.append(typeql)
                if len(batch) == batch_size:
                    batches.append(batch)
                    batch = []
    batches.append(batch)
    pool = ThreadPool(num_threads)
    pool.map(partial(write_batch, session), batches)
    pool.close()
    pool.join()
    print('.....')
    print('Finished inserting.')
    print('.....')


def classify_type(type):
    if type == "CORONAVIRUS":
        return "virus"
    if type == "VIRUS":
        return "virus"
    if type == "TISSUE":
        return "tissue"
    # if type == "CHEMICAL":
    #     return "chemical"
    if type == "GENE_OR_GENOME":
        return "gene or protein"
    if type == "DISEASE_OR_SYNDROME":
        return "disease"
# if type == "CELL_FUNCTION":
#     return "cell-function"
