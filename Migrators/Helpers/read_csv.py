import csv 


def read_tsv(path, num_rows=0, header=True, separator="\t"):
    with open(path, encoding='utf8') as file:
        reader = csv.reader(file, delimiter=separator)

        if header:
            next(reader)

        rows = list(reader)

    return rows
