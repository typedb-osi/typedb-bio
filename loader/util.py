import csv
import gzip
import os
import re
import unicodedata
from functools import partial
from io import TextIOWrapper
from multiprocessing.dummy import Pool as ThreadPool
from urllib.error import URLError
from zipfile import ZipFile
import wget
from tqdm import tqdm
from typedb.api.connection.transaction import TransactionType


class DownloadError(URLError):
    def __init__(self, url):
        self.message = "Could not download data files from {}. Check internet connection and status of data host.".format(url)
        super(DownloadError, self).__init__(self.message)


def get_file(url, path):
    """downloads file and makes folder if missing

    example inputs:
    url = "https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv"
    path ='dataset/dgidb'
    """
    if not os.path.exists(path + "/" + url.split("/")[-1]):
        print("  Downloading dataset")

        if not os.path.isdir(path):
            os.mkdir(path)

        try:
            wget.download(url, path)
        except URLError:
            raise DownloadError(url)

        print("  Finished downloading")


def read_tsv(path, header=True, delimiter="\t", archive=None):
    if archive is None:
        with open(path, "r", encoding="utf8") as file:
            reader = csv.reader(file, delimiter=delimiter)

            if header:
                next(reader)

            rows = list(reader)
    elif archive == "gz":
        with gzip.open(path, "rt", encoding="utf8") as file:
            reader = csv.reader(file, delimiter=delimiter)

            if header:
                next(reader)

            rows = list(reader)
    elif archive == "zip":
        with ZipFile(path) as zip_file:
            with zip_file.open(".".join(path.split("/")[-1].split(".")[:-1]), "r") as file:
                reader = csv.reader(TextIOWrapper(file, "utf-8"), delimiter=delimiter)
                if header:
                    next(reader)

                rows = list(reader)
    else:
        raise ValueError("Unknown archive type: {}".format(archive))

    return rows


def clean_string(string: str) -> str:
    """Remove special characters from a string.

    :param string: Raw string
    :return: Return string without special characters
    """
    return " ".join(re.findall(r"\w+", unicodedata.normalize("NFC", string), re.UNICODE))


def write_batches(session, queries, num_jobs, batch_size):
    with ThreadPool(num_jobs) as pool:
        with tqdm(total=len(queries)) as pbar:
            chunk_size = max(1, len(queries) // num_jobs // batch_size // 10)

            for n in pool.imap_unordered(partial(write_batch, session), iter_batches(queries, batch_size), chunk_size):
                pbar.update(n)


def iter_batches(a, batch_size):
    for i in range(0, len(a), batch_size):
        yield a[i:i+batch_size]


def write_batch(session, queries):
    with session.transaction(TransactionType.WRITE) as transaction:
        for query in queries:
            transaction.query().insert(query)

        transaction.commit()

    return len(queries)
