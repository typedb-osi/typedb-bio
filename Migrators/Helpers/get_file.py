import os
from urllib.error import URLError
import wget


class DownloadError(URLError):
    def __init__(self, url):
        self.message = "Could not download data files from {}. Check internet connection and status of data host.".format(url)
        super(DownloadError, self).__init__(self.message)


def get_file(url, path):
    """downloads file and makes folder if missing

    example inputs:
    url = "https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv"
    dest_path ='Dataset/DGIdb/'
    """
    if not os.path.isdir(path):
        os.mkdir(path)

    try:
        wget.download(url, path)
    except URLError:
        raise DownloadError(url)
