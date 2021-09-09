import wget, ssl, os

def get_file(url, dest_path):
    """downloads file and makes folder if missing

    example inputs:
    url = "https://www.dgidb.org/data/monthly_tsvs/2021-Jan/drugs.tsv"
    dest_path ='Dataset/DGIdb/'
    """

    ssl._create_default_https_context = ssl._create_unverified_context

    if os.path.isdir(dest_path) !=True: #make the folder if its missing
        os.mkdir(dest_path)
    wget.download(url, dest_path)