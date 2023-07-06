# -*- coding: utf-8 -*-
"""Define functions for fetching article metadata from the NCBI E-utilities API."""
import json
from pathlib import Path
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from loader.util import clean_string


def _fetch_metadata_from_api(pm_ids: list[str]) -> tuple[dict, int]:
    """Fetch article metadata from the NCBI E-utilities API.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :return: A tuple of json-encoded response and status code
    :rtype: tuple
    """
    id_arg = ','.join(pm_ids)
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={}&retmode=json".format(id_arg)
    response = requests.get(url)
    data = response.json() if response.status_code == 200 else None
    return data, response.status_code


def _read_metadata_from_cache(pm_id: str, cache_dir: Path) -> tuple[dict | None, int]:
    """Read a publication's metadata from the cache.

    :param pm_id: The PubMed ID of the article
    :type pm_id: str
    :param cache_dir: The path to the cache directory
    :type cache_dir: Path
    """
    file_path = cache_dir / "{}.json".format(pm_id)

    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            return data, 200

    return None, 404


def _fetch_metadata_from_cache(
    pm_ids: list[str], cache_dir: Path
) -> tuple[list[dict], list[str]]:
    """Fetch publication metadata from the cache.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :param cache_dir: The path to the cache directory
    :type cache_dir: Path
    :return: A tuple of a list of json-encoded responses and
        a list of PubMed IDs that were not found in the cache
    :rtype: tuple
    """
    publications = list()
    uncached_ids = list()

    for pm_id in pm_ids:
        data, status_code = _read_metadata_from_cache(pm_id, cache_dir)

        if status_code == 200 and data is not None:
            publications.append(data)
        else:
            uncached_ids.append(pm_id)

    return publications, uncached_ids


def _fetch_metadata(
    pm_ids: list[str], batch_size: int, cache_dir: Path
) -> tuple[list[dict], list[str]]:
    """Fetch publication metadata from the cache and the NCBI E-utilities API.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :param batch_size: The number of PubMed IDs to fetch at a time
    :type batch_size: int
    :param cache_dir: The path to the cache directory
    :type cache_dir: Path
    :return: A tuple of a list of json-encoded responses and
        a list of PubMed IDs for which the metadata could not be fetched
    """
    publications: list[dict] = list()
    failed_ids: list[str] = list()

    for i in tqdm(range(0, len(pm_ids), batch_size)):
        publications, uncached_pm_ids = _fetch_metadata_from_cache(
            pm_ids[i: i + batch_size], cache_dir
        )

        if len(uncached_pm_ids) > 0:
            data, status_code = _fetch_metadata_from_api(uncached_pm_ids)

            if status_code == 200 and data is not None:
                for uid in data["result"]:
                    if uid != "uids":
                        publications.append(data["result"][uid])

                        with open(cache_dir / "{}.json".format(uid), "w", encoding="utf-8") as file:
                            file.write(json.dumps(data["result"][uid], indent=4))
            else:
                print("Failed to fetch data for pm_ids: {}".format(",".join(uncached_pm_ids)))
                failed_ids = uncached_pm_ids

    return publications, failed_ids


def _fetch_metadata_with_retries(
    pm_ids: list[str], batch_size: int, retries: int, cache_dir: Path
) -> tuple[list[dict], list[str]]:
    """Fetch publication metadata from the cache and the NCBI API with retries.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :param batch_size: The number of PubMed IDs to fetch at a time
    :type batch_size: int
    :param retries: The number of times to retry fetching metadata for failed PubMed IDs
    :type retries: int
    :param cache_dir: The path to the cache directory
    :type cache_dir: Path
    :return: A tuple of a list of json-encoded responses and
        a list of PubMed IDs for which the metadata could not be fetched
    """
    publications, failed_ids = _fetch_metadata(pm_ids, batch_size, cache_dir)

    for _ in range(retries):
        additional_pubs, failed_ids = _fetch_metadata(failed_ids, batch_size, cache_dir)
        publications.extend(additional_pubs)

    return publications, failed_ids


def fetch_data(
    file_path: str, max_rows: int | None, cache_dir: Path
) -> tuple[pd.DataFrame, list[dict]]:
    """Fetch SemMed data for the given file path.

    :param file_path: The path to the file specifying the SemMed data
    :type file_path: str
    :param max_rows: The maximum number of publications to import
    :type max_rows: int | None
    :param cache_dir: The path to the cache directory
    :type cache_dir: Path
    :return: A tuple of a dataframe of relations and a list of publications
    :rtype: tuple[pd.DataFrame, list[dict]]
    """
    columns = {
        "P_PMID": "pmid",
        "P_PREDICATE": "predicate",
        "P_SUBJECT_NAME": "subject",
        "P_OBJECT_NAME": "object",
        "S_SENTENCE": "sentence",
    }

    relations = pd.read_csv(file_path, sep=";", dtype=str, usecols=columns.keys())
    relations = relations.rename(columns=columns)
    relations = relations.drop_duplicates(subset=["pmid"])

    if max_rows is not None:
        relations = relations[:max_rows]

    relations = relations.apply(np.vectorize(clean_string))
    publications, failed_ids = _fetch_metadata_with_retries(relations["pmid"], batch_size=400, retries=1, cache_dir=cache_dir)

    with open(cache_dir / "failed_ids.json", "w", encoding="utf-8") as file:
        file.write(json.dumps({"failed_ids": failed_ids}, indent=4))

    return relations, publications
