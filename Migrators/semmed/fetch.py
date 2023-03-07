# -*- coding: utf-8 -*-
"""Define functions for fetching article metadata from the NCBI E-utilities API."""
import json
from pathlib import Path

import requests
from tqdm import tqdm


def _fetch_metadata_from_api(pm_ids: list[str]) -> tuple[dict, int]:
    """Fetch article metadata from the NCBI E-utilities API.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :return: A tuple of json-encoded response and status code
    :rtype: tuple
    """
    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?"
        f"db=pubmed&id={','.join(pm_ids)}&retmode=json"
    )
    response = requests.get(url)
    data = response.json() if response.status_code == 200 else None
    return data, response.status_code


def _read_metadata_from_cache(pm_id: str) -> tuple[dict | None, int]:
    """Read a publication's metadata from the cache.

    :param pm_id: The PubMed ID of the article
    :type pm_id: str
    """
    file_path = Path(f".cache/{pm_id}.json")
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            return data, 200
    return None, 404


def _fetch_metadata_from_cache(pm_ids: list[str]) -> tuple[list[dict], list[str]]:
    """Fetch publication metadata from the cache.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :return: A tuple of a list of json-encoded responses and
        a list of PubMed IDs that were not found in the cache
    :rtype: tuple
    """
    publications, uncached_ids = [], []
    for pm_id in pm_ids:
        data, status_code = _read_metadata_from_cache(pm_id)
        if status_code == 200 and data is not None:
            publications.append(data)
        else:
            uncached_ids.append(pm_id)
    return publications, uncached_ids


def _fetch_metadata(pm_ids, batch_size) -> tuple[list[dict], list[str]]:
    """Fetch publication metadata from the cache and the NCBI E-utilities API.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :param batch_size: The number of PubMed IDs to fetch at a time
    :type batch_size: int
    :return: A tuple of a list of json-encoded responses and
        a list of PubMed IDs for which the metadata could not be fetched
    """
    publications: list[dict] = []
    failed_ids: list[str] = []
    for i in tqdm(range(0, len(pm_ids), batch_size)):
        publications, uncached_pm_ids = _fetch_metadata_from_cache(
            pm_ids[i : i + batch_size]
        )
        if len(uncached_pm_ids) > 0:
            data, status_code = _fetch_metadata_from_api(uncached_pm_ids)
            if status_code == 200 and data is not None:
                for uid in data["result"]:
                    if uid != "uids":
                        publications.append(data["result"][uid])
                        with open(f".cache/{uid}.json", "w", encoding="utf-8") as file:
                            file.write(json.dumps(data["result"][uid], indent=4))
            else:
                print(f'Failed to fetch data for pm_ids: {",".join(uncached_pm_ids)}')
                failed_ids = uncached_pm_ids
    return publications, failed_ids


def fetch_metadata(
    pm_ids: list[str], batch_size: int, retries: int
) -> tuple[list[dict], list[str]]:
    """Fetch publication metadata from the cache and the NCBI API with retries.

    :param pm_ids: A list of PubMed IDs
    :type pm_ids: list[str]
    :param batch_size: The number of PubMed IDs to fetch at a time
    :type batch_size: int
    :param retries: The number of times to retry fetching metadata for failed PubMed IDs
    :type retries: int
    :return: A tuple of a list of json-encoded responses and
        a list of PubMed IDs for which the metadata could not be fetched
    """
    Path(".cache").mkdir(parents=True, exist_ok=True)
    publications, failed_ids = _fetch_metadata(pm_ids, batch_size)

    for _ in range(retries):
        additional_pubs, failed_ids = _fetch_metadata(failed_ids, batch_size)
        publications.extend(additional_pubs)

    return publications, failed_ids
