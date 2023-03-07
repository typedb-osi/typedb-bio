# -*- coding: utf-8 -*-
"""Define functions for parsing article metadata."""
import os

import pandas as pd


def get_journal_names(publications: list[dict]) -> list[str]:
    """Retrieve journal names from a list of publications.

    :param publications: A list of publications
    :type publications: list[dict]
    :return: A list of journal names
    :rtype: list[str]
    """
    return list(
        {p["fulljournalname"] for p in publications if "Journal Article" in p["pubtype"]}
    )


def get_author_names(publications: list[dict]) -> list[str]:
    """Retrieve author names from a list of publications.

    :param publications: List of publications
    :type publications: list[dict]
    :return: List of author names
    :rtype: list[str]
    """
    return list({author["name"] for p in publications for author in p["authors"]})


def get_publication_data(publications: list[dict]):
    """Retrieve relevant data for each publication from a list of publications.

    :param publications: The list of dictionaries with publication data
    :type publications: list[dict]
    :return: A list of dictionaries with relevant publication data
    :rtype: list[dict]
    """
    parsed_publications = {}
    for publication in publications:
        if publication["uid"] not in parsed_publications:
            pub = {}
            pub["paper-id"] = pub["pmid"] = publication["uid"]

            for article_id in publication["articleids"]:
                if article_id["idtype"] == "doi":
                    pub["doi"] = article_id["value"]

            pub["authors"] = []
            for author in publication["authors"]:
                pub["authors"].append(author["name"])

            pub["issn"] = publication["issn"]
            pub["volume"] = publication["volume"]

            pub["journal-name"] = ""
            if "Journal Article" in publication["pubtype"]:
                pub["journal-name"] = publication["fulljournalname"]

            pub["publish-time"] = publication["pubdate"]

            pub["title"] = publication["title"].replace('"', "'")

            parsed_publications[pub["paper-id"]] = pub

    return list(parsed_publications.values())


def get_relationship_data(dataset_path: str | os.PathLike) -> list[list]:
    """Return unique relations in a form of list of lists.

    :param dataset_path: The path to the file with relations
    :type dataset_path: str | os.PathLike
    :return: The P_PMID, P_PREDICATE, P_SUBJECT_NAME, P_OBJECT_NAME, and S_SENTENCE
        columns from the dataset as a list of lists
    :rtype: list[list]
    """
    data_df = pd.read_csv(
        dataset_path,
        sep=";",
        usecols=[
            "P_PMID",
            "P_PREDICATE",
            "P_SUBJECT_NAME",
            "P_OBJECT_NAME",
            "S_SENTENCE",
        ],
    )
    data_df = data_df.drop_duplicates()
    return data_df.values.tolist()
