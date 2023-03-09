# -*- coding: utf-8 -*-
"""Define functions for parsing article metadata."""
from Migrators.Helpers.utils import clean_string


def get_journal_names(publications: list[dict]) -> list[str]:
    """Retrieve journal names from a list of publications.

    :param publications: A list of publications
    :type publications: list[dict]
    :return: A list of journal names
    :rtype: list[str]
    """
    return list(
        {
            clean_string(p["fulljournalname"])
            for p in publications
            if "Journal Article" in p["pubtype"]
        }
    )


def get_author_names(publications: list[dict]) -> list[str]:
    """Retrieve author names from a list of publications.

    :param publications: List of publications
    :type publications: list[dict]
    :return: List of author names
    :rtype: list[str]
    """
    return list(
        {clean_string(author["name"]) for p in publications for author in p["authors"]}
    )


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
            pub["paper-id"] = pub["pmid"] = clean_string(publication["uid"])

            pub["doi"] = ""
            for article_id in publication["articleids"]:
                if article_id["idtype"] == "doi":
                    pub["doi"] = article_id["value"]

            pub["authors"] = []  # type: ignore
            for author in publication["authors"]:
                pub["authors"].append(clean_string(author["name"]))  # type: ignore

            pub["issn"] = publication["issn"] if "issn" in publication else ""
            pub["volume"] = publication["volume"] if "volume" in publication else ""

            pub["journal-name"] = ""
            if "Journal Article" in publication["pubtype"]:
                pub["journal-name"] = clean_string(publication["fulljournalname"])

            pub["publish-time"] = (
                publication["pubdate"] if "pubdate" in publication else ""
            )

            pub["title"] = (
                publication["title"].replace('"', "'") if "title" in publication else ""
            )

            parsed_publications[pub["paper-id"]] = pub

    return list(parsed_publications.values())
