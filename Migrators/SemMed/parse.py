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
    return list({clean_string(publication["fulljournalname"]) for publication in publications if "Journal Article" in publication["pubtype"]})


def get_author_names(publications: list[dict]) -> list[str]:
    """Retrieve author names from a list of publications.

    :param publications: List of publications
    :type publications: list[dict]
    :return: List of author names
    :rtype: list[str]
    """
    return list({clean_string(author["name"]) for publication in publications for author in publication["authors"]})


def get_publication_data(publications: list[dict]):
    """Retrieve relevant data for each publication from a list of publications.

    :param publications: The list of dictionaries with publication data
    :type publications: list[dict]
    :return: A list of dictionaries with relevant publication data
    :rtype: list[dict]
    """
    parsed_publications = dict()

    for publication in publications:
        uid = clean_string(publication["uid"])

        if uid not in parsed_publications.keys():
            parsed_publication = {
                "paper-id": uid,
                "pmid": uid,
                "authors": list(),
            }

            if "issn" in publication and publication["issn"].strip() != "":
                parsed_publication["issn"] = publication["issn"]

            if "volume" in publication and publication["volume"].strip() != "":
                parsed_publication["volume"] = publication["volume"]

            if "pubdate" in publication and publication["pubdate"].strip() != "":
                parsed_publication["year"] = publication["pubdate"][:4]

            if "title" in publication and publication["title"].strip() != "":
                parsed_publication["title"] = publication["title"].replace("\"", "'")

            for article_id in publication["articleids"]:
                if article_id["idtype"] == "doi":
                    parsed_publication["doi"] = article_id["value"]
                    break

            for author in publication["authors"]:
                name = clean_string(author["name"])

                if name != "" and name not in parsed_publication["authors"]:
                    parsed_publication["authors"].append(name)

            if "Journal Article" in publication["pubtype"]:
                name = clean_string(publication["fulljournalname"])

                if name != "":
                    parsed_publication["journal-name"] = name

            parsed_publications[uid] = parsed_publication

    return list(parsed_publications.values())
