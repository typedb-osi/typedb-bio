# -*- coding: utf-8 -*-
"""Define mapping from relation name to roles."""


def relationship_mapper(relationship: str) -> dict:
    """Return a dictionary with relation name and roles for specific predicate.

    :param relationship: predicate name from SemMedDB
    :type relationship: str
    :return: dictionary with relation name and roles for the specified predicate
    :rtype: dict
    """
    mapper = {
        "INHIBITS": {
            "relation-name": "gene-inhibition",
            "passive-role": "inhibited-gene",
            "active-role": "inhibiting-gene",
            "negated": False,
        },
        "INTERACTS_WITH": {
            "relation-name": "gene-interaction",
            "passive-role": "interacted-gene",
            "active-role": "interacting-gene",
            "negated": False,
        },
        "COEXISTS_WITH": {
            "relation-name": "gene-coexistance",
            "passive-role": "coexisting-gene",
            "active-role": "coexisting-gene",
            "negated": False,
        },
        "compared_with": {
            "relation-name": "gene-comparison",
            "passive-role": "compared-gene",
            "active-role": "comparing-gene",
            "negated": False,
        },
        "STIMULATES": {
            "relation-name": "gene-stimulation",
            "passive-role": "stimulated-gene",
            "active-role": "stimulating-gene",
            "negated": False,
        },
        "CONVERTS_TO": {
            "relation-name": "gene-conversion",
            "passive-role": "converted-gene",
            "active-role": "converting-gene",
            "negated": False,
        },
        "PRODUCES": {
            "relation-name": "gene-production",
            "passive-role": "produced-gene",
            "active-role": "producing-gene",
            "negated": False,
        },
        "NEG_COEXISTS_WITH": {
            "relation-name": "gene-neg-coexistance",
            "passive-role": "neg-coexisting-gene",
            "active-role": "neg-coexisting-gene",
            "negated": True,
        },
        "NEG_INHIBITS": {
            "relation-name": "gene-neg-inhibition",
            "passive-role": "neg-inhibited-gene",
            "active-role": "neg-inhibiting-gene",
            "negated": True,
        },
        "NEG_INTERACTS_WITH": {
            "relation-name": "gene-neg-interaction",
            "passive-role": "neg-interacted-gene",
            "active-role": "neg-interacting-gene",
            "negated": True,
        },
        "NEG_STIMULATES": {
            "relation-name": "gene-neg-stimulation",
            "passive-role": "neg-stimulating-gene",
            "active-role": "neg-stimulated-gene",
            "negated": True,
        },
        "NEG_PRODUCES": {
            "relation-name": "gene-neg-production",
            "passive-role": "neg-producing-gene",
            "active-role": "neg-produced-gene",
            "negated": True,
        },
        "NEG_PART_OF": {
            "relation-name": "gene-neg-constitution",
            "passive-role": "neg-constituting-gene",
            "active-role": "neg-constituted-gene",
            "negated": True,
        },
        "same_as": {
            "relation-name": "gene-similarity",
            "passive-role": "similar-gene",
            "active-role": "similar-gene",
            "negated": False,
        },
        "NEG_same_as": {
            "relation-name": "gene-neg-similarity",
            "passive-role": "neg-similar-gene",
            "active-role": "neg-similar-gene",
            "negated": True,
        },
        "LOCATION_OF": {
            "relation-name": "gene-location",
            "passive-role": "located-gene",
            "active-role": "locating-gene",
            "negated": False,
        },
        "PART_OF": {
            "relation-name": "gene-constitution",
            "passive-role": "constituted-gene",
            "active-role": "constituting-gene",
            "negated": False,
        },
        "NEG_higher_than": {
            "relation-name": "gene-neg-comparison",
            "passive-role": "neg-higher-gene",
            "active-role": "neg-lower-gene",
            "negated": True,
        },
        "NEG_CONVERTS_TO": {
            "relation-name": "gene-neg-conversion",
            "passive-role": "neg-converting-gene",
            "active-role": "neg-converted-gene",
            "negated": True,
        },
        "DISRUPTS": {
            "relation-name": "gene-disruption",
            "passive-role": "disrupted-gene",
            "active-role": "disrupting-gene",
            "negated": False,
        },
        "AUGMENTS": {
            "relation-name": "gene-augmentation",
            "passive-role": "augmented-gene",
            "active-role": "augmenting-gene",
            "negated": False,
        },
        "AFFECTS": {
            "relation-name": "gene-affection",
            "passive-role": "affected-gene",
            "active-role": "affecting-gene",
            "negated": False,
        },
        "ASSOCIATED_WITH": {
            "relation-name": "gene-association",
            "passive-role": "associated-gene",
            "active-role": "associating-gene",
            "negated": False,
        },
    }

    return mapper[relationship]
