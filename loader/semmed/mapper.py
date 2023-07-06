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
            "relation-name": "inhibition",
            "passive-role": "inhibited",
            "active-role": "inhibiting",
            "negated": False,
        },
        "INTERACTS_WITH": {
            "relation-name": "interaction",
            "passive-role": "interacted",
            "active-role": "interacting",
            "negated": False,
        },
        "COEXISTS_WITH": {
            "relation-name": "coexistance",
            "passive-role": "coexisting",
            "active-role": "coexisting",
            "negated": False,
        },
        "compared_with": {
            "relation-name": "comparison",
            "passive-role": "compared",
            "active-role": "comparing",
            "negated": False,
        },
        "higher_than": {
            "relation-name": "comparison",
            "passive-role": "lower",
            "active-role": "higher",
            "negated": False,
        },
        "STIMULATES": {
            "relation-name": "stimulation",
            "passive-role": "stimulated",
            "active-role": "stimulating",
            "negated": False,
        },
        "CONVERTS_TO": {
            "relation-name": "conversion",
            "passive-role": "converted",
            "active-role": "converting",
            "negated": False,
        },
        "PRODUCES": {
            "relation-name": "production",
            "passive-role": "produced",
            "active-role": "producing",
            "negated": False,
        },
        "NEG_COEXISTS_WITH": {
            "relation-name": "neg-coexistance",
            "passive-role": "neg-coexisting",
            "active-role": "neg-coexisting",
            "negated": True,
        },
        "NEG_INHIBITS": {
            "relation-name": "neg-inhibition",
            "passive-role": "neg-inhibited",
            "active-role": "neg-inhibiting",
            "negated": True,
        },
        "NEG_INTERACTS_WITH": {
            "relation-name": " neg-interaction",
            "passive-role": "neg-interacted",
            "active-role": "neg-interacting",
            "negated": True,
        },
        "NEG_STIMULATES": {
            "relation-name": "neg-stimulation",
            "passive-role": "neg-stimulating",
            "active-role": "neg-stimulated",
            "negated": True,
        },
        "NEG_PRODUCES": {
            "relation-name": "neg-production",
            "passive-role": "neg-producing",
            "active-role": "neg-produced",
            "negated": True,
        },
        "lower_than": {
            "relation-name": "comparison",
            "passive-role": "higher",
            "active-role": "lower",
            "negated": False,
        },
        "NEG_PART_OF": {
            "relation-name": "neg-constitution",
            "passive-role": "neg-constituting",
            "active-role": "neg-constituted",
            "negated": True,
        },
        "same_as": {
            "relation-name": "similarity",
            "passive-role": "similar",
            "active-role": "similar",
            "negated": False,
        },
        "NEG_same_as": {
            "relation-name": "neg-similarity",
            "passive-role": "neg-similar",
            "active-role": "neg-similar",
            "negated": True,
        },
        "LOCATION_OF": {
            "relation-name": "location",
            "passive-role": "locating",
            "active-role": "located",
            "negated": False,
        },
        "PART_OF": {
            "relation-name": "constitution",
            "passive-role": "constituting",
            "active-role": "constituted",
            "negated": False,
        },
        "NEG_higher_than": {
            "relation-name": "neg-comparison",
            "passive-role": "neg-higher",
            "active-role": "neg-lower",
            "negated": True,
        },
        "NEG_CONVERTS_TO": {
            "relation-name": "neg-conversion",
            "passive-role": "neg-converting",
            "active-role": "neg-converted",
            "negated": True,
        },
        "DISRUPTS": {
            "relation-name": "disruption",
            "passive-role": "disrupting",
            "active-role": "disrupted",
            "negated": False,
        },
        "AUGMENTS": {
            "relation-name": "augmentation",
            "passive-role": "augmenting",
            "active-role": "augmented",
            "negated": False,
        },
        "AFFECTS": {
            "relation-name": "affection",
            "passive-role": "affecting",
            "active-role": "affected",
            "negated": False,
        },
        "ASSOCIATED_WITH": {
            "relation-name": "association",
            "passive-role": "associated",
            "active-role": "associating",
            "negated": False,
        },
    }

    return mapper[relationship]
