# -*- coding: utf-8 -*-
"""Define mapping from relation name to roles."""


def relationship_mapper(relationship: str) -> dict[str, str]:
    """Return a dictionary with relation name and roles for specific predicate.

    :param relationship: predicate name from SemMedDB
    :type relationship: str
    :return: dictionary with relation name and roles for the specified predicate
    :rtype: dict[str, str]
    """
    mapper = {
        "INHIBITS": {
            "relation-name": "inhibition",
            "passive-role": "inhibited",
            "active-role": "inhibiting",
        },
        "INTERACTS_WITH": {
            "relation-name": "interaction",
            "passive-role": "interacted",
            "active-role": "interacting",
        },
        "COEXISTS_WITH": {
            "relation-name": "coexistance",
            "passive-role": "coexisting",
            "active-role": "coexisting",
        },
        "compared_with": {
            "relation-name": "comparison",
            "passive-role": "compared",
            "active-role": "comparing",
        },
        "higher_than": {
            "relation-name": "comparison",
            "passive-role": "lower",
            "active-role": "higher",
        },
        "STIMULATES": {
            "relation-name": "stimulation",
            "passive-role": "stimulated",
            "active-role": "stimulating",
        },
        "CONVERTS_TO": {
            "relation-name": "conversion",
            "passive-role": "converted",
            "active-role": "converting",
        },
        "PRODUCES": {
            "relation-name": "production",
            "passive-role": "produced",
            "active-role": "producing",
        },
        "NEG_COEXISTS_WITH": {
            "relation-name": "neg-coexistance",
            "passive-role": "neg-coexisting",
            "active-role": "neg-coexisting",
        },
        "NEG_INHIBITS": {
            "relation-name": "neg-inhibition",
            "passive-role": "neg-inhibited",
            "active-role": "neg-inhibiting",
        },
        "NEG_INTERACTS_WITH": {
            "relation-name": " neg-interaction",
            "passive-role": "neg-interacted",
            "active-role": "neg-interacting",
        },
        "NEG_STIMULATES": {
            "relation-name": "neg-stimulation",
            "passive-role": "neg-stimulating",
            "active-role": "neg-stimulated",
        },
        "NEG_PRODUCES": {
            "relation-name": "neg-production",
            "passive-role": "neg-producing",
            "active-role": "neg-produced",
        },
        "lower_than": {
            "relation-name": "comparison",
            "passive-role": "higher",
            "active-role": "lower",
        },
        "NEG_PART_OF": {
            "relation-name": "neg-constitution",
            "passive-role": "neg-constituting",
            "active-role": "neg-constituted",
        },
        "same_as": {
            "relation-name": "similarity",
            "passive-role": "similar",
            "active-role": "similar",
        },
        "NEG_same_as": {
            "relation-name": "neg-similarity",
            "passive-role": "neg-similar",
            "active-role": "neg-similar",
        },
        "LOCATION_OF": {
            "relation-name": "location",
            "passive-role": "locating",
            "active-role": "located",
        },
        "PART_OF": {
            "relation-name": "constitution",
            "passive-role": "constituting",
            "active-role": "constituted",
        },
        "NEG_higher_than": {
            "relation-name": "neg-comparison",
            "passive-role": "neg-higher",
            "active-role": "neg-lower",
        },
        "NEG_CONVERTS_TO": {
            "relation-name": "neg-conversion",
            "passive-role": "neg-converting",
            "active-role": "neg-converted",
        },
        "DISRUPTS": {
            "relation-name": "disruption",
            "passive-role": "disrupting",
            "active-role": "disrupted",
        },
        "AUGMENTS": {
            "relation-name": "augmentation",
            "passive-role": "augmenting",
            "active-role": "augmented",
        },
        "AFFECTS": {
            "relation-name": "affection",
            "passive-role": "affecting",
            "active-role": "affected",
        },
        "ASSOCIATED_WITH": {
            "relation-name": "association",
            "passive-role": "associated",
            "active-role": "associating",
        },
    }
    mapping = mapper[relationship]

    return mapping
