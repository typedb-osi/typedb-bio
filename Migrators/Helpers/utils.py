# -*- coding: utf-8 -*-
"""Define utility functions for the migrators."""
import re
import unicodedata


def clean_string(string: str) -> str:
    """Remove special characters from a string.

    :param string: Raw string
    :return: Return string without special characters
    """
    return "".join(re.findall(r"\w+", unicodedata.normalize("NFC", string), re.UNICODE))
