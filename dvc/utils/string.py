from difflib import get_close_matches


def fuzzy_match(string: str, dictionary: list) -> list:
    """Performs fuzzy match of the string with the given dictionary
    and returns the list of possible best "good enough" matches
    """
    if not string or not dictionary:
        return []

    return get_close_matches(
        word=string, possibilities=dictionary, n=3, cutoff=0.6
    )
