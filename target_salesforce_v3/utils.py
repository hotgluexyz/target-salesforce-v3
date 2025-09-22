def flatten_string_list(string_list) -> list[str]:
    """
    Takes list of strings and/or lists of strings and flattens them into a single list of strings.
    For example:
    ["a", ["b", "c"], "d"] -> ["a", "b", "c", "d"]
    """
    return [v for item in string_list for v in (item if isinstance(item, list) else [item])]

