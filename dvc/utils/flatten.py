def flatten(d):
    import flatten_dict

    return flatten_dict.flatten(d, reducer="dot")


def unflatten(d):
    import flatten_dict

    return flatten_dict.unflatten(d, splitter="dot")
