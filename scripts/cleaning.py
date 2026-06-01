def clean_text(value):
    if value == "" or value is None:
        return None

    return value


def clean_int(value):
    if value == "" or value is None:
        return None

    return int(value)


def clean_float(value):
    if value == "" or value is None:
        return None

    return float(value)


def clean_bool(value):
    if value == "" or value is None:
        return None

    return float(value) == 1.0
