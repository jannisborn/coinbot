import re
from typing import List

replace_dict = {
    "ä": "ä",
    "ö": "ö",
    "ü": "ü",
    "Ä": "Ä",
    "Ö": "Ö",
    "Ü": "Ü",
    "ß": "ß",
    " ": " ",
    "\xa0": " ",
    "  ": " ",
}


def fix_string(text: str) -> str:
    """
    Replaces undesired characters and removes references as e.g., in :[123]

    Args:
        text: _description_

    Returns:
        _description_
    """
    for key, value in replace_dict.items():
        text = text.replace(key, value)
    pattern = r":(?=\[\d+\])|\[(\d+)\]|:$"
    text = re.sub(pattern, lambda m: m.group(1) if m.group(1) else "", text)
    text = "" if is_float(text) else text
    return text


def is_float(x):
    return x.replace(".", "", 1).isdigit() and x.count(".") < 2


def isint(x: str) -> bool:
    try:
        int(x.replace(".", ""))
    except Exception:
        return False
    return True


def non_alphabetic(x: str) -> bool:
    return not any(c.isalpha() for c in x)


def get_years(text: str) -> List[int]:
    text = text.strip()
    if text.count(" ") == 2:
        if text.count(".") == 2:
            # Date is in format 14.05.2004
            return [int(text.split(".")[-1])]
        elif text.count(".") == 1:
            # Date is in format 14. May 2004
            return [int(text.split(" ")[-1])]
        else:
            raise ValueError(f"Unkown format of year {text}")

    elif text.count(" ") == 5:
        chunks = text.split(" ")
        first = " ".join(chunks[:3])
        second = " ".join(chunks[3:])
        return [get_years(first)[0], get_years(second)[0]]
    else:
        raise ValueError(text)
