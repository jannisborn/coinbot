def convert_to_thousands(value) -> int:
    if isinstance(value, int):
        return value / 1000  # Convert to thousands if it's an integer
    elif isinstance(value, str) and value.isdigit():
        return int(value) / 1000  # Convert to thousands if it's a digit string
    else:
        return -1  # Default to -1 for non-integer strings


def large_int_to_readable(n):
    # Values will always be above one thousand
    billion = n // 1000000000
    million = (n % 1000000000) // 1000000
    thousand = (n % 1000000) // 1000

    # Round the numbers
    if billion > 0:
        # If there are billions, round to the nearest billion
        rounded = round(n / 1000000000)
        readable = f"{rounded} Billion"
    elif million > 0:
        # If there are millions, round to the nearest million
        rounded = round(n / 1000000)
        readable = f"{rounded} Million"
    elif thousand > 0:
        # If there are thousands, round to the nearest thousand
        rounded = round(n / 1000)
        readable = f"{rounded} Thousand"

    return readable
