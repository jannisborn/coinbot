def convert_to_thousands(value) -> int:
    if isinstance(value, int):
        return value / 1000  # Convert to thousands if it's an integer
    elif isinstance(value, str) and value.isdigit():
        return int(value) / 1000  # Convert to thousands if it's a digit string
    else:
        return -1  # Default to -1 for non-integer strings
