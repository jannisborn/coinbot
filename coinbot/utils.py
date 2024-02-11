def convert_to_thousands(value) -> int:
    if isinstance(value, int):
        return value / 1000  # Convert to thousands if it's an integer
    elif isinstance(value, str) and value.isdigit():
        return int(value) / 1000  # Convert to thousands if it's a digit string
    else:
        return -1  # Default to -1 for non-integer strings


def convert_number_to_readable(number):
    """
    Converts a number to a human-readable format, using the most appropriate
    unit such as 'thousand', 'million', or 'billion'.
    """
    if number < 1:
        # For numbers less than 1 thousand, convert to hundreds
        return f"{(number * 1000):.3f} hundred"
    elif number < 1000:
        # For numbers less than 1 million (but more than or equal to 1 thousand), keep as thousand
        return f"{number:.3f} thousand"
    elif number < 1000000:
        # For numbers less than 1 billion (but more than or equal to 1 million), convert to million
        return f"{(number / 1000):.3f} million"
    else:
        # For numbers more than or equal to 1 billion, convert to billion
        return f"{(number / 1000000):.3f} billion"
