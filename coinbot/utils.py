import csv
import os
import re
from datetime import datetime

from thefuzz import process as fuzzysearch

from coinbot.metadata import germany


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


def log_to_csv(input_text: str, output_text: str):
    """
    Logs the input message, output message, and date to a CSV file.

    Parameters:
        date: The current date when the log entry is made.
        input_text: The text message received from the user.
        output_text: The text message sent as a response.
    """
    file_path = "messages.csv"
    # Check if file exists to decide on writing headers
    file_exists = os.path.isfile(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["date", "input", "output"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()  # Write headers only if file doesn't exist
        current_date = datetime.now().strftime("%Y-%m-%d")
        writer.writerow(
            {
                "date": current_date,
                "input": input_text.replace("\n", " "),
                "output": output_text.replace("\n", " "),
            }
        )


def contains_germany(sentence: str, threshold: int = 80):
    """
    Checks if a sentence contains the word "Germany" in any language, using fuzzy matching.

    Parameters:
        sentence: The sentence to check.
        threshold: The minimum score to consider a match (default is 80).

    Returns:
        bool: True if "Germany" is detected in any language, False otherwise.
    """
    words = sentence.split()
    for word in words:
        match, score = fuzzysearch.extract(word, germany, limit=1)[0]
        if score >= threshold:
            return True
    return False


def get_tuple(country: str, value: str, year: int, source: str):
    if country == "germany":
        return f"({country}, {year}, {source.upper()}, {value})"
    else:
        return f"({country}, {year}, {value})"


def string_to_bool(input_string: str) -> bool:
    # Remove all non-alphabetic characters and convert to lowercase
    cleaned_string = re.sub(r"[^a-zA-Z]", "", input_string).lower()

    # Check if the cleaned string represents a true or false value
    if cleaned_string == "true":
        return True
    elif cleaned_string == "false":
        return False
    else:
        # Handle the case where the string does not represent a boolean
        raise ValueError("Input string does not represent a boolean value")
