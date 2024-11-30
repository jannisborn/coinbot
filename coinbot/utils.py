import csv
import os
import re
from datetime import date, datetime
from typing import List, Tuple

import numpy as np
import requests
from Levenshtein import distance as levenshtein
from loguru import logger
from thefuzz import process as fuzzysearch

from coinbot.metadata import countries_all_languages, country_to_english, germany

CURRENT_YEAR = date.today().year


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


def contains_germany(sentence: str, threshold: int = 80) -> bool:
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


def get_tuple(
    country: str,
    year: int,
    source: str,
    name: str = "",
    value: str = "2 euro",
    isspecial: bool = False,
) -> str:
    if country == "germany":
        if isspecial:
            return f"({country.capitalize()}, {year}, {name}, SOURCE: {source.upper()})"
        else:
            return f"({country.capitalize()}, {year}, {source.upper()}, {value})"
    else:
        if isspecial:
            return f"({country.capitalize()}, {year}, {name})"
        else:
            return f"({country.capitalize()}, {year}, {value})"


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


def get_year(text: str) -> int:
    """
    Extracts the year from a given text.

    Parameters:
        text: The text to extract the year from.

    Returns:
        int: The extracted year.
    """
    # Find all occurrences of 4 digits in the text
    years = re.findall(r"\b\d{4}\b", text)

    # Return the first year found
    if len(years) != 1:
        return -1
    return int(years[0])


COIN_SIZE_PATTERNS: List[str] = [
    r"\b(1|one|uno|eins|un)\b",  # English, Spanish, German, French for 1
    r"\b(2|two|dos|zwei|deux)\b",  # English, Spanish, German, French for 2
    r"\b(5|five|cinco|fünf|cinq)\b",  # English, Spanish, German, French for 5
    r"\b(10|ten|diez|zehn|dix)\b",  # English, Spanish, German, French for 10
    r"\b(20|twenty|veinte|zwanzig|vingt)\b",  # English, Spanish, German, French for 20
    r"\b(50|fifty|cincuenta|fünfzig|cinquante)\b",  # English, Spanish, German, French for 50
]


def has_coin_value(text: str) -> bool:
    """
    Checks whether a string contains a coin value

    Args:
        text: String to check.

    Returns:
        bool: Whether the string contains a coin value or not.
    """
    # Check for coin size (assuming sizes are the same in any language)
    coin_pattern = r"|".join(COIN_SIZE_PATTERNS)
    num_found = re.search(coin_pattern, text, re.IGNORECASE) is not None
    order_found = any([x in text.lower.strip() for x in ["euro", "cent", "€"]])
    return num_found and order_found


def sane_no_country(text: str) -> bool:
    """
    Checks whether an input contains a coin size, a year and a source
        (A, D, F, G, J) but NO country.
    Args:
        text: Text to check
    Returns:
        bool
    """
    coin_found = has_coin_value(text)
    year_found = re.search(r"\b\d{4}\b", text) is not None
    source_found = re.search(r"\b(A|D|F|G|J)\b", text, re.IGNORECASE) is not None
    text_after_removal = re.sub(
        r"\b(1|2|5|10|20|50)\b|\b\d{4}\b|\b(A|D|F|G|J|a|d|f|g|j)\b|\beuro\b|\bcent\b|€",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    no_country_assumed = len(text_after_removal.strip()) <= 5

    return coin_found and year_found and source_found and no_country_assumed


def fuzzy_search_country(text: str, threshold: int = 95) -> Tuple[str, str]:
    """
    Fuzzy search for a country name in a long string

    Args:
        text: String to search for country.
        threshold: Threshold to consider a match in fuzzy search. Defaults to 95.

    Returns:
        Tuple consisting of (english_country_name, matched_country_name).
    """
    for word in text.split():
        dists = [levenshtein(c, word) for c in countries_all_languages]
        if np.min(dists) <= 2:
            match = countries_all_languages[np.argmin(dists)]
            country = country_to_english[match].strip().lower()
            return country, word
    return "", ""


def get_file_content(url: str):
    """Downloads file content directly into memory."""
    if ".svg" in url:
        return None

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, stream=True, headers=headers)

    if response.status_code == 200:
        logger.debug(f"Retrieved: {url}")
        return response.content
    else:
        logger.error(f"Failed to retrieve file {url}: {response.status_code}")
        return None
