import json
import os

import requests
from loguru import logger

from coinbot.db import DataBase
from coinbot.vectorstorage import VectorStorage


def main():
    """Start the bot."""
    with open(
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "secrets.json"), "r"
    ) as f:
        secrets = json.load(f)
    anyscale_token = secrets["anyscale"]
    file_link = secrets["file_link"]
    response = requests.get(file_link)
    # Check if the request was successful
    if response.status_code == 200:
        # Write the content of the response to a file
        with open("tmp.xlsm", "wb") as f:
            f.write(response.content)
        logger.debug(f"File downloaded successfully from {file_link}")
    else:
        logger.warning(f"Failed to download file from {file_link}")

    db = DataBase("tmp.xlsm")

    vectorstorage = VectorStorage(
        token=anyscale_token, embedding_model="BAAI/bge-large-en-v1.5"
    )
    special_texts = db.df[(db.df.Special)].Name.values
    # descs = db.df[(db.df.Special)].Description.values
    # special_texts = [f"{n} - Details: {d}" for n, d in zip(names, descs)]
    print(special_texts[0])
    print(special_texts[-1])
    print(len(special_texts))
    vectorstorage.fit(special_texts)
    vectorstorage.save("special_coins")


if __name__ == "__main__":
    main()
