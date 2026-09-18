"""Rebuild the special coin index with Fireworks embeddings."""

import argparse
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import requests

from coinbot.db import DataBase
from coinbot.vectorstorage import VectorStorage


ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "data" / "special_coins.npz"


def spreadsheet_names(file_link: str) -> list[str]:
    response = requests.get(file_link, timeout=60)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".xlsm") as spreadsheet:
        spreadsheet.write(response.content)
        spreadsheet.flush()
        db = DataBase(
            spreadsheet.name,
            latest_csv_path=str(ROOT / "data" / "latest_collection.csv"),
        )
        return db.df[db.df.Special].Name.tolist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-spreadsheet", action="store_true",
        help="Fetch current special coin names instead of reusing names in the existing index",
    )
    parser.add_argument(
        "--embedding-model", default="fireworks/qwen3-embedding-8b",
        help="Fireworks embedding model used to build the index",
    )
    args = parser.parse_args()

    with open(ROOT / "secrets.json") as f:
        secrets = json.load(f)
    token = os.getenv("FIREWORKS_API_KEY") or secrets.get("fireworks")
    if not token:
        raise ValueError("Set FIREWORKS_API_KEY or add `fireworks` to secrets.json")

    if args.from_spreadsheet or not INDEX.exists():
        names = spreadsheet_names(secrets["file_link"])
    else:
        with np.load(INDEX, allow_pickle=True) as data:
            names = data["text"].tolist()
    if not names:
        raise ValueError("No special coin names found")

    storage = VectorStorage(token=token, embedding_model=args.embedding_model)
    storage.fit(names)

    # Keep the old index intact until all embeddings have succeeded.
    with tempfile.NamedTemporaryFile(dir=INDEX.parent, suffix=".npz", delete=False) as temp:
        temp_path = Path(temp.name)
    try:
        storage.save(str(temp_path))
        with np.load(temp_path) as data:
            if len(data["text"]) != len(names) or data["embeddings"].shape[0] != len(names):
                raise ValueError("Incomplete special coin index")
        os.replace(temp_path, INDEX)
    finally:
        temp_path.unlink(missing_ok=True)

    print(f"Saved {len(names)} special coin embeddings ({storage.embeddings.shape[1]} dimensions) to {INDEX}")


if __name__ == "__main__":
    main()
