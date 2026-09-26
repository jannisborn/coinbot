from typing import List
import sys
import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

from .llm import Embedding


class VectorStorage:

    def __init__(self, token: str, embedding_model: str):
        self.model_name = embedding_model
        self.model = Embedding(token=token, model=self.model_name)

    def register_data(self, text: List[str]):
        self.raw_data = text
        self.embeddings = np.array([
            self.model.embed(t)
            for t in tqdm(text, desc="Embedding data", disable=not sys.stdout.isatty())
        ])

    def fit(self, *args, **kwargs):
        self.register_data(*args, **kwargs)

    def query(self, text: str, df: pd.DataFrame, nn: int = -1) -> pd.DataFrame:
        """
        Query the vector storage for the best match to the given text.

        Args:
            text: The text to query for.
            df: A pandas DataFrame with a column `Name` which holds a subset of
                the data to query.
            nn: The number of nearest neighbors to retrieve. Defaults to -1, meaning
                everything is retrieved.

        Returns:
            pd.DataFrame: The filtered DF.
        """
        if len(df) <= nn:
            logger.warning(
                f"Received only {len(df)} potential but asking for {nn} matches, will display all"
            )
            return df
        logger.debug(f"Querying vector storage with {len(df)} coins and: {text}")
        query_embedding = self.model.embed(text)
        if self.embeddings.shape[1] != len(query_embedding):
            raise ValueError("Embedding dimensions differ; rebuild the special coin index")
        distances = np.linalg.norm(self.embeddings - query_embedding, axis=1)
        possible_matches = list(df["Name"].values)
        df_distances = [
            (
                distances[self.raw_data.index(possible_matches[i])]
                if possible_matches[i] in self.raw_data
                else 10**6
            )
            for i in range(len(df))
        ]
        df.insert(0, "Distance", df_distances)
        df = df.sort_values(by="Distance", ascending=True)
        return df.head(n=len(df) if nn == -1 else nn)

    def __call__(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    def eval(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    def save(self, path: str):
        # Save embedding, embedding model and raw data with numpy
        np.savez(
            path,
            embeddings=self.embeddings,
            text=self.raw_data,
            model_name=self.model_name,
        )

    @staticmethod
    def load(path: str, token: str, embedding_model: str):
        # Load embedding, embedding model and raw data with numpy
        with np.load(path, allow_pickle=True) as data:
            model_name = str(data["model_name"])
            if model_name != embedding_model:
                raise ValueError(
                    f"Special coin index uses {model_name}; rebuild it with "
                    "`uv run python scripts/create_storage.py` before starting the bot"
                )
            vectorstorage = VectorStorage(token=token, embedding_model=embedding_model)
            vectorstorage.embeddings = data["embeddings"]
            vectorstorage.raw_data = list(data["text"])
        if len(vectorstorage.embeddings) != len(vectorstorage.raw_data):
            raise ValueError("Unequal number of embeddings and raw data")
        if vectorstorage.embeddings.ndim != 2 or not np.isfinite(vectorstorage.embeddings).all():
            raise ValueError("Invalid embeddings in special coin index")
        logger.debug(
            f"Restored vectorstorage {model_name} shape {vectorstorage.embeddings.shape}"
        )
        return vectorstorage
