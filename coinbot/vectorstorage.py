from typing import List, Tuple

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
        self.embeddings = np.zeros((len(text), 1024))
        for i, t in tqdm(enumerate(text), desc="Embedding data", total=len(text)):
            self.embeddings[i] = self.model.embed(t)

    def fit(self, *args, **kwargs):
        self.register_data(*args, **kwargs)

    def query(
        self, text: str, df: pd.DataFrame, verbose: bool = False
    ) -> Tuple[str, float]:
        """
        Query the vector storage for the best match to the given text.

        Args:
            text: The text to query for.
            verbose: Whether best results should be displayed. Defaults to False.
            df: A pandas DataFrame with a column `Name` which holds a subset of
                the data to query.

        Returns:
            Tuple[str, float]: The best match and the distance to the query.
        """
        query_embedding = self.model.embed(text)
        distances = np.linalg.norm(self.embeddings - query_embedding, axis=1)
        possible_matches = list(df["Name"].values)
        print(df)
        print(len(distances), "here")
        for i in range(len(df)):
            if possible_matches[i] in self.raw_data:
                df.loc[i, "Distance"] = distances[
                    self.raw_data.index(possible_matches[i])
                ]
        if not verbose:
            return df

        nns = np.argsort(distances)
        matches = [self.raw_data[i] for i in nns]
        for i, (n, m) in enumerate(zip(nns, matches)):
            logger.info(f"{i+1}: {m} with distance {n}")
            if i > 4:
                break
        return df

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
            token=self.model.token,
        )

    @staticmethod
    def load(path: str):
        # Load embedding, embedding model and raw data with numpy
        data = np.load(path, allow_pickle=True)
        vectorstorage = VectorStorage(
            token=str(data["token"]), embedding_model=str(data["model_name"])
        )
        vectorstorage.embeddings = data["embeddings"]
        vectorstorage.raw_data = list(data["text"])
        return vectorstorage
