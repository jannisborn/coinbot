import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

from coinbot.llm import Embedding, LLM
from coinbot.vectorstorage import VectorStorage


class FireworksMigrationTests(unittest.TestCase):
    @patch("coinbot.llm.OpenAI")
    def test_chat_stream_uses_serverless_model(self, client_class):
        chunks = [
            SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content="Spain "))]),
            SimpleNamespace(choices=[]),
            SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content=None))]),
            SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content="2010"))]),
        ]
        client_class.return_value.chat.completions.create.return_value = chunks
        llm = LLM(
            token="test-key",
            task_prompt="Extract coin details",
            model="accounts/fireworks/models/gpt-oss-120b",
        )

        self.assertEqual(llm("Spain 2010 1 Euro"), "Spain 2010")
        self.assertEqual(llm.message_history[-1], {"role": "assistant", "content": "Spain 2010"})
        self.assertEqual(client_class.call_args.kwargs["base_url"], "https://api.fireworks.ai/inference/v1")
        self.assertEqual(
            client_class.return_value.chat.completions.create.call_args.kwargs["model"],
            "accounts/fireworks/models/gpt-oss-120b",
        )

    @patch("coinbot.llm.OpenAI")
    def test_embedding_uses_input_unchanged(self, client_class):
        client_class.return_value.embeddings.create.return_value = SimpleNamespace(
            data=[SimpleNamespace(index=0, embedding=[1.0, 0.0])]
        )
        embedding = Embedding(token="test-key", model="fireworks/qwen3-embedding-8b")
        vector = embedding.embed("Hamburg 2023")
        np.testing.assert_array_equal(vector, [1, 0])
        self.assertEqual(
            client_class.return_value.embeddings.create.call_args.kwargs["model"],
            "fireworks/qwen3-embedding-8b",
        )
        self.assertEqual(
            client_class.return_value.embeddings.create.call_args.kwargs["input"],
            "Hamburg 2023",
        )

    def test_old_index_is_rejected_and_new_index_uses_euclidean_distance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "special_coins.npz"
            np.savez(path, text=["A", "B"], embeddings=[[1, 0], [100, 100]],
                     model_name="intfloat/multilingual-e5-large-instruct", token="old-key")
            with self.assertRaisesRegex(ValueError, "rebuild"):
                VectorStorage.load(
                    str(path), token="test-key",
                    embedding_model="fireworks/qwen3-embedding-8b",
                )

            storage = VectorStorage(
                token="test-key", embedding_model="fireworks/qwen3-embedding-8b"
            )
            storage.raw_data = ["A", "B"]
            storage.embeddings = np.asarray([[1, 0], [100, 100]], dtype=np.float32)
            storage.save(str(path))
            with np.load(path) as data:
                self.assertNotIn("token", data.files)
            loaded = VectorStorage.load(
                str(path), token="test-key",
                embedding_model="fireworks/qwen3-embedding-8b",
            )
            loaded.model.embed = Mock(return_value=np.asarray([1, 1], dtype=np.float32))
            result = loaded.query("B", pd.DataFrame({"Name": ["A", "B"]}), nn=1)
            self.assertEqual(result.iloc[0]["Name"], "A")


if __name__ == "__main__":
    unittest.main()
