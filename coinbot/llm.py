from copy import deepcopy

import numpy as np
import openai
import requests

INSTRUCTION_MESSAGE = """
I'm helping you to identify & collect **rare** EURO coins. Just ask me about a coin. I always need the value, the country and the year of the coin. I will let you know how many times the coin was minted and if it's already available in Jannis' coin collection. 
If it's not in the collection, please keep it and give it to Jannis soon, I'm sure he will be happy 🤩 \n
For example, if you write: \n\n`Spain 2010 1 Euro`\n\n I will tell you that the coin was minted 40 million times but that it's already in Jannis' collection.

Remember that for German coins you also need to enter the minting site which is a single character (A, D, F, G, J), e.g.,: \n`20 Cent 2021 Germany D`.\

You can also request a list of coins, just start your message with `Status`, for example:
`Status France 2010` ➡️ Lists all coins from France from 2010.

To search a 2 Euro special coin (the official term is "commemorative coin"), use the "Special" keyword:

`Special Austria` ➡️ Lists all special coins from Austria.

`Special Germany 2015` ➡️ Lists all special coins from Germany from 2015.

`Special Olympics` ➡️ Lists all special coins with the word "Olympics" in the name.

`Special Germany Hamburg 2023` ➡️ Lists all German special coins from 2023 related to Hamburg.

Now you're ready! Get started and happy coin collecting 😊
"""


def get_feature_value(output: str, feature: str) -> str:
    if feature not in output:
        return ""
    return output.split(f"{feature}:")[-1].split("\n")[0].strip()


class LLM:
    def __init__(
        self,
        token: str,
        task_prompt: str,
        model: str = "mistralai/Mistral-7B-Instruct-v0.1",
        temperature: float = 0.7,
    ):
        self.token = token
        self.api_base = "https://api.endpoints.anyscale.com/v1"
        self.session = requests.Session()
        self.url = f"{self.api_base}/chat/completions"
        self.model = model
        self.temperature = temperature
        self.task_prompt = task_prompt
        self.body = {
            "model": model,
            "messages": [
                {"role": "system", "content": task_prompt},
            ],
            "temperature": temperature,
        }
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.counter = 0

    def send_message(self, message: str):
        body = deepcopy(self.body)
        # Seems like we have to re-provide the task prompt every time...
        body["messages"].append({"role": "user", "content": message})
        with self.session.post(self.url, headers=self.headers, json=body) as resp:
            output = resp.json()
        self.counter += 1
        return output["choices"][0]["message"]["content"]

    def __call__(self, *args, **kwargs):
        return self.send_message(*args, **kwargs)


class Embedding:
    def __init__(self, token: str, model: str = "thenlper/gte-large"):
        self.token = token
        self.api_base = "https://api.endpoints.anyscale.com/v1"
        self.model = model

        self.client = openai.OpenAI(base_url=self.api_base, api_key=self.token)

    def embed(self, message: str):
        embedding = self.client.embeddings.create(model=self.model, input=message)
        output = embedding.model_dump()
        embedding = np.array(output["data"][0]["embedding"])
        return embedding

    def __call__(self, *args, **kwargs):
        return self.embed(*args, **kwargs)
