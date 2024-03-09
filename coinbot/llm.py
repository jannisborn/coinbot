from copy import deepcopy

import requests

INSTRUCTION_MESSAGE = """
I'm helping you to identify and collect *unique* and *rare* EURO coins. Just ask me about
a coin. I always need the value, the country and the year of the coin. I will let you know
how many times the coin was minted and if it's already available in Jannis' coin collection. 
If it's not, please keep it and give it to Jannis soon, I'm sure he will be happy 🤩 \n
For example, if you write: \n`Spain 2010 1 Euro`\n I will tell you that the coin was minted 40 million times but that it's already in Jannis' collection.
Remember that for German coins you also need to enter the minting site which is a single character (A, D, F, G, J), e.g.,: \n`Germany 2002 1 Euro D`\n
Now you're ready! Get started and happy coin collecting 😊
"""


def get_feature_value(output: str, feature: str) -> str:
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
        if self.counter == 0:

            body["messages"].append({"role": "user", "content": message})
        else:
            # body["messages"][0].update(
            #     {"role": "user", "content": f"Same task: {message}"}
            # )

            # TODO: Seems like we have to re-provide the task prompt every time...
            body["messages"].append({"role": "user", "content": message})
        with self.session.post(self.url, headers=self.headers, json=body) as resp:
            output = resp.json()
        self.counter += 1
        return output["choices"][0]["message"]["content"]

    def __call__(self, *args, **kwargs):
        return self.send_message(*args, **kwargs)
        return self.send_message(*args, **kwargs)
        return self.send_message(*args, **kwargs)
        return self.send_message(*args, **kwargs)
