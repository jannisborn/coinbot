from copy import deepcopy

import requests


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
