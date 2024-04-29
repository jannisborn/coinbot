import numpy as np
import openai

INSTRUCTION_MESSAGE = """
I'm helping you to identify & collect **rare** EURO coins. Just ask me about a coin. I always need the value, the country and the year of the coin. I will let you know how many times the coin was minted and if it's already available in Jannis' coin collection. 
If it's not in the collection, please keep it and give it to Jannis soon, I'm sure he will be happy 🤩 \n
For example, if you write: \n\n`Spain 2010 1 Euro`\n\n I will tell you that the coin was minted 40 million times but that it's already in Jannis' collection.

Remember that for German coins you also need to enter the minting site which is a single character (A, D, F, G, J), e.g.,: \n`20 Cent 2021 Germany D`.\

You can also request a list of coins, just start your message with `Series`, for example:
`Series France 2010` ➡️ Lists all coins from France from 2010.

To search a 2 Euro special coin (the official term is "commemorative coin"), use the "Special" keyword:
`Special Austria` ➡️ Lists all special coins from Austria.
`Special Germany 2015` ➡️ Lists all special coins from Germany from 2015.
`Special Olympics` ➡️ Lists all special coins with the word "Olympics" in the name.
`Special Germany Hamburg 2023` ➡️ Lists all German special coins from 2023 related to Hamburg.

Last, to get a report about the current collection status write: `Status`

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
        model: str = "meta-llama/Llama-2-70b-chat-hf",
        temperature: float = 0.7,
        remind_task: int = 10,
    ):

        self.token = token
        self.temperature = temperature
        self.task = task_prompt
        self.message_history = [{"role": "system", "content": task_prompt}]
        self.model = model
        self.client = openai.OpenAI(
            api_key=token, base_url="https://api.endpoints.anyscale.com/v1"
        )
        self.reminder = remind_task
        self.counter = 0

    def _add_to_message_history(self, role: str, content: str):
        self.message_history.append({"role": role, "content": content})

    def send_message(self, message: str):
        if self.counter > 0 and self.counter % self.reminder == 0:
            self._add_to_message_history("user", f"Remember the task: {self.task}")
        # Add user's message to the conversation history.
        self._add_to_message_history("user", message)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.message_history,
            stream=True,
            temperature=self.temperature,
        )

        # Process and stream the response.
        response_content = ""
        first_token = True
        self.counter += 1

        for token in response:
            delta = token.choices[0].delta.content
            if first_token:
                # Skip first token to unblock response.
                first_token = False
                continue
            elif not delta:
                # End token indicating the end of the response.
                self._add_to_message_history("assistant", response_content)
                break
            else:
                # Append content to message and stream it.
                response_content += delta
                yield delta

    def __call__(self, *args, **kwargs):
        response = self.send_message(*args, **kwargs)
        full_text = "".join([part for part in response])
        return full_text


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
