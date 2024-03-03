from loguru import logger
from slack import WebClient
from slack.errors import SlackApiError


class SlackClient:
    def __init__(self, token):
        self.client = WebClient(token=token)
        self.channel = "#coinbot"
        self.username = "CoinBot"

    def __call__(self, message: str):
        return self.send_message(message)

    def send_message(self, message: str):
        try:
            response = self.client.chat_postMessage(
                channel=self.channel, text=message, username=self.username
            )
            return response
        except SlackApiError as e:
            logger.error(f"Error sending message: {e.response['error']}")
            return e.response["error"]
