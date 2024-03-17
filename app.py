import json
import os

from coinbot import CoinBot


def main():
    """Start the bot."""
    with open(os.path.join(os.path.dirname(__file__), "secrets.json"), "r") as f:
        secrets = json.load(f)
    telegram_token = secrets["telegram-token"]
    anyscale_token = secrets["anyscale"]
    file_link = secrets["file_link"]

    bot = CoinBot(
        public_link=file_link,
        telegram_token=telegram_token,
        anyscale_token=anyscale_token,
        slack_token=secrets["slack"],
        vectorstorage_path=os.path.join(os.path.dirname(__file__), "vectorstorage.npz"),
    )
    bot.run()


if __name__ == "__main__":
    main()
