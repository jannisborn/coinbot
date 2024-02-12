import json
import os

from coinbot import CoinBot
from coinbot.db import DataBase


def main():
    """Start the bot."""
    db = DataBase(
        os.path.join(os.path.dirname(__file__), "coins.xlsm"),
    )

    with open(os.path.join(os.path.dirname(__file__), "secrets.json"), "r") as f:
        secrets = json.load(f)
    telegram_token = secrets["telegram-token"]
    anyscale_token = secrets["anyscale"]

    bot = CoinBot(
        database=db, telegram_token=telegram_token, anyscale_token=anyscale_token
    )
    bot.run()


if __name__ == "__main__":
    main()
