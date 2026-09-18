import json
import os

from loguru import logger

from coinbot import CoinBot


def main():
    """Start the bot."""
    with open(os.path.join(os.path.dirname(__file__), "secrets.json"), "r") as f:
        secrets = json.load(f)
    telegram_token = secrets["telegram-token"]
    llm_token = os.getenv("FIREWORKS_API_KEY") or secrets.get("fireworks")
    if not llm_token:
        raise ValueError("Set FIREWORKS_API_KEY or add `fireworks` to secrets.json")
    file_link = secrets["file_link"]
    slack_token = secrets.get("slack")
    vectorstorage_path = os.path.join(
        os.path.dirname(__file__), "data", "special_coins.npz"
    )

    while True:
        try:
            bot = CoinBot(
                public_link=file_link,
                telegram_token=telegram_token,
                llm_token=llm_token,
                slack_token=slack_token,
                latest_csv_path=os.path.join(
                    os.path.dirname(__file__), "data", "latest_collection.csv"
                ),
                vectorstorage_path=vectorstorage_path,
                base_llm="accounts/fireworks/models/gpt-oss-120b",
                embedding_model="fireworks/qwen3-embedding-8b",
            )
            bot.run()
        except Exception as e:
            logger.error(f"Bot terminated with: {e}")


if __name__ == "__main__":
    main()
