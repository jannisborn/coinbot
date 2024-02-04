import json
import os

from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from coinbot.db import DataBase
from coinbot.llm import LLM, get_feature_value
from coinbot.metadata import translate_countries

# Load tokens
with open(os.path.join(os.path.dirname(__file__), "secrets.json"), "r") as f:
    secrets = json.load(f)
    telegram_token = secrets["telegram-token"]
    anyscale_token = secrets["anyscale"]


# Load data
db = DataBase("/Users/jannisborn/Dropbox/github/telegram-coin-bot/coins.xlsm")

feat_llm = LLM(
    model="Open-Orca/Mistral-7B-OpenOrca",
    token=anyscale_token,
    task_prompt="From the below, extract 3 features, Country, value and Year. Use a colon (:) before each feature value",
    temperature=0.0,
)


def search_coin_in_db(update, context):
    """Search for a coin in the database when a message is received."""
    try:
        # Parse the message
        message = update.message.text

        output = feat_llm(message)
        c = get_feature_value(output, "Country")
        value = get_feature_value(output, "Value").lower()
        year = int(get_feature_value(output, "Year"))
        country = translate_countries[c] if c in translate_countries.keys() else c
        country = country.capitalize()

        # TODO: Integrate more DFs

        # Search in the dataframe
        coin_status = db.eu_df[
            (db.eu_df["Country"] == country)
            & (db.eu_df["Coin Value"] == value)
            & (db.eu_df["Year"] == year)
        ]["Status"].values
        print(coin_status)

        # Respond to the user
        if coin_status.size > 0:
            response = (
                f"The status of {value} from {country} in {year} is: {coin_status[0]}"
            )
        else:
            response = "Coin not found."

        update.message.reply_text(response)

    except Exception as e:
        update.message.reply_text(f"An error occurred: {e}")


def main():
    """Start the bot."""
    # Replace 'YOUR_TOKEN' with the token given by BotFather
    updater = Updater(telegram_token, use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # On different commands - answer in Telegram
    dp.add_handler(
        CommandHandler(
            "start", lambda update, context: update.message.reply_text("Hi!")
        )
    )

    # On noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text, search_coin_in_db))

    # Start the Bot
    print("Starting bot")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
    main()
