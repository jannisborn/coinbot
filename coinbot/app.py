import json
import os
from random import random

from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from coinbot.db import DataBase
from coinbot.llm import LLM, get_feature_value
from coinbot.metadata import translate_countries
from coinbot.utils import contains_germany, large_int_to_readable, log_to_csv

missing_hints = ["feature", "missing", "provided", "not"]

# Load tokens
with open(os.path.join(os.path.dirname(__file__), "secrets.json"), "r") as f:
    secrets = json.load(f)
    telegram_token = secrets["telegram-token"]
    anyscale_token = secrets["anyscale"]


# Load data
db = DataBase(os.path.join(os.path.dirname(os.path.dirname(__file__)), "coins.xlsm"))

eu_llm = LLM(
    model="Open-Orca/Mistral-7B-OpenOrca",
    token=anyscale_token,
    task_prompt="You are a feature extractor! Extract 3 features, Country, value and Year. Use a colon (:) before each feature value. If one of the three features is missing reply simply with `Missing feature`",
    temperature=0.0,
)
ger_llm = LLM(
    model="Open-Orca/Mistral-7B-OpenOrca",
    token=anyscale_token,
    task_prompt=(
        "You are a feature extractor! Extract 4 features, Country, value, year and source. The source is given as single character, A, D, F, G or J. If one of the three features is missing reply simply with `Missing feature`"
        "Use a colon (:) before each feature value"
    ),
    temperature=0.0,
)
joke_llm = LLM(
    model="Open-Orca/Mistral-7B-OpenOrca",
    token=anyscale_token,
    task_prompt=(
        "Tell me a very short joke about the following coin. Start with `Here's a funny story about your coin:`"
    ),
    temperature=0.0,
)
language_llm = LLM(
    model="mlabonne/NeuralHermes-2.5-Mistral-7B",
    token=anyscale_token,
    task_prompt=(
        "Detect the language of the text. NOTE: The text contains the NAME of a country. This name is NOT the language. Most importantly: Reply with ONE word only"
    ),
    temperature=0.0,
)


def return_message(
    update, text: str, language: str, amount: int = 0, org_msg: str = ""
):
    if amount > 0:
        number_text = large_int_to_readable(amount * 1000)
        text = f"{text}\n\n(Coin was minted {number_text} times)"

    if language == "english":
        update.message.reply_text(text)
    else:
        translate_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=anyscale_token,
            task_prompt=(
                f"You are a translation chatbot. Translate the following into {language}, use colloquial language like in a personal chat"
            ),
            temperature=0.0,
        )

        text = translate_llm(text)
        update.message.reply_text(text)

    log_to_csv(org_msg, text)


def get_tuple(country: str, value: str, year: int, source: str):
    if country == "deutschland":
        return f"({country}, {year}, {source.upper()}, {value})"
    else:
        return f"({country}, {year}, {value})"


def search_coin_in_db(update, context):
    """Search for a coin in the database when a message is received."""
    try:
        # Parse the message
        message = update.message.text
        print("Received: ", message)

        language = language_llm(message).lower()

        if random() < 0.001:
            output = joke_llm(message)
            return_message(update, output, language, org_msg=message)
            return

        if contains_germany(message, threshold=99):
            output = ger_llm(message)
            if any([x in output.lower() for x in missing_hints]) or any(
                [
                    x not in output.lower()
                    for x in ["source", "year", "country", "value"]
                ]
            ):
                return_message(
                    update,
                    text=output
                    + "\nYou need to provide the features `year`, `country`, `coin value` and `mint location` (A, D, F, G or J)",
                    language=language,
                    org_msg=message,
                )
                return

            source = get_feature_value(output, "Source").lower()
        else:
            output = eu_llm(message)
            if any([x in output.lower() for x in missing_hints]) or any(
                [x not in output.lower() for x in ["year", "country", "value"]]
            ):
                return_message(
                    update,
                    text=output
                    + "\nYou need to provide the features `year`, `country` and `coin value`",
                    language=language,
                    org_msg=message,
                )
                return
            source = None
        c = get_feature_value(output, "Country")
        value = get_feature_value(output, "Value").lower()
        value = value.replace("€", " euro").replace("  ", " ")
        year = int(get_feature_value(output, "Year"))
        country = translate_countries[c] if c in translate_countries.keys() else c
        country = country.lower()
        print("LLM says", output)

        # Search in the dataframe
        coin_df = db.df[
            (db.df["Country"] == country)
            & (db.df["Coin Value"] == value)
            & (db.df["Year"] == year)
            & (
                ((db.df["Country"] == "deutschland") & (db.df["Source"] == source))
                | ((db.df["Country"] != "deutschland") & (db.df["Source"].isna()))
            )
        ]

        match = get_tuple(country, value, year, source)

        # Respond to the user
        if len(coin_df) == 0:
            response = f"🤷🏻‍♂️ The coin {match} was not found. Check your input 🧐"
            print(f"Returns: {response}\n")
            return_message(update, response, language, org_msg=message)
            return

        coin_status = coin_df["Status"].values[0]
        if coin_status == "unavailable":
            response = f"🤯 The coin {match} should not exist. If you indeed have it, it's a SUPER rare find!"
            amount = 0
        elif coin_status == "missing":
            response = f"🚀🎉 The coin {match} is not yet in the collection 🤩"
            amount = coin_df["Amount"].values[0]
        elif coin_status == "collected":
            response = f"😢 The coin {match} was already collected 😢"
            amount = coin_df["Amount"].values[0]
        else:
            response = "❓Coin not found."

        res = response.split("\n")[0]
        print(f"Returns: {res}\n")
        return_message(update, response, language, amount=amount, org_msg=message)

    except Exception as e:
        response = f"An error occurred: {e}"
        return_message(update, response, language, org_msg=message)


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
