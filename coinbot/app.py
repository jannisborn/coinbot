import json
import os
from random import random

from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from coinbot.db import DataBase
from coinbot.llm import LLM, get_feature_value
from coinbot.metadata import translate_countries
from coinbot.utils import convert_number_to_readable

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
    task_prompt="You are a feature extractor! Extract 3 features, Country, value and Year. Use a colon (:) before each feature value",
    temperature=0.0,
)
ger_llm = LLM(
    model="Open-Orca/Mistral-7B-OpenOrca",
    token=anyscale_token,
    task_prompt=(
        "You are a feature extractor! Extract 4 features, Country, value, year and source. The source is given as single character, A, D, F, G or J. "
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


def return_message(update, text: str, language: str):
    if language == "english":
        update.message.reply_text(text)
    else:
        translate_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=anyscale_token,
            task_prompt=(
                f"You are a translation tool. Translate the following into {language} but be colloquial and use emojis. Translate numerals literally, e.g., `8.720 million` --> `8.72 Millionen`"
            ),
            temperature=0.0,
        )
        print("Translate", text, language)
        translated_text = translate_llm(text)
        update.message.reply_text(translated_text)


def get_tuple(country: str, value: str, year: int, source: str):
    if country == "deutschland":
        return f"({country}, {year}, {source}, {value})"
    else:
        return f"({country}, {year}, {value})"


def update_response_with_amount(response, amount_raw):
    if amount_raw == 0:
        return response
    amount = convert_number_to_readable(amount_raw)
    return f"{response}\n\n (Coin was minted {amount} times)"


def search_coin_in_db(update, context):
    """Search for a coin in the database when a message is received."""
    try:
        # Parse the message
        message = update.message.text

        language = language_llm(message).lower()

        if random() < 0.001:
            output = joke_llm(message)
            return_message(update, output, language)
            return

        if "germany" in message.lower() or "deutschland" in message.lower():
            output = ger_llm(message)
            source = get_feature_value(output, "Source").lower()
        else:
            output = eu_llm(message)
            source = None
        print("LLM says", output, "\n")
        c = get_feature_value(output, "Country")
        value = get_feature_value(output, "Value").lower()
        value = value.replace("€", " euro").replace("  ", " ")
        year = int(get_feature_value(output, "Year"))
        country = translate_countries[c] if c in translate_countries.keys() else c
        country = country.lower()

        # Search in the dataframe
        print(country, value, year, source)

        coin_df = db.df[
            (db.df["Country"] == country)
            & (db.df["Coin Value"] == value)
            & (db.df["Year"] == year)
            & (
                ((db.df["Country"] == "deutschland") & (db.df["Source"] == source))
                | ((db.df["Country"] != "deutschland") & (db.df["Source"].isna()))
            )
        ]
        print(coin_df, len(coin_df))

        match = get_tuple(country, value, year, source)

        # Respond to the user
        if len(coin_df) == 0:
            response = f"🤷🏻‍♂️ The coin {match} was not found. Check your prompt!🧐"
            return_message(update, response, language)
            return

        coin_status = coin_df["Status"].values[0]
        if coin_status == "unavailable":
            response = f"🤯 The coin {match} should not exist. If you indeed have it, it's a SUPER rare find!"
        elif coin_status == "missing":
            response = f"🚀🎉 The coin {match} is missing! Please keep it safe 🤩"
            amount_raw = coin_df["Amount"].values[0]
            response = update_response_with_amount(response, amount_raw)
        elif coin_status == "collected":
            response = f"😢 The coin {match} was already collected 😢"
            amount_raw = coin_df["Amount"].values[0]
            response = update_response_with_amount(response, amount_raw)
        else:
            response = "❓Coin not found."

        return_message(update, response, language)

    except Exception as e:
        response = f"An error occurred: {e}"
        return_message(update, response, language)


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
